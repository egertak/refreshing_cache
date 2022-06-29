%%%-------------------------------------------------------------------
%%% @author gergelytakacs
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache_store).

-behaviour(gen_server).

%% API
-export([store/4]).
-export([get/2]).

%% gen_server callbacks
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(cache_store_state, {table,
                            trefs = #{}}).
-record(data, {key,
               value}).

%% API callbacks

store(_Store, Key, Value, Ttl) ->
  gen_server:call(?MODULE, {store, Key, Value, Ttl}).

get(_Store, Key) ->
  gen_server:call(?MODULE, {get, Key}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  %% we enable concurrency to avoid bottlenecks,
  %% there is max one writer for each value
  Table = ets:new(?MODULE, [named_table, set, {keypos, 2},
    {read_concurrency, true}, {write_concurrency, true}]),
  {ok, #cache_store_state{table = Table}}.

handle_call({store, Key, Value, Ttl}, _From, State = #cache_store_state{table = Table,
                                                                        trefs = Trefs}) ->
  true = ets:insert(Table, #data{key = Key, value = Value}),
  %% cancel old ttl timer if exists, to avoid kicking out new value
  Tref = maps:get(Key, Trefs, undefined),
  (Tref /= undefined) andalso timer:cancel(Tref),
  %% create new ttl timer and store the new ref in state
  {ok, NewTref} = timer:send_after(Ttl, {expired, Key}),
  {reply, ok, State#cache_store_state{trefs = Trefs#{Key => NewTref}}};

handle_call({get, Key}, _From, State = #cache_store_state{table = Table}) ->
  Reply = case ets:lookup(Table, Key) of
    [#data{value = Value}] ->
      Value;
    _ ->
      undefined
  end,
  {reply, Reply, State}.

handle_cast(_Request, State = #cache_store_state{}) ->
  {noreply, State}.

handle_info({expired, Key}, State = #cache_store_state{table = Table}) ->
  true = ets:delete(Table, Key),
  {noreply, State}.

terminate(_Reason, _State = #cache_store_state{}) ->
  ok.

code_change(_OldVsn, State = #cache_store_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
