%%%-------------------------------------------------------------------
%%% @author gergelytakacs
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(cache).

-behaviour(gen_server).

%% API
-export([register_function/4]).
-export([get/1, get/2, get/3]).
-export([refresher/3]).

%% gen_server callbacks
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(cache_state, {functions = #{},
                      clients = #{}}).

%% API

register_function(Fun, Key, Ttl, Refresh_interval) when is_function(Fun, 0), is_integer(Ttl), Ttl > 0,
                                                        is_integer(Refresh_interval), Refresh_interval < Ttl ->
  gen_server:call(?SERVER, {register_function, Fun, Key, Ttl, Refresh_interval}).

get(Key) ->
  get(Key, 30000).

get(Key, Timeout) ->
  get(Key, Timeout, []).

get(Key, Timeout, _Options) when is_integer(Timeout), Timeout > 0 ->
  gen_server:call(?SERVER, {get, Key}, Timeout).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  %% trapping exits so we can handle wrong functions
  process_flag(trap_exit, true),
  {ok, #cache_state{}}.

handle_call({register_function, Fun, Key, Ttl, Refresh}, _From, State = #cache_state{functions = Funs}) ->
  case maps:is_key(Key, Funs) of
    true ->
      {reply, {error, already_registered}, State};
    false ->
      %% create a timer that periodically triggers refresh
      {ok, Tref} = timer:send_interval(Refresh, {refresh, Key}),
      {reply, ok, State#cache_state{functions = Funs#{Key => {Fun, Tref, Ttl, Refresh}}}}
  end;
handle_call({get, Key}, From, State = #cache_state{clients = Clients}) ->
  case cache_store:get(?SERVER, Key) of
    undefined ->
      %% value is not available, append client to the list of waiting clients
      {noreply, State#cache_state{clients = Clients#{Key => maps:get(Key, Clients, []) ++ [From]}}};
    Value ->
      %% value is available in cache store, so we just return it
      {reply, Value, State}
  end.

handle_cast(_Request, State = #cache_state{}) ->
  {noreply, State}.

handle_info({refresh, Key}, State = #cache_state{functions = Functions}) ->
  %% spawns a helper process that executes the function
  #{Key := {Fun, _Tref, _Ttl, _Refresh}} = Functions,
  _Pid = spawn_link(?MODULE, refresher, [Fun, Key, self()]),
  {noreply, State};

handle_info({store, Key, Value}, State = #cache_state{functions = Functions,
                                                      clients = Clients}) ->
  %% helper process returns with the new value and we store that
  {_Fun, _Tref, Ttl, _Refresh} = maps:get(Key, Functions),
  ok = cache_store:store(?SERVER, Key, Value, Ttl),
  %% we also notify clients that are waiting for the value
  [ok = gen_server:reply(Client, Value) || Client <- maps:get(Key, Clients, [])],
  {noreply, State#cache_state{clients = maps:remove(Key, Clients)}};

handle_info({'EXIT',_Pid,_}, State) ->
  %% TODO: log error here, maybe unregister function
  {noreply, State}.

terminate(_Reason, _State = #cache_state{}) ->
  ok.

code_change(_OldVsn, State = #cache_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

refresher(Fun, Key, Server) ->
  case apply(Fun, []) of
    {ok, Value} ->
      %% send the result back to cache server
      Server ! {store, Key, Value},
      ok;
    {error, _} ->
      %% don't do anything, next send interval will trigger a retry
      ok
  end.
