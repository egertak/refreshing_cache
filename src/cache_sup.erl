%%%-------------------------------------------------------------------
%% @doc cache top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(cache_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [cache_store_spec(), cache_spec()],
    {ok, {SupFlags, ChildSpecs}}.

cache_store_spec() ->
  #{id => cache_store,
    start => {cache_store, start_link, []},
    restart => permanent,
    shutdown => brutal_kill,
    type => worker,
    modules => [cache_store]}.

cache_spec() ->
  #{id => cache,
    start => {cache, start_link, []},
    restart => permanent,
    shutdown => brutal_kill,
    type => worker,
    modules => [cache]}.

%% internal functions
