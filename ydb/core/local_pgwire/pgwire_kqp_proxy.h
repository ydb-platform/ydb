#pragma once

#include <ydb/core/pgproxy/pg_proxy_events.h>
#include "local_pgwire_util.h"

namespace NLocalPgWire {

NActors::IActor* CreatePgwireKqpProxyQuery(const NActors::TActorId& owner,
                                           std::unordered_map<TString, TString> params,
                                           const TConnectionState& connection,
                                           TEvEvents::TEvSingleQuery::TPtr&& evQuery);

NActors::IActor* CreatePgwireKqpProxyParse(const NActors::TActorId& owner,
                                           std::unordered_map<TString, TString> params,
                                           const TConnectionState& connection,
                                           NPG::TEvPGEvents::TEvParse::TPtr&& evParse);

NActors::IActor* CreatePgwireKqpProxyExecute(const NActors::TActorId& owner,
                                             std::unordered_map<TString, TString> params,
                                             const TConnectionState& connection,
                                             NPG::TEvPGEvents::TEvExecute::TPtr&& evExecute,
                                             const TPortal& portal);

}
