#pragma once
#include "state.h"
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>

namespace NKikimr::NKqp {

class TKqpShutdownController {
public:
    TKqpShutdownController(NActors::TActorId kqpProxyActorId, const NKikimrConfig::TTableServiceConfig& tableServiceConfig, bool gracefulEnabled);
    ~TKqpShutdownController() = default;

    void Initialize(NActors::TActorSystem* actorSystem);
    void Stop();

private:
    NActors::TActorId KqpProxyActorId_;
    NActors::TActorSystem* ActorSystem_;
    bool EnableGraceful;
    NKikimrConfig::TTableServiceConfig TableServiceConfig;
    TIntrusivePtr<TKqpShutdownState> ShutdownState_;
};


} // namespace NKikimr::NKqp
