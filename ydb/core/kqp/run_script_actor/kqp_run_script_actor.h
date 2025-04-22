#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
    class TQueryServiceConfig;
}

namespace NKikimr::NKqp {

struct TKqpRunScriptActorSettings {
    TString Database;
    TString ExecutionId;
    ui64 LeaseGeneration = 0;
    TDuration LeaseDuration;
    TDuration ResultsTtl;
    TDuration ProgressStatsPeriod;
    TIntrusivePtr<TKqpCounters> Counters;
};

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, const TKqpRunScriptActorSettings& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig);

} // namespace NKikimr::NKqp
