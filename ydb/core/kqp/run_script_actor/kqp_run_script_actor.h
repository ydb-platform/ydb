#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
// #include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {

class TQueryServiceConfig;

} // namespace NKikimrConfig

namespace NYql::NPq::NProto {

class StreamingDisposition;

} // namespace NYql::NPq::NProto

namespace NKikimr::NKqp {

struct TKqpRunScriptActorSettings {
    TString Database;
    TString ExecutionId;
    i64 LeaseGeneration = 0;
    TDuration LeaseDuration;
    TDuration ResultsTtl;
    TDuration ProgressStatsPeriod;
    TIntrusivePtr<TKqpCounters> Counters;
    bool SaveQueryPhysicalGraph = false;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    bool DisableDefaultTimeout = false;
    TString CheckpointId;
    TString StreamingQueryPath;
    TString CustomerSuppliedId;
    std::shared_ptr<NYql::NPq::NProto::StreamingDisposition> StreamingDisposition;
};

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig);

} // namespace NKikimr::NKqp
