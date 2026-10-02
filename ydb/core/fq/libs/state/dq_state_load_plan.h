#pragma once

#include <ydb/core/fq/libs/checkpointing/checkpoint_provider_integration.h>
#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>


namespace NFq {


struct TStateLoadPlanResolverSettings {
    NActors::TActorId StorageProxy;
    TString GraphId;
    NYql::NDqProto::TCheckpoint Checkpoint;
    ui64 CoordinatorGeneration = 0;
    TMaybe<ui64> OutputStartTimeUs;
    bool UseSourceDisposition = false;
    bool Force = false;
    TCheckpointProviderIntegrations ProviderIntegrations;
};

NActors::IActor* CreateStateLoadPlanResolver(NProto::TGraphParams src, NProto::TGraphParams dst, TStateLoadPlanResolverSettings settings, ui64 cookie);

} // namespace NFq
