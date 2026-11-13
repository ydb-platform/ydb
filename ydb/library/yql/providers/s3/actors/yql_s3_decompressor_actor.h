#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

namespace NYql::NDq {

NActors::IActor* CreateS3DecompressorActor(
    const NActors::TActorId& parent, const TString& compression, IDqSchedulableWorkFactoryPtr workFactory = nullptr,
    ::NMonitoring::TDynamicCounterPtr counters = nullptr);

}   // namespace NYql::NDq
