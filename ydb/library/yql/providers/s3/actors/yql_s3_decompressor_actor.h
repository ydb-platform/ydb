#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

namespace NYql::NDq {

NActors::IActor* CreateS3DecompressorActor(
    const NActors::TActorId& parent, const TString& compression, IDqSchedulableWorkFactoryPtr workFactory = nullptr);

}   // namespace NYql::NDq
