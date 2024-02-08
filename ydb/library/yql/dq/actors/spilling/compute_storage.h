#pragma once

#include "compute_storage_actor.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

// This class will be refactored to be non-actor spiller part
class TDqComputeStorage : public NKikimr::NMiniKQL::ISpiller
{
public:

    TDqComputeStorage(TTxId txId, std::function<void()> wakeUpCallback, NActors::TActorSystem* actorSystem);

    ~TDqComputeStorage();

    NThreading::TFuture<TKey> Put(TRope&& blob);

    std::optional<NThreading::TFuture<TRope>> Get(TKey key);

    std::optional<NThreading::TFuture<TRope>> Extract(TKey key);

    NThreading::TFuture<void> Delete(TKey key);

private:
    NActors::TActorSystem* ActorSystem_;
    IDqComputeStorageActor* ComputeStorageActor_;
    NActors::TActorId ComputeStorageActorId_;
};

} // namespace NYql::NDq
