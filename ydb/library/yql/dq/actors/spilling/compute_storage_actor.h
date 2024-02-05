#pragma once

#include "ydb/library/yql/dq/common/dq_common.h"

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

// This class will be refactored to be the Actor part of the spiller
class IDqComputeStorageActor
{
public:
    using TPtr = std::shared_ptr<IDqComputeStorageActor>;
    using TKey = ui64;

    virtual ~IDqComputeStorageActor() = default;

    virtual NActors::IActor* GetActor() = 0;

    virtual NThreading::TFuture<TKey> Put(TRope&& blob) = 0;

    virtual std::optional<NThreading::TFuture<TRope>> Get(TKey key) = 0;

    virtual std::optional<NThreading::TFuture<TRope>> Extract(TKey key) = 0;

    virtual NThreading::TFuture<void> Delete(TKey key) = 0;
};

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId, const TString& spillerName, std::function<void()> wakeupCallback, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq
