#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

// This class will be refactored to be non-actor spiller part
class IDqComputeStorage
{
public:
    using TPtr = std::shared_ptr<IDqComputeStorage>;
    using TKey = ui64;

    virtual ~IDqComputeStorage() = default;

    virtual NThreading::TFuture<TKey> Put(TRope&& blob) = 0;

    virtual std::optional<NThreading::TFuture<TRope>> Get(TKey key) = 0;

    virtual std::optional<NThreading::TFuture<TRope>> Extract(TKey key) = 0;

    virtual NThreading::TFuture<void> Delete(TKey key) = 0;
};

IDqComputeStorage::TPtr MakeComputeStorage(const TString& spillerName, std::function<void()>&& wakeUpCallback);

} // namespace NYql::NDq