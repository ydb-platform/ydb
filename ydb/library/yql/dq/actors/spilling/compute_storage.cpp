#include "compute_storage.h"

#include "compute_storage_actor.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>


namespace NYql::NDq {

using namespace NActors;

class TDqComputeStorage : public IDqComputeStorage {
public:
    TDqComputeStorage(TTxId txId, const TString& spillerName, std::function<void()>&& wakeUpCallback) {

        ComputeStorageActor_ = CreateDqComputeStorageActor(txId, spillerName, std::move(wakeUpCallback));
        ComputeStorageActorId_ = TlsActivationContext->AsActorContext().Register(ComputeStorageActor_->GetActor());
    }

    ~TDqComputeStorage() {
        TlsActivationContext->AsActorContext().Send(ComputeStorageActorId_, new TEvents::TEvPoison);
    }

    NThreading::TFuture<TKey> Put(TRope&& blob) override {
        return ComputeStorageActor_->Put(std::move(blob));
    }

    std::optional<NThreading::TFuture<TRope>> Get(TKey key) override {
        return ComputeStorageActor_->Get(key);
    }

    NThreading::TFuture<void> Delete(TKey key) override {
        return ComputeStorageActor_->Delete(key);
    }

    std::optional<NThreading::TFuture<TRope>> Extract(TKey key) override {
        return ComputeStorageActor_->Extract(key);
    }

private:
    IDqComputeStorageActor* ComputeStorageActor_;
    TActorId ComputeStorageActorId_;
};

IDqComputeStorage::TPtr MakeComputeStorage(const TString& spillerName, std::function<void()>&& wakeUpCallback) {
    return std::make_shared<TDqComputeStorage>(TTxId(), spillerName, std::move(wakeUpCallback));
}

} // namespace NYql::NDq