#include "compute_storage.h"

#include <util/generic/guid.h>

namespace NYql::NDq {

using namespace NActors;

TDqComputeStorage::TDqComputeStorage(TTxId txId, TWakeUpCallback wakeUpCallback, TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, TActorSystem* actorSystem) : ActorSystem_(actorSystem) {
    TStringStream spillerName;
    spillerName << "Spiller" << "_" << CreateGuidAsString();
    ComputeStorageActor_ = CreateDqComputeStorageActor(txId, spillerName.Str(), wakeUpCallback, spillingTaskCounters, errorCallback);
    ComputeStorageActorId_ = ActorSystem_->Register(ComputeStorageActor_->GetActor());
}

TDqComputeStorage::~TDqComputeStorage() {
    ActorSystem_->Send(ComputeStorageActorId_, new TEvents::TEvPoison);
}

NThreading::TFuture<NKikimr::NMiniKQL::ISpiller::TKey> TDqComputeStorage::Put(TRope&& blob) {
    auto promise = NThreading::NewPromise<NKikimr::NMiniKQL::ISpiller::TKey>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvPut(std::move(blob), std::move(promise)));
    return future;
}

NThreading::TFuture<std::optional<TRope>> TDqComputeStorage::Get(TKey key) {
    return GetInternal(key, false);
}

NThreading::TFuture<void> TDqComputeStorage::Delete(TKey key) {
    auto promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvDelete(key, std::move(promise)));

    return future;
}

NThreading::TFuture<std::optional<TRope>> TDqComputeStorage::Extract(TKey key) {
    return GetInternal(key, true);
}

NThreading::TFuture<std::optional<TRope>> TDqComputeStorage::GetInternal(TKey key, bool removeBlobAfterRead) {

    auto promise = NThreading::NewPromise<std::optional<TRope>>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvGet(key, std::move(promise), removeBlobAfterRead));
    return future;
}

} // namespace NYql::NDq
