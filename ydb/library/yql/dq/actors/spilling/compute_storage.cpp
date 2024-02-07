#include "compute_storage.h"

#include <util/generic/guid.h>

namespace NYql::NDq {

using namespace NActors;

TDqComputeStorage::TDqComputeStorage(TTxId txId, std::function<void()> wakeUpCallback, TActorSystem* actorSystem) : ActorSystem_(actorSystem) {
    TStringStream spillerName;
    spillerName << "Spiller" << "_" << CreateGuidAsString();
    ComputeStorageActor_ = CreateDqComputeStorageActor(txId, spillerName.Str(), wakeUpCallback, ActorSystem_);
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

std::optional<NThreading::TFuture<TRope>> TDqComputeStorage::Get(TKey key) {
    auto promise = NThreading::NewPromise<TRope>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvGet(key, std::move(promise)));
    return future;
}

NThreading::TFuture<void> TDqComputeStorage::Delete(TKey) {
    return {};
}

std::optional<NThreading::TFuture<TRope>> TDqComputeStorage::Extract(TKey) {
    return {};
}

} // namespace NYql::NDq
