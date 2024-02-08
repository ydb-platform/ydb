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
    return ComputeStorageActor_->Put(std::move(blob));
}

std::optional<NThreading::TFuture<TRope>> TDqComputeStorage::Get(TKey key) {
    return ComputeStorageActor_->Get(key);
}

NThreading::TFuture<void> TDqComputeStorage::Delete(TKey key) {
    return ComputeStorageActor_->Delete(key);
}

std::optional<NThreading::TFuture<TRope>> TDqComputeStorage::Extract(TKey key) {
    return ComputeStorageActor_->Extract(key);
}

} // namespace NYql::NDq
