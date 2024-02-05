#include "compute_storage.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/yql/minikql/computation/mkql_spiller.h>

namespace NYql::NDq {

using namespace NActors;

TDqComputeStorage::TDqComputeStorage(TTxId txId, std::function<void()> wakeUpCallback, TActorSystem* ActorSystem)
    :  ActorSystem_(ActorSystem)
{
    TStringStream spillerName;
    spillerName << "Spiller" << "_" << static_cast<const void*>(this);
    ComputeStorageActor_ = CreateDqComputeStorageActor(txId, spillerName.Str(), wakeUpCallback);
    ComputeStorageActorId_ = ActorSystem->Register(ComputeStorageActor_->GetActor());
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
