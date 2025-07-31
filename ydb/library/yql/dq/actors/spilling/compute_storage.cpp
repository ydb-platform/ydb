#include "compute_storage.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <util/generic/guid.h>

namespace NYql::NDq {

using namespace NActors;

TDqComputeStorage::TDqComputeStorage(TTxId txId, TWakeUpCallback wakeUpCallback, TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, TSpillerMemoryUsageReporter::TPtr memoryUsageReporter, TActorSystem* actorSystem) 
    : ActorSystem_(actorSystem)
    , MemoryUsageReporter_(memoryUsageReporter)
{
    TStringStream spillerName;
    spillerName << "Spiller" << "_" << CreateGuidAsString();
    ComputeStorageActor_ = CreateDqComputeStorageActor(txId, spillerName.Str(), wakeUpCallback, errorCallback, spillingTaskCounters);
    ComputeStorageActorId_ = ActorSystem_->Register(ComputeStorageActor_->GetActor());
}

TDqComputeStorage::~TDqComputeStorage() {
    ActorSystem_->Send(ComputeStorageActorId_, new TEvents::TEvPoison);
}

NThreading::TFuture<NKikimr::NMiniKQL::ISpiller::TKey> TDqComputeStorage::Put(TChunkedBuffer&& blob) {
    auto promise = NThreading::NewPromise<NKikimr::NMiniKQL::ISpiller::TKey>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvPut(std::move(blob), std::move(promise)));
    return future;
}

NThreading::TFuture<std::optional<TChunkedBuffer>> TDqComputeStorage::Get(TKey key) {
    return GetInternal(key, false);
}

NThreading::TFuture<void> TDqComputeStorage::Delete(TKey key) {
    auto promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvDelete(key, std::move(promise)));

    return future;
}

NThreading::TFuture<std::optional<TChunkedBuffer>> TDqComputeStorage::Extract(TKey key) {
    return GetInternal(key, true);
}

void TDqComputeStorage::ReportAlloc(ui64 bytes) {
    // TODO: collect 10KB and only then report
    MemoryUsageReporter_->ReportAlloc(bytes);
}

void TDqComputeStorage::ReportFree(ui64 bytes)  {
    MemoryUsageReporter_->ReportFree(bytes);
}

NThreading::TFuture<std::optional<TChunkedBuffer>> TDqComputeStorage::GetInternal(TKey key, bool removeBlobAfterRead) {

    auto promise = NThreading::NewPromise<std::optional<TChunkedBuffer>>();
    auto future = promise.GetFuture();

    ActorSystem_->Send(ComputeStorageActorId_, new TEvGet(key, std::move(promise), removeBlobAfterRead));
    return future;
}

} // namespace NYql::NDq
