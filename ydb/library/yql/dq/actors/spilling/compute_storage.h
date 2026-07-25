#pragma once

#include "compute_storage_actor.h"
#include "spiller_memory_reporter.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

// This class will be refactored to be non-actor spiller part
class TDqComputeStorage : public NKikimr::NMiniKQL::ISpiller
{
public:
    TDqComputeStorage(TTxId txId, TWakeUpCallback wakeUpCallback, TErrorCallback errorCallback,
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, TSpillerMemoryUsageReporter::TPtr memoryUsageReporter, NActors::TActorSystem* actorSystem);

    ~TDqComputeStorage();

    NThreading::TFuture<TKey> Put(TChunkedBuffer&& blob) override;

    NThreading::TFuture<std::optional<TChunkedBuffer>> Get(TKey key) override;

    NThreading::TFuture<std::optional<TChunkedBuffer>> Extract(TKey key) override;

    NThreading::TFuture<void> Delete(TKey key) override;

    void ReportAlloc(ui64 bytes) override;
    void ReportFree(ui64 bytes) override;

private:
    NThreading::TFuture<std::optional<TChunkedBuffer>> GetInternal(TKey key, bool removeBlobAfterRead);

    NActors::TActorSystem* ActorSystem_;
    IDqComputeStorageActor* ComputeStorageActor_;
    NActors::TActorId ComputeStorageActorId_;
    TSpillerMemoryUsageReporter::TPtr MemoryUsageReporter_;
};

} // namespace NYql::NDq
