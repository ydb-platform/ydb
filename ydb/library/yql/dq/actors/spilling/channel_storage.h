#pragma once

#include "spilling_counters.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <ydb/library/actors/core/actor.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId,
    TWakeUpCallback wakeUpCallback,
    TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters,
    NActors::TActorSystem* actorSystem);

// Create channel storage with shared spiller
IDqChannelStorage::TPtr CreateDqChannelStorageWithSharedSpiller(ui64 channelId,
    NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters);

// Channel spiller that implements ISpiller interface and uses shared spiller
class TDqChannelSpiller : public NKikimr::NMiniKQL::ISpiller {
public:
    TDqChannelSpiller(ui64 channelId, NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller,
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters);
    
    NThreading::TFuture<TKey> Put(NYql::TChunkedBuffer&& blob) override;
    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Get(TKey key) override;
    NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> Extract(TKey key) override;
    NThreading::TFuture<void> Delete(TKey key) override;

private:
    const ui64 ChannelId_;
    NKikimr::NMiniKQL::ISpiller::TPtr SharedSpiller_;
    TIntrusivePtr<TSpillingTaskCounters> SpillingTaskCounters_;
};

// Create channel spiller (implements ISpiller interface)
NKikimr::NMiniKQL::ISpiller::TPtr CreateDqChannelSpiller(ui64 channelId,
    NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters);

} // namespace NYql::NDq
