#pragma once

#include "events.h"
#include "partition_writer.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcProxy::V1 {

class TPartitionWriterCacheActor : public NActors::TActorBootstrapped<TPartitionWriterCacheActor> {
public:
    TPartitionWriterCacheActor(const TActorId& owner,
                               ui32 partition,
                               ui64 tabletId,
                               const NPQ::TPartitionWriterOpts& opts);

    void Bootstrap(const TActorContext& ctx);

private:
    using TPartitionWriterPtr = std::unique_ptr<TPartitionWriter>;
    using EErrorCode = NPQ::TEvPartitionWriter::TEvWriteResponse::EErrorCode;

    template <class TEvent>
    struct TEventQueue {
        TEventQueue() :
            Expected(Max<ui64>())
        {
        }

        ui64 Expected;
        THashMap<ui64, std::unique_ptr<TEvent>> Events;
    };

    static constexpr const size_t MAX_TRANSACTIONS_COUNT = 4;

    STFUNC(StateWork);
    STFUNC(StateBroken);

    void Handle(NPQ::TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx);
    void HandleOnBroken(NPQ::TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);

    void ReplyError(const TString& sessionId, const TString& txId,
                    EErrorCode code, const TString& reason,
                    ui64 cookie,
                    const TActorContext& ctx);

    TPartitionWriter* GetPartitionWriter(const TString& sessionId, const TString& txId,
                                         const TActorContext& ctx);
    bool TryDeleteOldestWriter(const TActorContext& ctx);
    void RegisterPartitionWriter(const TString& sessionId, const TString& txId,
                                 const TActorContext& ctx);
    void RegisterDefaultPartitionWriter(const TActorContext& ctx);
    TActorId CreatePartitionWriter(const TString& sessionId, const TString& txId,
                                   const TActorContext& ctx);

    template <class TEvent>
    void TryForwardToOwner(TEvent* event, TEventQueue<TEvent>& queue,
                           ui64 cookie,
                           const TActorContext& ctx);

    TActorId Owner; // WriteSessionActor
    ui32 Partition;
    ui64 TabletId;
    NPQ::TPartitionWriterOpts Opts;

    THashMap<std::pair<TString, TString>, TPartitionWriterPtr> Writers;

    TEventQueue<NPQ::TEvPartitionWriter::TEvWriteAccepted> PendingWriteAccepted;
    TEventQueue<NPQ::TEvPartitionWriter::TEvWriteResponse> PendingWriteResponse;
};

}
