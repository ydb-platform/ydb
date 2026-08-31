#pragma once

#include "partition_writer.h"

#include <ydb/core/persqueue/common/actor.h>

#include <optional>

namespace NKikimr::NPQ::NDataplane::NWrite {

class TPartitionWriterCacheActor : public TBaseActor<TPartitionWriterCacheActor>
                                 , public TConstantLogPrefix {
public:
    using TBase = TBaseActor<TPartitionWriterCacheActor>;

    TPartitionWriterCacheActor(const TActorId& owner,
                               ui32 partition,
                               ui64 tabletId,
                               const TPartitionWriterOpts& opts);

    void Bootstrap(const TActorContext& ctx);
    void PassAway() override;
    void OnException(const std::exception& exc) override;

    TString BuildLogPrefix() const override;

private:
    using TPartitionWriterPtr = std::unique_ptr<TCachedPartitionWriter>;
    using EErrorCode = TEvPartitionWriter::TEvWriteResponse::EErrorCode;

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

    void Handle(TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx);
    void HandleDeferredDestinationUpsertRequest(TEvPartitionWriter::TEvRequestDeferredDestinationUpsert::TPtr& ev, const TActorContext& ctx);
    void HandleOnBroken(TEvPartitionWriter::TEvTxWriteRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvInitResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPartitionWriter::TEvDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);

    void ReplyError(const TString& sessionId, const TString& txId,
                    EErrorCode code, const TString& reason,
                    ui64 cookie);
    void ReplyTxWriterInitError(TCachedPartitionWriter& writer,
                                const TEvPartitionWriter::TEvInitResult& result,
                                const TActorContext& ctx);
    void PoisonWriters();

    TCachedPartitionWriter* GetPartitionWriter(const TString& sessionId, const TString& txId,
                                               const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
                                               const TActorContext& ctx);
    bool TryDeleteOldestWriter(const TActorContext& ctx);
    void RegisterPartitionWriter(const TString& sessionId, const TString& txId,
                                 const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
                                 const TActorContext& ctx);
    void RegisterDefaultPartitionWriter(const TActorContext& ctx);
    TActorId CreatePartitionWriter(const TString& sessionId, const TString& txId,
                                   const std::optional<TDeferredPublishWriterOpts>& deferredPublish,
                                   const TActorContext& ctx);

    template <class TEvent>
    void TryForwardToOwner(TEvent* event, TEventQueue<TEvent>& queue,
                           ui64 cookie,
                           const TActorContext& ctx);

    TActorId Owner;
    ui32 Partition;
    ui64 TabletId;
    TPartitionWriterOpts Opts;

    THashMap<std::pair<TString, TString>, TPartitionWriterPtr> Writers;

    TEventQueue<TEvPartitionWriter::TEvWriteAccepted> PendingWriteAccepted;
    TEventQueue<TEvPartitionWriter::TEvWriteResponse> PendingWriteResponse;
};

} // namespace NKikimr::NPQ::NDataplane::NWrite
