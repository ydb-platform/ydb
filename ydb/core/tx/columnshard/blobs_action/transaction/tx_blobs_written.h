#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>
#include <ydb/core/tx/columnshard/tracing/probes.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

LWTRACE_USING(YDB_CS);

class TColumnShard;

class TTxBlobsWritingFinished: public TExtendedTransactionBase {
private:
    using TBase = TExtendedTransactionBase;
    TInsertedPortions Pack;
    const std::shared_ptr<NOlap::IBlobsWritingAction> WritingActions;
    std::optional<NOlap::TSnapshot> CommitSnapshot;
    TInstant StartTime;
    TDuration TransactionTime;

    class TReplyInfo {
    private:
        std::unique_ptr<NActors::IEventBase> Event;
        TActorId DestinationForReply;
        const ui64 Cookie;

    public:
        TReplyInfo(std::unique_ptr<NActors::IEventBase>&& ev, const TActorId& destinationForReply, const ui64 cookie)
            : Event(std::move(ev))
            , DestinationForReply(destinationForReply)
            , Cookie(cookie) {
        }

        void DoSendReply(const TActorContext& ctx) {
            ctx.Send(DestinationForReply, Event.release(), 0, Cookie);
        }
    };

    std::vector<TInsertWriteId> InsertWriteIds;
    std::vector<TReplyInfo> Results;
    std::optional<EOperationBehaviour> PackBehaviour;

public:
    TTxBlobsWritingFinished(TColumnShard* self, const NKikimrProto::EReplyStatus writeStatus,
        const std::shared_ptr<NOlap::IBlobsWritingAction>& writingActions, TInsertedPortions&& pack);

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_WRITE_PORTIONS_FINISHED;
    }
};

class TTxBlobsWritingFailed: public TExtendedTransactionBase {
private:
    using TBase = TExtendedTransactionBase;
    TInsertedPortions Pack;

    class TReplyInfo {
    private:
        std::unique_ptr<NActors::IEventBase> Event;
        TActorId DestinationForReply;
        const ui64 Cookie;

    public:
        TReplyInfo(std::unique_ptr<NActors::IEventBase>&& ev, const TActorId& destinationForReply, const ui64 cookie)
            : Event(std::move(ev))
            , DestinationForReply(destinationForReply)
            , Cookie(cookie) {
        }

        void DoSendReply(const TActorContext& ctx) {
            ctx.Send(DestinationForReply, Event.release(), 0, Cookie);
        }
    };

    std::vector<TReplyInfo> Results;

public:
    TTxBlobsWritingFailed(TColumnShard* self, TInsertedPortions&& pack)
        : TBase(self)
        , Pack(std::move(pack)) {
    }

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_WRITE_PORTIONS_FAILED;
    }
};

}   // namespace NKikimr::NColumnShard
