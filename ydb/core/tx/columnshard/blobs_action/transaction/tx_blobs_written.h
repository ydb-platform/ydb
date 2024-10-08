#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>
#include <ydb/core/tx/columnshard/operations/events.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {

class TColumnShard;

class TTxBlobsWritingFinished: public NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard> {
private:
    using TBase = NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard>;
    const NKikimrProto::EReplyStatus PutBlobResult;
    std::vector<TInsertedPortions> Packs;
    const std::shared_ptr<NOlap::IBlobsWritingAction> WritingActions;
    std::optional<NOlap::TSnapshot> CommitSnapshot;

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
    TTxBlobsWritingFinished(TColumnShard* self, const NKikimrProto::EReplyStatus writeStatus,
        const std::shared_ptr<NOlap::IBlobsWritingAction>& writingActions, std::vector<TInsertedPortions>&& packs,
        const std::vector<TFailedWrite>& fails);

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_WRITE;
    }
};

}   // namespace NKikimr::NColumnShard
