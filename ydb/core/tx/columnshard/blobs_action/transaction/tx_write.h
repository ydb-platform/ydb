#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/engines/writer/indexed_blob_constructor.h>

namespace NKikimr::NColumnShard {

class TTxWrite: public NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard> {
private:
    using TBase = NOlap::NDataSharing::TExtendedTransactionBase<TColumnShard>;

public:
    TTxWrite(TColumnShard* self, const TEvPrivate::TEvWriteBlobsResult::TPtr& putBlobResult)
        : TBase(self, "TTxWrite")
        , PutBlobResult(putBlobResult) {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override;
    void DoComplete(const TActorContext& ctx) override;
    TTxType GetTxType() const override {
        return TXTYPE_WRITE;
    }

private:
    TEvPrivate::TEvWriteBlobsResult::TPtr PutBlobResult;
    std::optional<NOlap::TSnapshot> CommitSnapshot;

    bool CommitOneBlob(TTransactionContext& txc, const NOlap::TWideSerializedBatch& batch, const TInsertWriteId writeId);
    bool InsertOneBlob(TTransactionContext& txc, const NOlap::TWideSerializedBatch& batch, const TInsertWriteId writeId);

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
};

}   // namespace NKikimr::NColumnShard
