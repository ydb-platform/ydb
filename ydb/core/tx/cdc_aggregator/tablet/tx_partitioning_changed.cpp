#include "tablet_impl.h"

#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>
#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxPartitioningChanged : public TTxBase {
    using TResult = NKikimrCdcAggregator::TEvPartitioningChangedAck;

    TEvCdcAggregator::TEvPartitioningChanged::TPtr Ev;
    THolder<TEvCdcAggregator::TEvPartitioningChangedAck> Result;

public:
    explicit TTxPartitioningChanged(TCdcAggregator* self, TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev)
        : TTxBase(self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PARTITIONING_CHANGED;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        const auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvCdcAggregator::TEvPartitioningChangedAck>();
        Result->Record.SetOrigin(Self->TabletID());

        const auto [change, error] = TCdcAggregatorStreamProtoConverter().ParsePartitioningChange(record);
        if (!change) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError(error);
            return true;
        }

        if (!Self->CdcAggregatorStream) {
            Result->Record.SetStatus(TResult::NOT_READY);
            Result->Record.SetError("Tracking is not configured");
            return true;
        }

        const auto diff = Self->CdcAggregatorStream->ApplyPartitioningChange(*change);
        if (!diff) {
            Result->Record.SetStatus(TResult::UNKNOWN_SOURCE);
            Result->Record.SetError(TStringBuilder() << "Unknown partitioning change " << change->OperationId);
            return true;
        }

        if (!diff->Deleted.empty()) {
            NIceDb::TNiceDb db(txc.DB);
            Self->CdcAggregatorStreamRepository.ApplyCdcAggregatorStreamPartitioningChange(db, *change, *diff);
        }

        YDB_LOG_CREATE_CONTEXT({"tabletId", Self->TabletID()});
        YDB_LOG_INFO("Partitioning change applied",
            {"operationId", change->OperationId},
            {"parentDataShardIds", JoinSeq(",", change->ParentDataShardIds)},
            {"inserted", JoinSeq(",", diff->Inserted)},
            {"deleted", JoinSeq(",", diff->Deleted)});

        Result->Record.SetStatus(TResult::SUCCESS);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT({"tabletId", Self->TabletID()});
        YDB_LOG_DEBUG("Reply",
            {"result", Result->ToString()});

        ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
    }
};

void TCdcAggregator::Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    Execute(new TTxPartitioningChanged(this, ev), ctx);
}

} // namespace NKikimr::NCdcAggregator
