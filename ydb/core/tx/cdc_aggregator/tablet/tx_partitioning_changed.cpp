#include "tablet_impl.h"

#include <util/string/builder.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

class TCdcAggregator::TTxPartitioningChanged : public TTxBase {
    using TResult = NKikimrCdcAggregator::TEvPartitioningChangedAck;

    TEvCdcAggregator::TEvPartitioningChanged::TPtr Ev;
    THolder<TEvCdcAggregator::TEvPartitioningChangedAck> Result;

    static bool IsValidPartitioningChange(ui64 parentDataShardId, const TVector<ui64>& childDataShardIds) {
        return parentDataShardId
            && !childDataShardIds.empty()
            && childDataShardIds.front() != 0
            && std::adjacent_find(childDataShardIds.begin(), childDataShardIds.end()) == childDataShardIds.end()
            && !std::binary_search(childDataShardIds.begin(), childDataShardIds.end(), parentDataShardId);
    }

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

        const ui64 parentDataShardId = record.GetParentDataShardId();
        TVector<ui64> childDataShardIds(record.GetChildDataShardIds().begin(), record.GetChildDataShardIds().end());
        std::sort(childDataShardIds.begin(), childDataShardIds.end());
        if (!IsValidPartitioningChange(parentDataShardId, childDataShardIds)) {
            Result->Record.SetStatus(TResult::BAD_REQUEST);
            Result->Record.SetError(TStringBuilder() << "Invalid partitioning change of DataShard " << parentDataShardId);
            return true;
        }

        if (!Self->Stream.IsConfigured()) {
            Result->Record.SetStatus(TResult::NOT_READY);
            Result->Record.SetError("Tracking is not configured");
            return true;
        }

        TStreamChanges changes;
        switch (Self->Stream.ChangePartitioning(parentDataShardId, childDataShardIds, changes)) {
            case ESourceOutcome::Success:
                Result->Record.SetStatus(TResult::SUCCESS);
                break;
            case ESourceOutcome::UnknownSource:
                Result->Record.SetStatus(TResult::UNKNOWN_SOURCE);
                Result->Record.SetError(TStringBuilder() << "DataShard " << parentDataShardId << " is not a source");
                break;
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistStreamChanges(db, changes);

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
