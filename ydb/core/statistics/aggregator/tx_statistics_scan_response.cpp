#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxStatisticsScanResponse : public TTxBase {
    NKikimrTxDataShard::TEvStatisticsScanResponse Record;
    bool IsCorrectShardId = false;

    TTxStatisticsScanResponse(TSelf* self, NKikimrTxDataShard::TEvStatisticsScanResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxStatisticsScanResponse::Execute");

        NIceDb::TNiceDb db(txc.DB);

        // TODO: handle scan errors

        if (Self->ShardRanges.empty()) {
            return true;
        }

        auto& range = Self->ShardRanges.front();
        auto replyShardId = Record.GetShardTabletId();

        if (replyShardId != range.DataShardId) {
            return true;
        }

        IsCorrectShardId = true;

        for (auto& column : Record.GetColumns()) {
            auto tag = column.GetTag();
            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == NKikimr::NStat::COUNT_MIN_SKETCH) {
                    auto* data = statistic.GetData().Data();
                    auto* sketch = reinterpret_cast<const TCountMinSketch*>(data);

                    if (Self->ColumnNames.find(tag) == Self->ColumnNames.end()) {
                        continue;
                    }
                    if (Self->CountMinSketches.find(tag) == Self->CountMinSketches.end()) {
                        Self->CountMinSketches[tag].reset(TCountMinSketch::Create());
                    }

                    auto& current = Self->CountMinSketches[tag];
                    *current += *sketch;

                    auto currentStr = TString(current->AsStringBuf());
                    db.Table<Schema::Statistics>().Key(tag).Update(
                        NIceDb::TUpdate<Schema::Statistics::CountMinSketch>(currentStr));
                }
            }
        }

        Self->StartKey = range.EndKey;
        Self->PersistSysParam(db, Schema::SysParam_StartKey, Self->StartKey.GetBuffer());

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxStatisticsScanResponse::Complete");

        if (IsCorrectShardId && !Self->ShardRanges.empty()) {
            Self->ShardRanges.pop_front();
            Self->NextRange();
        }
    }
};

void TStatisticsAggregator::Handle(TEvDataShard::TEvStatisticsScanResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxStatisticsScanResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
