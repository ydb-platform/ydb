#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxDatashardScanResponse : public TTxBase {
    NKikimrStat::TEvStatisticsResponse Record;
    bool IsCorrectShardId = false;

    TTxDatashardScanResponse(TSelf* self, NKikimrStat::TEvStatisticsResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_SCAN_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxDatashardScanResponse::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        // TODO: handle scan errors

        if (Self->DatashardRanges.empty()) {
            return true;
        }

        auto& range = Self->DatashardRanges.front();
        auto replyShardId = Record.GetShardTabletId();

        if (replyShardId != range.DataShardId) {
            return true;
        }

        IsCorrectShardId = true;

        for (auto& column : Record.GetColumns()) {
            auto tag = column.GetTag();
            for (auto& statistic : column.GetStatistics()) {
                if (statistic.GetType() == static_cast<ui32>(EStatType::COUNT_MIN_SKETCH)) {
                    auto* data = statistic.GetData().data();
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
                    db.Table<Schema::ColumnStatistics>().Key(tag).Update(
                        NIceDb::TUpdate<Schema::ColumnStatistics::CountMinSketch>(currentStr));
                }
            }
        }

        Self->TraversalStartKey = range.EndKey;
        Self->PersistStartKey(db);

        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxDatashardScanResponse::Complete",
            {"tabletId", Self->TabletID()});

        if (IsCorrectShardId && !Self->DatashardRanges.empty()) {
            Self->DatashardRanges.pop_front();
            Self->ScanNextDatashardRange();
        }
    }
};

void TStatisticsAggregator::Handle(NStat::TEvStatistics::TEvStatisticsResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxDatashardScanResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
