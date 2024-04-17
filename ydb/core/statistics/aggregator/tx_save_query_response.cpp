#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxSaveQueryResponse : public TTxBase {

    TTxSaveQueryResponse(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_SAVE_QUERY_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Execute");

        NIceDb::TNiceDb db(txc.DB);

        if (!Self->ScanTablesByTime.empty()) {
            auto& topTable = Self->ScanTablesByTime.top();
            auto pathId = topTable.PathId;
            if (pathId == Self->ScanTableId.PathId) {
                TScanTable scanTable;
                scanTable.PathId = pathId;
                scanTable.SchemeShardId = topTable.SchemeShardId;
                scanTable.LastUpdateTime = Self->ScanStartTime;

                Self->ScanTablesByTime.pop();
                Self->ScanTablesByTime.push(scanTable);

                db.Table<Schema::ScanTables>().Key(pathId.OwnerId, pathId.LocalPathId).Update(
                    NIceDb::TUpdate<Schema::ScanTables::LastUpdateTime>(Self->ScanStartTime.MicroSeconds()));
            }
        }

        Self->ScanTableId.PathId = TPathId();
        Self->PersistScanTableId(db);

        for (auto& [tag, _] : Self->CountMinSketches) {
            db.Table<Schema::Statistics>().Key(tag).Delete();
        }
        Self->CountMinSketches.clear();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxSaveQueryResponse::Complete");

        if (Self->ReplyToActorId) {
            ctx.Send(Self->ReplyToActorId, new TEvStatistics::TEvScanTableResponse);
        }

        Self->ScheduleNextScan();
    }
};
void TStatisticsAggregator::Handle(TEvStatistics::TEvSaveStatisticsQueryResponse::TPtr&) {
    Execute(new TTxSaveQueryResponse(this), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
