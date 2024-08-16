#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeTableResponse : public TTxBase {
    NKikimrStat::TEvAnalyzeTableResponse Record;
    
    TTxAnalyzeTableResponse(TSelf* self, NKikimrStat::TEvAnalyzeTableResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_TABLE_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Execute");

        const TString operationId = Record.GetOperationId();
        const TPathId pathId = PathIdFromPathId(Record.GetPathId());
        auto operationTable = Self->ForceTraversalTable(operationId, pathId);
        if (!operationTable) {
            SA_LOG_E("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Complete. Unknown OperationTable. Record: " << Record.ShortDebugString());
            return true;
        }

        auto analyzedShard = std::find_if(operationTable->AnalyzedShards.begin(), operationTable->AnalyzedShards.end(), 
            [tabletId = Record.GetShardTabletId()] (TAnalyzedShard& analyzedShard) { return analyzedShard.ShardTabletId == tabletId;});
        if (analyzedShard == operationTable->AnalyzedShards.end()) {
            SA_LOG_E("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Complete. Unknown AnalyzedShards. Record: " << Record.ShortDebugString() << ", ShardTabletId " << Record.GetShardTabletId());
            return true;
        }
        if (analyzedShard->Status != TAnalyzedShard::EStatus::AnalyzeStarted) {
            SA_LOG_E("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Complete. Unknown AnalyzedShards Status. Record: " << Record.ShortDebugString() << ", ShardTabletId " << Record.GetShardTabletId());
        }

        analyzedShard->Status = TAnalyzedShard::EStatus::AnalyzeFinished;

        bool completeResponse = std::any_of(operationTable->AnalyzedShards.begin(), operationTable->AnalyzedShards.end(), 
            [] (const TAnalyzedShard& analyzedShard) { return analyzedShard.Status == TAnalyzedShard::EStatus::AnalyzeFinished;});

        if (!completeResponse)
            return true;   
        
        NIceDb::TNiceDb db(txc.DB);

        operationTable->Status = TForceTraversalTable::EStatus::AnalyzeFinished; 
        db.Table<Schema::ForceTraversalTables>().Key(operationId, pathId.OwnerId, pathId.LocalPathId)
            .Update(NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)operationTable->Status));

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxAnalyzeTableResponse::Complete.");
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeTableResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxAnalyzeTableResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
