#include "aggregator_impl.h"

#include <ydb/core/protos/hive.pb.h>
#include <ydb/core/statistics/service/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxAnalyzeShardResponse : public TTxBase {
    NKikimrStat::TEvAnalyzeShardResponse Record;

    TTxAnalyzeShardResponse(TSelf* self, NKikimrStat::TEvAnalyzeShardResponse&& record)
        : TTxBase(self)
        , Record(std::move(record))
    {}

    TTxType GetTxType() const override { return TXTYPE_ANALYZE_SHARD_RESPONSE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxAnalyzeShardResponse::Execute",
            {"tabletId", Self->TabletID()});

        const TString operationId = Record.GetOperationId();
        const TPathId pathId = TPathId::FromProto(Record.GetPathId());
        auto operationTable = Self->ForceTraversalTable(operationId, pathId);
        if (!operationTable) {
            YDB_LOG_ERROR("TTxAnalyzeShardResponse::Execute. Unknown OperationTable",
                {"tabletId", Self->TabletID()},
                {"record", Record.ShortDebugString()});
            return true;
        }

        auto analyzedShard = std::find_if(operationTable->AnalyzedShards.begin(), operationTable->AnalyzedShards.end(),
            [tabletId = Record.GetShardTabletId()] (TAnalyzedShard& analyzedShard) { return analyzedShard.ShardTabletId == tabletId;});
        if (analyzedShard == operationTable->AnalyzedShards.end()) {
            YDB_LOG_ERROR("TTxAnalyzeShardResponse::Execute. Unknown AnalyzedShards.",
                {"tabletId", Self->TabletID()},
                {"record", Record.ShortDebugString()},
                {"shardTabletId", Record.GetShardTabletId()});
            return true;
        }
        if (analyzedShard->Status != TAnalyzedShard::EStatus::AnalyzeStarted) {
            YDB_LOG_ERROR("TTxAnalyzeShardResponse::Execute. Unknown AnalyzedShards Status.",
                {"tabletId", Self->TabletID()},
                {"record", Record.ShortDebugString()},
                {"shardTabletId", Record.GetShardTabletId()});
        }

        analyzedShard->Status = TAnalyzedShard::EStatus::AnalyzeFinished;

        bool completeResponse = std::all_of(operationTable->AnalyzedShards.begin(), operationTable->AnalyzedShards.end(),
            [] (const TAnalyzedShard& analyzedShard) { return analyzedShard.Status == TAnalyzedShard::EStatus::AnalyzeFinished;});

        if (!completeResponse) {
            YDB_LOG_DEBUG("TTxAnalyzeShardResponse::Execute. There are shards which are not analyzed",
                {"tabletId", Self->TabletID()});
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        Self->UpdateForceTraversalTableStatus(TForceTraversalTable::EStatus::AnalyzeFinished, operationId, *operationTable,  db);
        YDB_LOG_DEBUG("TTxAnalyzeShardResponse::Execute. All shards are analyzed",
            {"tabletId", Self->TabletID()});
        return true;
    }

    void Complete(const TActorContext&) override {
        YDB_LOG_DEBUG("TTxAnalyzeShardResponse::Complete",
            {"tabletId", Self->TabletID()});
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvAnalyzeShardResponse::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxAnalyzeShardResponse(this, std::move(record)),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
