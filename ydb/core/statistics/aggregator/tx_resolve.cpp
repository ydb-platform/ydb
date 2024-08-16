#include "aggregator_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <util/string/vector.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxResolve : public TTxBase {
    std::unique_ptr<NSchemeCache::TSchemeCacheRequest> Request;
    bool Cancelled = false;
    bool StartColumnShardEventDistribution = true;

    TTxResolve(TSelf* self, NSchemeCache::TSchemeCacheRequest* request)
        : TTxBase(self)
        , Request(request)
    {}

    TTxType GetTxType() const override { return TXTYPE_RESOLVE; }

    bool ExecuteAnalyze(const NSchemeCache::TSchemeCacheRequest::TEntry& entry, NIceDb::TNiceDb& db) {
        Y_ABORT_UNLESS(Self->NavigateAnalyzeOperationId);
        Y_ABORT_UNLESS(Self->NavigatePathId);

        if (entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist) {
            // AnalyzedShards will be empty and Analyze will complete without sending event to shards
            return true;
        }

        if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Cancelled = true;
            return true;
        }

        auto& partitioning = entry.KeyDescription->GetPartitions();

        auto forceTraversalTable = Self->ForceTraversalTable(Self->NavigateAnalyzeOperationId, Self->NavigatePathId);
        Y_ABORT_UNLESS(forceTraversalTable);

        for (auto& part : partitioning) {
            if (part.Range) {
                forceTraversalTable->AnalyzedShards.push_back({
                    .ShardTabletId = part.ShardId,
                    .Status = TAnalyzedShard::EStatus::None
                });
            }
        }
        forceTraversalTable->Status = TForceTraversalTable::EStatus::AnalyzeStarted;
        db.Table<Schema::ForceTraversalTables>().Key(Self->NavigateAnalyzeOperationId, Self->NavigatePathId.OwnerId, Self->NavigatePathId.LocalPathId)
            .Update(
                NIceDb::TUpdate<Schema::ForceTraversalTables::Status>((ui64)forceTraversalTable->Status)
            );

        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::ExecuteAnalyze. Table OperationId " << Self->NavigateAnalyzeOperationId << ", PathId " << Self->NavigatePathId 
            << ", AnalyzedShards " << forceTraversalTable->AnalyzedShards.size());

        return true;
    }

    bool ExecuteTraversal(const NSchemeCache::TSchemeCacheRequest::TEntry& entry, NIceDb::TNiceDb& db) {
        if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Cancelled = true;

            if (entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist) {
                Self->DeleteStatisticsFromTable();
            } else {
                Self->FinishTraversal(db);
            }
            return true;
        }

        auto& partitioning = entry.KeyDescription->GetPartitions();

        if (Self->TraversalIsColumnTable) {
            Self->TabletsForReqDistribution.clear();
            Self->CountMinSketches.clear();
        } else {
            Self->DatashardRanges.clear();
        }

        for (auto& part : partitioning) {
            if (!part.Range) {
                continue;
            }
            if (Self->TraversalIsColumnTable) {
                Self->TabletsForReqDistribution.insert(part.ShardId);
            } else {
                TRange range;
                range.EndKey = part.Range->EndKeyPrefix;
                range.DataShardId = part.ShardId;
                Self->DatashardRanges.push_back(range);
            }
        }

        if (Self->TraversalIsColumnTable && Self->TabletsForReqDistribution.empty()) {
            Self->FinishTraversal(db);
            StartColumnShardEventDistribution = false;
        }

        return true;
    }


    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::Execute");

        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(Request->ResultSet.size() == 1);
        const auto& entry = Request->ResultSet.front();

        switch (Self->NavigateType) {
        case ENavigateType::Analyze:
            return ExecuteAnalyze(entry, db);
        case ENavigateType::Traversal:
            return ExecuteTraversal(entry, db);
        };
        
    }

    void CompleteTraversal(const TActorContext& ctx) {
        if (Cancelled) {
            return;
        }

        if (Self->TraversalIsColumnTable) {
            if (StartColumnShardEventDistribution) {
                ctx.Send(Self->SelfId(), new TEvPrivate::TEvRequestDistribution);
            }
        } else {
            Self->ScanNextDatashardRange();
        }
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::Complete");

        switch (Self->NavigateType) {
        case ENavigateType::Analyze:
            break;
        case ENavigateType::Traversal:
            CompleteTraversal(ctx);
            break;
        };

        Self->NavigateAnalyzeOperationId.clear();
        Self->NavigatePathId = {};
    }
};

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    Execute(new TTxResolve(this, ev->Get()->Request.Release()), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
