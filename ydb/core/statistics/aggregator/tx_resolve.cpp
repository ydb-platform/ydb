#include "aggregator_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/datashard/datashard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

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

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        YDB_LOG_DEBUG("TTxResolve::Execute",
            {"tabletId", Self->TabletID()});

        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(Request->ResultSet.size() == 1);
        const auto& entry = Request->ResultSet.front();

        if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Cancelled = true;

            if (entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist) {
                Self->DeleteStatisticsFromTable();
            } else {
                // Resolve failure -> mark the operation FAILED.
                Self->FinishTraversal(db, Ydb::Table::AnalyzeState::STATE_FAILED);
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
            // Natural completion of an empty table — pass nullopt so FinishTraversal marks only the
            // current table done; the operation flips to STATE_DONE when all its tables are done.
            Self->FinishTraversal(db, std::nullopt);
            StartColumnShardEventDistribution = false;
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxResolve::Complete",
            {"tabletId", Self->TabletID()});

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

        Self->NavigateDatabase = "";
        Self->NavigatePathId = {};
    }
};

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    Execute(new TTxResolve(this, ev->Get()->Request.Release()), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
