#include "aggregator_impl.h"

#include <ydb/core/base/hive.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxResolve : public TTxBase {
    std::unique_ptr<NSchemeCache::TSchemeCacheRequest> Request;
    bool Cancelled = false;

    TTxResolve(TSelf* self, NSchemeCache::TSchemeCacheRequest* request)
        : TTxBase(self)
        , Request(request)
    {}

    TTxType GetTxType() const override { return TXTYPE_RESOLVE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::Execute");

        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(Request->ResultSet.size() == 1);
        const auto& entry = Request->ResultSet.front();

        if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            Cancelled = true;

            if (entry.Status == NSchemeCache::TSchemeCacheRequest::EStatus::PathErrorNotExist) {
                Self->DeleteStatisticsFromTable();
            } else {
                Self->FinishScan(db);
            }
            return true;
        }

        auto& partitioning = entry.KeyDescription->GetPartitions();

        if (Self->IsColumnTable) {
            Self->TabletsForReqDistribution.clear();
        } else {
            Self->DatashardRanges.clear();
        }

        for (auto& part : partitioning) {
            if (!part.Range) {
                continue;
            }
            if (Self->IsColumnTable) {
                Self->TabletsForReqDistribution.insert(part.ShardId);
            } else {
                TRange range;
                range.EndKey = part.Range->EndKeyPrefix;
                range.DataShardId = part.ShardId;
                Self->DatashardRanges.push_back(range);
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::Complete");

        if (Cancelled) {
            return;
        }

        if (Self->IsColumnTable) {
            ctx.Send(Self->SelfId(), new TEvPrivate::TEvRequestDistribution);
        } else {
            Self->ScanNextDatashardRange();
        }
    }
};

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    Execute(new TTxResolve(this, ev->Get()->Request.Release()), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
