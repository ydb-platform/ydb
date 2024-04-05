#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxResolve : public TTxBase {
    std::unique_ptr<NSchemeCache::TSchemeCacheRequest> Request;

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

        Self->ShardRanges.clear();

        auto& partitioning = entry.KeyDescription->Partitioning;
        for (auto& part : *partitioning) {
            if (!part.Range) {
                continue;
            }
            auto& endKey = part.Range->EndKeyPrefix;
            Self->ShardRanges.emplace_back(endKey, part.ShardId);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxResolve::Complete");

        Self->NextRange();
    }
};

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
    Execute(new TTxResolve(this, ev->Get()->Request.Release()), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
