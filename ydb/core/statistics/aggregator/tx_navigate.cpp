#include "aggregator_impl.h"

#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxNavigate : public TTxBase {
    std::unique_ptr<NSchemeCache::TSchemeCacheNavigate> Request;
    bool Cancelled = false;

    TTxNavigate(TSelf* self, NSchemeCache::TSchemeCacheNavigate* request)
        : TTxBase(self)
        , Request(request)
    {}

    TTxType GetTxType() const override { return TXTYPE_NAVIGATE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxNavigate::Execute");

        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(Request->ResultSet.size() == 1);
        const auto& entry = Request->ResultSet.front();

        if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            Cancelled = true;

            if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
                Self->DeleteStatisticsFromTable();
            } else {
                Self->FinishTraversal(db);
            }
            return true;
        }

        Self->Columns.clear();
        Self->Columns.reserve(entry.Columns.size());
        Self->KeyColumnTypes.clear();
        Self->ColumnNames.clear();

        for (const auto& col : entry.Columns) {
            TKeyDesc::TColumnOp op = { col.second.Id, TKeyDesc::EColumnOperation::Read, col.second.PType, 0, 0 };
            Self->Columns.push_back(op);
            Self->ColumnNames[col.second.Id] = col.second.Name;

            if (col.second.KeyOrder == -1) {
                continue;
            }

            Self->KeyColumnTypes.resize(Max<size_t>(Self->KeyColumnTypes.size(), col.second.KeyOrder + 1));
            Self->KeyColumnTypes[col.second.KeyOrder] = col.second.PType;
        }

        if (Self->TraversalStartKey.GetCells().empty()) {
            TVector<TCell> minusInf(Self->KeyColumnTypes.size());
            Self->TraversalStartKey = TSerializedCellVec(minusInf);
            Self->PersistStartKey(db);
        }

        if (Self->TraversalIsColumnTable) {
            // TODO: serverless case
            if (entry.DomainInfo->Params.HasHive()) {
                Self->HiveId = entry.DomainInfo->Params.GetHive();
            } else {
                Self->HiveId = AppData()->DomainsInfo->GetHive();
            }
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxNavigate::Complete");

        if (Cancelled) {
            return;
        }

        Self->Resolve();
    }
};

void TStatisticsAggregator::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    Execute(new TTxNavigate(this, ev->Get()->Request.Release()), TActivationContext::AsActorContext());
}

} // NKikimr::NStat
