#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxUnmarkRestoreTables : public TTransactionBase<TSchemeShard> {
    static const ui32 BucketSize = 100;
    TVector<TPathId> RestoreTablesToUnmark;
    ui32 UnmarkedCount;

    TTxUnmarkRestoreTables(TSelf* self, TVector<TPathId> tablesToClean)
        : TTransactionBase<TSchemeShard>(self)
        , RestoreTablesToUnmark(std::move(tablesToClean))
        , UnmarkedCount(0)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_UNMARK_RESTORE_TABLES;
    }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        UnmarkedCount = 0;
        while (UnmarkedCount < BucketSize && RestoreTablesToUnmark) {
            TPathId tableId = RestoreTablesToUnmark.back();
            if (Self->Tables.contains(tableId)) {
                auto table = Self->Tables[tableId];
                table->IsRestore = false;
                Self->PersistTableIsRestore(db, tableId, table);        
            }

            ++UnmarkedCount;
            RestoreTablesToUnmark.pop_back();
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (UnmarkedCount) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, 
                "TTxUnmarkRestoreTables Complete"
                << ", done for " << UnmarkedCount << " tables"
                << ", left " << RestoreTablesToUnmark.size()
                << ", at schemeshard: "<< Self->TabletID()
            );
        }

        if (RestoreTablesToUnmark) {
            Self->Execute(Self->CreateTxUnmarkRestoreTables(std::move(RestoreTablesToUnmark)), ctx);
        }
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUnmarkRestoreTables(TVector<TPathId>&& tablesToUnmark) {
    return new TTxUnmarkRestoreTables(this, std::move(tablesToUnmark));
}

} // NKikimr::NSchemeShard

