#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TTxUpgradeSchema : public TTransactionBase<TSchemeShard> {
    bool IsOk = true;

    TTxUpgradeSchema(TSelf* self)
        : TTransactionBase<TSchemeShard>(self)
    {}

    bool UpgradeInitState(NIceDb::TNiceDb& db, const TActorContext& ctx) {
        ui64 initStateVal = (ui64)TTenantInitState::InvalidState;
        if (!Self->ReadSysValue(db, Schema::SysParam_TenantInitState, initStateVal, (ui64)TTenantInitState::InvalidState)) {
            return false;
        }

        auto state = TTenantInitState::EInitState(initStateVal);
        if (state != TTenantInitState::InvalidState) {
            // tenant SS with migrated path manage state from the start
            return true;
        }

        // tenant SS without migrated path or global SS relay on RootId record at Path table
        {
            // probe root path
            auto rootRow = db.Table<Schema::Paths>().Key(NSchemeShard::RootPathId).Select();
            if (!rootRow.IsReady()) {
                return false;
            }

            if (rootRow.IsValid()) {
                // has root row
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "UpgradeInitState as Done, schemeshardId: " << Self->TabletID());
                Self->InitState = TTenantInitState::Done;
                Self->PersistInitState(db);
            } else {
                // no root row
                LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                           "UpgradeInitState as Uninitialized, schemeshardId: " << Self->TabletID());
                Self->InitState = TTenantInitState::Uninitialized;
                Self->PersistInitState(db);
            }
            // and no matter what is the value of root row
        }

        return true;
    }

    bool ReplaceExtraPathSymbolsAllowed(NIceDb::TNiceDb& db, const TActorContext &) {
        auto srcRow = db.Table<Schema::UserAttributes>().Key(Self->RootPathId().LocalPathId, TString(ATTR_EXTRA_PATH_SYMBOLS_ALLOWED)).Select();
        if (!srcRow.IsReady()) {
            return false;
        }

        TString srcVal;
        if (srcRow.IsValid()) {
            srcVal = srcRow.GetValueOrDefault<Schema::UserAttributes::AttrValue>();
        }

        if (!srcVal) {
            // nothing to do
            return true;
        }

        auto dstRow = db.Table<Schema::SubDomains>().Key(Self->RootPathId().LocalPathId).Select();
        if (!dstRow.IsReady()) {
            return false;
        }

        TString dstVal;
        if (dstRow.IsValid()) {
            dstVal = dstRow.GetValueOrDefault<Schema::SubDomains::ExtraPathSymbolsAllowed>();
        }

        // we can delete src value after stable 19-6, 19-6 must contain this data for compatibility
        // db.Table<Schema::UserAttributes>().Key(Self->RootPathId().LocalPathId, ATTR_EXTRA_PATH_SYMBOLS_ALLOWED).Delete();

        if (!dstVal) {
            db.Table<Schema::SubDomains>().Key(Self->RootPathId().LocalPathId).Update(
                NIceDb::TUpdate<Schema::SubDomains::ExtraPathSymbolsAllowed>(srcVal));
        }

        return true;
    }

    TTxType GetTxType() const override {
        return TXTYPE_UPGRADE_SCHEME;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxUpgradeSchema.Execute");

        NIceDb::TNiceDb db(txc.DB);

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbitwise-instead-of-logical"
        bool precharged = db.Table<Schema::Paths>().Precharge()
                & db.Table<Schema::SubDomains>().Precharge()
                & db.Table<Schema::UserAttributes>().Precharge();

        if (!precharged) {
            return false;
        }

        return UpgradeInitState(db, ctx) & ReplaceExtraPathSymbolsAllowed(db, ctx);
#pragma clang diagnostic pop
    }

    void Complete(const TActorContext &ctx) override {
        if (!IsOk) {
            LOG_CRIT_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "send TEvPoisonPill to self " << Self->TabletID());
            ctx.Send(Self->SelfId(), new TEvents::TEvPoisonPill());
            return;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TTxUpgradeSchema.Complete");
        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

NTabletFlatExecutor::ITransaction* TSchemeShard::CreateTxUpgradeSchema() {
    return new TTxUpgradeSchema(this);
}

} // NSchemeShard
} // NKikimr
