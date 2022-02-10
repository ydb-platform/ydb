#include "mediator_impl.h"

namespace NKikimr {
namespace NTxMediator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxMediator::TTxUpgrade : public TTransactionBase<TTxMediator> {
    bool UpgradeFail;

    TTxUpgrade(TSelf *mediator)
        : TBase(mediator)
        , UpgradeFail(false)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext& ctx) override {
        NIceDb::TNiceDb db(txc.DB);


        auto row = db.Table<Schema::State>().Key(Schema::State::DatabaseVersion).Select<Schema::State::StateValue>();
        if (!row.IsReady()) {
            return false;
        }

        if (!row.IsValid()) {
            db.Table<Schema::State>().Key(Schema::State::DatabaseVersion).Update(NIceDb::TUpdate<Schema::State::StateValue>(Schema::CurrentVersion));
            return true;
        }

        Schema::State::StateValue::Type databaseVersion = row.GetValue<Schema::State::StateValue>();
        if (Schema::CurrentVersion == databaseVersion) {
            return true;
        }

        UpgradeFail = true;
        LOG_CRIT_S(ctx, NKikimrServices::TX_MEDIATOR,
             "tablet# " << Self->Tablet() <<
             " SEND to self TEvents::TEvPoisonPill" <<
             " databaseVersion# " <<  databaseVersion <<
             " CurrentDataBaseVersion# " << Schema::CurrentVersion <<
             " reason# no realisation for upgrade scheme present");
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        if (UpgradeFail) {
            Self->Become(&TSelf::StateBroken);
            ctx.Send(Self->Tablet(), new TEvents::TEvPoisonPill);
            return;
        }

        Self->Execute(Self->CreateTxInit(), ctx);
    }
};

ITransaction* TTxMediator::CreateTxUpgrade() {
    return new TTxUpgrade(this);
}

}
}
