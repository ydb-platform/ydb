#include "mediator_impl.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NTxMediator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxMediator::TTxInit : public TTransactionBase<TTxMediator> {
    ui64 Version;
    TVector<TCoordinatorId> Coordinators;
    ui32 TimeCastBuketsPerMediator;

    TTxInit(TSelf *mediator)
        : TBase(mediator)
        , Version(0)
        , TimeCastBuketsPerMediator(0)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::DomainConfiguration>().Range().Select();

        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui64 ver = rowset.GetValue<Schema::DomainConfiguration::Version>();
            TVector<TCoordinatorId> coordinators = rowset.GetValue<Schema::DomainConfiguration::Coordinators>();
            ui32 buckets = rowset.GetValue<Schema::DomainConfiguration::TimeCastBuckets>();

            if (ver >= Version) {
                Version = ver;
                Coordinators.swap(coordinators);
                TimeCastBuketsPerMediator = buckets;
            }

            if (!rowset.Next())
                return false;
        }

        return true;
    }

    bool IsTabletInStaticDomain(const TAppData *appdata) {
        for (auto domainMediatorId: appdata->DomainsInfo->GetDomain()->Mediators) {
            if (Self->TabletID() == domainMediatorId) {
                return true;
            }
        }

        return false;
    }

    void Complete(const TActorContext &ctx) override {
        if (Coordinators.size()) {
            LOG_INFO_S(ctx, NKikimrServices::TX_MEDIATOR
                       , "tablet# " << Self->TabletID()
                       << " CreateTxInit Complete");
            Self->Config.CoordinatorsVersion = Version;
            Self->Config.CoordinatorSeletor = new TCoordinators(std::move(Coordinators));
            Self->Config.Bukets = new TTimeCastBuckets(TimeCastBuketsPerMediator);
            Self->InitSelfState(ctx);
            Self->Become(&TThis::StateWork);
            Self->SignalTabletActive(ctx);
            return;
        }

        TAppData* appData = AppData(ctx);
        if (IsTabletInStaticDomain(appData)) {
            LOG_INFO_S(ctx, NKikimrServices::TX_MEDIATOR,
                 "tablet# " << Self->TabletID() <<
                 " CreateTxInit initialize himself");
            Self->DoConfigure(*CreateDomainConfigurationFromStatic(appData), ctx);
            return;
        }

        Self->Become(&TThis::StateSync);
        Self->SignalTabletActive(ctx);

        LOG_INFO_S(ctx, NKikimrServices::TX_MEDIATOR,
             "tablet# " << Self->TabletID() <<
             " CreateTxInit wait TEvMediatorConfiguration for switching to StateWork from external");
    }
};

ITransaction* TTxMediator::CreateTxInit() {
    return new TTxMediator::TTxInit(this);
}

}
}
