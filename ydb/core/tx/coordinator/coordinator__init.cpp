#include "coordinator_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/util/pb.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxInit : public TTransactionBase<TTxCoordinator> {
    ui64 Version = 0;
    TVector<TTabletId> Mediators;
    TVector<TTabletId> Coordinators;
    ui64 PlanResolution;
    ui64 LastPlanned = 0;
    ui64 LastAcquired = 0;
    TActorId LastBlockedActor;
    ui64 LastBlockedStep = 0;

    TTxInit(TSelf *coordinator)
        : TBase(coordinator)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        bool ready = true;
        ready &= LoadDomainConfiguration(db);
        ready &= LoadLastPlanned(db);
        ready &= LoadLastAcquired(db);
        ready &= LoadLastBlocked(db);

        return ready;
    }

    bool LoadDomainConfiguration(NIceDb::TNiceDb &db) {
        auto rowset = db.Table<Schema::DomainConfiguration>().Range().Select();

        if (!rowset.IsReady())
            return false;

        while (!rowset.EndOfSet()) {
            const ui64 ver = rowset.GetValue<Schema::DomainConfiguration::Version>();
            TVector<TTabletId> mediators = rowset.GetValue<Schema::DomainConfiguration::Mediators>();
            ui64 resolution = rowset.GetValue<Schema::DomainConfiguration::Resolution>();

            if (ver >= Version) {
                Version = ver;
                Mediators.swap(mediators);
                Coordinators.clear();
                PlanResolution = resolution;
                if (rowset.HaveValue<Schema::DomainConfiguration::Config>()) {
                    TProtoBox<NKikimrSubDomains::TProcessingParams> config(rowset.GetValue<Schema::DomainConfiguration::Config>());
                    for (ui64 coordinator : config.GetCoordinators()) {
                        Coordinators.push_back(coordinator);
                    }
                }
            }

            if (!rowset.Next())
                return false;
        }

        return true;
    }

    bool LoadLastPlanned(NIceDb::TNiceDb &db) {
        return Schema::LoadState(db, Schema::State::KeyLastPlanned, LastPlanned);
    }

    bool LoadLastAcquired(NIceDb::TNiceDb &db) {
        return Schema::LoadState(db, Schema::State::AcquireReadStepLast, LastAcquired);
    }

    bool LoadLastBlocked(NIceDb::TNiceDb &db) {
        ui64 x1 = 0;
        ui64 x2 = 0;
        ui64 step = 0;

        bool ready = true;
        ready &= Schema::LoadState(db, Schema::State::LastBlockedActorX1, x1);
        ready &= Schema::LoadState(db, Schema::State::LastBlockedActorX2, x2);
        ready &= Schema::LoadState(db, Schema::State::LastBlockedStep, step);

        if (!ready) {
            return false;
        }

        LastBlockedActor = TActorId(x1, x2);
        LastBlockedStep = step;
        return true;
    }

    bool IsTabletInStaticDomain(const TAppData *appdata) {
        const ui32 selfDomain = appdata->DomainsInfo->GetDomainUidByTabletId(Self->TabletID());
        Y_VERIFY(selfDomain != appdata->DomainsInfo->BadDomainId);
        const auto& domain = appdata->DomainsInfo->GetDomain(selfDomain);

        for (auto domainCoordinatorId: domain.Coordinators) {
            if (Self->TabletID() == domainCoordinatorId) {
                return true;
            }
        }

        return false;
    }

    void Complete(const TActorContext &ctx) override {
        // Assume worst case, everything up to LastBlockedStep was planned
        LastPlanned = Max(LastPlanned, LastBlockedStep);

        // Assume worst case, last planned step was also acquired
        LastAcquired = Max(LastAcquired, LastPlanned);

        Self->VolatileState.LastPlanned = LastPlanned;
        Self->VolatileState.LastSentStep = LastPlanned;
        Self->VolatileState.LastAcquired = LastAcquired;

        if (Mediators.size()) {
            LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
                 "tablet# " << Self->TabletID() <<
                 " CreateTxInit Complete");
            Self->Config.MediatorsVersion = Version;
            Self->Config.Mediators = new TMediators(std::move(Mediators));
            Self->Config.Coordinators = Coordinators;
            Self->Config.Resolution = PlanResolution;
            Self->Execute(Self->CreateTxRestoreTransactions(), ctx);
            return;
        }

        TAppData* appData = AppData(ctx);
        if (IsTabletInStaticDomain(appData)) {
            LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
                 "tablet# " << Self->TabletID() <<
                 " CreateTxInit initialize himself");
            Self->DoConfiguration(*CreateDomainConfigurationFromStatic(appData, Self->TabletID()), ctx);
            return;
        }

        Self->Become(&TThis::StateSync);
        Self->SignalTabletActive(ctx);

        LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
             "tablet# " << Self->TabletID() <<
             " CreateTxInit wait TEvCoordinatorConfiguration for switching to StateWork from external");
    }
};

ITransaction* TTxCoordinator::CreateTxInit() {
    return new TTxCoordinator::TTxInit(this);
}

}
}
