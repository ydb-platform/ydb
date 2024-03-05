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
    ui64 ReducedResolution;
    bool HaveProcessingParams = false;
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
                ReducedResolution = Self->Config.ReducedResolution;
                HaveProcessingParams = false;
                auto encodedConfig = rowset.GetValue<Schema::DomainConfiguration::Config>();
                if (!encodedConfig.empty()) {
                    TProtoBox<NKikimrSubDomains::TProcessingParams> config(encodedConfig);
                    for (ui64 coordinator : config.GetCoordinators()) {
                        Coordinators.push_back(coordinator);
                    }
                    if (config.HasIdlePlanResolution()) {
                        ReducedResolution = Max(config.GetIdlePlanResolution(), PlanResolution);
                    }
                    HaveProcessingParams = true;
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

    void Complete(const TActorContext &ctx) override {
        if (!LastBlockedActor) {
            // Assume worst case, everything up to LastBlockedStep was planned
            LastPlanned = Max(LastPlanned, LastBlockedStep);
        }

        // Assume worst case, last planned step was also acquired
        LastAcquired = Max(LastAcquired, LastPlanned);

        Self->VolatileState.LastPlanned = LastPlanned;
        Self->VolatileState.LastSentStep = LastPlanned;
        Self->VolatileState.LastAcquired = LastAcquired;

        if (Mediators.size()) {
            LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
                 "tablet# " << Self->TabletID() <<
                 " CreateTxInit Complete");
            Self->Config.Version = Version;
            Self->Config.Mediators = new TMediators(std::move(Mediators));
            Self->Config.Coordinators = Coordinators;
            Self->Config.Resolution = PlanResolution;
            Self->Config.ReducedResolution = ReducedResolution;
            Self->Config.HaveProcessingParams = HaveProcessingParams;
            Self->SetCounter(COUNTER_MISSING_CONFIG, HaveProcessingParams ? 1 : 0);

            if (LastBlockedActor && LastPlanned < LastBlockedStep) {
                Self->RestoreState(LastBlockedActor, LastBlockedStep);
                return;
            }

            if (LastBlockedActor) {
                // The previous state actor is no longer needed
                ctx.Send(LastBlockedActor, new TEvents::TEvPoison);
            }

            Self->Execute(Self->CreateTxRestoreTransactions(), ctx);
            return;
        }

        TAppData* appData = AppData(ctx);
        if (Self->IsTabletInStaticDomain(appData)) {
            LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
                 "tablet# " << Self->TabletID() <<
                 " CreateTxInit initialize himself");
            Self->DoConfiguration(*CreateDomainConfigurationFromStatic(appData), ctx);
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
