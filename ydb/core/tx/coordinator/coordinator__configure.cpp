#include "coordinator_impl.h"
#include "coordinator_hooks.h"

namespace NKikimr {
namespace NFlatTxCoordinator {

struct TTxCoordinator::TTxConfigure : public TTransactionBase<TTxCoordinator> {
    TActorId AckTo;
    ui64 Version;
    ui64 Resolution;
    TVector<TTabletId> Mediators;
    NKikimrSubDomains::TProcessingParams Config;
    TAutoPtr<TEvSubDomain::TEvConfigureStatus> Respond;
    bool ConfigurationApplied;

    TTxConfigure(TSelf *coordinator, TActorId ackTo,
                 ui64 version, ui64 resolution, const TVector<TTabletId>& mediators,
                 const NKikimrSubDomains::TProcessingParams& config)
        : TBase(coordinator)
        , AckTo(ackTo)
        , Version(version)
        , Resolution(resolution)
        , Mediators(mediators)
        , Config(config)
        , ConfigurationApplied(false)

    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::DomainConfiguration>().Range().Select();

        if (!rowset.IsReady())
            return false;

        ui64 curVersion = 0;
        bool curMissing = true;
        TVector<TTabletId> curMediators;
        ui64 curResolution = 0;
        bool curHaveConfig = false;

        while (!rowset.EndOfSet()) {
            const ui64 ver = rowset.GetValue<Schema::DomainConfiguration::Version>();
            TVector<TTabletId> mediators = rowset.GetValue<Schema::DomainConfiguration::Mediators>();
            ui64 resolution = rowset.GetValue<Schema::DomainConfiguration::Resolution>();

            if (ver >= curVersion) {
                curVersion = ver;
                curMissing = false;
                curMediators.swap(mediators);
                curResolution = resolution;
                curHaveConfig = !rowset.GetValue<Schema::DomainConfiguration::Config>().empty();
            }

            if (!rowset.Next())
                return false;
        }

        bool persistedConfig = false;
        auto persistConfig = [&]() {
            db.Table<Schema::DomainConfiguration>().Key(Version).Update(
                    NIceDb::TUpdate<Schema::DomainConfiguration::Mediators>(Mediators),
                    NIceDb::TUpdate<Schema::DomainConfiguration::Resolution>(Resolution));

            if (auto* hooks = ICoordinatorHooks::Get(); hooks && !hooks->PersistConfig(Self->TabletID(), Config)) {
                return;
            }

            TString encodedConfig;
            Y_ABORT_UNLESS(Config.SerializeToString(&encodedConfig));
            db.Table<Schema::DomainConfiguration>().Key(Version).Update(
                NIceDb::TUpdate<Schema::DomainConfiguration::Config>(encodedConfig));
            persistedConfig = true;
        };

        auto updateCurrentConfig = [&]() {
            Self->Config.Version = Version;
            Self->Config.Coordinators.clear();
            if (persistedConfig) {
                for (ui64 coordinator : Config.GetCoordinators()) {
                    Self->Config.Coordinators.push_back(coordinator);
                }
                if (Config.HasIdlePlanResolution()) {
                    Self->Config.ReducedResolution = Max(Config.GetIdlePlanResolution(), Self->Config.Resolution);
                }
                Self->Config.HaveProcessingParams = true;
            }
        };

        if (curMissing) {
            // First config version
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID());
            persistConfig();
            ConfigurationApplied = true;
        } else if (curVersion == Version && curMediators == Mediators && curResolution == Resolution) {
            // Same config version without mediator/resolution changes
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::ALREADY, Self->TabletID());
            if (!curHaveConfig) {
                persistConfig();
                updateCurrentConfig();
            }
        } else if (curVersion < Version && curMediators == Mediators && curResolution == Resolution) {
            // New config version without mediator/resolution changes
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID());
            persistConfig();
            updateCurrentConfig();
        } else {
            // Outdated config version, or attempt to change mediators/resolution
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::REJECT, Self->TabletID());
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR,
             "tablet# " << Self->TabletID() <<
             " version# " << Version <<
             " TTxConfigure Complete");
        if (ConfigurationApplied) {
            Self->Execute(Self->CreateTxInit(), ctx);
        } else {
            Self->SetCounter(COUNTER_MISSING_CONFIG, Self->Config.HaveProcessingParams ? 1 : 0);
            if (Self->Config.HaveProcessingParams) {
                Self->SubscribeToSiblings();
            }
        }

        if (AckTo && Respond)
            ctx.Send(AckTo, Respond.Release());
    }
};

ITransaction* TTxCoordinator::CreateTxConfigure(
        TActorId ackTo, ui64 version, ui64 resolution, const TVector<TTabletId> &mediators,
        const NKikimrSubDomains::TProcessingParams &config)
{
    return new TTxCoordinator::TTxConfigure(this, ackTo, version, resolution, mediators, config);
}

}
}
