#include "mediator_impl.h"

namespace NKikimr {
namespace NTxMediator {

using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

struct TTxMediator::TTxConfigure : public TTransactionBase<TTxMediator> {
    TActorId AckTo;
    ui64 Version;
    TVector<TCoordinatorId> Coordinators;
    ui32 TimeCastBuketsPerMediator;
    TAutoPtr<TEvSubDomain::TEvConfigureStatus> Respond;
    bool ConfigurationApplied;

    TTxConfigure(TSelf *mediator, TActorId ackTo, ui64 version, const TVector<TCoordinatorId>& coordinators, ui32 timeCastBuckets)
        : TBase(mediator)
        , AckTo(ackTo)
        , Version(version)
        , Coordinators(coordinators)
        , TimeCastBuketsPerMediator(timeCastBuckets)
        , ConfigurationApplied(false)
    {
        Y_ABORT_UNLESS(TimeCastBuketsPerMediator);
    }

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    bool Execute(TTransactionContext &txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);

        auto rowset = db.Table<Schema::DomainConfiguration>().Range().Select();

        if (!rowset.IsReady())
            return false;

        ui32 curVersion = 0;
        TVector<TCoordinatorId> curCoordinators;
        ui32 curBuckets = 0;

        while (!rowset.EndOfSet()) {
            const ui64 ver = rowset.GetValue<Schema::DomainConfiguration::Version>();
            TVector<TCoordinatorId> coordinators = rowset.GetValue<Schema::DomainConfiguration::Coordinators>();
            ui32 buckets = rowset.GetValue<Schema::DomainConfiguration::TimeCastBuckets>();

            if (ver >= curVersion) {
                curVersion = ver;
                curCoordinators.swap(coordinators);
                curBuckets = buckets;
            }

            if (!rowset.Next())
                return false;
        }

        if (curVersion == 0) {
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID());
            db.Table<Schema::DomainConfiguration>().Key(Version).Update(
                        NIceDb::TUpdate<Schema::DomainConfiguration::Coordinators>(Coordinators),
                        NIceDb::TUpdate<Schema::DomainConfiguration::TimeCastBuckets>(TimeCastBuketsPerMediator));
            ConfigurationApplied = true;
        } else if (curVersion == Version && curCoordinators == Coordinators && curBuckets == TimeCastBuketsPerMediator) {
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::ALREADY, Self->TabletID());
        } else {
            Respond = new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::REJECT, Self->TabletID());
        }


        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR,
             "tablet# " << Self->TabletID() <<
             " version# " << Version <<
             " TTxConfigure Complete");
        if (ConfigurationApplied)
            Self->Execute(Self->CreateTxInit(), ctx);

        if (AckTo)
            ctx.Send(AckTo, Respond.Release());
    }
};

ITransaction* TTxMediator::CreateTxConfigure(TActorId ackTo, ui64 version, const TVector<TCoordinatorId> &coordinators, ui32 timeCastBuckets) {
    return new TTxMediator::TTxConfigure(this, ackTo, version, coordinators, timeCastBuckets);
}

}
}
