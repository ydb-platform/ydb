#include "aggregator_impl.h"

#include <ydb/core/tx/tx.h>

namespace NKikimr::NStat {

struct TStatisticsAggregator::TTxConfigure : public TTxBase {
    NKikimrStat::TEvConfigureAggregator Record;
    TActorId Sender;

    TTxConfigure(TSelf* self, NKikimrStat::TEvConfigureAggregator&& record, const NActors::TActorId& sender)
        : TTxBase(self)
        , Record(std::move(record))
        , Sender(sender)
    {}

    TTxType GetTxType() const override { return TXTYPE_CONFIGURE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxConfigure::Execute: "
            << "database# " << Record.GetDatabase());

        NIceDb::TNiceDb db(txc.DB);

        bool needInitialize = !Self->Database;
        Self->Database = Record.GetDatabase();
        Self->PersistSysParam(db, Schema::SysParam_Database, Self->Database);

        if (needInitialize) {
            Self->InitializeStatisticsTable();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SA_LOG_D("[" << Self->TabletID() << "] TTxConfigure::Complete");

        ctx.Send(Sender, new TEvSubDomain::TEvConfigureStatus(
            NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID()));
    }
};

void TStatisticsAggregator::Handle(TEvStatistics::TEvConfigureAggregator::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxConfigure(this, std::move(record), ev->Sender),
        TActivationContext::AsActorContext());
}

} // NKikimr::NStat
