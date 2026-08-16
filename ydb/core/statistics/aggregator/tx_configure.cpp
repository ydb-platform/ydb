#include "aggregator_impl.h"

#include <ydb/core/tx/tx.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::STATISTICS

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
        YDB_LOG_DEBUG("TTxConfigure::Execute",
            {"tabletId", Self->TabletID()},
            {"database", Record.GetDatabase()});

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
        YDB_LOG_DEBUG("TTxConfigure::Complete",
            {"tabletId", Self->TabletID()});

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
