#include "processor_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::SYSTEM_VIEWS

namespace NKikimr {
namespace NSysView {

struct TSysViewProcessor::TTxConfigure : public TTxBase {
    NKikimrSysView::TEvConfigureProcessor Record;
    TActorId Sender;

    TTxConfigure(TSelf* self, NKikimrSysView::TEvConfigureProcessor&& record, const NActors::TActorId& sender)
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

        Self->Database = Record.GetDatabase();
        Self->PersistDatabase(db);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_DEBUG("TTxConfigure::Complete",
            {"tabletId", Self->TabletID()});

        ctx.Send(Sender, new TEvSubDomain::TEvConfigureStatus(
            NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, Self->TabletID()));
    }
};

void TSysViewProcessor::Handle(TEvSysView::TEvConfigureProcessor::TPtr& ev) {
    auto& record = ev->Get()->Record;
    Execute(new TTxConfigure(this, std::move(record), ev->Sender),
        TActivationContext::AsActorContext());
}

} // NSysView
} // NKikimr
