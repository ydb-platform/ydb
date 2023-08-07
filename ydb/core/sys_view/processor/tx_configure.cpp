#include "processor_impl.h"

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
        SVLOG_D("[" << Self->TabletID() << "] TTxConfigure::Execute: "
            << "database# " << Record.GetDatabase());

        NIceDb::TNiceDb db(txc.DB);

        Self->Database = Record.GetDatabase();
        Self->PersistDatabase(db);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        SVLOG_D("[" << Self->TabletID() << "] TTxConfigure::Complete");

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
