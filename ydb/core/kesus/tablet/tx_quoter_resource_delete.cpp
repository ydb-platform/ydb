#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxQuoterResourceDelete : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    NKikimrKesus::TEvDeleteQuoterResource Record;

    THolder<TEvKesus::TEvDeleteQuoterResourceResult> Reply;

    TTxQuoterResourceDelete(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvDeleteQuoterResource& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_QUOTER_RESOURCE_DELETE; }

    void ReplyOk() {
        NKikimrKesus::TEvDeleteQuoterResourceResult result;
        result.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        Reply = MakeHolder<TEvKesus::TEvDeleteQuoterResourceResult>(result);
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceDelete::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", id=" << Record.GetResourceId() << ", path=\"" << Record.GetResourcePath() << "\")");

        TQuoterResourceTree* resource = Record.GetResourceId() ?
            Self->QuoterResources.FindId(Record.GetResourceId()) :
            Self->QuoterResources.FindPath(Record.GetResourcePath());
        if (!resource) {
            Reply = MakeHolder<TEvKesus::TEvDeleteQuoterResourceResult>(
                Ydb::StatusIds::NOT_FOUND,
                "Resource doesn't exist.");
            return true;
        }

        const ui64 resourceId = resource->GetResourceId();
        const TString resourcePath = resource->GetPath();

        TString errorMessage;
        if (!Self->QuoterResources.DeleteResource(resource, errorMessage)) {
            Reply = MakeHolder<TEvKesus::TEvDeleteQuoterResourceResult>(
                Ydb::StatusIds::BAD_REQUEST,
                errorMessage);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::QuoterResources>().Key(resourceId).Delete();

        Self->TabletCounters->Simple()[COUNTER_QUOTER_RESOURCE_COUNT].Add(-1);
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] Deleted quoter resource "
                << resourceId << " \"" << resourcePath << "\"");

        ReplyOk();
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceDelete::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, std::move(Reply), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDeleteQuoterResource::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    TabletCounters->Cumulative()[COUNTER_REQS_QUOTER_RESOURCE_DELETE].Increment(1);

    if (record.GetResourcePath().empty() && !record.GetResourceId()) {
        Send(ev->Sender,
            new TEvKesus::TEvDeleteQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "You should specify resource path or resource id."),
            0, ev->Cookie);
        return;
    }

    if (!record.GetResourcePath().empty() && !TQuoterResources::IsResourcePathValid(record.GetResourcePath())) {
        Send(ev->Sender,
            new TEvKesus::TEvDeleteQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "Invalid resource path."),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxQuoterResourceDelete(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
