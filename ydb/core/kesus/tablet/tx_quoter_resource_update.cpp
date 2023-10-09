#include "tablet_impl.h"

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxQuoterResourceUpdate : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    NKikimrKesus::TEvUpdateQuoterResource Record;

    THolder<TEvKesus::TEvUpdateQuoterResourceResult> Reply;

    TTxQuoterResourceUpdate(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvUpdateQuoterResource& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_QUOTER_RESOURCE_UPDATE; }

    void ReplyOk(ui64 quoterResourceId) {
        NKikimrKesus::TEvUpdateQuoterResourceResult result;
        result.SetResourceId(quoterResourceId);
        result.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        Reply = MakeHolder<TEvKesus::TEvUpdateQuoterResourceResult>(result);
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceUpdate::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", id=" << Record.GetResource().GetResourceId() << ", path=\"" << Record.GetResource().GetResourcePath()
                    << "\", config=" << Record.GetResource().GetHierarchicalDRRResourceConfig() << ")");

        const auto& resourceDesc = Record.GetResource();
        TQuoterResourceTree* resource = resourceDesc.GetResourceId() ?
            Self->QuoterResources.FindId(resourceDesc.GetResourceId()) :
            Self->QuoterResources.FindPath(resourceDesc.GetResourcePath());
        if (!resource) {
            Reply = MakeHolder<TEvKesus::TEvUpdateQuoterResourceResult>(
                    Ydb::StatusIds::NOT_FOUND,
                    "No resource found.");
            return true;
        }
        TString errorMessage;
        if (!resource->Update(resourceDesc, errorMessage)) {
            Reply = MakeHolder<TEvKesus::TEvUpdateQuoterResourceResult>(
                    Ydb::StatusIds::BAD_REQUEST,
                    errorMessage);
            return true;
        }
        Self->QuoterResources.OnUpdateResourceProps(resource);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::QuoterResources>().Key(resource->GetResourceId()).Update(
            NIceDb::TUpdate<Schema::QuoterResources::Props>(resource->GetProps()));

        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] Updated quoter resource "
                << resource->GetResourceId() << " \"" << resource->GetPath() << "\"");

        ReplyOk(resource->GetResourceId());

        if (Self->QuoterTickProcessorQueue.Empty()) { // Ticks are not scheduled, so update all sessions with new props now.
            Self->QuoterResourceSessionsAccumulator.SendAll(ctx, Self->TabletID());
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceUpdate::Complete (sender=" << Sender
                    << ", cookie=" << Cookie << ")");

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, std::move(Reply), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvUpdateQuoterResource::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    TabletCounters->Cumulative()[COUNTER_REQS_QUOTER_RESOURCE_UPDATE].Increment(1);

    const auto& resourceDesc = record.GetResource();
    if (resourceDesc.GetResourcePath().empty() && !resourceDesc.GetResourceId()) {
        Send(ev->Sender,
            new TEvKesus::TEvUpdateQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "You should specify resource path or resource id."),
            0, ev->Cookie);
        return;
    }

    if (!resourceDesc.GetResourcePath().empty() && !TQuoterResources::IsResourcePathValid(resourceDesc.GetResourcePath())) {
        Send(ev->Sender,
            new TEvKesus::TEvUpdateQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "Invalid resource path."),
            0, ev->Cookie);
        return;
    }

    if (!resourceDesc.HasHierarchicalDRRResourceConfig()) {
        Send(ev->Sender,
            new TEvKesus::TEvUpdateQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "No resource config."),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxQuoterResourceUpdate(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
