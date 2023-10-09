#include "tablet_impl.h"

#include <library/cpp/protobuf/util/is_equal.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxQuoterResourceAdd : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    NKikimrKesus::TEvAddQuoterResource Record;

    THolder<TEvKesus::TEvAddQuoterResourceResult> Reply;

    TTxQuoterResourceAdd(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvAddQuoterResource& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_QUOTER_RESOURCE_ADD; }

    void ReplyOk(ui64 quoterResourceId) {
        NKikimrKesus::TEvAddQuoterResourceResult result;
        result.SetResourceId(quoterResourceId);
        result.MutableError()->SetStatus(Ydb::StatusIds::SUCCESS);
        Reply = MakeHolder<TEvKesus::TEvAddQuoterResourceResult>(result);
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceAdd::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", path=\"" << Record.GetResource().GetResourcePath()
                    << "\", config=" << Record.GetResource().GetHierarchicalDRRResourceConfig() << ")");

        const auto& resourceDesc = Record.GetResource();
        if (const TQuoterResourceTree* resource = Self->QuoterResources.FindPath(resourceDesc.GetResourcePath())) {
            if (NProtoBuf::IsEqual(resource->GetProps().GetHierarchicalDRRResourceConfig(), resourceDesc.GetHierarchicalDRRResourceConfig())) {
                THolder<TEvKesus::TEvAddQuoterResourceResult> reply =
                    MakeHolder<TEvKesus::TEvAddQuoterResourceResult>(
                        Ydb::StatusIds::ALREADY_EXISTS,
                        "Resource already exists and has same settings.");
                reply->Record.SetResourceId(resource->GetResourceId());
                Reply = std::move(reply);
            } else {
                Reply = MakeHolder<TEvKesus::TEvAddQuoterResourceResult>(
                    Ydb::StatusIds::BAD_REQUEST,
                    "Resource already exists and has different settings.");
            }
            return true;
        }

        Y_ABORT_UNLESS(Self->NextQuoterResourceId > 0);

        TString errorMessage;
        TQuoterResourceTree* resource = Self->QuoterResources.AddResource(Self->NextQuoterResourceId, Record.GetResource(), errorMessage);
        if (!resource) {
            Reply = MakeHolder<TEvKesus::TEvAddQuoterResourceResult>(
                    Ydb::StatusIds::BAD_REQUEST,
                    errorMessage);
            return true;
        }
        ++Self->NextQuoterResourceId;

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSysParam(db, Schema::SysParam_NextQuoterResourceId, ToString(Self->NextQuoterResourceId));
        db.Table<Schema::QuoterResources>().Key(resource->GetResourceId()).Update(
            NIceDb::TUpdate<Schema::QuoterResources::ParentId>(resource->GetParentId()),
            NIceDb::TUpdate<Schema::QuoterResources::Props>(resource->GetProps()));

        Self->TabletCounters->Simple()[COUNTER_QUOTER_RESOURCE_COUNT].Add(1);
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] Created new quoter resource "
                << resource->GetResourceId() << " \"" << resource->GetProps().GetResourcePath() << "\"");

        ReplyOk(resource->GetResourceId());
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxQuoterResourceAdd::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ")");

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, std::move(Reply), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvAddQuoterResource::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    TabletCounters->Cumulative()[COUNTER_REQS_QUOTER_RESOURCE_ADD].Increment(1);

    const auto& resourceDesc = record.GetResource();
    if (!TQuoterResources::IsResourcePathValid(resourceDesc.GetResourcePath())) {
        Send(ev->Sender,
            new TEvKesus::TEvAddQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "Invalid resource path."),
            0, ev->Cookie);
        return;
    }

    if (!resourceDesc.HasHierarchicalDRRResourceConfig()) {
        Send(ev->Sender,
            new TEvKesus::TEvAddQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "Not supported resource type. Today's only supported resource type is hierarchical DRR resource."),
            0, ev->Cookie);
        return;
    }

    if (resourceDesc.GetResourceId()) {
        Send(ev->Sender,
            new TEvKesus::TEvAddQuoterResourceResult(
                Ydb::StatusIds::BAD_REQUEST,
                "ResourceId specified."),
            0, ev->Cookie);
        return;
    }

    Execute(new TTxQuoterResourceAdd(this, ev->Sender, ev->Cookie, record), TActivationContext::AsActorContext());
}

}
}
