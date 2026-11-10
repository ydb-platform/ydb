#include "hive_impl.h"
#include "hive_schema.h"
#include "monitoring.h"
#include "reassign.h"

namespace NKikimr::NHive {

class TTxReassignTablets : public TTransactionBase<THive>, public TLoggedMonTransaction {
    const TActorId Source;
    const ui64 Cookie;
    const NKikimrHive::TEvReassignTablets Request;
    TReassignTabletsPlan Plan;

public:
    TTxReassignTablets(TEvHive::TEvReassignTablets::TPtr& ev, THive* hive)
        : TBase(hive)
        , TLoggedMonTransaction(ev->Get()->Record.GetUserSID(), hive)
        , Source(ev->Sender)
        , Cookie(ev->Cookie)
        , Request(ev->Get()->Record)
    {}

    TTxType GetTxType() const override { return NHive::TXTYPE_MON_REASSIGN_TABLET; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if ((Request.GetStoragePool().empty() && !Request.GetGroupId())
                || Request.GetMaxInFlight() == 0 || Request.GetPercent() > 100) {
            Plan.Error = "invalid reassign parameters";
            return true;
        }

        TReassignTabletsSettings settings;
        settings.StoragePool = Request.GetStoragePool();
        settings.GroupId = Request.GetGroupId();
        settings.Async = Request.GetAsync();
        settings.TabletPercent = Request.GetPercent();
        Plan = Self->PrepareReassignTablets(settings);
        if (!Plan.Error.empty()) {
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        NJson::TJsonValue operation;
        operation["Reassign"] = Plan.Description;
        operation["TabletsCount"] = Plan.Operations.size();
        operation["MaxInFlight"] = Request.GetMaxInFlight();
        operation["Async"] = Request.GetAsync();
        WriteOperation(db, operation, "Hive API: reassign requested");
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        auto reply = std::make_unique<TEvHive::TEvReassignTabletsReply>();
        reply->Record.SetSelectedTablets(Plan.Operations.size());
        if (Plan.Error.empty()) {
            reply->Record.SetReassignId(Self->StartReassignActor(
                std::move(Plan.Operations), {}, Request.GetMaxInFlight(), Plan.Description, nullptr));
            reply->Record.SetHiveGeneration(Self->HiveGeneration);
            reply->Record.SetStatus(NKikimrProto::OK);
        } else {
            reply->Record.SetStatus(NKikimrProto::ERROR);
            reply->Record.SetError(Plan.Error);
        }
        ctx.Send(Source, reply.release(), 0, Cookie);
    }
};

void THive::Handle(TEvHive::TEvReassignTablets::TPtr& ev) {
    Execute(new TTxReassignTablets(ev, this));
}

} // namespace NKikimr::NHive
