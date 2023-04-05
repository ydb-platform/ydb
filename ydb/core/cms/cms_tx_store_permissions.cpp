#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxStorePermissions : public TTransactionBase<TCms> {
public:
    TTxStorePermissions(TCms *self, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
            const TString &owner, TAutoPtr<TRequestInfo> scheduled)
        : TBase(self)
        , Request(std::move(req))
        , Response(std::move(resp))
        , Owner(owner)
        , Scheduled(scheduled)
        , NextPermissionId(self->State->NextPermissionId)
        , NextRequestId(self->State->NextRequestId)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_STORE_PERMISSIONS ; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStorePermissions Execute");

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Param>().Key(1).Update(NIceDb::TUpdate<Schema::Param::NextPermissionID>(NextPermissionId),
                                                NIceDb::TUpdate<Schema::Param::NextRequestID>(NextRequestId));

        const auto &rec = Response->Get<TEvCms::TEvPermissionResponse>()->Record;
        for (const auto &permission : rec.GetPermissions()) {
            const auto &id = permission.GetId();
            const auto &requestId = Scheduled ? Scheduled->RequestId : "";
            ui64 deadline = permission.GetDeadline();
            TString actionStr;
            google::protobuf::TextFormat::PrintToString(permission.GetAction(), &actionStr);

            auto row = db.Table<Schema::Permission>().Key(id);
            row.Update(NIceDb::TUpdate<Schema::Permission::Owner>(Owner),
                       NIceDb::TUpdate<Schema::Permission::Action>(actionStr),
                       NIceDb::TUpdate<Schema::Permission::Deadline>(deadline),
                       NIceDb::TUpdate<Schema::Permission::RequestID>(requestId));

            Self->AuditLog(ctx, TStringBuilder() << "Store permission"
                << ": id# " << id
                << ", validity# " << TInstant::MicroSeconds(deadline)
                << ", action# " << actionStr);
        }

        if (Scheduled) {
            auto &id = Scheduled->RequestId;
            auto &owner = Scheduled->Owner;

            if (Scheduled->Request.ActionsSize()) {
                ui64 order = Scheduled->Order;
                TString requestStr;
                google::protobuf::TextFormat::PrintToString(Scheduled->Request, &requestStr);

                auto row = db.Table<Schema::Request>().Key(id);
                row.Update(NIceDb::TUpdate<Schema::Request::Owner>(owner),
                           NIceDb::TUpdate<Schema::Request::Order>(order),
                           NIceDb::TUpdate<Schema::Request::Content>(requestStr));

                Self->AuditLog(ctx, TStringBuilder() << "Store request"
                    << ": id# " << id
                    << ", owner# " << owner
                    << ", order# " << order
                    << ", body# " << requestStr);
            } else {
                db.Table<Schema::Request>().Key(id).Delete();

                Self->AuditLog(ctx, TStringBuilder() << "Remove request"
                    << ": id# " << id
                    << ", owner# " << owner);
            }
        }

        Self->PersistNodeTenants(txc, ctx);
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStorePermissions complete");

        Self->Reply(Request.Get(), Response, ctx);
        Self->SchedulePermissionsCleanup(ctx);
    }

private:
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
    TString Owner;
    TAutoPtr<TRequestInfo> Scheduled;
    ui64 NextPermissionId;
    ui64 NextRequestId;
};

ITransaction *TCms::CreateTxStorePermissions(THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
        const TString &owner, TAutoPtr<TRequestInfo> scheduled)
{
    return new TTxStorePermissions(this, std::move(req), std::move(resp), owner, std::move(scheduled));
}

} // namespace NKikimr::NCms
