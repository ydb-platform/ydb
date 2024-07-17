#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxStorePermissions : public TTransactionBase<TCms> {
public:
    TTxStorePermissions(TCms *self, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
            const TString &owner, TAutoPtr<TRequestInfo> scheduled, const TMaybe<TString> &maintenanceTaskId)
        : TBase(self)
        , Request(std::move(req))
        , Response(std::move(resp))
        , Owner(owner)
        , Scheduled(scheduled)
        , MaintenanceTaskId(maintenanceTaskId)
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

        if (MaintenanceTaskId) {
            Y_ABORT_UNLESS(Scheduled);

            Self->State->MaintenanceRequests.emplace(Scheduled->RequestId, *MaintenanceTaskId);
            Self->State->MaintenanceTasks.emplace(*MaintenanceTaskId, TTaskInfo{
                .TaskId = *MaintenanceTaskId,
                .RequestId = Scheduled->RequestId,
                .Owner = Scheduled->Owner,
                .HasSingleCompositeActionGroup = !Scheduled->Request.GetPartialPermissionAllowed()
            });

            db.Table<Schema::MaintenanceTasks>().Key(*MaintenanceTaskId).Update(
                NIceDb::TUpdate<Schema::MaintenanceTasks::RequestID>(Scheduled->RequestId),
                NIceDb::TUpdate<Schema::MaintenanceTasks::Owner>(Scheduled->Owner),
                NIceDb::TUpdate<Schema::MaintenanceTasks::HasSingleCompositeActionGroup>(!Scheduled->Request.GetPartialPermissionAllowed())
            );
        }

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

            if (MaintenanceTaskId) {
                Y_ABORT_UNLESS(Self->State->MaintenanceTasks.contains(*MaintenanceTaskId));
                Self->State->MaintenanceTasks.at(*MaintenanceTaskId).Permissions.insert(id);
            }

            Self->AuditLog(ctx, TStringBuilder() << "Store permission"
                << ": id# " << id
                << ", validity# " << TInstant::MicroSeconds(deadline)
                << ", action# " << actionStr);

            if (Scheduled && Scheduled->Request.GetEvictVDisks()) {
                auto ret = Self->SetHostMarker(permission.GetAction().GetHost(), NKikimrCms::MARKER_DISK_FAULTY, txc, ctx);
                std::move(ret.begin(), ret.end(), std::back_inserter(UpdateMarkers));
            }
        }

        if (Scheduled) {
            auto &id = Scheduled->RequestId;
            auto &owner = Scheduled->Owner;

            if (Scheduled->Request.ActionsSize() || Scheduled->Request.GetEvictVDisks()) {
                ui64 order = Scheduled->Order;
                i32 priority = Scheduled->Priority;
                TString requestStr;
                google::protobuf::TextFormat::PrintToString(Scheduled->Request, &requestStr);

                auto row = db.Table<Schema::Request>().Key(id);
                row.Update(NIceDb::TUpdate<Schema::Request::Owner>(owner),
                           NIceDb::TUpdate<Schema::Request::Order>(order),
                           NIceDb::TUpdate<Schema::Request::Priority>(priority),
                           NIceDb::TUpdate<Schema::Request::Content>(requestStr));

                Self->AuditLog(ctx, TStringBuilder() << "Store request"
                    << ": id# " << id
                    << ", owner# " << owner
                    << ", order# " << order
                    << ", priority# " << priority
                    << ", body# " << requestStr);

                if (Scheduled->Request.GetEvictVDisks()) {
                    for (const auto &action : Scheduled->Request.GetActions()) {
                        auto ret = Self->SetHostMarker(action.GetHost(), NKikimrCms::MARKER_DISK_FAULTY, txc, ctx);
                        std::move(ret.begin(), ret.end(), std::back_inserter(UpdateMarkers));
                    }
                }
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
        Self->SentinelUpdateHostMarkers(std::move(UpdateMarkers), ctx);
    }

private:
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
    TString Owner;
    TAutoPtr<TRequestInfo> Scheduled;
    const TMaybe<TString> MaintenanceTaskId;
    ui64 NextPermissionId;
    ui64 NextRequestId;
    TVector<TEvSentinel::TEvUpdateHostMarkers::THostMarkers> UpdateMarkers;
};

ITransaction *TCms::CreateTxStorePermissions(THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
        const TString &owner, TAutoPtr<TRequestInfo> scheduled, const TMaybe<TString> &maintenanceTaskId)
{
    return new TTxStorePermissions(this, std::move(req), std::move(resp), owner, std::move(scheduled), maintenanceTaskId);
}

} // namespace NKikimr::NCms
