#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxRemovePermissions : public TTransactionBase<TCms> {
    void RemoveRequest(NIceDb::TNiceDb &db, const TString &reqId, const TActorContext &ctx, const TString &reason) {
        Self->State->ScheduledRequests.erase(reqId);
        db.Table<Schema::Request>().Key(reqId).Delete();
        Self->AuditLog(ctx, reason);
    }

public:
    TTxRemovePermissions(TCms *self, TVector<TString> &&ids, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp, bool expired)
        : TBase(self)
        , Request(std::move(req))
        , Response(resp)
        , Ids(ids)
        , Expired(expired)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_PERMISSIONS; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemovePermissions Execute");

        NIceDb::TNiceDb db(txc.DB);
        for (auto id : Ids) {
            if (!Self->State->Permissions.contains(id))
                continue;

            const auto &permission = Self->State->Permissions.find(id)->second;
            const TString requestId = permission.RequestId;

            auto it = Self->State->ScheduledRequests.find(requestId);
            if (it != Self->State->ScheduledRequests.end()) {
                if (Expired) {
                    RemoveRequest(db, requestId, ctx, TStringBuilder() << "Remove request"
                        << ": id# " << requestId
                        << ", reason# " << "permission " << id << " has expired");
                }

                auto& request = it->second.Request;
                
                if (request.GetEvictVDisks()) {
                    const TString host = permission.Action.GetHost();
                    auto ret = Self->ResetHostMarkers(host, txc, ctx);
                    std::move(ret.begin(), ret.end(), std::back_inserter(HostUpdateMarkers));

                    RemoveRequest(db, requestId, ctx, TStringBuilder() << "Remove request"
                        << ": id# " << requestId
                        << ", reason# " << "permission " << id << " was removed");
                }

                if (request.GetDecomissionPDisk()) {
                    for (auto& device : permission.Action.GetDevices()) {
                        TPDiskID pdiskId = TPDiskInfo::NameToId(device);

                        auto ret = Self->ResetPDiskMarkers(pdiskId, txc, ctx);
                        std::move(ret.begin(), ret.end(), std::back_inserter(PDiskUpdateMarkers));

                        RemoveRequest(db, requestId, ctx, TStringBuilder() << "Remove request"
                            << ": id# " << requestId
                            << ", reason# " << "permission " << id << " was removed");
                    }
                }
            }
            
            Self->State->Permissions.erase(id);
            db.Table<Schema::Permission>().Key(id).Delete();

            if (Self->State->WalleRequests.contains(requestId)) {
                auto taskId = Self->State->WalleRequests.find(requestId)->second;
                Self->State->WalleTasks.find(taskId)->second.Permissions.erase(id);
            }

            if (Self->State->MaintenanceRequests.contains(requestId)) {
                auto taskId = Self->State->MaintenanceRequests.find(requestId)->second;
                Self->State->MaintenanceTasks.find(taskId)->second.Permissions.erase(id);
            }

            Self->AuditLog(ctx, TStringBuilder() << "Remove permission"
                << ": id# " << id
                << ", reason# " << (Request ? "explicit remove" : "scheduled cleanup"));
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRemovePermissions Complete");

        if (Response) {
            Y_ABORT_UNLESS(Request);
            Self->Reply(Request.Get(), Response, ctx);
        }

        Self->RemoveEmptyTasks(ctx);
        Self->SentinelUpdateMarkers(std::move(HostUpdateMarkers), std::move(PDiskUpdateMarkers), ctx);
    }

private:
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
    TVector<TString> Ids;
    bool Expired;
    TVector<TEvSentinel::TEvUpdateMarkers::THostMarkers> HostUpdateMarkers;
    TVector<TEvSentinel::TEvUpdateMarkers::TPDiskMarkers> PDiskUpdateMarkers;
};

ITransaction *TCms::CreateTxRemovePermissions(TVector<TString> ids, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
        bool expired)
{
    return new TTxRemovePermissions(this, std::move(ids), std::move(req), std::move(resp), expired);
}

} // namespace NKikimr::NCms
