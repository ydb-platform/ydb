#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxRemovePermissions : public TTransactionBase<TCms> {
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

            auto requestId = Self->State->Permissions.find(id)->second.RequestId;
            Self->State->Permissions.erase(id);
            db.Table<Schema::Permission>().Key(id).Delete();

            if (Expired && Self->State->ScheduledRequests.contains(requestId)) {
                Self->State->ScheduledRequests.erase(requestId);
                db.Table<Schema::Request>().Key(requestId).Delete();

                Self->AuditLog(ctx, TStringBuilder() << "Remove request"
                    << ": id# " << requestId
                    << ", reason# " << "permission " << id << " has expired");
            }

            if (Self->State->WalleRequests.contains(requestId)) {
                auto taskId = Self->State->WalleRequests.find(requestId)->second;
                Self->State->WalleTasks.find(taskId)->second.Permissions.erase(id);
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
            Y_VERIFY(Request);
            Self->Reply(Request.Get(), Response, ctx);
        }

        Self->RemoveEmptyTasks(ctx);
    }

private:
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
    TVector<TString> Ids;
    bool Expired;
};

ITransaction *TCms::CreateTxRemovePermissions(TVector<TString> ids, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
        bool expired)
{
    return new TTxRemovePermissions(this, std::move(ids), std::move(req), std::move(resp), expired);
}

} // namespace NKikimr::NCms
