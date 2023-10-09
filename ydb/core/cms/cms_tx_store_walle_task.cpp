#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxStoreWalleTask : public TTransactionBase<TCms> {
public:
    TTxStoreWalleTask(TCms *self, const TTaskInfo &task, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp)
        : TBase(self)
        , Task(task)
        , Request(std::move(req))
        , Response(std::move(resp))
    {
        Y_ABORT_UNLESS(Request);
        Y_ABORT_UNLESS(Response);
    }

    TTxType GetTxType() const override { return TXTYPE_STORE_WALLE_TASK; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStoreWalleTask Execute");

        for (auto &perm : Task.Permissions) {
            if (Self->State->Permissions.find(perm) == Self->State->Permissions.end()) {
                Response.Reset(new IEventHandle(Response->Recipient, Response->Sender,
                        new TEvCms::TEvStoreWalleTaskFailed(Task.TaskId, TStringBuilder()
                            << "There are no stored permissions for this task. "
                            << "Maybe cleanup ran before task been stored. "
                            << "Try request again"),
                    0, Response->Cookie)
                );
                return true;
            }
        }

        Self->State->WalleTasks.emplace(Task.TaskId, Task);
        Self->State->WalleRequests.emplace(Task.RequestId, Task.TaskId);

        NIceDb::TNiceDb db(txc.DB);
        auto row = db.Table<Schema::WalleTask>().Key(Task.TaskId);
        row.Update(NIceDb::TUpdate<Schema::WalleTask::RequestID>(Task.RequestId));

        Self->AuditLog(ctx, TStringBuilder() << "Store wall-e task"
            << ": id# " << Task.TaskId
            << ", requestId# " << Task.RequestId);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStoreWalleTask Complete");
        Self->Reply(Request.Get(), Response, ctx);
    }

private:
    TTaskInfo Task;
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
};

ITransaction *TCms::CreateTxStoreWalleTask(const TTaskInfo &task, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp) {
    return new TTxStoreWalleTask(this, task, std::move(req), std::move(resp));
}

} // namespace NKikimr::NCms
