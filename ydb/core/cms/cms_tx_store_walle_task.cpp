#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NCms {

class TCms::TTxStoreWalleTask : public TTransactionBase<TCms> {
public:
    TTxStoreWalleTask(TCms *self, const TWalleTaskInfo &task, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp)
        : TBase(self)
        , TaskId(task.TaskId)
        , RequestId(task.RequestId)
        , Request(std::move(req))
        , Response(std::move(resp))
    {
        Y_VERIFY(Request);
        Y_VERIFY(Response);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStoreWalleTask Execute");

        NIceDb::TNiceDb db(txc.DB);
        auto row = db.Table<Schema::WalleTask>().Key(TaskId);
        row.Update(NIceDb::TUpdate<Schema::WalleTask::RequestID>(RequestId));

        Self->AuditLog(ctx, TStringBuilder() << "Store wall-e task"
            << ": id# " << TaskId
            << ", requestId# " << RequestId);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxStoreWalleTask Complete");
        Self->Reply(Request.Get(), Response, ctx);
    }

private:
    TString TaskId;
    TString RequestId;
    THolder<IEventBase> Request;
    TAutoPtr<IEventHandle> Response;
};

ITransaction *TCms::CreateTxStoreWalleTask(const TWalleTaskInfo &task, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp)
{
    return new TTxStoreWalleTask(this, task, std::move(req), std::move(resp));
}

} // NCms
} // NKikimr
