#include "walle.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NCms {

using namespace NKikimrCms;
using namespace NNodeWhiteboard;

class TWalleListTasksAdapter : public TActorBootstrapped<TWalleListTasksAdapter> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_WALLE_REQ;
    }

    TWalleListTasksAdapter(TEvCms::TEvWalleListTasksRequest::TPtr &event, const TCmsStatePtr state)
        : RequestEvent(event)
        , State(state)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        auto &rec = RequestEvent->Get()->Record;

        LOG_INFO(ctx, NKikimrServices::CMS, "Processing Wall-E request: %s",
                  rec.ShortDebugString().data());

        TAutoPtr<TEvCms::TEvWalleListTasksResponse> response = new TEvCms::TEvWalleListTasksResponse;

        for (auto &entry : State->WalleTasks) {
            auto &task = entry.second;
            auto &info = *response->Record.AddTasks();

            info.SetTaskId(task.TaskId);
            if (State->ScheduledRequests.contains(task.RequestId)) {
                auto &req = State->ScheduledRequests.find(task.RequestId)->second;
                for (auto &action : req.Request.GetActions())
                    *info.AddHosts() = action.GetHost();
                info.SetStatus("in-process");
            } else {
                for (auto &id : task.Permissions) {
                    if (State->Permissions.contains(id))
                        *info.AddHosts() = State->Permissions.find(id)->second.Action.GetHost();
                }
                info.SetStatus("ok");
            }
        }

        ReplyAndDie(response, ctx);
    }

private:
    void ReplyAndDie(TAutoPtr<TEvCms::TEvWalleListTasksResponse> resp, const TActorContext &ctx) {
        WalleAuditLog(RequestEvent->Get(), resp.Get(), ctx);
        ctx.Send(RequestEvent->Sender, resp.Release());
        Die(ctx);
    }

    TEvCms::TEvWalleListTasksRequest::TPtr RequestEvent;
    const TCmsStatePtr State;
};

IActor *CreateWalleAdapter(TEvCms::TEvWalleListTasksRequest::TPtr &ev, const TCmsStatePtr state) {
    return new TWalleListTasksAdapter(ev, state);
}

} // namespace NKikimr::NCms
