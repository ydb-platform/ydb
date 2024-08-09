#pragma once

#include "defs.h"

#include <ydb/core/protos/resource_broker.pb.h>

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <array>

namespace NKikimr {
namespace NResourceBroker {

constexpr size_t LAST_RESOURCE = NKikimrResourceBroker::MEMORY;
constexpr size_t RESOURCE_COUNT = LAST_RESOURCE + 1;

inline TActorId MakeResourceBrokerID(ui32 node = 0) {
    char x[12] = {'r','e','s','o','u','r','c','e','b','r','o','k'};
    return TActorId(node, TStringBuf(x, 12));
}

using TResourceValues = std::array<ui64, RESOURCE_COUNT>;

class IResourceBroker;

struct TEvResourceBroker {
    enum EEv {
        EvSubmitTask = EventSpaceBegin(TKikimrEvents::ES_RESOURCE_BROKER),
        EvUpdateTask,
        EvUpdateTaskCookie,
        EvRemoveTask,
        EvFinishTask,
        EvNotifyActorDied,
        EvConfigure,
        EvConfigRequest,
        EvTaskRemoved,
        EvResourceBrokerRequest,

        EvTaskOperationError = EvSubmitTask + 512,
        EvResourceAllocated,
        EvConfigureResult,
        EvConfigResponse,
        EvResourceBrokerResponse
    };

    static_assert(EvResourceBrokerRequest < EvTaskOperationError);

    struct TTask {
        /**
         * 0 is invalid task ID value and is used to choose task ID
         * by Resource Broker (which makes it impossible to manage
         * submitted task).
         */
        ui64 TaskId;
        TString Name;
        TResourceValues RequiredResources;
        TString Type;
        ui64 Priority;
        TIntrusivePtr<TThrRefBase> Cookie;
    };

    struct TEvSubmitTask : public TEventLocal<TEvSubmitTask, EvSubmitTask> {
        TTask Task;

        TEvSubmitTask(ui64 taskId, const TString &name, const TResourceValues &resources,
                      const TString &type, ui64 priority, TIntrusivePtr<TThrRefBase> cookie)
        {
            Task.TaskId = taskId;
            Task.Name = name;
            Task.RequiredResources = resources;
            Task.Type = type;
            Task.Priority = priority;
            Task.Cookie = cookie;
        }

        /**
         * Task ID is chosen by Resource Broker and client will get it only
         * after resource allocation.
         */
        TEvSubmitTask(const TString &name, const TResourceValues &resources,
                      const TString &type, ui64 priority, TIntrusivePtr<TThrRefBase> cookie)
        {
            Task.TaskId = 0;
            Task.Name = name;
            Task.RequiredResources = resources;
            Task.Type = type;
            Task.Priority = priority;
            Task.Cookie = cookie;
        }
    };

    struct TStatus {
        enum ECode {
            // Cannot submit task with already used ID.
            ALREADY_EXISTS,
            // Cannot update/remove/finish task with unknown ID.
            UNKNOWN_TASK,
            // Cannot remove task in-fly.
            TASK_IN_FLY,
            // Cannot finish task which is still in queue.
            TASK_IN_QUEUE
        };

        ECode Code;
        TString Message;
    };

    struct TEvTaskOperationError : public TEventLocal<TEvTaskOperationError, EvTaskOperationError> {
        ui64 TaskId;
        TStatus Status;
        TIntrusivePtr<TThrRefBase> Cookie;
    };

    struct TEvUpdateTask : public TEventLocal<TEvUpdateTask, EvUpdateTask> {
        ui64 TaskId;
        TResourceValues RequiredResources;
        TString Type;
        ui64 Priority;
        bool Resubmit;

        TEvUpdateTask(ui64 taskId, const TResourceValues &requiredResources,
                      const TString &type, ui64 priority, bool resubmit = false)
            : TaskId(taskId)
            , RequiredResources(requiredResources)
            , Type(type)
            , Priority(priority)
            , Resubmit(resubmit)
        {
            Y_ABORT_UNLESS(taskId);
        }
    };

    struct TEvUpdateTaskCookie : public TEventLocal<TEvUpdateTaskCookie, EvUpdateTaskCookie> {
        ui64 TaskId;
        TIntrusivePtr<TThrRefBase> Cookie;

        TEvUpdateTaskCookie(ui64 taskId, TIntrusivePtr<TThrRefBase> cookie)
            : TaskId(taskId)
            , Cookie(cookie)
        {
            Y_ABORT_UNLESS(taskId);
        }
    };

    struct TEvRemoveTask : public TEventLocal<TEvRemoveTask, EvRemoveTask> {
        ui64 TaskId;
        bool ReplyOnSuccess;

        TEvRemoveTask(ui64 taskId, bool replyOnSuccess = false)
            : TaskId(taskId)
            , ReplyOnSuccess(replyOnSuccess)
        {
            Y_ABORT_UNLESS(taskId);
        }
    };

    struct TEvTaskRemoved : public TEventLocal<TEvTaskRemoved, EvTaskRemoved> {
        ui64 TaskId;
        TIntrusivePtr<TThrRefBase> Cookie;
    };

    struct TEvFinishTask : public TEventLocal<TEvFinishTask, EvFinishTask> {
        ui64 TaskId;
        bool Cancel;

        TEvFinishTask(ui64 taskId, bool cancel = false)
            : TaskId(taskId)
            , Cancel(cancel)
        {
            Y_ABORT_UNLESS(taskId);
        }
    };

    struct TEvNotifyActorDied : public TEventLocal<TEvNotifyActorDied, EvNotifyActorDied> {
    };

    struct TEvResourceAllocated : public TEventLocal<TEvResourceAllocated, EvResourceAllocated> {
        ui64 TaskId;
        TIntrusivePtr<TThrRefBase> Cookie;

        TEvResourceAllocated(ui64 taskId, TIntrusivePtr<TThrRefBase> cookie)
            : TaskId(taskId)
            , Cookie(cookie)
        {
        }
    };

    struct TEvConfigure : public TEventPB<TEvConfigure,
                                          NKikimrResourceBroker::TResourceBrokerConfig,
                                          EvConfigure> {
        bool Merge = false;
    };

    struct TEvConfigureResult : public TEventPB<TEvConfigureResult,
                                                NKikimrResourceBroker::TResourceBrokerConfigResult,
                                                EvConfigureResult> {
    };

    struct TEvConfigRequest : public TEventLocal<TEvConfigRequest, EvConfigRequest> {
        TString Queue;

        TEvConfigRequest(const TString& queue)
            : Queue(queue) {}
    };

    struct TEvConfigResponse : public TEventLocal<TEvConfigResponse, EvConfigResponse> {
        TMaybe<NKikimrResourceBroker::TQueueConfig> QueueConfig;
    };

    struct TEvResourceBrokerRequest : public TEventLocal<TEvResourceBrokerRequest, EvResourceBrokerRequest> {};

    struct TEvResourceBrokerResponse : public TEventLocal<TEvResourceBrokerResponse, EvResourceBrokerResponse> {
        TIntrusivePtr<IResourceBroker> ResourceBroker;
    };
};

class IResourceBroker : public TThrRefBase {
public:
    virtual bool SubmitTaskInstant(const TEvResourceBroker::TEvSubmitTask &ev, const TActorId &sender) = 0;
    virtual bool FinishTaskInstant(const TEvResourceBroker::TEvFinishTask &ev, const TActorId &sender) = 0;
    /**
     * Merges in-fly tasks of the same type. All donor's resources goes to recipient's ones.
     * Donor task will be finished.
     */
    virtual bool MergeTasksInstant(ui64 recipientTaskId, ui64 donorTaskId, const TActorId &sender) = 0;
    virtual bool ReduceTaskResourcesInstant(ui64 taskId, const TResourceValues& reduceBy, const TActorId& sender) = 0;
};

NKikimrResourceBroker::TResourceBrokerConfig MakeDefaultConfig();

void MergeConfigUpdates(NKikimrResourceBroker::TResourceBrokerConfig &config,
                        const NKikimrResourceBroker::TResourceBrokerConfig &updates);

IActor* CreateResourceBrokerActor(const NKikimrResourceBroker::TResourceBrokerConfig &config,
                                  const ::NMonitoring::TDynamicCounterPtr& counters);

} // NResourceBroker
} // NKikimr
