#include "flat_exec_broker.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

using namespace NResourceBroker;

TBroker::TBroker(IOps* ops, TIntrusivePtr<TIdEmitter> emitter)
    : Ops(ops)
    , Emitter(std::move(emitter))
{ }

TTaskId TBroker::SubmitTask(TString name, TResourceParams params, TResourceConsumer consumer) {
    Y_ENSURE(consumer, "Resource consumer cannot be null");
    TTaskId taskId = Emitter->Do();
    Submitted[taskId].swap(consumer);
    SendToBroker(new TEvResourceBroker::TEvSubmitTask(
        taskId,
        std::move(name),
        {{ params.CPU, params.Memory }},
        std::move(params.Type),
        params.Priority,
        /* cookie */ nullptr));
    return taskId;
}

void TBroker::UpdateTask(TTaskId taskId, TResourceParams params) {
    SendToBroker(new TEvResourceBroker::TEvUpdateTask(
        taskId,
        {{ params.CPU, params.Memory }},
        std::move(params.Type),
        params.Priority));
}

void TBroker::FinishTask(TTaskId taskId, EResourceStatus status) {
    SendToBroker(new TEvResourceBroker::TEvFinishTask(
        taskId,
        status == EResourceStatus::Cancelled));
}

bool TBroker::CancelTask(TTaskId taskId) {
    SendToBroker(new TEvResourceBroker::TEvRemoveTask(taskId));

    if (auto it = Submitted.find(taskId); it != Submitted.end()) {
        Submitted.erase(it);
        return true;
    }

    return false;
}

void TBroker::OnResourceAllocated(TTaskId taskId) {
    if (auto it = Submitted.find(taskId); it != Submitted.end()) {
        TResourceConsumer consumer;
        consumer.swap(it->second);
        Submitted.erase(it);
        consumer(taskId);
    } else {
        // Assume it was a race between cancellation and resource allocation
        SendToBroker(new TEvResourceBroker::TEvFinishTask(taskId, /* cancelled */ true));
    }
}

void TBroker::SendToBroker(TAutoPtr<IEventBase> event) {
    Ops->Send(MakeResourceBrokerID(), event.Release(), 0);
}

}
}
