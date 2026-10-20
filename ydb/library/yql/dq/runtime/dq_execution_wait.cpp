#include "dq_execution_wait.h"

namespace NYql::NDq {
namespace {
ETaskWaitReason Convert(NKikimr::NMiniKQL::EExecutionWaitKind kind) {
    using EKind = NKikimr::NMiniKQL::EExecutionWaitKind;
    switch (kind) {
        case EKind::Input: return ETaskWaitReason::Input;
        case EKind::SpillRead: return ETaskWaitReason::SpillRead;
        case EKind::SpillWrite: return ETaskWaitReason::SpillWrite;
        case EKind::AsyncIo: return ETaskWaitReason::AsyncIo;
    }
    Y_ABORT("Unknown execution wait kind");
}
ETaskInputSide Convert(NKikimr::NMiniKQL::EExecutionInputSide side) {
    using ESide = NKikimr::NMiniKQL::EExecutionInputSide;
    switch (side) {
        case ESide::None: return ETaskInputSide::None;
        case ESide::Left: return ETaskInputSide::Left;
        case ESide::Right: return ETaskInputSide::Right;
        case ESide::Build: return ETaskInputSide::Build;
        case ESide::Probe: return ETaskInputSide::Probe;
    }
    Y_ABORT("Unknown execution input side");
}
} // namespace

NKikimr::NMiniKQL::TExecutionInputWaitScope TTaskExecutionWaitObserver::BeginInput() const {
    const auto scope = Task->BeginInputWait();
    return {scope.first, scope.second};
}

void TTaskExecutionWaitObserver::AwaitInput(NKikimr::NMiniKQL::TExecutionInputWaitScope scope, ui64 operatorId,
                                           NKikimr::NMiniKQL::EExecutionInputSide side) {
    Task->ReportInputWait({scope.Probes, scope.Reports}, operatorId, Convert(side));
}

void TTaskExecutionWaitObserver::AwaitFuture(ui64 operatorId, const void* operation,
    NKikimr::NMiniKQL::EExecutionWaitKind kind, const NThreading::TFuture<void>& future)
{
    auto it = AsyncWaits.find(operation);
    if (it == AsyncWaits.end() || it->second.Future.IsReady()) {
        const auto dependency = Task->NewAsyncDependency();
        it = AsyncWaits.insert_or_assign(operation, TAsyncWait{future, dependency}).first;
        future.Subscribe([weak = std::weak_ptr<TTaskSchedulingHandle>(Task), id = dependency.Id](const auto&) {
            if (auto task = weak.lock()) {
                task->Signal(id);
            }
        });
    }
    TTaskWaitDescription description;
    description.Reason = Convert(kind);
    description.Origin = ETaskWaitOrigin::Operator;
    description.Precision = ETaskWaitPrecision::Exact;
    description.OperatorId = operatorId;
    description.Condition = TTaskWaitCondition::Any({it->second.Dependency});
    Task->ReportWait(std::move(description));
}

void TTaskExecutionWaitObserver::AwaitCallback(ui64 operatorId, NKikimr::NMiniKQL::EExecutionWaitKind kind) {
    Task->ReportCallbackWait(operatorId, Convert(kind));
}

void TTaskExecutionWaitObserver::MakeRunnable() {
    Task->MakeRunnable();
}

void TTaskExecutionWaitObserver::BeginRun() {
    for (auto it = AsyncWaits.begin(); it != AsyncWaits.end();) {
        if (it->second.Future.IsReady()) {
            const auto previous = it++;
            AsyncWaits.erase(previous);
        } else {
            ++it;
        }
    }
}

} // namespace NYql::NDq
