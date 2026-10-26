#pragma once

#include <ydb/library/yql/dq/task_scheduler/task_scheduler.h>
#include <yql/essentials/minikql/computation/mkql_execution_wait.h>

namespace NYql::NDq {

class TTaskExecutionWaitObserver final : public NKikimr::NMiniKQL::IExecutionWaitObserver {
public:
    explicit TTaskExecutionWaitObserver(std::shared_ptr<TTaskSchedulingHandle> task) : Task(std::move(task)) {}
    NKikimr::NMiniKQL::TExecutionInputWaitScope BeginInput() const override;
    void AwaitInput(NKikimr::NMiniKQL::TExecutionInputWaitScope scope, ui64 operatorId,
                    NKikimr::NMiniKQL::EExecutionInputSide side) override;
    void AwaitFuture(ui64 operatorId, const void* operation, NKikimr::NMiniKQL::EExecutionWaitKind kind,
                     const NThreading::TFuture<void>& future) override;
    void AwaitCallback(ui64 operatorId, NKikimr::NMiniKQL::EExecutionWaitKind kind) override;
    void MakeRunnable() override;
    void ReportWork(ui64 operatorId, const char* name, NKikimr::NMiniKQL::EExecutionWorkKind work, const char* phase) override;
    void AwaitNamedInput(NKikimr::NMiniKQL::TExecutionInputWaitScope scope, ui64 operatorId,
                         NKikimr::NMiniKQL::EExecutionInputSide side, const char* name) override;
    void BeginRun();
    void EndRun();
    void ResetWork() {
        Work.Work = ETaskWorkKind::Unknown;
        Work.OperatorId = 0;
    }

private:
    struct TAsyncWait {
        NThreading::TFuture<void> Future;
        TTaskDependencyVersion Dependency;
    };
    const std::shared_ptr<TTaskSchedulingHandle> Task;
    THashMap<const void*, TAsyncWait> AsyncWaits;
    TTaskExecutionClassification Work;
};

} // namespace NYql::NDq
