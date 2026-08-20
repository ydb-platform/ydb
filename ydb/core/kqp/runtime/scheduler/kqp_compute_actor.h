#pragma once

#include "kqp_dq_scheduler_context.h"
#include "kqp_schedulable_base.h"
#include "kqp_schedulable_task.h"

#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

namespace NKikimr::NKqp::NScheduler {

    template <class TDerived>
    class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived>, TSchedulableBase {
        using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;

    public:
        template<typename ... TArgs>
        TSchedulableComputeActorBase(const TSchedulableOptions& options, TArgs&& ... args)
            : TBase(std::forward<TArgs>(args) ...)
            , TSchedulableBase(options)
            , SchedulerContext(std::make_shared<TDqSchedulerContext>(options.Query, options.IsSchedulable))
        {
        }

    protected:
        NYql::NDq::IDqSchedulerContextPtr GetSchedulerContext() const override {
            return SchedulerContext;
        }

        void DoBootstrap() {
            if (IsAccountable()) {
                RegisterForResume(this->SelfId());
            }
        }

        // Magic state-function name to overload
        STATEFN(BaseStateFuncBody) {
            // TODO: account mailbox usage?

            // we assume that exceptions are handled in parents/descendants
            switch (ev->GetTypeRewrite()) {
                hFunc(TSchedulableTask::TResumeEventType, TSchedulableComputeActorBase<TDerived>::Handle);
                default:
                    TBase::BaseStateFuncBody(ev);
            }
        }

        void PassAway() override {
            if (!PassedAway && IsAccountable()) {
                PassedAway = true;
                StopExecution(ForcedResume);
            }

            TBase::PassAway();
        }

    private:
        void Handle(TSchedulableTask::TResumeEventType::TPtr& ev) {
            if (TSchedulableTask::IsResumeEvent(ev)) {
                if (IsThrottled()) {
                    ForcedResume = ev->Sender != this->SelfId();
                    TBase::DoExecute();
                }
            } else {
                TBase::HandleExecuteBase(ev);
            }
        }

        void DoExecuteImpl() override {
            if (!IsAccountable()) {
                return TBase::DoExecuteImpl();
            }

            // TODO: account waiting on mailbox?

            const auto now = Now();

            if (StartExecution(now)) {
                TBase::DoExecuteImpl();
                if (!PassedAway) {
                    StopExecution(ForcedResume);
                }
                return;
            }

            this->Schedule(CalculateDelay(now), TSchedulableTask::GetResumeEvent().release());
        }

    private:
        const NYql::NDq::IDqSchedulerContextPtr SchedulerContext;
        bool PassedAway = false;
        bool ForcedResume = false;
    };

} // namespace NKikimr::NKqp::NScheduler
