#pragma once

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_base.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_task.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_work_factory.h>

#include <ydb/core/kqp/runtime/common/kqp_remote_embedder_stage.h>
#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

namespace NKikimr::NKqp::NScheduler {

    template <class TDerived>
    class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived> {
        using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;

    public:
        template<typename ... TArgs>
        TSchedulableComputeActorBase(const TSchedulableOptions& options, TArgs&& ... args)
            : TBase(std::forward<TArgs>(args) ...)
            , Schedulable(options)
            , WorkFactory(options.Query
                ? std::make_shared<TSchedulableWorkFactory>(options.Query, options.IsSchedulable)
                : nullptr)
        {
        }

    protected:
        void CheckRunStatus() override {
            if (!WaitingForRemoteEmbedder ||
                (!this->Checkpoints && this->ProcessOutputsState.AllOutputsFinished && !this->HasEffectsOutputs)) {
                TBase::CheckRunStatus();
                return;
            }
            // Acknowledge freed channel space without requesting another fetch.
            if (this->Channels) {
                for (const auto& [channelId, channel] : this->InputChannelsMap) {
                    this->Channels->PollChannel(channelId, channel.Channel->GetFreeSpace());
                }
            }
        }

        NYql::NDq::IDqSchedulableWorkFactoryPtr GetSchedulableWorkFactory() const override {
            return WorkFactory;
        }

        void DoBootstrap() {
            if (Schedulable.IsAccountable()) {
                Schedulable.RegisterForResume(this->SelfId());
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
            if (!PassedAway && Schedulable.IsAccountable()) {
                PassedAway = true;
                if (Schedulable.IsExecuting() || Schedulable.IsThrottled()) {
                    Schedulable.StopExecution();
                }
            }

            TBase::PassAway();
        }

    private:
        void Handle(TSchedulableTask::TResumeEventType::TPtr& ev) {
            if (TSchedulableTask::IsResumeEvent(ev)) {
                if (Schedulable.IsThrottled()) {
                    Schedulable.NotifyResumed(/* byScheduler = */ ev->Sender != this->SelfId());
                    TBase::DoExecute();
                }
            } else {
                TBase::HandleExecuteBase(ev);
            }
        }

        void DoExecuteImpl() override {
            if (!Schedulable.IsAccountable()) {
                return ExecuteTask();
            }

            // TODO: account waiting on mailbox?

            if (const auto delay = Schedulable.TryStartExecution(TMonotonic::Now())) {
                this->Schedule(*delay, TSchedulableTask::GetResumeEvent().release());
                return;
            }

            ExecuteTask();
            if (!PassedAway) {
                Schedulable.StopExecution();
            }
        }

        void ExecuteTask() {
            auto* actor = static_cast<TDerived*>(this);
            const auto sourcesState = actor->GetSourcesState();
            WaitingForRemoteEmbedder = false;
            try {
                TBase::DoExecuteImpl();
            } catch (const NMiniKQL::TKqpRemoteEmbedderPending&) {
                // Let DQ drain completed rows. The future sends the next
                // wakeup, so CheckRunStatus must skip input polling.
                WaitingForRemoteEmbedder = true;
                this->TaskRunner->LastFetchStatus = NYql::NDq::ERunStatus::PendingInput;
                this->TaskRunner->LastFetchTime = TInstant::Now();
                actor->PollSources(sourcesState);
                this->ProcessOutputsImpl(NYql::NDq::ERunStatus::PendingInput);
            }
        }

    private:
        TSchedulableBase Schedulable;
        const NYql::NDq::IDqSchedulableWorkFactoryPtr WorkFactory;
        bool PassedAway = false;
        bool WaitingForRemoteEmbedder = false;
    };

} // namespace NKikimr::NKqp::NScheduler
