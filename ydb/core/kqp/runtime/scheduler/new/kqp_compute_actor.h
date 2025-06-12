#pragma once

#include "kqp_schedulable_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_sync_compute_actor_base.h>

namespace NKikimr::NKqp::NScheduler {

    template <class TDerived>
    class TSchedulableComputeActorBase : public NYql::NDq::TDqSyncComputeActorBase<TDerived>, private TSchedulableActorHelper {
        using TBase = NYql::NDq::TDqSyncComputeActorBase<TDerived>;
        static constexpr ui64 TAG_WAKEUP_RESUME = 201; // TODO: why this value for magic number?

    public:
        template<typename ... TArgs>
        TSchedulableComputeActorBase(TOptions options, TArgs&& ... args)
            : TBase(std::forward<TArgs>(args) ...)
            , TSchedulableActorHelper(std::move(options))
        {
        }

    protected:
        void DoBootstrap() {
            // TODO: implement this
        }

        // Magic state function name to overload
        STATEFN(BaseStateFuncBody) {
            // TODO: account mailbox usage?
            // we assume that exceptions are handled in parents/descendants
            switch (ev->GetTypeRewrite()) {
                hFunc(NActors::TEvents::TEvWakeup, TSchedulableComputeActorBase<TDerived>::Handle);
                default:
                    TBase::BaseStateFuncBody(ev);
            }
        }

        void PassAway() override {
            PassedAway = true;

            if (IsSchedulable()) {
                if (!IsThrottled()) {
                    StopExecution();
                }

                // TODO: do we need to send anything to scheduler?
            }

            TBase::PassAway();
        }

    private:
        void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
            if (ev->Get()->Tag == TAG_WAKEUP_RESUME) {
                TBase::DoExecute();
            } else {
                TBase::HandleExecuteBase(ev);
            }
        }

        void DoExecuteImpl() override {
            if (!IsSchedulable()) {
                return TBase::DoExecuteImpl();
            }

            // TODO: use single "now" moment for delay, throttle and resume?
            // TODO: account waiting on mailbox?

            if (auto delay = CalculateDelay(Now())) {
                Throttle();
                this->Schedule(*delay, new NActors::TEvents::TEvWakeup(TAG_WAKEUP_RESUME));
                return;
            }

            StartExecution(IsThrottled() ? Resume() : 0);
            TBase::DoExecuteImpl();
            if (!PassedAway) {
                StopExecution();
            }
        }

    private:
        bool PassedAway = false;
    };

} // namespace NKikimr::NKqp::NScheduler
