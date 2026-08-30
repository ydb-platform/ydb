#pragma once

#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_read.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

namespace NKikimr::NConveyorComposite {

    class TWorkloadQuotaController {
    public:
        class TReservation {
            friend class TWorkloadQuotaController;

        private:
            TReservation(NKqp::NScheduler::TSchedulableReadPtr read,
                         NKqp::NScheduler::TSchedulableRead::TQuotaReservation quota)
                : Read(std::move(read))
                , Quota(std::move(quota))
            {
            }

            NKqp::NScheduler::TSchedulableReadPtr Read;
            std::optional<NKqp::NScheduler::TSchedulableRead::TQuotaReservation> Quota;
        };

        using TReservationPtr = std::shared_ptr<TReservation>;

        struct TReserveResult {
            bool Allowed = false;
            TReservationPtr Reservation;
        };

        explicit TWorkloadQuotaController(NKqp::NScheduler::TComputeSchedulerPtr scheduler);

        TReserveResult TryReserve(const TWorkloadContext& context, TDuration predicted);
        void Finish(TReservationPtr reservation, TDuration actual);

        std::optional<TMonotonic> ExtractNextWakeup();

    private:
        void RegisterRetry(TDuration delay);

        NKqp::NScheduler::TComputeSchedulerPtr Scheduler;
        NKqp::NScheduler::TSchedulableReadFactoryPtr Factory;
        THashMap<std::pair<TString, TString>, NKqp::NScheduler::TSchedulableReadPtr> Reads;
        THashSet<std::pair<TString, TString>> PendingRegistrations;
        std::optional<TMonotonic> NextWakeup;
    };

} // namespace NKikimr::NConveyorComposite
