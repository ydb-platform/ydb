#include "workload.h"

#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NConveyorComposite {

    namespace  {
        constexpr TDuration kMinTaskPrediction = TDuration::MilliSeconds(1);
    }  // anonymous namespace

    TWorkloadQuotaController::TWorkloadQuotaController(NKqp::NScheduler::TComputeSchedulerPtr scheduler)
        : Scheduler(std::move(scheduler))
    {
        if (Scheduler) {
            Factory = std::make_unique<NKqp::NScheduler::TSchedulableReadFactory>(Scheduler);
        }
    }

    TWorkloadQuotaController::TReserveResult TWorkloadQuotaController::TryReserve(
        const TWorkloadContext& context, TDuration predicted) {

        if (!context.IsDefined() || !Scheduler || !Scheduler->IsEnabled()) {
            return {.Allowed = true};
        }

        const auto key = std::make_pair(context.DatabaseId, context.PoolId);
        auto readIt = Reads.find(key);
        auto read = readIt != Reads.end() ? readIt->second : Factory->Get(context.DatabaseId, context.PoolId);
        if (!read) {
            if (NActors::TlsActivationContext && PendingRegistrations.emplace(key).second) {
                const auto schedulerServiceId = NKqp::MakeKqpSchedulerServiceId(
                    NActors::TActorContext::AsActorContext().SelfID.NodeId());
                NActors::TActorContext::AsActorContext().Send(
                    schedulerServiceId, new NKqp::NScheduler::TEvAddDatabase(context.DatabaseId));
                NActors::TActorContext::AsActorContext().Send(
                    schedulerServiceId, new NKqp::NScheduler::TEvAddPool(context.DatabaseId, context.PoolId));
            }
            RegisterRetry(TDuration::MilliSeconds(1));
            return {};
        }
        if (readIt == Reads.end()) {
            Reads.emplace(key, read);
        }
        PendingRegistrations.erase(key);
        if (!read->IsValid()) {
            RegisterRetry(TDuration::Minutes(1));
            return {};
        }

        predicted = Max(predicted, kMinTaskPrediction);
        auto quota = read->TryConsumeQuota(predicted);
        if (!quota) {
            RegisterRetry(read->EstimateQuotaDelay(predicted));
            return {};
        }

        return {
            .Allowed = true,
            .Reservation = TReservationPtr(new TReservation(std::move(read), std::move(*quota))),
        };
    }

    void TWorkloadQuotaController::Finish(TReservationPtr reservation, TDuration actual) {
        if (!reservation || !reservation->Quota) {
            return;
        }
        reservation->Read->ReturnQuota(std::move(*reservation->Quota), actual);
        reservation->Quota.reset();
    }

    std::optional<TMonotonic> TWorkloadQuotaController::ExtractNextWakeup() {
        return std::exchange(NextWakeup, std::nullopt);
    }

    void TWorkloadQuotaController::RegisterRetry(TDuration delay) {
        const auto deadline = TMonotonic::Now() + Max(delay, kMinTaskPrediction);
        if (!NextWakeup || deadline < *NextWakeup) {
            NextWakeup = deadline;
        }
    }

} // namespace NKikimr::NConveyorComposite
