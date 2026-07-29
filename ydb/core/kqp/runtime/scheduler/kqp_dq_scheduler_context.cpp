#include "kqp_dq_scheduler_context.h"

namespace NKikimr::NKqp::NScheduler {

namespace {

class TDqSchedulableWork final
    : public NYql::NDq::IDqSchedulableWork
    , private TSchedulableActorBase {
public:
    explicit TDqSchedulableWork(const TSchedulableActorOptions& options)
        : TSchedulableActorBase(options)
    {}

    bool StartExecution(TMonotonic now) override {
        return TSchedulableActorBase::StartExecution(now);
    }

    void StopExecution(bool& forcedResume) override {
        TSchedulableActorBase::StopExecution(forcedResume);
    }

    TDuration CalculateDelay(TMonotonic now) const override {
        return TSchedulableActorBase::CalculateDelay(now);
    }

    void RegisterForResume(const NActors::TActorId& actorId) override {
        TSchedulableActorBase::RegisterForResume(actorId);
    }

    void RecordUsage(TDuration elapsed) override {
        TSchedulableActorBase::IncreaseBurstUsage(elapsed);
    }
};

} // namespace

TDqSchedulerContext::TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable)
    : Query(std::move(query))
    , IsSchedulable(isSchedulable)
{}

std::shared_ptr<NYql::NDq::IDqSchedulableWork> TDqSchedulerContext::CreateSchedulableWork() {
    if (!Query) {
        return nullptr;
    }
    return std::make_shared<TDqSchedulableWork>(TSchedulableActorOptions{
        .Query = Query,
        .IsSchedulable = IsSchedulable,
    });
}

} // namespace NKikimr::NKqp::NScheduler
