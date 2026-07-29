#include "kqp_dq_scheduler_context.h"

#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

namespace {

class TDqSchedulableWork final
    : public NYql::NDq::IDqSchedulableWork
    , private TSchedulableActorBase {
public:
    TDqSchedulableWork(const TSchedulableActorOptions& options, TString poolId)
        : TSchedulableActorBase(options)
        , PoolId(std::move(poolId))
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

    TString GetPoolId() const override {
        return PoolId;
    }

private:
    const TString PoolId;
};

} // namespace

TDqSchedulerContext::TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable)
    : Query(std::move(query))
    , IsSchedulable(isSchedulable)
{
    if (Query && Query->GetParent()) {
        PoolId = std::get<NHdrf::TPoolId>(Query->GetParent()->GetId());
    }
}

std::shared_ptr<NYql::NDq::IDqSchedulableWork> TDqSchedulerContext::CreateSchedulableWork() {
    if (!Query) {
        return nullptr;
    }
    return std::make_shared<TDqSchedulableWork>(
        TSchedulableActorOptions{
            .Query = Query,
            .IsSchedulable = IsSchedulable,
        },
        PoolId);
}

} // namespace NKikimr::NKqp::NScheduler
