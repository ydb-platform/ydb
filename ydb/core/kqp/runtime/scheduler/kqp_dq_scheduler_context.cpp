#include "kqp_dq_scheduler_context.h"

#include "kqp_schedulable_base.h"
#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

TDqSchedulerContext::TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable)
    : Query(std::move(query))
    , IsSchedulable(isSchedulable)
{}

std::unique_ptr<NYql::NDq::IDqSchedulableWork> TDqSchedulerContext::CreateSchedulableWork() {
    if (!Query) {
        return nullptr;
    }
    return std::make_unique<TSchedulableBase>(TSchedulableOptions{
        .Query = Query,
        .IsSchedulable = IsSchedulable,
        .LazyDemand = true,
    });
}

} // namespace NKikimr::NKqp::NScheduler
