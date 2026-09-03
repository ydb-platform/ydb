#include "kqp_dq_scheduler_context.h"

#include "kqp_schedulable_base.h"
#include "tree/dynamic.h"

namespace NKikimr::NKqp::NScheduler {

TDqSchedulerContext::TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable)
    : Query(std::move(query))
    , IsSchedulable(isSchedulable)
{
    Y_ENSURE(!IsSchedulable || Query);
}

std::unique_ptr<NYql::NDq::IDqSchedulableWork> TDqSchedulerContext::CreateSchedulableWork() {
    if (!Query) {
        return nullptr;
    }
    return std::make_unique<TSchedulableBase>(TSchedulableOptions{
        .Query = Query,
        .IsSchedulable = IsSchedulable,
    });
}

NYql::NDq::TWorkScope TDqSchedulerContext::GetWorkScope() const {
    NYql::NDq::TWorkScope scope;
    if (!Query) {
        return scope;
    }
    if (auto* pool = Query->GetParent()) {
        scope.Name = std::get<NHdrf::TPoolId>(pool->GetId());
        if (auto* database = pool->GetParent()) {
            scope.Namespace = std::get<NHdrf::TDatabaseId>(database->GetId());
        }
    }
    return scope;
}

} // namespace NKikimr::NKqp::NScheduler
