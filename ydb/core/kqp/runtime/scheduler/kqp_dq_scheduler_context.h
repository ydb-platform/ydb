#pragma once

#include "fwd.h"
#include "kqp_schedulable_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

namespace NKikimr::NKqp::NScheduler {

// Implementation of NYql::NDq::IDqSchedulerContext that hands out
// per-work-unit wrappers backed by HDRF TSchedulableTask/TSchedulableActorBase.
// Passed through TSourceArguments so non-kqp sources (S3, ...) don't need to
// know about scheduler types directly.
class TDqSchedulerContext : public NYql::NDq::IDqSchedulerContext {
public:
    TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable);

    std::shared_ptr<NYql::NDq::IDqSchedulableWork> CreateSchedulableWork() override;

private:
    const NHdrf::NDynamic::TQueryPtr Query;
    const bool IsSchedulable;
};

} // namespace NKikimr::NKqp::NScheduler
