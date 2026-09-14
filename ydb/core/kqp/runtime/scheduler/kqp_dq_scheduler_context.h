#pragma once

#include "fwd.h"
#include "kqp_schedulable_base.h"

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

namespace NKikimr::NKqp::NScheduler {

class TDqSchedulerContext : public NYql::NDq::IDqSchedulerContext {
public:
    TDqSchedulerContext(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable);

    std::unique_ptr<NYql::NDq::IDqSchedulableWork> CreateSchedulableWork() override;
    NYql::NDq::TWorkScope GetWorkScope() const override;

private:
    const NHdrf::NDynamic::TQueryPtr Query;
    const bool IsSchedulable;
};

} // namespace NKikimr::NKqp::NScheduler
