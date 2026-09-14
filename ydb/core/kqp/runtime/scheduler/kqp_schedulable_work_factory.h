#pragma once

#include "fwd.h"

#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>

namespace NKikimr::NKqp::NScheduler {

// Binds schedulable work to a compute actor's query. Every consumer of this
// factory is created by that compute actor and dies with it: the source actor
// is destroyed by the CA, read coroutines are poisoned in its PassAway, and the
// file-queue actor copies the scope instead of keeping the factory.
class TSchedulableWorkFactory : public NYql::NDq::IDqSchedulableWorkFactory {
public:
    TSchedulableWorkFactory(NHdrf::NDynamic::TQueryPtr query, bool isSchedulable);

    std::unique_ptr<NYql::NDq::IDqSchedulableWork> CreateSchedulableWork() override;
    NYql::NDq::TWorkScope GetWorkScope() const override;

private:
    const NHdrf::NDynamic::TQueryPtr Query;
    const bool IsSchedulable;
};

} // namespace NKikimr::NKqp::NScheduler
