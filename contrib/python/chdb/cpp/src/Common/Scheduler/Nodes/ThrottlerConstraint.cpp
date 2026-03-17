#include <Common/Scheduler/Nodes/ThrottlerConstraint.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB_CHDB
{

void registerThrottlerConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<ThrottlerConstraint>("bandwidth_limit");
}

}
