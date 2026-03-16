#include <Common/Scheduler/Nodes/SemaphoreConstraint.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB_CHDB
{

void registerSemaphoreConstraint(SchedulerNodeFactory & factory)
{
    factory.registerMethod<SemaphoreConstraint>("inflight_limit");
}

}
