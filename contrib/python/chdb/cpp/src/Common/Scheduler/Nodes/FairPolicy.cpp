#include <Common/Scheduler/Nodes/FairPolicy.h>

#include <Common/Scheduler/Nodes/SchedulerNodeFactory.h>

namespace DB_CHDB
{

void registerFairPolicy(SchedulerNodeFactory & factory)
{
    factory.registerMethod<FairPolicy>("fair");
}

}
