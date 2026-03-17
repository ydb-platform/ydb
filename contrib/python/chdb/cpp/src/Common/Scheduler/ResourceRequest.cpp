#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ISchedulerConstraint.h>

namespace DB_CHDB
{

void ResourceRequest::finish()
{
    if (constraint)
        constraint->finishRequest(this);
}

}
