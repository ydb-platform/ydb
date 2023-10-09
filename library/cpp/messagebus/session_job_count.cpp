#include "session_job_count.h"

#include <util/system/yassert.h>

using namespace NBus;
using namespace NBus::NPrivate;

TBusSessionJobCount::TBusSessionJobCount()
    : JobCount(0)
{
}

TBusSessionJobCount::~TBusSessionJobCount() {
    Y_ABORT_UNLESS(JobCount == 0, "must be 0 job count to destroy job");
}

void TBusSessionJobCount::WaitForZero() {
    TGuard<TMutex> guard(Mutex);
    while (AtomicGet(JobCount) > 0) {
        CondVar.WaitI(Mutex);
    }
}
