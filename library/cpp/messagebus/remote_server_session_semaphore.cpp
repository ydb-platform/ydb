#include "remote_server_session_semaphore.h"

#include <util/stream/output.h>
#include <util/system/yassert.h>

using namespace NBus;
using namespace NBus::NPrivate;

TRemoteServerSessionSemaphore::TRemoteServerSessionSemaphore(
    TAtomicBase limitCount, TAtomicBase limitSize, const char* name)
    : Name(name)
    , LimitCount(limitCount)
    , LimitSize(limitSize)
    , CurrentCount(0)
    , CurrentSize(0)
    , PausedByUser(0)
    , StopSignal(0)
{
    Y_ABORT_UNLESS(limitCount > 0, "limit must be > 0");
    Y_UNUSED(Name);
}

TRemoteServerSessionSemaphore::~TRemoteServerSessionSemaphore() {
    Y_ABORT_UNLESS(AtomicGet(CurrentCount) == 0);
    // TODO: fix spider and enable
    //Y_ABORT_UNLESS(AtomicGet(CurrentSize) == 0);
}

bool TRemoteServerSessionSemaphore::TryWait() {
    if (Y_UNLIKELY(AtomicGet(StopSignal)))
        return true;
    if (AtomicGet(PausedByUser))
        return false;
    if (AtomicGet(CurrentCount) < LimitCount && (LimitSize < 0 || AtomicGet(CurrentSize) < LimitSize))
        return true;
    return false;
}

void TRemoteServerSessionSemaphore::IncrementMultiple(TAtomicBase count, TAtomicBase size) {
    AtomicAdd(CurrentCount, count);
    AtomicAdd(CurrentSize, size);
    Updated();
}

void TRemoteServerSessionSemaphore::ReleaseMultiple(TAtomicBase count, TAtomicBase size) {
    AtomicSub(CurrentCount, count);
    AtomicSub(CurrentSize, size);
    Updated();
}

void TRemoteServerSessionSemaphore::Stop() {
    AtomicSet(StopSignal, 1);
    Updated();
}

void TRemoteServerSessionSemaphore::PauseByUsed(bool pause) {
    AtomicSet(PausedByUser, pause);
    Updated();
}
