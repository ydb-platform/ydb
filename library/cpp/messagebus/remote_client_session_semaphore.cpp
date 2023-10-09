#include "remote_client_session_semaphore.h"

#include <util/stream/output.h>
#include <util/system/yassert.h>

using namespace NBus;
using namespace NBus::NPrivate;

TRemoteClientSessionSemaphore::TRemoteClientSessionSemaphore(TAtomicBase limit, const char* name)
    : Name(name)
    , Limit(limit)
    , Current(0)
    , StopSignal(0)
{
    Y_ABORT_UNLESS(limit > 0, "limit must be > 0");
    Y_UNUSED(Name);
}

TRemoteClientSessionSemaphore::~TRemoteClientSessionSemaphore() {
    Y_ABORT_UNLESS(AtomicGet(Current) == 0);
}

bool TRemoteClientSessionSemaphore::TryAcquire() {
    if (!TryWait()) {
        return false;
    }

    AtomicIncrement(Current);
    return true;
}

bool TRemoteClientSessionSemaphore::TryWait() {
    if (AtomicGet(Current) < Limit)
        return true;
    if (Y_UNLIKELY(AtomicGet(StopSignal)))
        return true;
    return false;
}

void TRemoteClientSessionSemaphore::Acquire() {
    Wait();

    Increment();
}

void TRemoteClientSessionSemaphore::Increment() {
    IncrementMultiple(1);
}

void TRemoteClientSessionSemaphore::IncrementMultiple(TAtomicBase count) {
    AtomicAdd(Current, count);
    Updated();
}

void TRemoteClientSessionSemaphore::Release() {
    ReleaseMultiple(1);
}

void TRemoteClientSessionSemaphore::ReleaseMultiple(TAtomicBase count) {
    AtomicSub(Current, count);
    Updated();
}

void TRemoteClientSessionSemaphore::Stop() {
    AtomicSet(StopSignal, 1);
    Updated();
}
