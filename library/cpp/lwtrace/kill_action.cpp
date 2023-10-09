#include "kill_action.h"

#ifndef _win_
#include <sys/types.h>
#include <signal.h>
#endif

#include <stdlib.h>

using namespace NLWTrace;
using namespace NLWTrace::NPrivate;

bool TKillActionExecutor::DoExecute(TOrbit&, const TParams&) {
#ifdef _win_
    abort();
#else
    int r = kill(getpid(), SIGABRT);
    Y_ABORT_UNLESS(r == 0, "kill failed");
    return true;
#endif
}
