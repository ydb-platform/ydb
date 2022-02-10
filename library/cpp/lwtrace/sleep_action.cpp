#include "sleep_action.h"

#include "control.h"

#include <util/system/datetime.h>

#include <stdlib.h>

using namespace NLWTrace;
using namespace NLWTrace::NPrivate;

bool TSleepActionExecutor::DoExecute(TOrbit&, const TParams&) {
    NanoSleep(NanoSeconds);
    return true;
}
