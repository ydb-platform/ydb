#include "sandbox_static.h"

extern "C" const char* GetSandboxTaskId() {
#if defined(SANDBOX_TASK_ID)
    return SANDBOX_TASK_ID;
#else
    return "";
#endif
}
