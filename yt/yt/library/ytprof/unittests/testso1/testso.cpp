#include <atomic>

static std::atomic<int> CallCount;

extern "C" void CallOtherNext(void (*next)())
{
    next();
    CallCount++;
}
