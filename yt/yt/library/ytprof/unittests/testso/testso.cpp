#include <atomic>

static std::atomic<int> CallCount;

extern "C" void CallNext(void (*next)())
{
    next();
    CallCount++;
}
