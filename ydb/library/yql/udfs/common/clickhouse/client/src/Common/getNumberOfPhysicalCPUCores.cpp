#include "getNumberOfPhysicalCPUCores.h"

#include <thread>

unsigned getNumberOfPhysicalCPUCores()
{
    static const unsigned number = []
    {
        /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
        /// (Actually, only Aarch64 is supported).
        return std::thread::hardware_concurrency();
    }();
    return number;
}
