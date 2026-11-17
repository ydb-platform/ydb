#include <chrono>
#include <cstdint>
#include <ctime>

extern "C" uint64_t WallNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

extern "C" uint64_t SteadyNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

extern "C" uint64_t TimeNow() {
    return std::time(nullptr);
}

namespace {
const auto WallAtLoad = WallNow();
const auto SteadyAtLoad = SteadyNow();
const auto TimeAtLoad = TimeNow();
}   // namespace

extern "C" uint64_t ConstructorWall() {
    return WallAtLoad;
}

extern "C" uint64_t ConstructorSteady() {
    return SteadyAtLoad;
}

extern "C" uint64_t ConstructorTime() {
    return TimeAtLoad;
}
