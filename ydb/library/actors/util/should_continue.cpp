#include "should_continue.h"

void TProgramShouldContinue::ShouldRestart() {
    State.store(Restart, std::memory_order_release);
}

void TProgramShouldContinue::ShouldStop(int returnCode) {
    ReturnCode.store(returnCode, std::memory_order_release);
    State.store(Stop, std::memory_order_release);
}

TProgramShouldContinue::EState TProgramShouldContinue::PollState() {
    return State.load(std::memory_order_acquire);
}

int TProgramShouldContinue::GetReturnCode() {
    return ReturnCode.load(std::memory_order_acquire);
}

void TProgramShouldContinue::Reset() {
    ReturnCode.store(0, std::memory_order_release);
    State.store(Continue, std::memory_order_release);
}
