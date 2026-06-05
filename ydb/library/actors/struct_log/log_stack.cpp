#include "log_stack.h"

#include <vector>

namespace NActors::NStructuredLog {

namespace {
    thread_local std::vector<TStructuredMessage> LogStack;
}

TStructuredMessage& TLogStack::GetTop() {
    if (LogStack.empty()) {
        LogStack.push_back({});
    }
    return LogStack.back();
}

void TLogStack::Push() {
    auto topItem = GetTop();
    LogStack.push_back(topItem);
}

void TLogStack::Pop() {
    if (!LogStack.empty()) {
        LogStack.pop_back();
    }
}

}  // namespace NActors::NStructuredLog
