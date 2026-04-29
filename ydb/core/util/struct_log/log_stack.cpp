#include "log_stack.h"

#include <vector>

namespace NKikimr::NStructLog {

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
    LogStack.push_back(GetTop());
}

void TLogStack::Pop() {
    if (!LogStack.empty()) {
        LogStack.pop_back();
    }
}

}
