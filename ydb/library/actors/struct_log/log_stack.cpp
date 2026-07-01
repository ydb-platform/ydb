#include "log_stack.h"

#include <vector>

namespace NActors::NStructuredLog {

namespace {
    struct TStackItem {
        TStructuredMessage Message;
        TMaybe<NActors::NLog::EComponent> Component;
    };
    thread_local std::vector<TStackItem> LogStack;
}

TStructuredMessage& TLogStack::GetTop() {
    if (LogStack.empty()) {
        LogStack.push_back({});
    }
    return LogStack.back().Message;
}

void TLogStack::Push(const TMaybe<NActors::NLog::EComponent>& component) {
    if (LogStack.empty()) {
        LogStack.push_back({});
    }
    auto topItem = LogStack.back();
    if (component.Defined()) {
        topItem.Component = component;
    }
    LogStack.push_back(topItem);
}

void TLogStack::Pop() {
    if (!LogStack.empty()) {
        LogStack.pop_back();
    }
}

NActors::NLog::EComponent TLogStack::GetComponent(NActors::NLog::EComponent defaultComponent) {
    if (LogStack.empty()) {
        return defaultComponent;
    }
    return LogStack.back().Component.GetOrElse(defaultComponent);
}

}  // namespace NActors::NStructuredLog
