#include "custom_action.h"

#include "control.h"

using namespace NLWTrace;

TCustomActionExecutor* TCustomActionFactory::Create(TProbe* probe, const TCustomAction& action, TSession* trace) const {
    auto iter = Callbacks.find(action.GetName());
    if (iter != Callbacks.end()) {
        return iter->second(probe, action, trace);
    } else {
        return nullptr;
    }
}

void TCustomActionFactory::Register(const TString& name, const TCustomActionFactory::TCallback& callback) {
    if (Callbacks.contains(name)) {
        ythrow yexception() << "duplicate custom action '" << name << "'";
    }
    Callbacks[name] = callback;
}
