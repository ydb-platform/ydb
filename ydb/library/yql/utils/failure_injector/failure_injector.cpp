#include "failure_injector.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/generic/singleton.h>

namespace NYql {

void TFailureInjector::Activate() {
    Singleton<TFailureInjector>()->ActivateImpl();
}

void TFailureInjector::Set(std::string_view name, ui64 skip, ui64 countOfFails) {
    Singleton<TFailureInjector>()->SetImpl(name, skip, countOfFails);
}

void TFailureInjector::Reach(std::string_view name, std::function<void()> action) {
    Singleton<TFailureInjector>()->ReachImpl(name, action);
}

void TFailureInjector::ActivateImpl() {
    Enabled_.store(true);
    YQL_LOG(DEBUG) << "TFailureInjector::Activate";
}

THashMap<TString, TFailureInjector::TFailureSpec> TFailureInjector::GetCurrentState() {
    return Singleton<TFailureInjector>()->GetCurrentStateImpl();
}

THashMap<TString, TFailureInjector::TFailureSpec> TFailureInjector::GetCurrentStateImpl() {
    THashMap<TString, TFailureInjector::TFailureSpec> copy;
    with_lock(Lock) {
        copy = FailureSpecs;
    }
    return copy;
}

void TFailureInjector::ReachImpl(std::string_view name, std::function<void()> action) {
    if (!Enabled_.load()) {
        return;
    }
    with_lock(Lock) {
        if (auto failureSpec = FailureSpecs.FindPtr(name)) {
            YQL_LOG(DEBUG) << "TFailureInjector::Reach: " << name << ", Skip=" << failureSpec->Skip << ", Fails=" << failureSpec->CountOfFails;
            if (failureSpec->Skip > 0) {
                --failureSpec->Skip;
            } else if (failureSpec->CountOfFails > 0) {
                YQL_LOG(DEBUG) << "TFailureInjector::OnReach: " << name;
                --failureSpec->CountOfFails;
                action();
            }
        }
    }
}

void TFailureInjector::SetImpl(std::string_view name, ui64 skip, ui64 countOfFails) {
    with_lock(Lock) {
        YQL_ENSURE(countOfFails > 0, "failure " << name << ", 'countOfFails' must be positive");
        FailureSpecs[TString{name}] = TFailureSpec{skip, countOfFails};
    }
}

} // NYql
