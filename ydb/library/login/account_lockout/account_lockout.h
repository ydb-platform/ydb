#pragma once

#include <chrono>
#include <util/generic/string.h>

namespace NLogin {

struct TAccountLockout {
    struct TInitializer {
        size_t AttemptThreshold = 0;
        TString AttemptResetDuration;
    };

    size_t AttemptThreshold = 4;
    std::chrono::system_clock::duration AttemptResetDuration = std::chrono::hours(1);

    TAccountLockout();
    TAccountLockout(const TInitializer& initializer);

    void Update(const TInitializer& initializer);

private:
    void SetAttemptResetDuration(const TString& newAttemptResetDuration);
};

} // NLogin
