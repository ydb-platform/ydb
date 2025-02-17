#include <util/datetime/base.h>
#include "account_lockout.h"

namespace NLogin {

TAccountLockout::TAccountLockout() = default;

TAccountLockout::TAccountLockout(const TInitializer& initializer)
    : AttemptThreshold(initializer.AttemptThreshold)
{
    SetAttemptResetDuration(initializer.AttemptResetDuration);
}

void TAccountLockout::Update(const TInitializer& initializer) {
    AttemptThreshold = initializer.AttemptThreshold;
    SetAttemptResetDuration(initializer.AttemptResetDuration);
}

void TAccountLockout::SetAttemptResetDuration(const TString& newAttemptResetDuration) {
    TDuration attemptResetDuration = TDuration::Zero();
    if (newAttemptResetDuration.empty()) {
        AttemptResetDuration = std::chrono::system_clock::duration(std::chrono::seconds(attemptResetDuration.Seconds()));
        return;
    }
    if (TDuration::TryParse(newAttemptResetDuration, attemptResetDuration)) {
        if (attemptResetDuration.Seconds() == 0) {
            attemptResetDuration = TDuration::Zero();
        }
    }

    AttemptResetDuration = std::chrono::system_clock::duration(std::chrono::seconds(attemptResetDuration.Seconds()));
}

} // NLogin
