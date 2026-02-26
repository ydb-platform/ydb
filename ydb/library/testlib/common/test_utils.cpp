#include "test_utils.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NTestUtils {

void WaitFor(TDuration timeout, const TString& description, std::function<bool(TString&)> predicate) {
    const TInstant start = TInstant::Now();
    TString errorString;
    while (TInstant::Now() - start <= timeout) {
        if (predicate(errorString)) {
            return;
        }

        Cerr << "Wait " << description << " " << TInstant::Now() - start << ": " << errorString << "\n";
        Sleep(TDuration::Seconds(1));
    }

    UNIT_FAIL("Waiting " << description << " timeout. Spent time " << TInstant::Now() - start << " exceeds limit " << timeout << ", last error: " << errorString);
}

void WaitFor(TDuration timeout, const TString& description, std::function<bool()> predicate) {
    WaitFor(timeout, description, [predicate](TString&) {
        return predicate();
    });
}

}  // namespace NTestUtils
