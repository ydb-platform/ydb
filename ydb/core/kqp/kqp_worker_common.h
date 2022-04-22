#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>


namespace NKikimr::NKqp {

struct TSessionShutdownState {
    TSessionShutdownState(ui32 softTimeout, ui32 hardTimeout)
        : HardTimeout(hardTimeout)
        , SoftTimeout(softTimeout)
    {}

    ui32 Step = 0;
    ui32 HardTimeout;
    ui32 SoftTimeout;

    void MoveToNextState() {
        ++Step;
    }

    ui32 GetNextTickMs() const {
        if (Step == 0) {
            return std::min(HardTimeout, SoftTimeout);
        } else if (Step == 1) {
            return std::max(HardTimeout, SoftTimeout) - std::min(HardTimeout, SoftTimeout) + 1;
        } else {
            return 50;
        }
    }

    bool SoftTimeoutReached() const {
        return Step == 1;
    }

    bool HardTimeoutReached() const {
        return Step == 2;
    }
};

inline bool IsExecuteAction(const NKikimrKqp::EQueryAction& action) {
    switch (action) {
        case NKikimrKqp::QUERY_ACTION_EXECUTE:
        case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
            return true;

        default:
            return false;
    }
}

inline bool IsQueryAllowedToLog(const TString& text) {
    static const TString user = "user";
    static const TString password = "password";
    auto itUser = std::search(text.begin(), text.end(), user.begin(), user.end(),
        [](const char a, const char b) -> bool { return std::tolower(a) == b; });
    if (itUser == text.end()) {
        return true;
    }
    auto itPassword = std::search(itUser, text.end(), password.begin(), password.end(),
        [](const char a, const char b) -> bool { return std::tolower(a) == b; });
    return itPassword == text.end();
}

}
