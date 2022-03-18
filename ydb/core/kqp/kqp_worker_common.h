#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>


namespace NKikimr::NKqp {

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
