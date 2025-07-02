#include "yql_warning.h"

#include <util/string/cast.h>
#include <util/string/join.h>


namespace NYql {

TWarningRule::EParseResult TWarningRule::ParseFrom(const TString& codePattern, const TString& action,
                                                   TWarningRule& result, TString& errorMessage)
{
    errorMessage.clear();
    result.IssueCodePattern_.clear();
    result.Action_ = EWarningAction::DEFAULT;

    if (!TryFromString<EWarningAction>(to_upper(action), result.Action_)) {
        errorMessage = "unknown warning action '" + action + "', expecting one of " +
                       Join(", ", EWarningAction::DEFAULT, EWarningAction::ERROR, EWarningAction::DISABLE);
        return EParseResult::PARSE_ACTION_FAIL;
    }

    if (codePattern != "*") {
        ui32 code;
        if (!TryFromString(codePattern, code)) {
            errorMessage = "unknown warning code '" + codePattern + "', expecting integer or '*'";
            return EParseResult::PARSE_PATTERN_FAIL;
        }
    }

    result.IssueCodePattern_ = codePattern;
    return EParseResult::PARSE_OK;
}

TWarningPolicy::TWarningPolicy(bool isReplay)
    : IsReplay_(isReplay)
{}

void TWarningPolicy::AddRule(const TWarningRule& rule)
{
    TString pattern = rule.GetPattern();
    if (pattern.empty()) {
        return;
    }

    if (pattern == "*" && IsReplay_) {
        return;
    }

    Rules_.push_back(rule);

    EWarningAction action = rule.GetAction();
    if (pattern == "*") {
        BaseAction_ = action;
        Overrides_.clear();
        return;
    }

    TIssueCode code;
    Y_ENSURE(TryFromString(pattern, code));

    if (action == BaseAction_) {
        Overrides_.erase(Overrides_.find(code));
    } else {
        Overrides_[code] = action;
    }
}

EWarningAction TWarningPolicy::GetAction(TIssueCode code) const
{
    auto it = Overrides_.find(code);
    return (it == Overrides_.end()) ? BaseAction_ : it->second;
}

void TWarningPolicy::Clear()
{
    BaseAction_ = EWarningAction::DEFAULT;
    Overrides_.clear();
    Rules_.clear();
}

}
