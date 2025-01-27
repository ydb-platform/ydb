#include "yql_warning.h"

#include <util/string/cast.h>
#include <util/string/join.h>


namespace NYql {

TWarningRule::EParseResult TWarningRule::ParseFrom(const TString& codePattern, const TString& action,
                                                   TWarningRule& result, TString& errorMessage)
{
    errorMessage.clear();
    result.IssueCodePattern.clear();
    result.Action = EWarningAction::DEFAULT;

    if (!TryFromString<EWarningAction>(to_upper(action), result.Action)) {
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

    result.IssueCodePattern = codePattern;
    return EParseResult::PARSE_OK;
}

void TWarningPolicy::AddRule(const TWarningRule& rule)
{
    TString pattern = rule.GetPattern();
    if (pattern.empty()) {
        return;
    }

    Rules.push_back(rule);

    EWarningAction action = rule.GetAction();
    if (pattern == "*") {
        BaseAction = action;
        Overrides.clear();
        return;
    }

    TIssueCode code;
    Y_ENSURE(TryFromString(pattern, code));

    if (action == BaseAction) {
        Overrides.erase(Overrides.find(code));
    } else {
        Overrides[code] = action;
    }
}

EWarningAction TWarningPolicy::GetAction(TIssueCode code) const
{
    auto it = Overrides.find(code);
    return (it == Overrides.end()) ? BaseAction : it->second;
}

void TWarningPolicy::Clear()
{
    BaseAction = EWarningAction::DEFAULT;
    Overrides.clear();
    Rules.clear();
}

}
