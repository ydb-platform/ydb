#pragma once

#include "yql_issue_id.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

#if defined(_win_)
#undef ERROR
#endif

namespace NYql {

enum class EWarningAction {
    DISABLE,
    ERROR,
    DEFAULT,
};

class TWarningRule {
public:
    const TString& GetPattern() const { return IssueCodePattern_; }
    EWarningAction GetAction() const  { return Action_; }

    enum class EParseResult { PARSE_OK, PARSE_PATTERN_FAIL, PARSE_ACTION_FAIL };
    static EParseResult ParseFrom(const TString& codePattern, const TString& action,
                                  TWarningRule& result, TString& errorMessage);
private:
    TString IssueCodePattern_;
    EWarningAction Action_ = EWarningAction::DEFAULT;
};

using TWarningRules = TVector<TWarningRule>;

class TWarningPolicy {
public:
    TWarningPolicy(bool isReplay = false);

    void AddRule(const TWarningRule& rule);

    EWarningAction GetAction(TIssueCode code) const;

    const TWarningRules& GetRules() const { return Rules_; }

    void Clear();

private:
    const  bool IsReplay_;
    TWarningRules Rules_;
    EWarningAction BaseAction_ = EWarningAction::DEFAULT;
    THashMap<TIssueCode, EWarningAction> Overrides_;
};

}
