#pragma once

#include "yql_issue.h"
#include "yql_warning.h"

#include <util/generic/maybe.h>
#include <util/generic/stack.h>
#include <util/generic/hash_set.h>

#include <array>

namespace NYql {

class TIssueManager: private TNonCopyable {
public:
    void AddScope(std::function<TIssuePtr()> fn);
    void LeaveScope();
    void RaiseIssue(const TIssue& issue);
    void RaiseIssues(const TIssues& issues);
    bool RaiseWarning(TIssue issue);
    void AddIssues(const TIssues& errors);
    void AddIssues(const TPosition& pos, const TIssues& issues);

    TIssues GetIssues();
    TIssues GetCompletedIssues() const;

    void Reset(const TIssues& issues);
    void Reset();

    void AddWarningRule(const TWarningRule &rule);
    void SetWarningToErrorTreatMessage(const TString& msg);

    void SetIssueCountLimit(size_t limit) {
        IssueLimit_ = limit;
    }

    void RaiseIssueForEmptyScope();

private:
    TIssuePtr CheckUniqAndLimit(const TIssue& issue);
    TIssuePtr CheckUniqAndLimit(TIssuePtr issue);
    void LeaveAllScopes();
    bool HasOpenScopes() const;

    struct TIssueHash {
        ui64 operator()(const TIssuePtr& p) {
            return p->Hash();
        }
    };
    struct TIssueEqual {
        bool operator()(const TIssuePtr& l, const TIssuePtr& r) {
            return *l == *r;
        }
    };
    TStack<std::pair<TMaybe<TIssuePtr>, std::function<TIssuePtr()>>> RawIssues_;
    TIssues CompletedIssues_;
    TMaybe<TString> WarningToErrorTreatMessage_;
    TWarningPolicy WarningPolicy_;
    std::array<TIssuePtr, NYql::TSeverityIds::ESeverityId_ARRAYSIZE> OverflowIssues_;
    std::array<THashSet<TIssuePtr, TIssueHash, TIssueEqual>, NYql::TSeverityIds::ESeverityId_ARRAYSIZE> UniqueIssues_;
    size_t IssueLimit_ = 0;
};

class TIssueScopeGuard: private TNonCopyable {
    TIssueManager& Manager_;
public:
    TIssueScopeGuard(TIssueManager& manager, std::function<TIssuePtr()> fn)
        : Manager_(manager)
    {
        Manager_.AddScope(fn);
    }

    void RaiseIssueForEmptyScope() {
        Manager_.RaiseIssueForEmptyScope();
    }

    ~TIssueScopeGuard()
    {
        Manager_.LeaveScope();
    }
};

}
