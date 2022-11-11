#include "yql_issue_manager.h"

#include <util/string/cast.h>
#include <util/string/builder.h>

using namespace NYql;

void TIssueManager::AddScope(std::function<TIssuePtr()> fn) {
    RawIssues_.emplace(std::make_pair(TMaybe<TIssuePtr>(), fn));
}

void TIssueManager::LeaveScope() {
    Y_ASSERT(HasOpenScopes());
    if (RawIssues_.size() == 1) {
        // Last scope, just dump it
        auto maybeIssue = RawIssues_.top().first;
        if (maybeIssue) {
            if ((*maybeIssue)->GetCode() == Max<ui32>()) {
                for (const auto& nestedIssue : (*maybeIssue.Get())->GetSubIssues()) {
                    CompletedIssues_.AddIssue(*nestedIssue);
                }
            } else {
                CompletedIssues_.AddIssue(**maybeIssue);
            }
        }
        RawIssues_.pop();
        return;
    }

    if (RawIssues_.top().first.Empty()) {
        // No issues in this scope
        RawIssues_.pop();
        return;
    }

    auto subIssue = *RawIssues_.top().first;
    if (subIssue->GetSubIssues().size() == 1) {
        auto nestedIssue = subIssue->GetSubIssues().front();
        if (!nestedIssue->GetSubIssues().empty() && nestedIssue->Position == subIssue->Position && nestedIssue->EndPosition == subIssue->EndPosition) {
            auto msg = subIssue->GetMessage();
            if (nestedIssue->GetMessage()) {
                if (msg) {
                    msg.append(", ");
                }
                msg.append(nestedIssue->GetMessage());
            }
            subIssue = nestedIssue;
            subIssue->SetMessage(msg);
        }
    }
    RawIssues_.pop();
    if (RawIssues_.top().first.Empty()) {
        RawIssues_.top().first = RawIssues_.top().second();
        if (!*RawIssues_.top().first) {
            RawIssues_.top().first = new TIssue();
            (*RawIssues_.top().first)->SetCode(Max<ui32>(), ESeverity::TSeverityIds_ESeverityId_S_INFO);
        } else {
           (*RawIssues_.top().first)->Severity = ESeverity::TSeverityIds_ESeverityId_S_INFO;
        }
    }

    if (subIssue->GetCode() == Max<ui32>()) {
        for (const auto& nestedIssue : subIssue->GetSubIssues()) {
            RawIssues_.top().first->Get()->AddSubIssue(nestedIssue);
        }
    } else {
        RawIssues_.top().first->Get()->AddSubIssue(subIssue);
    }
}

void TIssueManager::RaiseIssueForEmptyScope() {
    if (RawIssues_.top().first.Empty()) {
        TIssuePtr materialized = RawIssues_.top().second();
        if (auto p = CheckUniqAndLimit(materialized)) {
            RawIssues_.top().first = p;
        }
    }
}

void TIssueManager::LeaveAllScopes() {
    while (!RawIssues_.empty()) {
        LeaveScope();
    }
}

TIssuePtr TIssueManager::CheckUniqAndLimit(TIssuePtr issue) {
    const auto severity = issue->GetSeverity();
    if (OverflowIssues_[severity]) {
        return {};
    }
    if (UniqueIssues_[severity].contains(issue)) {
        return {};
    }
    if (IssueLimit_ && UniqueIssues_[severity].size() == IssueLimit_) {
        OverflowIssues_[severity] = MakeIntrusive<TIssue>(TStringBuilder()
            << "Too many " << SeverityToString(issue->GetSeverity()) << " issues");
        OverflowIssues_[severity]->Severity = severity;
        return {};
    }
    UniqueIssues_[severity].insert(issue);
    return issue;
}

TIssuePtr TIssueManager::CheckUniqAndLimit(const TIssue& issue) {
    const auto severity = issue.GetSeverity();
    if (OverflowIssues_[severity]) {
        return {};
    }
    return CheckUniqAndLimit(MakeIntrusive<TIssue>(issue));
}

void TIssueManager::RaiseIssue(const TIssue& issue) {
    TIssuePtr p = CheckUniqAndLimit(issue);
    if (!p) {
        return;
    }
    if (RawIssues_.empty()) {
        CompletedIssues_.AddIssue(issue);
        return;
    }
    if (RawIssues_.top().first.Empty()) {
        RawIssues_.top().first = RawIssues_.top().second();
        if (!*RawIssues_.top().first) {
            RawIssues_.top().first = new TIssue();
            (*RawIssues_.top().first)->SetCode(Max<ui32>(), ESeverity::TSeverityIds_ESeverityId_S_INFO);
        } else {
            (*RawIssues_.top().first)->Severity = ESeverity::TSeverityIds_ESeverityId_S_INFO;
       }
    }
    RawIssues_.top().first->Get()->AddSubIssue(p);
}

void TIssueManager::RaiseIssues(const TIssues& issues) {
    for (const auto& x : issues) {
        RaiseIssue(x);
    }
}

bool TIssueManager::RaiseWarning(TIssue issue) {
    bool isWarning = true;
    if (issue.GetSeverity() == ESeverity::TSeverityIds_ESeverityId_S_WARNING) {
        const auto action = WarningPolicy_.GetAction(issue.GetCode());
        switch (action) {
            case EWarningAction::DISABLE:
                return isWarning;
            case EWarningAction::ERROR:
                issue.Severity = ESeverity::TSeverityIds_ESeverityId_S_ERROR;
                if (WarningToErrorTreatMessage_) {
                    TIssue newIssue;
                    newIssue.SetCode(issue.GetCode(), ESeverity::TSeverityIds_ESeverityId_S_ERROR);
                    newIssue.SetMessage(WarningToErrorTreatMessage_.GetRef());
                    newIssue.AddSubIssue(new TIssue(issue));
                    issue = newIssue;
                }
                isWarning = false;
                break;
            case EWarningAction::DEFAULT:
                break;
            default:
                Y_ENSURE(false, "Unknown action");
        }
    }

    RaiseIssue(issue);
    return isWarning;
}

bool TIssueManager::HasOpenScopes() const {
    return !RawIssues_.empty();
}

TIssues TIssueManager::GetIssues() {
    auto tmp = RawIssues_;
    LeaveAllScopes();
    auto result = GetCompletedIssues();
    RawIssues_ = tmp;
    return result;
}

TIssues TIssueManager::GetCompletedIssues() const {
    TIssues res;
    for (auto& p: OverflowIssues_) {
        if (p) {
            res.AddIssue(*p);
        }
    }
    res.AddIssues(CompletedIssues_);
    return res;
}

void TIssueManager::AddIssues(const TIssues& issues) {
    for (auto& issue: issues) {
        if (auto p = CheckUniqAndLimit(issue)) {
            CompletedIssues_.AddIssue(*p);
        }
    }
}

void TIssueManager::AddIssues(const TPosition& pos, const TIssues& issues) {
    for (auto& issue: issues) {
        if (auto p = CheckUniqAndLimit(TIssue(pos, issue.GetMessage()))) {
            CompletedIssues_.AddIssue(*p);
        }
    }
}

void TIssueManager::Reset(const TIssues& issues) {
    for (auto& p: OverflowIssues_) {
        p.Drop();
    }

    for (auto& s: UniqueIssues_) {
        s.clear();
    }
    CompletedIssues_.Clear();

    while (!RawIssues_.empty()) {
        RawIssues_.pop();
    }
    AddIssues(issues);
}

void TIssueManager::Reset() {
    Reset(TIssues());
}

void TIssueManager::AddWarningRule(const TWarningRule &rule)
{
    WarningPolicy_.AddRule(rule);
}

void TIssueManager::SetWarningToErrorTreatMessage(const TString& msg) {
    WarningToErrorTreatMessage_ = msg;
}
