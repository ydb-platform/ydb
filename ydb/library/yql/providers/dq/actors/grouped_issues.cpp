#include "grouped_issues.h"


NYql::TIssues NYql::NDq::GroupedIssues::ToIssues() {
    NYql::TIssues issues;
    TVector<std::pair<NYql::TIssue, IssueGroupInfo>> issueVector;

    for (auto& p: Issues) {
        issueVector.push_back(p);
    }

    std::sort(issueVector.begin(), issueVector.end(), [](const auto& a, const auto& b) -> bool {
        return a.second.LastEncountered > b.second.LastEncountered;
    });

    for (auto& [issue, meta]: issueVector) {
        auto modified_issue_message = issue.GetMessage() + " " + meta.InfoString();
        auto modified_issue = NYql::TIssue(issue.Position, issue.EndPosition, modified_issue_message);
        for (const auto& subIssue: issue.GetSubIssues()) {
            modified_issue.AddSubIssue(subIssue);
        }
        issues.AddIssue(modified_issue);
    }
    return issues;
}

TString NYql::NDq::GroupedIssues::IssueGroupInfo::InfoString()  {
    TString message = TStringBuilder() <<
    "(appeared " << EncountersNumber <<
    (EncountersNumber == 1?" time at ":" times, last at ") <<
    LastEncountered.ToString() << ")";

    return message;
}

bool NYql::NDq::GroupedIssues::Empty() {
    return Issues.empty();
}

void NYql::NDq::GroupedIssues::AddIssue(const NYql::TIssue& issue) {
    auto& inserted = Issues[issue];
    ++inserted.EncountersNumber;
    inserted.LastEncountered = TimeProvider->Now();
    RemoveOldIssues();
}

void NYql::NDq::GroupedIssues::RemoveOldIssues() {
    NYql::TIssue oldest;
    IssueGroupInfo oldestInfo;
    TVector<TIssue> toRemove;
    for (auto& [issue, meta]: Issues) {
        if (meta.LastEncountered < oldestInfo.LastEncountered ||
            oldestInfo.LastEncountered == TInstant::Zero()) {
            oldest = issue;
            oldestInfo = meta;
        }
        if (meta.LastEncountered + IssueExpiration <= TimeProvider->Now()) {
            toRemove.push_back(issue);
        }
    }
    if (Issues.size() > MaxIssues) {
        toRemove.push_back(oldest);
    }
    for (auto& issue: toRemove) {
        if (Issues.contains(issue)) { // oldest could be added twice
            Issues.erase(issue);
        }
    }
}

void NYql::NDq::GroupedIssues::AddIssues(const NYql::TIssues& issues) {
    for (auto& issue: issues) {
        AddIssue(issue);
    }
}
