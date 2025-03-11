#include "yql_issue_message.h"

#include <ydb-cpp-sdk/type_switcher.h>

#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/join.h>

#include <deque>

namespace NYdb {
inline namespace Dev {
namespace NIssue {

TIssue IssueFromMessage(const Ydb::Issue::IssueMessage& issueMessage) {
    TIssue topIssue;
    std::deque<std::pair<TIssue*, const Ydb::Issue::IssueMessage*>> queue;
    queue.push_front(std::make_pair(&topIssue, &issueMessage));
    while (!queue.empty()) {
        TIssue& issue = *queue.back().first;
        const auto& message = *queue.back().second;
        queue.pop_back();
        TPosition position(message.position().column(), message.position().row(), message.position().file());
        TPosition endPosition(message.end_position().column(), message.end_position().row());
        if (position.HasValue()) {
            if (endPosition.HasValue()) {
                issue = TIssue(position, endPosition, message.message());
            } else {
                issue = TIssue(position, message.message());
            }
        } else {
            issue = TIssue(message.message());
        }

        for (const auto& subMessage : message.issues()) {
            auto subIssue = new TIssue();
            issue.AddSubIssue(subIssue);
            queue.push_front(std::make_pair(subIssue, &subMessage));
        }

        issue.SetCode(message.issue_code(), static_cast<ESeverity>(message.severity()));
    }
    return topIssue;
}

void IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> &message, TIssues &issues) {
    issues.Clear();
    if (message.size()) {
        issues.Reserve(message.size());
        for (auto &x : message) {
            issues.AddIssue(IssueFromMessage(x));
        }
    }
}

void IssueToMessage(const TIssue& topIssue, Ydb::Issue::IssueMessage* issueMessage) {
    std::deque<std::pair<const TIssue*, Ydb::Issue::IssueMessage*>> queue;
    queue.push_front(std::make_pair(&topIssue, issueMessage));
    while (!queue.empty()) {
        const TIssue& issue = *queue.back().first;
        auto& message = *queue.back().second;
        queue.pop_back();
        if (issue.Position) {
            auto& position = *message.mutable_position();
            position.set_row(issue.Position.Row);
            position.set_column(issue.Position.Column);
            position.set_file(NYdb::TStringType{issue.Position.File});
        }
        if (issue.EndPosition) {
            auto& endPosition = *message.mutable_end_position();
            endPosition.set_row(issue.EndPosition.Row);
            endPosition.set_column(issue.EndPosition.Column);
        }
        message.set_message(NYdb::TStringType{issue.GetMessage()});
        message.set_issue_code(issue.GetCode());
        message.set_severity(static_cast<uint32_t>(issue.GetSeverity()));

        for (auto subIssue : issue.GetSubIssues()) {
            Ydb::Issue::IssueMessage* subMessage = message.add_issues();
            queue.push_front(std::make_pair(subIssue.Get(), subMessage));
        }
    }
}

void IssuesToMessage(const TIssues& issues, ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> *message) {
    message->Clear();
    if (!issues)
        return;
    message->Reserve(issues.Size());
    for (const auto &issue : issues) {
        IssueToMessage(issue, message->Add());
    }
}

}
}
}
