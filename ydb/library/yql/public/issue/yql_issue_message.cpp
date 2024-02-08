#include "yql_issue_message.h"

#include <ydb/library/yql/public/issue/protos/issue_message.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/join.h>

#include <tuple>

namespace NYql {

using namespace NIssue::NProto;

template<typename TIssueMessage>
TIssue IssueFromMessage(const TIssueMessage& issueMessage) {
    TIssue topIssue;
    TDeque<std::pair<TIssue*, const TIssueMessage*>> queue;
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

template<typename TIssueMessage>
void IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<TIssueMessage> &message, TIssues &issues) {
    issues.Clear();
    if (message.size()) {
        issues.Reserve(message.size());
        for (auto &x : message)
            issues.AddIssue(IssueFromMessage(x));
    }
}

template<typename TIssueMessage>
void IssueToMessage(const TIssue& topIssue, TIssueMessage* issueMessage) {
    TDeque<std::pair<const TIssue*, TIssueMessage*>> queue;
    queue.push_front(std::make_pair(&topIssue, issueMessage));
    while (!queue.empty()) {
        const TIssue& issue = *queue.back().first;
        auto& message = *queue.back().second;
        queue.pop_back();
        if (issue.Position) {
            auto& position = *message.mutable_position();
            position.set_row(issue.Position.Row);
            position.set_column(issue.Position.Column);
            position.set_file(issue.Position.File);
        }
        if (issue.EndPosition) {
            auto& endPosition = *message.mutable_end_position();
            endPosition.set_row(issue.EndPosition.Row);
            endPosition.set_column(issue.EndPosition.Column);
        }
        message.set_message(issue.GetMessage());
        message.set_issue_code(issue.GetCode());
        message.set_severity(issue.GetSeverity());

        for (auto subIssue : issue.GetSubIssues()) {
            TIssueMessage* subMessage = message.add_issues();
            queue.push_front(std::make_pair(subIssue.Get(), subMessage));
        }
    }
}

template<typename TIssueMessage>
void IssuesToMessage(const TIssues& issues, ::google::protobuf::RepeatedPtrField<TIssueMessage> *message) {
    message->Clear();
    if (!issues)
        return;
    message->Reserve(issues.Size());
    for (const auto &issue : issues) {
        IssueToMessage(issue, message->Add());
    }
}

template
TIssue IssueFromMessage<Ydb::Issue::IssueMessage>(const Ydb::Issue::IssueMessage& issueMessage);
template
TIssue IssueFromMessage<NYql::NIssue::NProto::IssueMessage>(const NYql::NIssue::NProto::IssueMessage& issueMessage);

template
void IssuesFromMessage<Ydb::Issue::IssueMessage>(const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& message, TIssues& issues);
template
void IssuesFromMessage<NYql::NIssue::NProto::IssueMessage>(const ::google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>& message, TIssues& issues);

template
void IssueToMessage<Ydb::Issue::IssueMessage>(const TIssue& topIssue, Ydb::Issue::IssueMessage* issueMessage);
template
void IssueToMessage<NYql::NIssue::NProto::IssueMessage>(const TIssue& topIssue, NYql::NIssue::NProto::IssueMessage* issueMessage);

template
void IssuesToMessage<Ydb::Issue::IssueMessage>(const TIssues& issues, ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* message);
template
void IssuesToMessage<NYql::NIssue::NProto::IssueMessage>(const TIssues& issues, ::google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>* message);

NIssue::NProto::IssueMessage IssueToMessage(const TIssue& topIssue) {
    NIssue::NProto::IssueMessage issueMessage;
    IssueToMessage(topIssue, &issueMessage);
    return issueMessage;
}

TString IssueToBinaryMessage(const TIssue& issue) {
    TString result;
    Ydb::Issue::IssueMessage protobuf;
    IssueToMessage(issue, &protobuf);
    Y_PROTOBUF_SUPPRESS_NODISCARD protobuf.SerializeToString(&result);
    return result;
}

TIssue IssueFromBinaryMessage(const TString& binaryMessage) {
    Ydb::Issue::IssueMessage protobuf;
    if (!protobuf.ParseFromString(binaryMessage)) {
        ythrow yexception() << "unable to parse binary string as issue protobuf";
    }
    return IssueFromMessage(protobuf);
}

}

Y_DECLARE_OUT_SPEC(, google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>, stream, issues) {
    stream << JoinSeq("", issues);
}
Y_DECLARE_OUT_SPEC(, google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>, stream, issues) {
    stream << JoinSeq("", issues);
}