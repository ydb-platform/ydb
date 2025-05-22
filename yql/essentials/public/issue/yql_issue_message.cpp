#include "yql_issue_message.h"
#include "yql_issue_message_impl.h"

#include <yql/essentials/public/issue/protos/issue_message.pb.h>

#include <util/generic/yexception.h>

#include <tuple>

namespace NYql {

using namespace NIssue::NProto;

template
TIssue IssueFromMessage<NYql::NIssue::NProto::IssueMessage>(const NYql::NIssue::NProto::IssueMessage& issueMessage);

template
void IssuesFromMessage<NYql::NIssue::NProto::IssueMessage>(const ::google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>& message, TIssues& issues);

template
void IssueToMessage<NYql::NIssue::NProto::IssueMessage>(const TIssue& topIssue, NYql::NIssue::NProto::IssueMessage* issueMessage);

template
void IssuesToMessage<NYql::NIssue::NProto::IssueMessage>(const TIssues& issues, ::google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>* message);

NIssue::NProto::IssueMessage IssueToMessage(const TIssue& topIssue) {
    NIssue::NProto::IssueMessage issueMessage;
    IssueToMessage(topIssue, &issueMessage);
    return issueMessage;
}

}

Y_DECLARE_OUT_SPEC(, google::protobuf::RepeatedPtrField<NYql::NIssue::NProto::IssueMessage>, stream, issues) {
    stream << JoinSeq("", issues);
}