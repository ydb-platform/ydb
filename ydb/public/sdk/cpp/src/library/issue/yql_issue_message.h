#pragma once

#include <ydb-cpp-sdk/library/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NYdb {
inline namespace Dev {
namespace NIssue {

TIssue IssueFromMessage(const Ydb::Issue::IssueMessage& issueMessage);
void IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& message, TIssues& issues);

void IssueToMessage(const TIssue& topIssue, Ydb::Issue::IssueMessage* message);
void IssuesToMessage(const TIssues& issues, ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* message);

}
}
}
