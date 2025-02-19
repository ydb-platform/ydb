#pragma once

#include <ydb-cpp-sdk/library/issue/yql_issue.h>
#include <ydb-cpp-sdk/type_switcher.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NYdb::NIssue {

TIssue IssueFromMessage(const NYdbProtos::Issue::IssueMessage& issueMessage);
void IssuesFromMessage(const ::google::protobuf::RepeatedPtrField<NYdbProtos::Issue::IssueMessage>& message, TIssues& issues);

void IssueToMessage(const TIssue& topIssue, NYdbProtos::Issue::IssueMessage* message);
void IssuesToMessage(const TIssues& issues, ::google::protobuf::RepeatedPtrField<NYdbProtos::Issue::IssueMessage>* message);

}
