#include "issue.h"

#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <yql/essentials/public/issue/yql_issue_message.h>


namespace NYdb::NAdapters {

NYql::TIssue ToYqlIssue(const NYdb::NIssue::TIssue& sdkIssue) {
    Ydb::Issue::IssueMessage message;
    NYdb::NIssue::IssueToMessage(sdkIssue, &message);
    return NYql::IssueFromMessage(message);
}

NYql::TIssues ToYqlIssues(const NYdb::NIssue::TIssues& sdkIssues) {
    google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> message;
    NYdb::NIssue::IssuesToMessage(sdkIssues, &message);

    NYql::TIssues yqlIssues;
    NYql::IssuesFromMessage(message, yqlIssues);
    return yqlIssues;
}

NYdb::NIssue::TIssue ToSdkIssue(const NYql::TIssue& yqlIssue) {
    Ydb::Issue::IssueMessage message;
    NYql::IssueToMessage(yqlIssue, &message);
    return NYdb::NIssue::IssueFromMessage(message);
}

NYdb::NIssue::TIssues ToSdkIssues(const NYql::TIssues& yqlIssues) {
    google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> message;
    NYql::IssuesToMessage(yqlIssues, &message);

    NYdb::NIssue::TIssues sdkIssues;
    NYdb::NIssue::IssuesFromMessage(message, sdkIssues);
    return sdkIssues;
}

}
