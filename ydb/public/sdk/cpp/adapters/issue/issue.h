#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue.h>


namespace NYdb::NAdapters {

NYql::TIssue ToYqlIssue(const NYdb::NIssue::TIssue& sdkIssue);
NYql::TIssues ToYqlIssues(const NYdb::NIssue::TIssues& sdkIssues);

NYdb::NIssue::TIssue ToSdkIssue(const NYql::TIssue& yqlIssue);
NYdb::NIssue::TIssues ToSdkIssues(const NYql::TIssues& yqlIssues);

}
