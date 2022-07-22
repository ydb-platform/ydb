#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/time_provider/time_provider.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <algorithm>



namespace NYql::NDq {
struct GroupedIssues {
    NYql::TIssues ToIssues();

    explicit GroupedIssues(TIntrusivePtr<ITimeProvider> timeProvider): TimeProvider(timeProvider) {
    }

    struct IssueGroupInfo {
        TString InfoString();

        TInstant LastEncountered = TInstant::Zero();
        uint64_t EncountersNumber;
    };

    bool Empty();

    void AddIssue(const NYql::TIssue& issue);

    void AddIssues(const NYql::TIssues& issues);

    void RemoveOldIssues();

    THashMap<NYql::TIssue, IssueGroupInfo> Issues;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TDuration IssueExpiration = TDuration::Hours(1);
    ui64 MaxIssues = 20;

};
} // namespace NYql::NDq
