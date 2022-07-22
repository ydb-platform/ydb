#include <ydb/core/yq/libs/ydb/ydb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/providers/dq/actors/grouped_issues.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/system/env.h>

namespace NYq {

using namespace NYql;

struct AgileTimeProvider: ITimeProvider {
    AgileTimeProvider(ui64 secs): Value(TInstant::Seconds(secs)) {
    }

    void IncreaseTime(ui64 secs_to_add) {
        Value = TInstant::Seconds(Value.Seconds() + secs_to_add);
    }

    TInstant Now() {
        return Value;
    }

    TInstant Value = Now();
};

TIntrusivePtr<AgileTimeProvider> CreateAgileTimeProvider(ui64 initial) {
    return TIntrusivePtr<AgileTimeProvider>(new AgileTimeProvider(initial));
}

Y_UNIT_TEST_SUITE(TestIssuesGrouping) {
    Y_UNIT_TEST(ShouldCountEveryIssue) {
        int iterations = 4;
        int issueTypes = 4;
        int expectedNumberOfIssues[issueTypes];

        for (int i = 0; i < iterations; ++i) {
            NDq::GroupedIssues holder(CreateDefaultTimeProvider());
            for (int j = 0; j < issueTypes; ++j) {
                expectedNumberOfIssues[j] = rand() % 100;
                for (int k = 0; k < expectedNumberOfIssues[j]; ++k) {
                    holder.AddIssue(TIssue(ToString(j)));
                }
            }
            UNIT_ASSERT(holder.Issues.size() == 4);
            for (int j = 0; j < issueTypes; ++j) {
                int encounters = holder.Issues[TIssue(ToString(j))].EncountersNumber;
                UNIT_ASSERT_C(encounters == expectedNumberOfIssues[j],
                    "expected " << expectedNumberOfIssues[j] << " got " << encounters << " at index " << j << " at iteration " << i);
            }
        }
    }

    Y_UNIT_TEST(ShouldRemoveOldIssues) {
        TIntrusivePtr<AgileTimeProvider> timeProvider = CreateAgileTimeProvider(1);
        NDq::GroupedIssues holder(timeProvider);
        holder.IssueExpiration = TDuration::Seconds(5);
        holder.AddIssue(TIssue("a"));
        timeProvider->IncreaseTime(10);
        holder.AddIssue(TIssue("b"));
        UNIT_ASSERT_C(holder.Issues.size() < 2, "old issue is not removed");
    }

    Y_UNIT_TEST(ShouldRemoveIfMoreThanMaxIssues) {
        NDq::GroupedIssues holder(CreateDefaultTimeProvider());
        for (int i = 0; i < 30; ++i) {
            holder.AddIssue(TIssue(ToString(i)));
        }
        UNIT_ASSERT_C(holder.Issues.size() <= holder.MaxIssues, "overflow issues are not removed");
    }

    Y_UNIT_TEST(ShouldRemoveTheOldestIfMoreThanMaxIssues) {
        TIntrusivePtr<AgileTimeProvider> timeProvider = CreateAgileTimeProvider(1);
        NDq::GroupedIssues holder(timeProvider);
        auto eldery = TIssue("there is a simple honor in poverty");
        holder.AddIssue(eldery);
        timeProvider->IncreaseTime(1);
        for (int i = 0; i < 20; ++i) {
            holder.AddIssue(TIssue(ToString(i)));
        }
        UNIT_ASSERT_C(!holder.Issues.contains(eldery), "the oldest issue is not removed");
    }
}
} // namespace NYq
