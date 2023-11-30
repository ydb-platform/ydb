#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>

#include "yql_dq_statistics_json.h"

using namespace NYql;

Y_UNIT_TEST_SUITE(TestCommon) {

Y_UNIT_TEST(Empty) { }

Y_UNIT_TEST(ParseCounterName) {
    TString prefix = "Prefix";
    std::map<TString, TString> labels = {
        {"Label1", "Value1"},
        {"Lavel2", "Value2"}
    };
    TString name = "CounterName";

    TString counterName = TCounters::GetCounterName(prefix, labels, name);

    TString prefix2, name2;
    std::map<TString, TString> labels2;

    NCommon::ParseCounterName(
        &prefix2, &labels2, &name2, counterName
    );

    UNIT_ASSERT_EQUAL(prefix, prefix2);
    UNIT_ASSERT_EQUAL(name, name2);
    UNIT_ASSERT_EQUAL(labels, labels2);
}

Y_UNIT_TEST(CollectTaskRunnerStatisticsByStage) {
    TOperationStatistics taskRunner;

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Input", "2"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter1"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Output", "3"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter2"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter3"),
            1, 2, 3, 4, 5
        )
    );

    TStringStream result;
    {
        NYson::TYsonWriter writer(&result, NYT::NYson::EYsonFormat::Pretty);
        CollectTaskRunnerStatisticsByStage(writer, taskRunner, true);
    }
    TString expected = R"__({
    "Stage=10" = {
        "Input" = {
            "total" = {
                "Counter1" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        };
        "Output" = {
            "total" = {
                "Counter2" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        };
        "Source" = {
            "total" = {}
        };
        "Sink" = {
            "total" = {}
        };
        "Task" = {
            "total" = {
                "Counter3" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        }
    }
})__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, result.Str());
}

Y_UNIT_TEST(CollectTaskRunnerStatisticsByTask) {
    TOperationStatistics taskRunner;

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"InputChannel", "2"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter1"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"OutputChannel", "3"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter2"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter3"),
            1, 2, 3, 4, 5
        )
    );

    TStringStream result;
    {
        NYson::TYsonWriter writer(&result, NYT::NYson::EYsonFormat::Pretty);
        CollectTaskRunnerStatisticsByTask(writer, taskRunner);
    }
    TString expected = R"__({
    "Stage=10" = {
        "Task=1" = {
            "Input=2" = {
                "Counter1" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            };
            "Output=3" = {
                "Counter2" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            };
            "Generic" = {
                "Counter3" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            }
        }
    }
})__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, result.Str());
}

}
