#include "frequency.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(FrequencyTests) {

    Y_UNIT_TEST(FrequencyDataJson) {
        TFrequencyData actual = Pruned(ParseJsonFrequencyData(R"([
            {"parent":"FUNC","rule":"ABC","sum":1},
            {"parent":"TYPE","rule":"BIGINT","sum":7101},
            {"parent":"KEYWORD","rule":"UNION","sum":65064443},
            {"parent":"MODULE_FUNC","rule":"Compress::BZip2","sum":2},
            {"parent":"MODULE","rule":"re2","sum":3094},
            {"parent":"READ_HINT","rule":"COLUMNS","sum":826110},
            {"parent":"INSERT_HINT","rule":"COLUMN_GROUPS","sum":225},
            {"parent":"TRule_action_or_subquery_args","rule":"TRule_action_or_subquery_args.Block2","sum":4874480}
        ])"));

        TFrequencyData expected = {
            .Keywords = {
                {"union", 65064443},
            },
            .Types = {
                {"bigint", 7101},
            },
            .Functions = {
                {"abc", 1},
                {"compress::bzip2", 2},
            },
            .Hints = {
                {"columns", 826110},
                {"columngroups", 225},
            },
        };

        UNIT_ASSERT_VALUES_EQUAL(actual.Keywords, expected.Keywords);
        UNIT_ASSERT_VALUES_EQUAL(actual.Types, expected.Types);
        UNIT_ASSERT_VALUES_EQUAL(actual.Functions, expected.Functions);
        UNIT_ASSERT_VALUES_EQUAL(actual.Hints, expected.Hints);
    }

    Y_UNIT_TEST(FrequencyDataResouce) {
        TFrequencyData data = LoadFrequencyData();
        Y_UNUSED(data);
    }

} // Y_UNIT_TEST_SUITE(FrequencyTests)
