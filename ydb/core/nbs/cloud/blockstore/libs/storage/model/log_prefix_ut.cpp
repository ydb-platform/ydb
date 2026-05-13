#include "log_prefix.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogPrefixTest)
{
    Y_UNIT_TEST(CreateWithMixedTypes)
    {
        TLogPrefix prefix(
            {{"d", TString("test1")},
             {"g", 42},
             {"online", true},
             {"load", 0.87},
             {"v", (ui64)12345}});

        auto result = prefix.Get();
        UNIT_ASSERT_STRINGS_EQUAL(
            result,
            "[d:test1 g:42 online:1 load:0.87 v:12345]");
    }

    Y_UNIT_TEST(AddSingleTag)
    {
        TLogPrefix prefix({{"d", TString("test2")}});

        auto result = prefix.Get();

        UNIT_ASSERT_STRINGS_EQUAL(result, "[d:test2]");

        prefix.AddTag("v", (ui64)12345);
        prefix.AddTag("cp", TString("cp2"));

        result = prefix.Get();

        UNIT_ASSERT_STRINGS_EQUAL(result, "[d:test2 v:12345 cp:cp2]");
    }

    Y_UNIT_TEST(AddBatchTags)
    {
        TLogPrefix prefix({{"d", TString("test3")}});

        auto result = prefix.Get();

        UNIT_ASSERT_STRINGS_EQUAL(result, "[d:test3]");

        prefix.AddTags({{"v", 1234}, {"error", false}});

        result = prefix.Get();
        UNIT_ASSERT_STRINGS_EQUAL(result, "[d:test3 v:1234 error:0]");
    }
}

}   // namespace NYdb::NBS
