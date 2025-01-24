#include "yql_issue_utils.h"
#include "yql_issue.h"
#include "yql_issue_id.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/subst.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TIssueUtilsTest) {
    Y_UNIT_TEST(TruncLevels1) {
        auto level0 = MakeIntrusive<TIssue>("level0");
        auto level1 = MakeIntrusive<TIssue>("level1");
        auto level2 = MakeIntrusive<TIssue>("level2");
        auto level30 = MakeIntrusive<TIssue>("level30");
        auto level31 = MakeIntrusive<TIssue>("level31");
        auto level40 = MakeIntrusive<TIssue>("level40");
        auto level41 = MakeIntrusive<TIssue>("level41");
        auto level51 = MakeIntrusive<TIssue>("level51");

        /*
         *           * 0
         *           |
         *           * 1
         *           |
         *           * 2 --
         *           |     |
         *           * 30  * 31
         *           |     |
         *           * 40  * 41
         *                 |
         *                 * 51
         */

        level0->AddSubIssue(level1);
        level1->AddSubIssue(level2);
        level2->AddSubIssue(level30);
        level2->AddSubIssue(level31);
        level30->AddSubIssue(level40);
        level31->AddSubIssue(level41);
        level41->AddSubIssue(level51);

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(4).SetKeepTailLevels(2))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: level1
        <main>: Error: (skipped levels)
            <main>: Error: level30
                <main>: Error: level40
            <main>: Error: level41
                <main>: Error: level51
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(3).SetKeepTailLevels(1))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: level1
        <main>: Error: (skipped levels)
            <main>: Error: level40
            <main>: Error: level51
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }
    }

    Y_UNIT_TEST(TruncLevels2) {
        auto level0 = MakeIntrusive<TIssue>("level0");
        auto level1 = MakeIntrusive<TIssue>("level1");
        auto level2 = MakeIntrusive<TIssue>("level2");
        auto level3 = MakeIntrusive<TIssue>("level3");
        auto level40 = MakeIntrusive<TIssue>("level40");
        auto level41 = MakeIntrusive<TIssue>("level41");

        /*
         *           * 0
         *           |
         *           * 1
         *           |
         *           * 2
         *           |
         *           * 3 --
         *           |     |
         *           * 40  * 41
         */

        level0->AddSubIssue(level1);
        level1->AddSubIssue(level2);
        level2->AddSubIssue(level3);
        level3->AddSubIssue(level40);
        level3->AddSubIssue(level41);

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(4).SetKeepTailLevels(2))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: level1
        <main>: Error: (skipped levels)
            <main>: Error: level3
                <main>: Error: level40
                <main>: Error: level41
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }
    }

    Y_UNIT_TEST(TruncLevels3) {
        auto level0 = MakeIntrusive<TIssue>("level0");
        auto level1 = MakeIntrusive<TIssue>("level1");
        auto level2 = MakeIntrusive<TIssue>("level2");
        auto level3 = MakeIntrusive<TIssue>("level3");
        auto level40 = MakeIntrusive<TIssue>("level40");
        auto level41 = MakeIntrusive<TIssue>("level41");
        auto level50 = MakeIntrusive<TIssue>("level50");

        /*
         *           * 0
         *           |
         *           * 1
         *           |
         *           * 2
         *           |
         *           * 3 --
         *           |     |
         *           * 40  |
         *           |     |
         *           * 50  * 41
         */

        level0->AddSubIssue(level1);
        level1->AddSubIssue(level2);
        level2->AddSubIssue(level3);
        level3->AddSubIssue(level40);
        level3->AddSubIssue(level41);
        level40->AddSubIssue(level50);

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(4).SetKeepTailLevels(1))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: level1
        <main>: Error: level2
            <main>: Error: (skipped levels)
                <main>: Error: level41
                <main>: Error: level50
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(4).SetKeepTailLevels(2))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: level1
        <main>: Error: (skipped levels)
            <main>: Error: level3
                <main>: Error: level41
            <main>: Error: level40
                <main>: Error: level50
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }

        {
            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(4).SetKeepTailLevels(3))}).ToString();
            const auto expected =
R"___(<main>: Error: level0
    <main>: Error: (skipped levels)
        <main>: Error: level2
            <main>: Error: level3
                <main>: Error: level40
                    <main>: Error: level50
                <main>: Error: level41
)___";
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }
    }

    Y_UNIT_TEST(KeepSeverity) {
        const auto templ =
R"___(<main>: {severity}: level0, code: 1
    <main>: {severity}: level1, code: 1
)___";
        for (auto severity: {ESeverity::TSeverityIds_ESeverityId_S_INFO, ESeverity::TSeverityIds_ESeverityId_S_WARNING, ESeverity::TSeverityIds_ESeverityId_S_ERROR, ESeverity::TSeverityIds_ESeverityId_S_FATAL}) {

            auto level0 = MakeIntrusive<TIssue>(TIssue("level0").SetCode(1, severity));
            auto level1 = MakeIntrusive<TIssue>(TIssue("level1").SetCode(1, severity));

            level0->AddSubIssue(level1);

            const auto res = TIssues({TruncateIssueLevels(*level0, TTruncateIssueOpts().SetMaxLevels(15).SetKeepTailLevels(3))}).ToString();
            const auto expected = SubstGlobalCopy<TString, TString>(templ, "{severity}", SeverityToString(severity));
            UNIT_ASSERT_STRINGS_EQUAL(res, expected);
        }
    }
}
