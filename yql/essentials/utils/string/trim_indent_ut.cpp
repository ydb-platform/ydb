#include "trim_indent.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TrimIndentTests) {

Y_UNIT_TEST(Empty) {
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent(""), "");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent(" "), "");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("  "), "");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("   "), "");

    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("\n"), "");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("\n\n"), "");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("\n\n\n"), "\n");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("\n\n\n\n"), "\n\n");
    UNIT_ASSERT_VALUES_EQUAL(TrimIndent("\n\n\n\n\n"), "\n\n\n");
}

Y_UNIT_TEST(Blank) {
    TString x;

    x = TrimIndent(R"()");
    UNIT_ASSERT_VALUES_EQUAL(x, "");

    x = TrimIndent(R"(
)");
    UNIT_ASSERT_VALUES_EQUAL(x, "");

    x = TrimIndent(R"(
    )");
    UNIT_ASSERT_VALUES_EQUAL(x, "");

    x = TrimIndent(R"(

    )");
    UNIT_ASSERT_VALUES_EQUAL(x, "");
}

Y_UNIT_TEST(WithPrefixSuffix) {
    TString x;

    x = TrimIndent(R"(x
    )");
    UNIT_ASSERT_VALUES_EQUAL(x, "x");

    x = TrimIndent(R"(
    x)");
    UNIT_ASSERT_VALUES_EQUAL(x, "x");

    x = TrimIndent(R"(x
    x)");
    UNIT_ASSERT_VALUES_EQUAL(x, "x\n    x");

    x = TrimIndent(R"(x

    )");
    UNIT_ASSERT_VALUES_EQUAL(x, "x\n");

    x = TrimIndent(R"(x
        y
    )");
    UNIT_ASSERT_VALUES_EQUAL(x, "x\n        y");

    x = TrimIndent(R"(
        x
    y)");
    UNIT_ASSERT_VALUES_EQUAL(x, "    x\ny");
}

Y_UNIT_TEST(Simple) {
    TString x;
    TString y;

    x = TrimIndent(R"sql(
        COMMIT;
    )sql");
    UNIT_ASSERT_VALUES_EQUAL(x, "COMMIT;");

    x = TrimIndent(R"sql(
        $x = (
            SELECT
                1
        );
    )sql");
    y = TrimIndent(R"sql(
    $x = (
        SELECT
            1
    );
)sql");
    UNIT_ASSERT_VALUES_EQUAL(x, y);
}

Y_UNIT_TEST(Safety) {
    TString x = TrimIndent(R"sql(
        SELECT (SELECT 1);
        SELECT (SELECT * FROM t WHERE p);
        SELECT * FROM t WHERE x > (SELECT 1);
    )sql");

    UNIT_ASSERT_STRING_CONTAINS(x, "SELECT (SELECT 1)");
    UNIT_ASSERT_STRING_CONTAINS(x, "SELECT (SELECT * FROM t WHERE p)");
    UNIT_ASSERT_STRING_CONTAINS(x, "SELECT * FROM t WHERE x > (SELECT 1)");
}

} // Y_UNIT_TEST_SUITE(TrimIndentTests)
