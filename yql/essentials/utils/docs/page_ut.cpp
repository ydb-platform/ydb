#include "page.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDocs;

Y_UNIT_TEST_SUITE(PageTests) {

Y_UNIT_TEST(ResolveURL) {
    TString markdown = R"(
# List of window functions in YQL

The syntax for calling window functions is detailed in a
[separate article](../syntax/window.md).

## Aggregate functions {#aggregate-functions}

All the [aggregate functions](aggregation.md) can also be used as window functions.
In this case, each row includes an aggregation result obtained on a set of rows from
the [window frame](../syntax/window.md#frame).

## SOME {#some}

Get the value for an expression specified as an argument, for one of the table rows.
Gives no guarantee of which row is used. It's similar to the
[any()](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/)
function in ClickHouse.

{% note alert %}

If one of the compared arguments is 0.0, the function always returns false.

{% endnote %}

```yql
SELECT 1;
```

End.
)";

    TPages pages = {{"builtins/window", ParseMarkdownPage(markdown)}};
    pages = Resolved(std::move(pages), "https://ytsaurus.tech/docs/en/yql");
    pages = ExtendedSyntaxRemoved(std::move(pages));
    pages = CodeListingsTagRemoved(std::move(pages));

    TVector<TString> changes = {
        "[separate article](https://ytsaurus.tech/docs/en/yql/builtins/window/../../syntax/window)",
        "[aggregate functions](https://ytsaurus.tech/docs/en/yql/builtins/window/../aggregation)",
        "[window frame](https://ytsaurus.tech/docs/en/yql/builtins/window/../../syntax/window#frame)",
        "[any()](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/)",
    };

    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, changes.at(0));
    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, changes.at(1));
    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, changes.at(2));
    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, changes.at(3));

    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, "the function always returns false");
    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, "End.");
    UNIT_ASSERT(!pages["builtins/window"].Text.Contains("{% note alert %}"));
    UNIT_ASSERT(!pages["builtins/window"].Text.Contains("{% endnote %}"));

    UNIT_ASSERT(!pages["builtins/window"].Text.Contains("```yql\nSELECT"));
    UNIT_ASSERT_STRING_CONTAINS(pages["builtins/window"].Text, "```\nSELECT");
}

} // Y_UNIT_TEST_SUITE(PageTests)
