#include "markdown.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NDocs;

Y_UNIT_TEST_SUITE(MarkdownParserTests) {

    Y_UNIT_TEST(ParseMarkdown) {
        TString markdown = R"(
# Basic built-in functions

Below are the general-purpose functions.

## COALESCE {#coalesce}

Iterates through the arguments from left to right.

#### Examples

```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

## Random... {#random}

Generates a pseudorandom number:

* `Random()`: A floating point number (Double) from 0 to 1.
* `RandomNumber()`: An integer from the complete Uint64 range.
* `RandomUuid()`: [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

#### Signatures

```yql
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

No arguments are used for random number generation.

#### Examples

```yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```
)";
        TMarkdownPage page = ParseMarkdownPage(markdown);

        UNIT_ASSERT_VALUES_EQUAL(page.SectionsByAnchor.size(), 2);

        const auto& coelcese = page.SectionsByAnchor["coalesce"];
        UNIT_ASSERT_STRING_CONTAINS(coelcese.Header.Content, "COALESCE");
        UNIT_ASSERT_VALUES_EQUAL(coelcese.Header.Anchor, "coalesce");
        UNIT_ASSERT_STRING_CONTAINS(coelcese.Body, "Iterates");
        UNIT_ASSERT_STRING_CONTAINS(coelcese.Body, "COALESCE");
        UNIT_ASSERT_GE(Count(coelcese.Body, '\n'), 5);

        const auto& random = page.SectionsByAnchor["random"];
        UNIT_ASSERT_STRING_CONTAINS(random.Header.Content, "Random");
        UNIT_ASSERT_VALUES_EQUAL(random.Header.Anchor, "random");
        UNIT_ASSERT_STRING_CONTAINS(random.Body, "Generates");
        UNIT_ASSERT_STRING_CONTAINS(random.Body, "Random");
        UNIT_ASSERT_GE(Count(random.Body, '\n'), 5);
    }

} // Y_UNIT_TEST_SUITE(MarkdownParserTests)
