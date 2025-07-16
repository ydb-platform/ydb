#include "markdown.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

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
        TVector<TMarkdownSection> sections;

        TStringStream input(markdown);
        ParseMarkdown(input, [&](TMarkdownSection&& section) {
            sections.emplace_back(std::move(section));
        });

        UNIT_ASSERT_VALUES_EQUAL(sections.size(), 2);

        UNIT_ASSERT_STRING_CONTAINS(sections[0].Header.Content, "COALESCE");
        UNIT_ASSERT_VALUES_EQUAL(sections[0].Header.Anchor, "#coalesce");
        UNIT_ASSERT_STRING_CONTAINS(sections[0].Body, "Iterates");
        UNIT_ASSERT_STRING_CONTAINS(sections[0].Body, "COALESCE");

        UNIT_ASSERT_STRING_CONTAINS(sections[1].Header.Content, "Random");
        UNIT_ASSERT_VALUES_EQUAL(sections[1].Header.Anchor, "#random");
        UNIT_ASSERT_STRING_CONTAINS(sections[1].Body, "Generates");
        UNIT_ASSERT_STRING_CONTAINS(sections[1].Body, "Random");
        UNIT_ASSERT_STRING_CONTAINS(sections[1].Body, "Random");
    }

} // Y_UNIT_TEST_SUITE(MarkdownParserTests)
