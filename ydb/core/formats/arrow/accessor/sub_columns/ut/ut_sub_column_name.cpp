#include <ydb/core/formats/arrow/accessor/sub_columns/sub_column_name.h>

#include <library/cpp/testing/unittest/registar.h>

using NKikimr::NArrow::NAccessor::NSubColumns::TCanonicalSubColumnName;

Y_UNIT_TEST_SUITE(CanonicalSubColumnName) {
    Y_UNIT_TEST(DistinguishesFullColumnAndEmptyName) {
        const TCanonicalSubColumnName fullColumn;
        const auto emptyName = TCanonicalSubColumnName::Parse("");

        UNIT_ASSERT_VALUES_EQUAL("", fullColumn.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(R"("")", emptyName.GetValue());
        UNIT_ASSERT(fullColumn != emptyName);
    }

    Y_UNIT_TEST(EquivalentPathsHaveEqualHashes) {
        const auto canonical = TCanonicalSubColumnName::Parse("$.a.b");
        for (const TStringBuf path : {"$.a.b", "strict $.a.b", "lax $.\"a\".\"b\""}) {
            const auto equivalent = TCanonicalSubColumnName::Parse(path);
            UNIT_ASSERT(canonical == equivalent);
            UNIT_ASSERT_VALUES_EQUAL(canonical.GetHash(), equivalent.GetHash());
        }
    }
} // Y_UNIT_TEST_SUITE(CanonicalSubColumnName)
