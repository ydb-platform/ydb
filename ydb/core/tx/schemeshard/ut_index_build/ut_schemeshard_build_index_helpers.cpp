#include "schemeshard_build_index_helpers.h"

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme/scheme_type_order.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSchemeShard {

using TCols = std::pair<std::optional<ui32>, std::optional<TString>>;
using TRange = std::pair<TCols, TCols>;

TCell MakeCell(std::optional<ui32> x) {
    if (x) {
        return TCell::Make<ui32>(*x);
    } else {
        return TCell();
    }
}

TCell MakeCell(std::optional<TString> x) {
    if (x) {
        return TCell(x->data(), x->size());
    } else {
        return TCell();
    }
}

bool RunValidation(std::initializer_list<TRange> ranges, TString& errorDescription) {
    std::vector<TString> indexColumns = {
        "index_key_1",
        "index_key_2",
    };
    std::vector<NScheme::TTypeInfoOrder> typeInfos = {
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::Uint32),
        NScheme::TTypeInfoOrder(NScheme::NTypeIds::String),
    };
    std::vector<TSerializedTableRange> serializedRanges;
    std::vector<const TSerializedTableRange*> serializedRangesPtrs;
    serializedRanges.reserve(ranges.size());
    serializedRangesPtrs.reserve(serializedRanges.size());
    for (auto i = std::begin(ranges); i != std::end(ranges); ++i) {
        auto& r = serializedRanges.emplace_back();
        r.From = TSerializedCellVec(TSerializedCellVec::Serialize({MakeCell(i->first.first), MakeCell(i->first.second)}));
        r.To = TSerializedCellVec(TSerializedCellVec::Serialize({MakeCell(i->second.first), MakeCell(i->second.second)}));
        serializedRangesPtrs.push_back(&r);
    }

    return PerformCrossShardUniqIndexValidation(typeInfos, indexColumns, serializedRangesPtrs, errorDescription);
}

#define UNIT_ASSERT_VALIDATION_TRUE(...) do {                            \
    std::initializer_list<TRange> r = { __VA_ARGS__ };                   \
    TString errorDescription;                                            \
    UNIT_ASSERT_C(RunValidation(r, errorDescription), errorDescription); \
} while (false);                                                         \
/**/

#define UNIT_ASSERT_VALIDATION_FALSE(expectedError, ...) do {            \
    std::initializer_list<TRange> r = { __VA_ARGS__ };                   \
    TString errorDescription;                                            \
    UNIT_ASSERT(!RunValidation(r, errorDescription));                    \
    UNIT_ASSERT_VALUES_EQUAL(errorDescription, (expectedError));         \
} while (false);                                                         \
/**/

Y_UNIT_TEST_SUITE(CrossShardUniqIndexValidationTest) {
    Y_UNIT_TEST(Validation) {
        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, "1")),
            TRange(TCols(1, "2"), TCols(10, "10")),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, std::nullopt)),
            TRange(TCols(1, std::nullopt), TCols(1, std::nullopt)),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(1, "1"), TCols(1, "1")),
        );

        UNIT_ASSERT_VALIDATION_TRUE(
            TRange(TCols(std::nullopt, std::nullopt), TCols(std::nullopt, std::nullopt)),
            TRange(TCols(std::nullopt, std::nullopt), TCols(std::nullopt, std::nullopt)),
        );

        UNIT_ASSERT_VALIDATION_FALSE("Duplicate key found: (index_key_1=1, index_key_2=~1)",
            TRange(TCols(1, "~1"), TCols(1, "~1")),
            TRange(TCols(1, "~1"), TCols(1, "~1")),
        );

        UNIT_ASSERT_VALIDATION_FALSE("Duplicate key found: (index_key_1=6, index_key_2=~6)",
            TRange(TCols(1, "~1"), TCols(2, "~2")),
            TRange(TCols(3, "~3"), TCols(4, "~4")),
            TRange(TCols(5, "~5"), TCols(6, "~6")),
            TRange(TCols(6, "~6"), TCols(8, std::nullopt)),
        );
    }
}

} // namespace NKikimr::NSchemeShard
