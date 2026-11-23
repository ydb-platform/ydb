#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/formats/arrow/rows/view_v0.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <deque>
#include <limits>

namespace NKikimr::NOlap {

namespace {

class TKeyValues {
private:
    std::deque<TString> Storage;
    TReallyFastRng32 Random{ 42 };

public:
    std::vector<TCell> Cells;

    template <class T>
    TKeyValues& AddInts(const T low = std::numeric_limits<T>::min(), const T high = std::numeric_limits<T>::max()) {
        for (const T value : { low, high, T(0), T(1), T(T(0) - 1) }) {
            if (low <= value && value <= high) {
                Cells.push_back(TCell::Make(value));
            }
        }
        const ui64 span = ui64(high) - ui64(low);
        for (ui32 i = 0; i < 20; ++i) {
            const ui64 offset = span == Max<ui64>() ? Random.GenRand64() : Random.GenRand64() % (span + 1);
            Cells.push_back(TCell::Make(T(ui64(low) + offset)));
        }
        return *this;
    }

    TKeyValues& AddBytes(const TString& value) {
        Storage.push_back(value);
        Cells.emplace_back(Storage.back().data(), Storage.back().size());
        return *this;
    }

    // short strings of zero bytes, 0xff and letters: prefixes of each other and equal ones
    TKeyValues& AddStrings() {
        AddBytes("");
        for (ui32 i = 0; i < 40; ++i) {
            TString value;
            for (ui32 length = Random.Uniform(5); length; --length) {
                value.push_back("\0ab\xff"[Random.Uniform(4)]);
            }
            AddBytes(value);
        }
        return *this;
    }

    TKeyValues& AddUuids() {
        AddBytes(TString(16, '\0'));
        AddBytes(TString(16, '\xff'));
        for (ui32 i = 0; i < 20; ++i) {
            TString value(16, '\0');
            value[Random.Uniform(16)] = char(Random.Uniform(256));
            value[Random.Uniform(16)] = char(Random.Uniform(256));
            AddBytes(value);
        }
        return *this;
    }

    TKeyValues& AddDyNumbers() {
        for (const TStringBuf number : { "0", "1", "-1", "1.5", "-1.5", "10", "-10", "100", "0.001", "-0.001", "1e30", "-1e30" }) {
            const auto bytes = NDyNumber::ParseDyNumberString(number);
            UNIT_ASSERT_C(bytes, number);
            AddBytes(*bytes);
        }
        return *this;
    }
};

std::vector<TCell> ToCells(const TString& row, const arrow::Schema& schema) {
    std::vector<TCell> cells;
    UNIT_ASSERT(NArrow::TSimpleRowViewV0(row)
                    .ForEachValue(schema,
                        [&](const std::optional<std::string_view> value) {
                            cells.push_back(value ? TCell(value->data(), value->size()) : TCell());
                        })
                    .IsSuccess());
    return cells;
}

// rows go through the shard's own conversion to a portion key; the index stores the same bytes as cells
void AssertSameOrder(const std::vector<std::pair<TString, NScheme::TTypeInfo>>& columns, const std::vector<std::vector<TCell>>& rows) {
    NArrow::TArrowBatchBuilder builder;
    UNIT_ASSERT(builder.Start(columns).ok());
    for (const auto& row : rows) {
        builder.AddRow(row);
    }
    const auto batch = builder.FlushBatch(false);
    std::vector<TString> keys;
    std::vector<std::vector<TCell>> cells;
    for (ui32 i = 0; i < rows.size(); ++i) {
        keys.push_back(NArrow::TSimpleRowViewV0::BuildString(batch, i));
    }
    for (const auto& key : keys) {
        cells.push_back(ToCells(key, *batch->schema()));
    }
    std::vector<NScheme::TTypeInfoOrder> types;
    for (const auto& column : columns) {
        types.emplace_back(column.second);
    }
    for (ui32 i = 0; i < keys.size(); ++i) {
        for (ui32 j = 0; j < keys.size(); ++j) {
            auto shardOrder = NArrow::TSimpleRowViewV0(keys[i]).Compare(NArrow::TSimpleRowViewV0(keys[j]), *batch->schema());
            UNIT_ASSERT(shardOrder.IsSuccess());
            const int localDbOrder = CompareTypedCellVectors(cells[i].data(), cells[j].data(), types.data(), types.size());
            const std::partial_ordering expected = shardOrder.DetachResult();
            UNIT_ASSERT_C((localDbOrder < 0) == (expected == std::partial_ordering::less) &&
                              (localDbOrder == 0) == (expected == std::partial_ordering::equivalent),
                "type " << columns.front().second.GetTypeId() << ": rows " << i << " and " << j << ", local-db " << localDbOrder);
        }
    }
}

void AssertSameOrder(const NScheme::TTypeInfo& type, const TKeyValues& values) {
    std::vector<std::vector<TCell>> rows;
    for (const auto& cell : values.Cells) {
        rows.push_back({ cell });
    }
    AssertSameOrder({ { "key", type } }, rows);
}

}   // namespace

Y_UNIT_TEST_SUITE(TIndexKeyOrder) {
    // Decimal is left out until C10: local-db compares it as a number, the shard as bytes
    Y_UNIT_TEST(LocalDbOrdersEveryKeyTypeAsTheShard) {
        using namespace NScheme::NTypeIds;
        AssertSameOrder(NScheme::TTypeInfo(Bool), TKeyValues().AddBytes(TString(1, '\0')).AddBytes(TString(1, '\1')));
        AssertSameOrder(NScheme::TTypeInfo(Int8), TKeyValues().AddInts<i8>());
        AssertSameOrder(NScheme::TTypeInfo(Uint8), TKeyValues().AddInts<ui8>());
        AssertSameOrder(NScheme::TTypeInfo(Int16), TKeyValues().AddInts<i16>());
        AssertSameOrder(NScheme::TTypeInfo(Uint16), TKeyValues().AddInts<ui16>());
        AssertSameOrder(NScheme::TTypeInfo(Int32), TKeyValues().AddInts<i32>());
        AssertSameOrder(NScheme::TTypeInfo(Uint32), TKeyValues().AddInts<ui32>());
        AssertSameOrder(NScheme::TTypeInfo(Int64), TKeyValues().AddInts<i64>());
        AssertSameOrder(NScheme::TTypeInfo(Uint64), TKeyValues().AddInts<ui64>());
        AssertSameOrder(NScheme::TTypeInfo(Date), TKeyValues().AddInts<ui16>(0, NYql::NUdf::MAX_DATE - 1));
        AssertSameOrder(NScheme::TTypeInfo(Datetime), TKeyValues().AddInts<ui32>(0, NYql::NUdf::MAX_DATETIME - 1));
        AssertSameOrder(NScheme::TTypeInfo(Timestamp), TKeyValues().AddInts<ui64>(0, NYql::NUdf::MAX_TIMESTAMP - 1));
        AssertSameOrder(NScheme::TTypeInfo(Interval), TKeyValues().AddInts<i64>());
        AssertSameOrder(NScheme::TTypeInfo(Date32), TKeyValues().AddInts<i32>());
        AssertSameOrder(NScheme::TTypeInfo(Datetime64), TKeyValues().AddInts<i64>());
        AssertSameOrder(NScheme::TTypeInfo(Timestamp64), TKeyValues().AddInts<i64>());
        AssertSameOrder(NScheme::TTypeInfo(Interval64), TKeyValues().AddInts<i64>());
        AssertSameOrder(NScheme::TTypeInfo(String), TKeyValues().AddStrings());
        AssertSameOrder(NScheme::TTypeInfo(Utf8), TKeyValues().AddStrings());
        AssertSameOrder(NScheme::TTypeInfo(Uuid), TKeyValues().AddUuids());
        AssertSameOrder(NScheme::TTypeInfo(DyNumber), TKeyValues().AddDyNumbers());
    }

    Y_UNIT_TEST(LocalDbOrdersMultiColumnKeyAsTheShard) {
        TKeyValues strings;
        strings.AddStrings();
        std::vector<std::vector<TCell>> rows;
        for (const auto& string : strings.Cells) {
            for (const i32 number : { Min<i32>(), -1, 0, 1, Max<i32>() }) {
                rows.push_back({ string, TCell::Make(number) });
            }
        }
        AssertSameOrder({ { "s", NScheme::TTypeInfo(NScheme::NTypeIds::Utf8) }, { "i", NScheme::TTypeInfo(NScheme::NTypeIds::Int32) } }, rows);
    }
}

}   // namespace NKikimr::NOlap
