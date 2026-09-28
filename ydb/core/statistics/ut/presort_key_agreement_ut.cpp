// memcmp(TPresortEncoder(key)) must agree with CompareTypedCellVectors.

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/scheme_types/scheme_decimal_type.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/utility.h>
#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

namespace {

using namespace NKikimr;
using namespace NYql::NUdf;

// A typed value: the raw bytes of a TCell plus the type id.
struct TTypedValue {
    NScheme::TTypeId TypeId;
    bool IsNull = false;
    bool IsOptional = false;
    TString Data; // TCell payload

    TCell ToCell() const {
        if (IsNull) {
            return TCell();
        }
        return TCell(Data.data(), Data.size());
    }

    NScheme::TTypeInfoOrder TypeInfoOrder() const {
        // Decimal needs TDecimalType; TypeId alone is not enough.
        if (TypeId == NScheme::NTypeIds::Decimal) {
            return NScheme::TTypeInfoOrder(NScheme::TTypeInfo(NScheme::TDecimalType::Default()));
        }
        return NScheme::TTypeInfoOrder(NScheme::TTypeInfo(TypeId));
    }
};

using TTypedTuple = std::vector<TTypedValue>;

struct TColDesc {
    NScheme::TTypeId TypeId;
    bool IsOptional = false;
};

// Cell bytes → TUnboxedValuePod for TPresortEncoder::Encode().
TUnboxedValue MakePod(const TTypedValue& v) {
    if (v.IsNull) {
        return TUnboxedValuePod();
    }

    const auto slot = GetDataSlot(v.TypeId);
    TUnboxedValue pod;

    switch (slot) {
        case EDataSlot::Bool:
            pod = TUnboxedValuePod(v.Data.empty() ? false : ReadUnaligned<bool>(v.Data.data()));
            break;
        case EDataSlot::Int8:
            pod = TUnboxedValuePod(ReadUnaligned<i8>(v.Data.data()));
            break;
        case EDataSlot::Uint8:
            pod = TUnboxedValuePod(ReadUnaligned<ui8>(v.Data.data()));
            break;
        case EDataSlot::Int16:
            pod = TUnboxedValuePod(ReadUnaligned<i16>(v.Data.data()));
            break;
        case EDataSlot::Uint16:
        case EDataSlot::Date:
            pod = TUnboxedValuePod(ReadUnaligned<ui16>(v.Data.data()));
            break;
        case EDataSlot::Int32:
        case EDataSlot::Date32:
            pod = TUnboxedValuePod(ReadUnaligned<i32>(v.Data.data()));
            break;
        case EDataSlot::Uint32:
        case EDataSlot::Datetime:
            pod = TUnboxedValuePod(ReadUnaligned<ui32>(v.Data.data()));
            break;
        case EDataSlot::Int64:
        case EDataSlot::Interval:
        case EDataSlot::Interval64:
        case EDataSlot::Datetime64:
        case EDataSlot::Timestamp64:
            pod = TUnboxedValuePod(ReadUnaligned<i64>(v.Data.data()));
            break;
        case EDataSlot::Uint64:
        case EDataSlot::Timestamp:
            pod = TUnboxedValuePod(ReadUnaligned<ui64>(v.Data.data()));
            break;
        case EDataSlot::Float:
            pod = TUnboxedValuePod(ReadUnaligned<float>(v.Data.data()));
            break;
        case EDataSlot::Double:
            pod = TUnboxedValuePod(ReadUnaligned<double>(v.Data.data()));
            break;
        case EDataSlot::Decimal: {
            NYql::NDecimal::TInt128 val;
            std::memcpy(&val, v.Data.data(), sizeof(val));
            pod = TUnboxedValuePod(val);
            break;
        }
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::DyNumber:
        case EDataSlot::Json:
        case EDataSlot::Yson:
        case EDataSlot::Uuid: {
            pod = NMiniKQL::MakeString(TStringRef(v.Data.data(), v.Data.size()));
            break;
        }
        default:
            pod = NMiniKQL::MakeString(TStringRef(v.Data.data(), v.Data.size()));
            break;
    }

    if (v.IsOptional) {
        return TUnboxedValue(pod.MakeOptional());
    }
    return pod;
}

TString EncodeTuple(const TTypedTuple& t) {
    NMiniKQL::TPresortEncoder enc;
    for (const auto& v : t) {
        const auto slot = GetDataSlot(v.TypeId);
        enc.AddType(slot, v.IsOptional, /*isDesc=*/false);
    }
    enc.Start();
    for (const auto& v : t) {
        enc.Encode(MakePod(v));
    }
    const TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

int CompareEncoded(const TString& a, const TString& b) {
    const int c = memcmp(a.data(), b.data(), Min(a.size(), b.size()));
    if (c != 0) {
        return c < 0 ? -1 : 1;
    }
    if (a.size() < b.size()) {
        return -1;
    }
    if (a.size() > b.size()) {
        return 1;
    }
    return 0;
}

int CompareWithYdb(const TTypedTuple& a, const TTypedTuple& b) {
    TVector<TCell> cellsA, cellsB;
    TVector<NScheme::TTypeInfoOrder> types;
    for (const auto& v : a) {
        cellsA.push_back(v.ToCell());
        types.push_back(v.TypeInfoOrder());
    }
    for (const auto& v : b) {
        cellsB.push_back(v.ToCell());
    }
    return CompareTypedCellVectors(
        cellsA.data(), cellsB.data(), types.data(),
        static_cast<ui32>(Min(cellsA.size(), cellsB.size())));
}

int Sign(int x) {
    return (x > 0) - (x < 0);
}

TTypedTuple SingleElem(NScheme::TTypeId typeId, bool isNull, TString data,
                       bool isOptional = false) {
    return {{typeId, isNull, isOptional, std::move(data)}};
}

} // namespace

struct TPresortKeyAgreementFixture : public NUnitTest::TBaseTestCase {
    NMiniKQL::TScopedAlloc Alloc{__LOCATION__};
};

Y_UNIT_TEST_SUITE_F(PresortKeyAgreement, TPresortKeyAgreementFixture) {

Y_UNIT_TEST(NullBeforePresent) {
    TTypedTuple a = SingleElem(NScheme::NTypeIds::Int32, true, {}, true);
    TTypedTuple b = SingleElem(NScheme::NTypeIds::Int32, false, TString(4, '\x00'), true);

    int ydbCmp = CompareWithYdb(a, b);
    int encCmp = CompareEncoded(EncodeTuple(a), EncodeTuple(b));

    UNIT_ASSERT_VALUES_EQUAL(Sign(ydbCmp), -1);
    UNIT_ASSERT_VALUES_EQUAL(Sign(encCmp), -1);
}

Y_UNIT_TEST(SignedIntegers) {
    std::vector<i32> vals = {-100, -1, 0, 1, 100, 12345, -12345};
    for (size_t i = 0; i + 1 < vals.size(); ++i) {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::Int32, false,
            TString(reinterpret_cast<const char*>(&vals[i]), sizeof(i32)));
        TTypedTuple b = SingleElem(NScheme::NTypeIds::Int32, false,
            TString(reinterpret_cast<const char*>(&vals[i + 1]), sizeof(i32)));

        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "Int32 " << vals[i] << " vs " << vals[i + 1]
                     << ": ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

Y_UNIT_TEST(UnsignedIntegers) {
    std::vector<ui64> vals = {0, 1, 127, 128, 255, 256, 65535, 65536, UINT64_MAX / 2};
    for (size_t i = 0; i + 1 < vals.size(); ++i) {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::Uint64, false,
            TString(reinterpret_cast<const char*>(&vals[i]), sizeof(ui64)));
        TTypedTuple b = SingleElem(NScheme::NTypeIds::Uint64, false,
            TString(reinterpret_cast<const char*>(&vals[i + 1]), sizeof(ui64)));

        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "Uint64 " << vals[i] << " vs " << vals[i + 1]
                      << ": ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

Y_UNIT_TEST(Floats) {
    std::vector<float> vals = {
        -std::numeric_limits<float>::infinity(),
        -1.0f,
        0.0f,
        1.0f,
        std::numeric_limits<float>::infinity(),
    };
    for (size_t i = 0; i + 1 < vals.size(); ++i) {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::Float, false,
            TString(reinterpret_cast<const char*>(&vals[i]), sizeof(float)));
        TTypedTuple b = SingleElem(NScheme::NTypeIds::Float, false,
            TString(reinterpret_cast<const char*>(&vals[i + 1]), sizeof(float)));

        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "Float " << vals[i] << " vs " << vals[i + 1]
                     << ": ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

// -0.0 and +0.0 must encode identically (YDB reports them equal).
Y_UNIT_TEST(MinusZeroVsPlusZero) {
    float negZero = -0.0f;
    float posZero = +0.0f;
    TTypedTuple a = SingleElem(NScheme::NTypeIds::Float, false,
        TString(reinterpret_cast<const char*>(&negZero), sizeof(float)));
    TTypedTuple b = SingleElem(NScheme::NTypeIds::Float, false,
        TString(reinterpret_cast<const char*>(&posZero), sizeof(float)));

    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareWithYdb(a, b)), 0);
    UNIT_ASSERT_VALUES_EQUAL(EncodeTuple(a), EncodeTuple(b));

    double negZeroD = -0.0;
    double posZeroD = +0.0;
    TTypedTuple ad = SingleElem(NScheme::NTypeIds::Double, false,
        TString(reinterpret_cast<const char*>(&negZeroD), sizeof(double)));
    TTypedTuple bd = SingleElem(NScheme::NTypeIds::Double, false,
        TString(reinterpret_cast<const char*>(&posZeroD), sizeof(double)));
    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareWithYdb(ad, bd)), 0);
    UNIT_ASSERT_VALUES_EQUAL(EncodeTuple(ad), EncodeTuple(bd));
}

// Not in RandomAgreement: YDB float `<` is not an order on NaN. Presort puts NaN above +inf.
Y_UNIT_TEST(NanPlacement) {
    float nan = std::numeric_limits<float>::quiet_NaN();
    float posInf = std::numeric_limits<float>::infinity();
    TTypedTuple a = SingleElem(NScheme::NTypeIds::Float, false,
        TString(reinterpret_cast<const char*>(&nan), sizeof(float)));
    TTypedTuple b = SingleElem(NScheme::NTypeIds::Float, false,
        TString(reinterpret_cast<const char*>(&posInf), sizeof(float)));

    int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
    UNIT_ASSERT_VALUES_EQUAL_C(encCmp, 1,
        "NaN must sort above +inf in presort, got enc=" << encCmp);
}

Y_UNIT_TEST(Strings) {
    std::vector<TString> vals = {"", "a", "aa", "ab", "b", "z", "9", "19"};
    for (size_t i = 0; i + 1 < vals.size(); ++i) {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::String, false, vals[i]);
        TTypedTuple b = SingleElem(NScheme::NTypeIds::String, false, vals[i + 1]);

        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "String \"" << vals[i] << "\" vs \"" << vals[i + 1]
                       << "\": ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

Y_UNIT_TEST(StringsWithEmbeddedNul) {
    TString a = "a\0b";
    TString b = "ab";
    TTypedTuple ta = SingleElem(NScheme::NTypeIds::String, false, a);
    TTypedTuple tb = SingleElem(NScheme::NTypeIds::String, false, b);

    int ydbCmp = Sign(CompareWithYdb(ta, tb));
    int encCmp = Sign(CompareEncoded(EncodeTuple(ta), EncodeTuple(tb)));
    UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
        "String \"a\\0b\" vs \"ab\": ydb=" << ydbCmp << " enc=" << encCmp);
    UNIT_ASSERT_C(encCmp < 0, "\"a\\0b\" must sort below \"ab\"");
}

// 15-byte vs 16-byte: trailing length 0x0F meets the next block marker 0x1F.
Y_UNIT_TEST(StringBlockBoundary) {
    TString s15(15, 'a');
    TString s16(16, 'a');
    TString s14(14, 'a');

    {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::String, false, s14);
        TTypedTuple b = SingleElem(NScheme::NTypeIds::String, false, s15);
        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "14-byte vs 15-byte string: ydb=" << ydbCmp << " enc=" << encCmp);
    }
    {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::String, false, s15);
        TTypedTuple b = SingleElem(NScheme::NTypeIds::String, false, s16);
        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "15-byte vs 16-byte string: ydb=" << ydbCmp << " enc=" << encCmp);
    }
    {
        TString s15a(15, 'a');
        TString s15b(14, 'a');
        s15b.append('b');
        TTypedTuple a = SingleElem(NScheme::NTypeIds::String, false, s15a);
        TTypedTuple b = SingleElem(NScheme::NTypeIds::String, false, s15b);
        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "15-byte aaa..a vs 14a+b: ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

Y_UNIT_TEST(MultiColumnTuples) {
    i32 one = 1;
    i32 two = 2;
    TTypedTuple a = {
        {NScheme::NTypeIds::Int32, false, false, TString(reinterpret_cast<const char*>(&one), sizeof(i32))},
        {NScheme::NTypeIds::String, false, false, "abc"},
    };
    TTypedTuple b = {
        {NScheme::NTypeIds::Int32, false, false, TString(reinterpret_cast<const char*>(&one), sizeof(i32))},
        {NScheme::NTypeIds::String, false, false, "abd"},
    };
    TTypedTuple c = {
        {NScheme::NTypeIds::Int32, false, false, TString(reinterpret_cast<const char*>(&two), sizeof(i32))},
        {NScheme::NTypeIds::String, false, false, "aaa"},
    };

    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b))),
                              Sign(CompareWithYdb(a, b)));
    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(c))),
                              Sign(CompareWithYdb(a, c)));
    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareEncoded(EncodeTuple(b), EncodeTuple(c))),
                              Sign(CompareWithYdb(b, c)));
}

Y_UNIT_TEST(NullInSecondColumn) {
    i32 five = 5;
    i32 zero = 0;
    TTypedTuple a = {
        {NScheme::NTypeIds::Int32, false, false, TString(reinterpret_cast<const char*>(&five), sizeof(i32))},
        {NScheme::NTypeIds::Int32, true, true, {}},
    };
    TTypedTuple b = {
        {NScheme::NTypeIds::Int32, false, false, TString(reinterpret_cast<const char*>(&five), sizeof(i32))},
        {NScheme::NTypeIds::Int32, false, true, TString(reinterpret_cast<const char*>(&zero), sizeof(i32))},
    };

    int ydbCmp = Sign(CompareWithYdb(a, b));
    int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
    UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
        "NULL in second column: ydb=" << ydbCmp << " enc=" << encCmp);
    UNIT_ASSERT_C(encCmp < 0, "NULL must sort before present value");
}

// Ordinary decimals only (YQL Inf/NaN/error are outside YDB's pair<ui64,i64> cell).
Y_UNIT_TEST(Decimal) {
    auto makeDecimal = [](i64 high, ui64 low) -> TTypedValue {
        TTypedValue v;
        v.TypeId = NScheme::NTypeIds::Decimal;
        v.IsNull = false;
        std::pair<ui64, i64> p{low, high};
        v.Data = TString(reinterpret_cast<const char*>(&p), sizeof(p));
        return v;
    };

    std::vector<std::pair<i64, ui64>> vals = {
        {0, 0},
        {0, 1},
        {0, 100},
        {1, 0},
        {-1, 0},
        {0, 0},
    };
    std::vector<TTypedTuple> tuples;
    for (auto& [high, low] : vals) {
        tuples.push_back({makeDecimal(high, low)});
    }

    // Compare each pair.
    for (size_t i = 0; i < tuples.size(); ++i) {
        for (size_t j = i + 1; j < tuples.size(); ++j) {
            int ydbCmp = Sign(CompareWithYdb(tuples[i], tuples[j]));
            int encCmp = Sign(CompareEncoded(EncodeTuple(tuples[i]), EncodeTuple(tuples[j])));
            UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
                "Decimal (" << vals[i].first << "," << vals[i].second << ") vs ("
                << vals[j].first << "," << vals[j].second
                << "): ydb=" << ydbCmp << " enc=" << encCmp);
        }
    }
}

Y_UNIT_TEST(Uuid) {
    TString a(16, '\x00');
    TString b(16, '\x00');
    b[15] = 1;
    TString c(16, '\xFF');

    TTypedTuple ta = SingleElem(NScheme::NTypeIds::Uuid, false, a);
    TTypedTuple tb = SingleElem(NScheme::NTypeIds::Uuid, false, b);
    TTypedTuple tc = SingleElem(NScheme::NTypeIds::Uuid, false, c);

    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareEncoded(EncodeTuple(ta), EncodeTuple(tb))),
                              Sign(CompareWithYdb(ta, tb)));
    UNIT_ASSERT_VALUES_EQUAL(Sign(CompareEncoded(EncodeTuple(tb), EncodeTuple(tc))),
                              Sign(CompareWithYdb(tb, tc)));
    UNIT_ASSERT_C(Sign(CompareEncoded(EncodeTuple(ta), EncodeTuple(tb))) < 0,
        "all-zero Uuid must sort below ...01");
}

Y_UNIT_TEST(DyNumber) {
    std::vector<TString> vals = {"", "1", "10", "2", "a"};
    for (size_t i = 0; i + 1 < vals.size(); ++i) {
        TTypedTuple a = SingleElem(NScheme::NTypeIds::DyNumber, false, vals[i]);
        TTypedTuple b = SingleElem(NScheme::NTypeIds::DyNumber, false, vals[i + 1]);

        int ydbCmp = Sign(CompareWithYdb(a, b));
        int encCmp = Sign(CompareEncoded(EncodeTuple(a), EncodeTuple(b)));
        UNIT_ASSERT_VALUES_EQUAL_C(encCmp, ydbCmp,
            "DyNumber \"" << vals[i] << "\" vs \"" << vals[i + 1]
                          << "\": ydb=" << ydbCmp << " enc=" << encCmp);
    }
}

Y_UNIT_TEST(RandomAgreement) {
    std::mt19937 rng(12345);

    auto randomInt = [&](i64 lo, i64 hi) -> i64 {
        return std::uniform_int_distribution<i64>(lo, hi)(rng);
    };

    auto randomString = [&]() -> TString {
        const size_t len = std::uniform_int_distribution<size_t>(0, 20)(rng);
        TString s;
        for (size_t i = 0; i < len; ++i) {
            const int c = std::uniform_int_distribution<int>(0, 255)(rng);
            s.append(static_cast<char>(c));
        }
        return s;
    };

    const std::vector<TColDesc> columnTypes = {
        {NScheme::NTypeIds::Int32, true},
        {NScheme::NTypeIds::Uint64, false},
        {NScheme::NTypeIds::String, true},
    };

    auto randomValue = [&](const TColDesc& col) -> TTypedValue {
        TTypedValue v;
        v.TypeId = col.TypeId;
        v.IsOptional = col.IsOptional;
        // 10% chance of NULL (only meaningful for optional columns).
        if (col.IsOptional && std::uniform_int_distribution<int>(0, 9)(rng) == 0) {
            v.IsNull = true;
            return v;
        }
        switch (col.TypeId) {
            case NScheme::NTypeIds::Int32: {
                i32 val = static_cast<i32>(randomInt(-100000, 100000));
                v.Data = TString(reinterpret_cast<const char*>(&val), sizeof(i32));
                break;
            }
            case NScheme::NTypeIds::Uint64: {
                ui64 val = static_cast<ui64>(randomInt(0, std::numeric_limits<i64>::max()));
                v.Data = TString(reinterpret_cast<const char*>(&val), sizeof(ui64));
                break;
            }
            case NScheme::NTypeIds::String: {
                v.Data = randomString();
                break;
            }
            default:
                break;
        }
        return v;
    };

    const int N = 200;
    std::vector<TTypedTuple> tuples;
    std::vector<TString> encoded;
    for (int i = 0; i < N; ++i) {
        TTypedTuple t;
        for (const auto& col : columnTypes) {
            t.push_back(randomValue(col));
        }
        tuples.push_back(t);
        encoded.push_back(EncodeTuple(t));
    }

    int mismatches = 0;
    for (int i = 0; i < N; ++i) {
        for (int j = i + 1; j < N; ++j) {
            int ydbCmp = Sign(CompareWithYdb(tuples[i], tuples[j]));
            int encCmp = Sign(CompareEncoded(encoded[i], encoded[j]));
            if (ydbCmp != encCmp) {
                ++mismatches;
                Cerr << "Mismatch at (" << i << "," << j << "): "
                     << "ydb=" << ydbCmp << " enc=" << encCmp << Endl;
            }
        }
    }
    UNIT_ASSERT_VALUES_EQUAL_C(mismatches, 0,
        "TPresortEncoder disagrees with CompareTypedCellVectors in "
        << mismatches << " out of " << (N * (N - 1) / 2) << " pairs");
}

} // Y_UNIT_TEST_SUITE(PresortKeyAgreement)
