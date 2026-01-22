#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <array>
#include <numeric>
#include <string>
#include <vector>

using namespace NYql::NUdf;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

struct TBlockLayoutConverterTestData {
    TBlockLayoutConverterTestData()
        : FunctionRegistry(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Memory")
        , ArrowPool(GetYqlMemoryPool())
    {}

    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    NMiniKQL::TProgramBuilder PgmBuilder;
    NMiniKQL::TMemoryUsageInfo MemInfo;
    arrow::MemoryPool* const ArrowPool;
};

static arrow::Datum SliceIf(const arrow::Datum& d, bool slice) {
    UNIT_ASSERT(d.is_array());
    if (!slice) {
        return d;
    }
    auto a = d.array();
    UNIT_ASSERT(a);
    UNIT_ASSERT_C(a->length >= 4, "Slice mode needs length >= 4");
    return arrow::Datum(a->Slice(1, a->length - 2));
}

template <class TFillFn, class TCheckFn>
static void RunRoundTripOneColumn(
    TBlockLayoutConverterTestData& data,
    NKikimr::NMiniKQL::TType* type,
    NPackedTuple::EColumnRole role,
    ui32 n,
    bool slice,
    TFillFn fillFn,
    TCheckFn checkFn
) {
    const size_t itemSize = NMiniKQL::CalcMaxBlockItemSize(type);
    const ui32 blockLen = (ui32)NMiniKQL::CalcBlockLen(itemSize);
    UNIT_ASSERT_C(blockLen > 8, "Unexpected too small blockLen");

    auto builder = MakeArrayBuilder(NMiniKQL::TTypeInfoHelper(), type, *data.ArrowPool, blockLen, nullptr);
    fillFn(*builder, n);

    auto d0 = builder->Build(true);
    UNIT_ASSERT(d0.is_array());
    auto d = SliceIf(d0, slice);

    auto converter = MakeBlockLayoutConverter(NMiniKQL::TTypeInfoHelper(), {type}, {role}, data.ArrowPool);

    TPackResult packRes;
    converter->Pack({d}, packRes);
    UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, (ui32)d.array()->length, "Pack returned unexpected tuple count");

    TVector<arrow::Datum> out;
    converter->Unpack(packRes, out);
    UNIT_ASSERT_VALUES_EQUAL(out.size(), 1u);
    UNIT_ASSERT(out[0].is_array());

    checkFn(d.array(), out[0].array(), type);
}

template <class T>
static auto FillFixed(bool optional) {
    return [optional](IArrayBuilder& b, ui32 n) {
        for (ui32 i = 0; i < n; ++i) {
            if (optional && (i % 3 == 0)) {
                b.Add(TBlockItem()); // null
                continue;
            }
            if constexpr (std::is_same_v<T, double>) {
                b.Add(TBlockItem((double)i * 0.5 + 0.25));
            } else if constexpr (std::is_same_v<T, bool>) {
                b.Add(TBlockItem((i & 1) != 0));
            } else {
                const i64 v = (i64)i * 17 - 5;
                b.Add(TBlockItem((T)v));
            }
        }
    };
}

template <class T>
static void CheckFixed(
    const std::shared_ptr<arrow::ArrayData>& before,
    const std::shared_ptr<arrow::ArrayData>& after,
    NKikimr::NMiniKQL::TType* type
) {
    UNIT_ASSERT(before);
    UNIT_ASSERT(after);
    UNIT_ASSERT_VALUES_EQUAL(before->length, after->length);

    auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), type);
    for (ui32 i = 0; i < (ui32)before->length; ++i) {
        const TBlockItem lhs = reader->GetItem(*before, i);
        const TBlockItem rhs = reader->GetItem(*after, i);

        UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs), bool(rhs), "Optionality mismatch at i=" << i);
        if (!lhs) {
            continue;
        }

        if constexpr (std::is_same_v<T, double>) {
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<double>(), rhs.Get<double>(), "Value mismatch at i=" << i);
        } else if constexpr (std::is_same_v<T, bool>) {
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<bool>(), rhs.Get<bool>(), "Value mismatch at i=" << i);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(lhs.Get<T>(), rhs.Get<T>(), "Value mismatch at i=" << i);
        }
    }
}


struct TStringArena {
    std::string Buf;

    TStringArena() {
        Buf.reserve(1 << 20);
    }

    TStringRef Put(const std::string& s) {
        const size_t off = Buf.size();
        Buf.append(s);
        return TStringRef(Buf.data() + off, s.size());
    }
};

static auto FillStrings(bool optional, TStringArena& arena) {
    return [optional, &arena](IArrayBuilder& b, ui32 n) {
        for (ui32 i = 0; i < n; ++i) {
            if (optional && (i % 4 == 0)) {
                b.Add(TBlockItem()); // null
                continue;
            }
            if (i % 5 == 0) {
                b.Add(TBlockItem(TStringRef())); // empty
                continue;
            }
            std::string s = "Manufacturer#" + std::to_string(i);
            b.Add(TBlockItem(arena.Put(s)));
        }
    };
}

static void CheckStrings(
    const std::shared_ptr<arrow::ArrayData>& before,
    const std::shared_ptr<arrow::ArrayData>& after,
    NKikimr::NMiniKQL::TType* type
) {
    UNIT_ASSERT(before);
    UNIT_ASSERT(after);
    UNIT_ASSERT_VALUES_EQUAL(before->length, after->length);

    auto reader = MakeBlockReader(NMiniKQL::TTypeInfoHelper(), type);
    for (ui32 i = 0; i < (ui32)before->length; ++i) {
        const TBlockItem lhs = reader->GetItem(*before, i);
        const TBlockItem rhs = reader->GetItem(*after, i);

        UNIT_ASSERT_VALUES_EQUAL_C(bool(lhs), bool(rhs), "Optionality mismatch at i=" << i);
        if (!lhs) {
            continue;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(lhs.AsStringRef(), rhs.AsStringRef(), "String mismatch at i=" << i);
    }
}

#define FIXED_CASES(X) \
    X(Int64,  NUdf::EDataSlot::Int64,  i64)  \
    X(Int32,  NUdf::EDataSlot::Int32,  i32)  \
    X(Uint64, NUdf::EDataSlot::Uint64, ui64) \
    X(Uint32, NUdf::EDataSlot::Uint32, ui32) \
    X(Double, NUdf::EDataSlot::Double, double) \
    X(Bool,   NUdf::EDataSlot::Bool,   bool)

#define STRING_CASES(X) \
    X(String, NUdf::EDataSlot::String)

#define GEN_FIXED_TEST(NAME, SLOT, CPP_T, OPT, SLICE)                                    \
Y_UNIT_TEST(Test_##NAME##_opt##OPT##_slice##SLICE) {                                     \
    TBlockLayoutConverterTestData data;                                                   \
    const bool isOpt = (OPT);                                                             \
    const bool doSlice = (SLICE);                                                         \
    auto type = data.PgmBuilder.NewDataType((SLOT), isOpt);                                \
    constexpr ui32 N = 1024;                                                              \
    RunRoundTripOneColumn(                                                                 \
        data, type, NPackedTuple::EColumnRole::Key, N, doSlice,                           \
        FillFixed<CPP_T>(isOpt),                                                          \
        [](const std::shared_ptr<arrow::ArrayData>& a,                                    \
           const std::shared_ptr<arrow::ArrayData>& b,                                    \
           NKikimr::NMiniKQL::TType* t) {                                                  \
            CheckFixed<CPP_T>(a, b, t);                                                   \
        }                                                                                  \
    );                                                                                     \
}

#define GEN_STRING_TEST(NAME, SLOT, OPT, SLICE)                                           \
Y_UNIT_TEST(Test_##NAME##_opt##OPT##_slice##SLICE) {                                      \
    TBlockLayoutConverterTestData data;                                                   \
    TStringArena arena;                                                                   \
    const bool isOpt = (OPT);                                                             \
    const bool doSlice = (SLICE);                                                         \
    auto type = data.PgmBuilder.NewDataType((SLOT), isOpt);                                \
    constexpr ui32 N = 1024;                                                              \
    RunRoundTripOneColumn(                                                                 \
        data, type, NPackedTuple::EColumnRole::Key, N, doSlice,                           \
        FillStrings(isOpt, arena),                                                        \
        [](const std::shared_ptr<arrow::ArrayData>& a,                                    \
           const std::shared_ptr<arrow::ArrayData>& b,                                    \
           NKikimr::NMiniKQL::TType* t) {                                                  \
            CheckStrings(a, b, t);                                                        \
        }                                                                                  \
    );                                                                                     \
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TBlockLayoutConverterGeneratedTest) {

#define X_FIXED(NAME, SLOT, CPP_T) \
    GEN_FIXED_TEST(NAME, SLOT, CPP_T, false, false) \
    GEN_FIXED_TEST(NAME, SLOT, CPP_T, false, true)  \
    GEN_FIXED_TEST(NAME, SLOT, CPP_T, true,  false) \
    GEN_FIXED_TEST(NAME, SLOT, CPP_T, true,  true)

FIXED_CASES(X_FIXED)
#undef X_FIXED

#define X_STR(NAME, SLOT) \
    GEN_STRING_TEST(NAME, SLOT, false, false) \
    GEN_STRING_TEST(NAME, SLOT, false, true)  \
    GEN_STRING_TEST(NAME, SLOT, true,  false) \
    GEN_STRING_TEST(NAME, SLOT, true,  true)

STRING_CASES(X_STR)
#undef X_STR

} // Y_UNIT_TEST_SUITE

