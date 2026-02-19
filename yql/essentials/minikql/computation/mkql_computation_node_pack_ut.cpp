#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_holders.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"
#include "mkql_block_trimmer.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/public/udf/arrow/util.h>

#include <library/cpp/random_provider/random_provider.h>
#include <yql/essentials/minikql/aligned_page_pool.h>

#include <yql/essentials/public/udf/udf_value.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ylimits.h>
#include <util/generic/xrange.h>
#include <util/generic/maybe.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/system/hp_timer.h>
#include <util/system/sanitizers.h>

namespace NKikimr::NMiniKQL {

using NYql::TChunkedBuffer;

#ifdef WITH_VALGRIND
constexpr static size_t PERFORMANCE_COUNT = 0x1000;
#elif defined(NDEBUG)
constexpr static size_t PERFORMANCE_COUNT = NSan::PlainOrUnderSanitizer(0x4000000, 0x1000);
#else
constexpr static size_t PERFORMANCE_COUNT = NSan::PlainOrUnderSanitizer(0x1000000, 0x1000);
#endif

template <bool Fast, bool Transport>
struct TPackerTraits;

template <bool Fast>
struct TPackerTraits<Fast, false> {
    using TPackerType = TValuePackerGeneric<Fast>;
};

template <bool Fast>
struct TPackerTraits<Fast, true> {
    using TPackerType = TValuePackerTransport<Fast>;
};

using NDetails::TChunkedInputBuffer;

template <bool Fast, bool Transport>
class TMiniKQLComputationNodePackTest: public TTestBase {
    using TValuePackerType = typename TPackerTraits<Fast, Transport>::TPackerType;

    TValuePackerType MakeValuePacker(bool stable, const TType* type,
                                     EValuePackerVersion valuePackerVersion, TMaybe<size_t> bufferPageAllocSize = Nothing(),
                                     arrow::MemoryPool* pool = nullptr, TMaybe<ui8> minFillPercentage = Nothing()) {
        if constexpr (Transport) {
            return TValuePackerType(stable, type, valuePackerVersion, bufferPageAllocSize, pool, minFillPercentage);
        } else {
            Y_UNUSED(valuePackerVersion);
            return TValuePackerType(stable, type);
        }
    }

protected:
    TMiniKQLComputationNodePackTest()
        : FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , RandomProvider_(CreateDefaultRandomProvider())
        , Alloc_(__LOCATION__)
        , Env_(Alloc_)
        , PgmBuilder_(Env_, *FunctionRegistry_)
        , MemInfo_("Memory")
        , HolderFactory_(Alloc_.Ref(), MemInfo_, FunctionRegistry_.Get())
        , ArrowPool_(NYql::NUdf::GetYqlMemoryPool())
    {
    }

    void TestNumericTypes() {
        TestNumericType<bool>(NUdf::TDataType<bool>::Id);
        TestNumericType<ui8>(NUdf::TDataType<ui8>::Id);
        TestNumericType<i32>(NUdf::TDataType<i32>::Id);
        TestNumericType<i64>(NUdf::TDataType<i64>::Id);
        TestNumericType<ui32>(NUdf::TDataType<ui32>::Id);
        TestNumericType<ui64>(NUdf::TDataType<ui64>::Id);
        TestNumericType<float>(NUdf::TDataType<float>::Id);
        TestNumericType<double>(NUdf::TDataType<double>::Id);
    }

    void TestOptionalNumericTypes() {
        TestOptionalNumericType<bool>(NUdf::TDataType<bool>::Id);
        TestOptionalNumericType<ui8>(NUdf::TDataType<ui8>::Id);
        TestOptionalNumericType<i32>(NUdf::TDataType<i32>::Id);
        TestOptionalNumericType<i64>(NUdf::TDataType<i64>::Id);
        TestOptionalNumericType<ui32>(NUdf::TDataType<ui32>::Id);
        TestOptionalNumericType<ui64>(NUdf::TDataType<ui64>::Id);
        TestOptionalNumericType<float>(NUdf::TDataType<float>::Id);
        TestOptionalNumericType<double>(NUdf::TDataType<double>::Id);
    }

    void TestStringTypes() {
        TestStringType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TestStringType(NUdf::TDataType<char*>::Id);
        TestStringType(NUdf::TDataType<NUdf::TYson>::Id);
        TestStringType(NUdf::TDataType<NUdf::TJson>::Id);
    }

    void TestOptionalStringTypes() {
        TestOptionalStringType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TestOptionalStringType(NUdf::TDataType<char*>::Id);
        TestOptionalStringType(NUdf::TDataType<NUdf::TYson>::Id);
        TestOptionalStringType(NUdf::TDataType<NUdf::TJson>::Id);
    }

    void TestListType() {
        TDefaultListRepresentation listValues;
        for (ui32 val : xrange(0, 10)) {
            listValues = listValues.Append(NUdf::TUnboxedValuePod(val));
        }
        TType* listType = PgmBuilder_.NewListType(PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id));
        const NUdf::TUnboxedValue value = HolderFactory_.CreateDirectListHolder(std::move(listValues));
        const auto uValue = TestPackUnpack(listType, value, "Type:List(ui32)");

        UNIT_ASSERT_VALUES_EQUAL(uValue.GetListLength(), 10);
        ui32 i = 0;
        const auto iter = uValue.GetListIterator();
        for (NUdf::TUnboxedValue item; iter.Next(item); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui32>(), i);
        }
        UNIT_ASSERT_VALUES_EQUAL(i, 10);
    }

    void TestListOfOptionalsType() {
        TDefaultListRepresentation listValues;
        for (ui32 i : xrange(0, 10)) {
            NUdf::TUnboxedValue uVal = NUdf::TUnboxedValuePod();
            if (i % 2) {
                uVal = MakeString(TString(i * 2, '0' + i));
            }
            listValues = listValues.Append(std::move(uVal));
        }
        TType* listType = PgmBuilder_.NewListType(PgmBuilder_.NewOptionalType(PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)));
        const NUdf::TUnboxedValue value = HolderFactory_.CreateDirectListHolder(std::move(listValues));
        const auto uValue = TestPackUnpack(listType, value, "Type:List(Optional(utf8))");

        UNIT_ASSERT_VALUES_EQUAL(uValue.GetListLength(), 10);
        ui32 i = 0;
        const auto iter = uValue.GetListIterator();
        for (NUdf::TUnboxedValue uVal; iter.Next(uVal); ++i) {
            if (i % 2) {
                UNIT_ASSERT(uVal);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(uVal.AsStringRef()), TString(i * 2, '0' + i));
            } else {
                UNIT_ASSERT(!uVal);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(i, 10);
    }

    void TestTupleType() {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[3]));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[3]));
        TType* tupleType = PgmBuilder_.NewTupleType(tupleElemenTypes);

        TUnboxedValueVector tupleElemens;
        tupleElemens.push_back(MakeString("01234567890123456789"));
        tupleElemens.push_back(MakeString("01234567890"));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));

        const NUdf::TUnboxedValue value = HolderFactory_.VectorAsArray(tupleElemens);
        const auto uValue = TestPackUnpack(tupleType, value, "Type:Tuple");

        {
            auto e = uValue.GetElement(0);
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890123456789");
        }
        {
            auto e = uValue.GetElement(1);
            UNIT_ASSERT(e);
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890");
        }
        {
            auto e = uValue.GetElement(2);
            UNIT_ASSERT(!e);
        }
        {
            auto e = uValue.GetElement(3);
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345);
        }
        {
            auto e = uValue.GetElement(4);
            UNIT_ASSERT(!e);
        }
        {
            auto e = uValue.GetElement(5);
            UNIT_ASSERT(e);
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345);
        }
    }

    void TestStructType() {
        const std::vector<std::pair<std::string_view, TType*>> structElemenTypes = {
            {"a", PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)},
            {"b", PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"c", PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"d", PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id)},
            {"e", PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id, true)},
            {"f", PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id, true)}};
        TType* structType = PgmBuilder_.NewStructType(structElemenTypes);

        TUnboxedValueVector structElemens;
        structElemens.push_back(MakeString("01234567890123456789"));
        structElemens.push_back(MakeString("01234567890"));
        structElemens.push_back(NUdf::TUnboxedValuePod());
        structElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        structElemens.push_back(NUdf::TUnboxedValuePod());
        structElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));

        const NUdf::TUnboxedValue value = HolderFactory_.VectorAsArray(structElemens);
        const auto uValue = TestPackUnpack(structType, value, "Type:Struct");

        {
            auto e = uValue.GetElement(0);
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890123456789");
        }
        {
            auto e = uValue.GetElement(1);
            UNIT_ASSERT(e);
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890");
        }
        {
            auto e = uValue.GetElement(2);
            UNIT_ASSERT(!e);
        }
        {
            auto e = uValue.GetElement(3);
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345);
        }
        {
            auto e = uValue.GetElement(4);
            UNIT_ASSERT(!e);
        }
        {
            auto e = uValue.GetElement(5);
            UNIT_ASSERT(e);
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345);
        }
    }

    void TestOptionalType() {
        TType* type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        type = PgmBuilder_.NewOptionalType(type);
        type = PgmBuilder_.NewOptionalType(type);
        type = PgmBuilder_.NewOptionalType(type);

        NUdf::TUnboxedValue uValue = NUdf::TUnboxedValuePod();
        uValue = TestPackUnpack(type, uValue, "Type:Optional, Value:null");
        UNIT_ASSERT(!uValue);

        uValue = NUdf::TUnboxedValuePod(ui64(123));
        uValue = TestPackUnpack(type, uValue, "Type:Optional, Value:opt(opt(opt(123)))");
        UNIT_ASSERT_VALUES_EQUAL(uValue.Get<ui64>(), 123);

        uValue = NUdf::TUnboxedValuePod().MakeOptional().MakeOptional();
        uValue = TestPackUnpack(type, uValue, "Type:Optional, Value:opt(opt(null))");
        UNIT_ASSERT(uValue);
        uValue = uValue.GetOptionalValue();
        UNIT_ASSERT(uValue);
        uValue = uValue.GetOptionalValue();
        UNIT_ASSERT(!uValue);

        uValue = NUdf::TUnboxedValuePod().MakeOptional();
        uValue = TestPackUnpack(type, uValue, "Type:Optional, Value:opt(null)");
        UNIT_ASSERT(uValue);
        uValue = uValue.GetOptionalValue();
        UNIT_ASSERT(!uValue);
    }

    void TestDictType() {
        TType* keyType = PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id);
        TType* payloadType = PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TType* dictType = PgmBuilder_.NewDictType(keyType, payloadType, false);

        TValuesDictHashSingleFixedMap<ui32> map;
        map[4] = NUdf::TUnboxedValuePod::Embedded("4");
        map[10] = NUdf::TUnboxedValuePod::Embedded("10");
        map[1] = NUdf::TUnboxedValuePod::Embedded("1");
        const NUdf::TUnboxedValue value = HolderFactory_.CreateDirectHashedSingleFixedMapHolder<ui32, false>(std::move(map), std::nullopt);

        const auto uValue = TestPackUnpack(dictType, value, "Type:Dict");

        UNIT_ASSERT_VALUES_EQUAL(uValue.GetDictLength(), 3);
        const auto it = uValue.GetDictIterator();
        for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
            UNIT_ASSERT_VALUES_EQUAL(ToString(key.template Get<ui32>()), std::string_view(payload.AsStringRef()));
        }
    }

    void TestVariantTypeOverStruct() {
        const std::vector<std::pair<std::string_view, TType*>> structElemenTypes = {
            {"a", PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)},
            {"b", PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"d", PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id)}};
        TType* structType = PgmBuilder_.NewStructType(structElemenTypes);
        TestVariantTypeImpl(PgmBuilder_.NewVariantType(structType));
    }

    void TestVariantTypeOverTuple() {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id));
        TType* tupleType = PgmBuilder_.NewTupleType(tupleElemenTypes);
        TestVariantTypeImpl(PgmBuilder_.NewVariantType(tupleType));
    }

    void ValidateEmbeddedLength(TChunkedBuffer buf, const TString& info) {
        size_t size = buf.Size();
        TChunkedInputBuffer chunked(std::move(buf));
        return ValidateEmbeddedLength(chunked, size, info);
    }

    void ValidateEmbeddedLength(TStringBuf buf, const TString& info) {
        TChunkedInputBuffer chunked(buf);
        return ValidateEmbeddedLength(chunked, buf.size(), info);
    }

    void ValidateEmbeddedLength(TChunkedInputBuffer& buf, size_t totalSize, const TString& info) {
        if constexpr (!Fast) {
            if (totalSize > 8) {
                ui32 len = NDetails::GetRawData<ui32>(buf);
                UNIT_ASSERT_VALUES_EQUAL_C(len + 4, totalSize, info);
            } else {
                ui32 len = NDetails::GetRawData<ui8>(buf);
                UNIT_ASSERT_VALUES_EQUAL_C(((len & 0x0f) >> 1) + 1, totalSize, info);
            }
        }
    }

    void TestPackPerformance(TType* type, const NUdf::TUnboxedValuePod& uValue)
    {
        auto packer = MakeValuePacker(false, type, EValuePackerVersion::V0);
        const THPTimer timer;
        for (size_t i = 0U; i < PERFORMANCE_COUNT; ++i) {
            packer.Pack(uValue);
        }
        Cerr << timer.Passed() << Endl;
    }

    NUdf::TUnboxedValue TestPackUnpack(TValuePackerType& packer, const NUdf::TUnboxedValuePod& uValue,
                                       const TString& additionalMsg, const std::optional<ui32>& expectedLength = {})
    {
        auto packedValue = packer.Pack(uValue);
        if constexpr (Transport) {
            if (expectedLength) {
                UNIT_ASSERT_VALUES_EQUAL_C(packedValue.Size(), *expectedLength, additionalMsg);
            }
            ValidateEmbeddedLength(packedValue, additionalMsg);
            return packer.Unpack(std::move(packedValue), HolderFactory_);
        } else {
            if (expectedLength) {
                UNIT_ASSERT_VALUES_EQUAL_C(packedValue.Size(), *expectedLength, additionalMsg);
            }
            ValidateEmbeddedLength(packedValue, additionalMsg);
            return packer.Unpack(packedValue, HolderFactory_);
        }
    }

    NUdf::TUnboxedValue TestPackUnpack(TType* type, const NUdf::TUnboxedValuePod& uValue, const TString& additionalMsg,
                                       const std::optional<ui32>& expectedLength = {})
    {
        auto packer = MakeValuePacker(false, type, EValuePackerVersion::V0);
        return TestPackUnpack(packer, uValue, additionalMsg, expectedLength);
    }

    template <typename T>
    void TestNumericValue(T value, TValuePackerType& packer, const TString& typeDesc) {
        TString additionalMsg = TStringBuilder() << typeDesc << ", Value:" << value;
        auto uValue = TestPackUnpack(packer, NUdf::TUnboxedValuePod(value), additionalMsg);
        UNIT_ASSERT_VALUES_EQUAL_C(uValue.template Get<T>(), value, additionalMsg);
    }

    template <typename T>
    void TestNumericType(NUdf::TDataTypeId schemeType) {
        TString typeDesc = TStringBuilder() << ", Type:" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Name;
        auto packer = MakeValuePacker(false, PgmBuilder_.NewDataType(schemeType), EValuePackerVersion::V0);

        TestNumericValue<T>(Max<T>(), packer, typeDesc);
        TestNumericValue<T>(Min<T>(), packer, typeDesc);
        TestNumericValue<T>(T(0), packer, typeDesc);
        TestNumericValue<T>(T(1), packer, typeDesc);
    }

    template <typename T>
    void TestOptionalNumericValue(std::optional<T> value, TValuePackerType& packer, const TString& typeDesc,
                                  const std::optional<ui32>& expectedLength = {})
    {
        TString additionalMsg = TStringBuilder() << typeDesc << "), Value:" << (value ? ToString(*value) : TString("null"));
        const auto v = value ? NUdf::TUnboxedValuePod(*value) : NUdf::TUnboxedValuePod();
        const auto uValue = TestPackUnpack(packer, v, additionalMsg, expectedLength);
        if (value) {
            UNIT_ASSERT_VALUES_EQUAL_C(uValue.template Get<T>(), *value, additionalMsg);
        } else {
            UNIT_ASSERT_C(!uValue, additionalMsg);
        }
    }

    template <typename T>
    void TestOptionalNumericType(NUdf::TDataTypeId schemeType) {
        TString typeDesc = TStringBuilder() << ", Type:Optional(" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Name;
        auto packer = MakeValuePacker(false, PgmBuilder_.NewOptionalType(PgmBuilder_.NewDataType(schemeType)), EValuePackerVersion::V0);
        TestOptionalNumericValue<T>(std::optional<T>(Max<T>()), packer, typeDesc);
        TestOptionalNumericValue<T>(std::optional<T>(Min<T>()), packer, typeDesc);
        TestOptionalNumericValue<T>(std::optional<T>(), packer, typeDesc, 1);
        TestOptionalNumericValue<T>(std::optional<T>(0), packer, typeDesc);
        TestOptionalNumericValue<T>(std::optional<T>(1), packer, typeDesc);
    }

    void TestStringValue(const std::string_view& value, TValuePackerType& packer, const TString& typeDesc, ui32 expectedLength) {
        TString additionalMsg = TStringBuilder() << typeDesc << ", Value:" << value;
        const auto v = NUdf::TUnboxedValue(MakeString(value));
        const auto uValue = TestPackUnpack(packer, v, additionalMsg, expectedLength);
        UNIT_ASSERT_VALUES_EQUAL_C(std::string_view(uValue.AsStringRef()), value, additionalMsg);
    }

    void TestStringType(NUdf::TDataTypeId schemeType) {
        TString typeDesc = TStringBuilder() << ", Type:" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Name;
        auto packer = MakeValuePacker(false, PgmBuilder_.NewDataType(schemeType), EValuePackerVersion::V0);
        TestStringValue("0123456789012345678901234567890123456789", packer, typeDesc, 40 + 4);
        TestStringValue("[]", packer, typeDesc, Fast ? (2 + 4) : (2 + 1));
        TestStringValue("1234567", packer, typeDesc, Fast ? (7 + 4) : (7 + 1));
        TestStringValue("", packer, typeDesc, Fast ? (0 + 4) : (0 + 1));
        TestStringValue("12345678", packer, typeDesc, 8 + 4);

        TString hugeString(12345678, 'X');
        TestStringValue(hugeString, packer, typeDesc, hugeString.size() + 4);
    }

    void TestUuidType() {
        auto schemeType = NUdf::TDataType<NUdf::TUuid>::Id;
        TString typeDesc = TStringBuilder() << ", Type:" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Name;
        auto packer = MakeValuePacker(false, PgmBuilder_.NewDataType(schemeType), EValuePackerVersion::V0);
        TestStringValue("0123456789abcdef", packer, typeDesc, Fast ? 16 : (16 + 4));
    }

    void TestOptionalStringValue(std::optional<std::string_view> value, TValuePackerType& packer, const TString& typeDesc, ui32 expectedLength) {
        TString additionalMsg = TStringBuilder() << typeDesc << "), Value:" << (value ? *value : "null");
        const auto v = value ? NUdf::TUnboxedValue(MakeString(*value)) : NUdf::TUnboxedValue();
        const auto uValue = TestPackUnpack(packer, v, additionalMsg, expectedLength);
        if (value) {
            UNIT_ASSERT_VALUES_EQUAL_C(std::string_view(uValue.AsStringRef()), *value, additionalMsg);
        } else {
            UNIT_ASSERT_C(!uValue, additionalMsg);
        }
    }

    void TestOptionalStringType(NUdf::TDataTypeId schemeType) {
        TString typeDesc = TStringBuilder() << ", Type:Optional(" << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(schemeType)).Name;
        auto packer = MakeValuePacker(/*stable=*/false, PgmBuilder_.NewOptionalType(PgmBuilder_.NewDataType(schemeType)), EValuePackerVersion::V0);
        TestOptionalStringValue("0123456789012345678901234567890123456789", packer, typeDesc, Fast ? (40 + 4 + 1) : (40 + 4));
        TestOptionalStringValue(std::nullopt, packer, typeDesc, 1);
        TestOptionalStringValue("[]", packer, typeDesc, Fast ? (2 + 4 + 1) : (2 + 1));
        TestOptionalStringValue("1234567", packer, typeDesc, Fast ? (7 + 4 + 1) : (7 + 1));
        TestOptionalStringValue("", packer, typeDesc, Fast ? (0 + 4 + 1) : 1);
        TestOptionalStringValue("12345678", packer, typeDesc, Fast ? (8 + 4 + 1) : (8 + 4));
    }

    void TestVariantTypeImpl(TType* variantType) {
        TString descr = TStringBuilder() << "Type:Variant("
                                         << static_cast<TVariantType*>(variantType)->GetUnderlyingType()->GetKindAsStr() << ')';
        {
            const NUdf::TUnboxedValue value = HolderFactory_.CreateVariantHolder(MakeString("01234567890123456789"), 0);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 0);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890123456789");
        }
        {
            const NUdf::TUnboxedValue value = HolderFactory_.CreateVariantHolder(NUdf::TUnboxedValuePod(), 1);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 1);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT(!e);
        }
        {
            const NUdf::TUnboxedValue value = HolderFactory_.CreateVariantHolder(NUdf::TUnboxedValuePod(ui64(12345)), 2);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 2);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345ull);
        }
    }

    NUdf::TUnboxedValue MakeTupleValue(TType*& tupleType, bool forPerf = false) {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[3]));
        tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(tupleElemenTypes[3]));
        if (!forPerf) {
            tupleElemenTypes.push_back(PgmBuilder_.NewDecimalType(16, 8));
            tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(PgmBuilder_.NewDecimalType(22, 3)));
            tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(PgmBuilder_.NewDecimalType(35, 2)));
            tupleElemenTypes.push_back(PgmBuilder_.NewOptionalType(PgmBuilder_.NewDecimalType(29, 0)));
        }
        tupleType = PgmBuilder_.NewTupleType(tupleElemenTypes);

        auto inf = NYql::NDecimal::FromString("inf", 16, 8);
        auto dec1 = NYql::NDecimal::FromString("12345.673", 22, 3);
        auto dec2 = NYql::NDecimal::FromString("-9781555555.99", 35, 2);

        TUnboxedValueVector tupleElemens;
        tupleElemens.push_back(MakeString("01234567890123456789"));
        tupleElemens.push_back(MakeString("01234567890"));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        if (!forPerf) {
            tupleElemens.push_back(NUdf::TUnboxedValuePod(inf));
            tupleElemens.push_back(NUdf::TUnboxedValuePod(dec1));
            tupleElemens.push_back(NUdf::TUnboxedValuePod(dec2));
            tupleElemens.push_back(NUdf::TUnboxedValuePod());
        }

        return HolderFactory_.VectorAsArray(tupleElemens);
    }

    void ValidateTupleValue(const NUdf::TUnboxedValue& value, bool forPerf = false) {
        using NYql::NUdf::TStringValue;
        UNIT_ASSERT(value.IsBoxed());

        auto e0 = value.GetElement(0);
        auto e1 = value.GetElement(1);
        UNIT_ASSERT_VALUES_EQUAL(std::string_view(e0.AsStringRef()), "01234567890123456789");
        UNIT_ASSERT_VALUES_EQUAL(std::string_view(e1.AsStringRef()), "01234567890");
        UNIT_ASSERT(!value.GetElement(2).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(3).Get<ui64>(), 12345);
        UNIT_ASSERT(!value.GetElement(4).HasValue());
        UNIT_ASSERT_VALUES_EQUAL(value.GetElement(5).Get<ui64>(), 12345);
        if (!forPerf) {
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(NYql::NDecimal::ToString(value.GetElement(6).GetInt128(), 16, 8)), "inf");
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(NYql::NDecimal::ToString(value.GetElement(7).GetInt128(), 22, 3)), "12345.673");
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(NYql::NDecimal::ToString(value.GetElement(8).GetInt128(), 35, 2)), "-9781555555.99");
            UNIT_ASSERT(!value.GetElement(9).HasValue());
        }
    }

    void TestTuplePackPerformance() {
        TType* tupleType;
        const auto value = MakeTupleValue(tupleType, true);
        TestPackPerformance(tupleType, value);
    }

    void TestPairPackPerformance() {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id));
        tupleElemenTypes.push_back(PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id));
        TType* tupleType = PgmBuilder_.NewTupleType(tupleElemenTypes);

        TUnboxedValueVector tupleElemens;
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui32(12345)));
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui32(67890)));

        const NUdf::TUnboxedValue value = HolderFactory_.VectorAsArray(tupleElemens);
        TestPackPerformance(tupleType, value);
    }

    void TestShortStringPackPerformance() {
        const auto v = NUdf::TUnboxedValuePod::Embedded("01234");
        TType* type = PgmBuilder_.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TestPackPerformance(type, v);
    }

    void TestIntegerPackPerformance() {
        const auto& v = NUdf::TUnboxedValuePod(ui64("123456789ULL"));
        TType* type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        TestPackPerformance(type, v);
    }

    void TestRopeSplit() {
        if constexpr (Transport) {
            TType* tupleType;
            const auto value = MakeTupleValue(tupleType);

            auto packer = MakeValuePacker(false, tupleType, EValuePackerVersion::V0);

            auto buffer = packer.Pack(value);

            TString packed;
            TStringOutput sout(packed);
            buffer.CopyTo(sout);
            TStringBuf packedBuf(packed);

            if constexpr (Fast) {
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 73);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 54);
            }

            for (size_t chunk = 1; chunk < packed.size(); ++chunk) {
                TStringBuf first = packedBuf.substr(0, chunk);
                TStringBuf second = packedBuf.substr(chunk);

                TChunkedBuffer result(first, {});
                result.Append(second, {});

                UNIT_ASSERT_VALUES_EQUAL(result.Size(), packed.size());
                UNIT_ASSERT(result.Size() != result.ContigousSize());

                ValidateTupleValue(packer.Unpack(std::move(result), HolderFactory_));
            }
        }
    }

    void TestIncrementalPacking() {
        if constexpr (Transport) {
            auto itemType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
            auto listType = PgmBuilder_.NewListType(itemType);
            auto packer = MakeValuePacker(false, itemType, EValuePackerVersion::V0);
            auto listPacker = MakeValuePacker(false, listType, EValuePackerVersion::V0);

            TStringBuf str = "01234567890ABCDEFG";

            size_t count = 500000;

            for (size_t i = 0; i < count; ++i) {
                NUdf::TUnboxedValue item(MakeString(str));
                packer.AddItem(item);
            }

            auto serialized = packer.Finish();

            auto listObj = listPacker.Unpack(TChunkedBuffer(serialized), HolderFactory_);
            UNIT_ASSERT_VALUES_EQUAL(listObj.GetListLength(), count);
            const auto iter = listObj.GetListIterator();
            for (NUdf::TUnboxedValue uVal; iter.Next(uVal);) {
                UNIT_ASSERT(uVal);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(uVal.AsStringRef()), str);
            }

            TUnboxedValueBatch items;
            packer.UnpackBatch(std::move(serialized), HolderFactory_, items);
            UNIT_ASSERT_VALUES_EQUAL(items.RowCount(), count);
            items.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                UNIT_ASSERT(value);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(value.AsStringRef()), str);
            });
        }
    }

    struct TBlockTestArgs {
        ui64 Offset = 0;
        ui64 Len = 0;
        bool LegacyStruct = false;
        bool TrimBlock = false;
        TMaybe<ui8> MinFillPercentage;
        EValuePackerVersion PackerVersion;

        TString ToString() const {
            auto result = TStringBuilder() << "Offset: " << Offset << ", Len: " << Len << ", LegacyStruct: " << LegacyStruct << ", TrimBlock: " << TrimBlock;
            if (MinFillPercentage) {
                result << ", MinFillPercentage: " << ui64(*MinFillPercentage);
            }
            result << ", PackerVersion: " << static_cast<int>(PackerVersion);

            return result;
        }
    };

    class IArgsDispatcher: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IArgsDispatcher>;

        virtual ui64 GetSize() const = 0;
        virtual void Set(ui64 index) = 0;
    };

    template <typename TValue>
    class TArgsDispatcher: public IArgsDispatcher {
    public:
        TArgsDispatcher(TValue& dst, const std::vector<TValue>& choices)
            : Dst_(dst)
            , Choices_(choices)
        {
            UNIT_ASSERT_C(!choices.empty(), "Choices should not be empty");
        }

        ui64 GetSize() const override {
            return Choices_.size();
        }

        void Set(ui64 index) override {
            UNIT_ASSERT_LE_C(index + 1, Choices_.size(), "Invalid args dispatcher index");
            Dst_ = Choices_[index];
        }

    private:
        TValue& Dst_;
        const std::vector<TValue> Choices_;
    };

    void DoTestBlockPacking(const TBlockTestArgs& args) {
        bool legacyStruct = args.LegacyStruct;
        ui64 offset = args.Offset;
        ui64 len = args.Len;
        auto strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto ui32Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id);
        auto ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto optStrType = PgmBuilder_.NewOptionalType(strType);
        auto optUi32Type = PgmBuilder_.NewOptionalType(ui32Type);
        auto optOptUi32Type = PgmBuilder_.NewOptionalType(optUi32Type);

        auto tupleOptUi32StrType = PgmBuilder_.NewTupleType({optUi32Type, strType});
        auto optTupleOptUi32StrType = PgmBuilder_.NewOptionalType(tupleOptUi32StrType);

        auto blockUi32Type = PgmBuilder_.NewBlockType(ui32Type, TBlockType::EShape::Many);
        auto blockOptStrType = PgmBuilder_.NewBlockType(optStrType, TBlockType::EShape::Many);
        auto scalarOptStrType = PgmBuilder_.NewBlockType(optStrType, TBlockType::EShape::Scalar);
        auto blockOptTupleOptUi32StrType = PgmBuilder_.NewBlockType(optTupleOptUi32StrType, TBlockType::EShape::Many);
        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptOptUi32Type = PgmBuilder_.NewBlockType(optOptUi32Type, TBlockType::EShape::Many);
        auto tzDateType = PgmBuilder_.NewDataType(NUdf::EDataSlot::TzDate);
        auto blockTzDateType = PgmBuilder_.NewBlockType(tzDateType, TBlockType::EShape::Many);
        auto nullType = PgmBuilder_.NewNullType();
        auto blockNullType = PgmBuilder_.NewBlockType(nullType, TBlockType::EShape::Many);

        auto rowType =
            legacyStruct
                ? PgmBuilder_.NewStructType({{"A", blockUi32Type},
                                             {"B", blockOptStrType},
                                             {"_yql_block_length", scalarUi64Type},
                                             {"a", scalarOptStrType},
                                             {"b", blockOptTupleOptUi32StrType},
                                             {"c", blockTzDateType},
                                             {"nill", blockNullType},
                                             {"optOptInt32", blockOptOptUi32Type}})
                : PgmBuilder_.NewMultiType(
                      {blockUi32Type, blockOptStrType, scalarOptStrType,
                       blockOptTupleOptUi32StrType, blockTzDateType, blockNullType, blockOptOptUi32Type, scalarUi64Type});

        ui64 blockLen = 1000;
        UNIT_ASSERT_LE(offset + len, blockLen);

        auto builder1 = MakeArrayBuilder(TTypeInfoHelper(), ui32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(ui32Type)), nullptr);
        auto builder2 = MakeArrayBuilder(TTypeInfoHelper(), optStrType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optStrType)), nullptr);
        auto builder3 = MakeArrayBuilder(TTypeInfoHelper(), optTupleOptUi32StrType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optTupleOptUi32StrType)), nullptr);
        auto builder4 = MakeArrayBuilder(TTypeInfoHelper(), tzDateType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(tzDateType)), nullptr);
        auto builder5 = MakeArrayBuilder(TTypeInfoHelper(), nullType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(nullType)), nullptr);
        auto builder6 = MakeArrayBuilder(TTypeInfoHelper(), optOptUi32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optOptUi32Type)), nullptr);

        for (ui32 i = 0; i < blockLen; ++i) {
            TBlockItem b1(i);
            builder1->Add(b1);

            TString a = "a string " + ToString(i);
            TBlockItem b2 = (i % 2) ? TBlockItem(a) : TBlockItem();
            builder2->Add(b2);

            auto b3items = std::to_array<TBlockItem>({(i % 2) ? TBlockItem(i) : TBlockItem(), TBlockItem(a)});
            TBlockItem b3 = (i % 7) ? TBlockItem(b3items.data()) : TBlockItem();
            builder3->Add(b3);

            TBlockItem tzDate{i};
            tzDate.SetTimezoneId(i % 100);
            builder4->Add(tzDate);
            builder5->Add(TBlockItem());
            switch (i % 3) {
                case 0:
                    builder6->Add(TBlockItem(i));
                    break;
                case 1:
                    builder6->Add(TBlockItem().MakeOptional());
                    break;
                case 2:
                    builder6->Add(TBlockItem());
                    break;
            }
        }

        std::string_view testScalarString = "foobar";
        auto strbuf = std::make_shared<arrow::Buffer>((const ui8*)testScalarString.data(), testScalarString.size());

        TVector<arrow::Datum> datums;
        if (legacyStruct) {
            datums.emplace_back(builder1->Build(true));
            datums.emplace_back(builder2->Build(true));
            datums.emplace_back(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
            datums.emplace_back(arrow::Datum(std::make_shared<arrow::BinaryScalar>(strbuf)));
            datums.emplace_back(builder3->Build(true));
            datums.emplace_back(builder4->Build(true));
            datums.emplace_back(builder5->Build(true));
            datums.emplace_back(builder6->Build(true));
        } else {
            datums.emplace_back(builder1->Build(true));
            datums.emplace_back(builder2->Build(true));
            datums.emplace_back(arrow::Datum(std::make_shared<arrow::BinaryScalar>(strbuf)));
            datums.emplace_back(builder3->Build(true));
            datums.emplace_back(builder4->Build(true));
            datums.emplace_back(builder5->Build(true));
            datums.emplace_back(builder6->Build(true));
            datums.emplace_back(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
        }

        const ui32 blockLenIndex = legacyStruct ? 2 : datums.size() - 1;
        if (offset != 0 || len != blockLen) {
            for (auto& datum : datums) {
                if (datum.is_array()) {
                    datum = NYql::NUdf::DeepSlice(datum.array(), offset, len);
                }
            }
            datums[blockLenIndex] = arrow::Datum(std::make_shared<arrow::UInt64Scalar>(len));
        }

        const auto trimmerFactory = [&](ui32 index) {
            const TType* columnType = legacyStruct ? static_cast<const TStructType*>(rowType)->GetMemberType(index)
                                                   : static_cast<const TMultiType*>(rowType)->GetElementType(index);
            return MakeBlockTrimmer(NMiniKQL::TTypeInfoHelper(), static_cast<const TBlockType*>(columnType)->GetItemType(), ArrowPool_);
        };
        if (args.TrimBlock) {
            for (ui32 index = 0; index < datums.size(); ++index) {
                auto& datum = datums[index];
                if (!datum.is_array()) {
                    continue;
                }
                datum = trimmerFactory(index)->Trim(datum.array());
            }
        }
        TUnboxedValueVector columns;
        for (auto& datum : datums) {
            columns.emplace_back(HolderFactory_.CreateArrowBlock(std::move(datum)));
        }

        auto senderPacker = MakeValuePacker(false, rowType, args.PackerVersion, {}, ArrowPool_, args.MinFillPercentage);
        if (legacyStruct) {
            TUnboxedValueVector columnsCopy = columns;
            NUdf::TUnboxedValue row = HolderFactory_.VectorAsArray(columnsCopy);
            senderPacker.AddItem(row);
        } else {
            senderPacker.AddWideItem(columns.data(), columns.size());
        }
        TChunkedBuffer packed = senderPacker.Finish();

        TUnboxedValueBatch unpacked(rowType);
        auto receiverPacker = MakeValuePacker(false, rowType, args.PackerVersion, {}, ArrowPool_, args.MinFillPercentage);
        receiverPacker.UnpackBatch(std::move(packed), HolderFactory_, unpacked);

        UNIT_ASSERT_VALUES_EQUAL(unpacked.RowCount(), 1);

        TUnboxedValueVector unpackedColumns;
        if (legacyStruct) {
            auto elements = unpacked.Head()->GetElements();
            unpackedColumns.insert(unpackedColumns.end(), elements, elements + columns.size());
        } else {
            unpacked.ForEachRowWide([&](const NYql::NUdf::TUnboxedValue* values, ui32 count) {
                unpackedColumns.insert(unpackedColumns.end(), values, values + count);
            });
        }

        UNIT_ASSERT_VALUES_EQUAL(unpackedColumns.size(), columns.size());
        if (legacyStruct) {
            UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[2]).GetDatum().scalar_as<arrow::UInt64Scalar>().value, len);
            UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[3]).GetDatum().scalar_as<arrow::BinaryScalar>().value->ToString(), testScalarString);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value, len);
            UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[2]).GetDatum().scalar_as<arrow::BinaryScalar>().value->ToString(), testScalarString);
        }

        if (args.MinFillPercentage) {
            for (size_t i = 0; i < unpackedColumns.size(); ++i) {
                auto datum = TArrowBlock::From(unpackedColumns[i]).GetDatum();
                if (datum.is_scalar()) {
                    continue;
                }
                const auto unpackedSize = NUdf::GetSizeOfArrayDataInBytes(*datum.array());
                const auto trimmedSize = NUdf::GetSizeOfArrayDataInBytes(*trimmerFactory(i)->Trim(datum.array()));
                UNIT_ASSERT_GE_C(trimmedSize, unpackedSize * *args.MinFillPercentage / 100, "column: " << i);
            }
        }

        auto reader1 = MakeBlockReader(TTypeInfoHelper(), ui32Type);
        auto reader2 = MakeBlockReader(TTypeInfoHelper(), optStrType);
        auto reader3 = MakeBlockReader(TTypeInfoHelper(), optTupleOptUi32StrType);
        auto reader4 = MakeBlockReader(TTypeInfoHelper(), tzDateType);
        auto reader5 = MakeBlockReader(TTypeInfoHelper(), nullType);
        auto reader6 = MakeBlockReader(TTypeInfoHelper(), optOptUi32Type);

        for (ui32 i = offset; i < len; ++i) {
            TBlockItem b1 = reader1->GetItem(*TArrowBlock::From(unpackedColumns[0]).GetDatum().array(), i - offset);
            UNIT_ASSERT_VALUES_EQUAL(b1.As<ui32>(), i);

            TString a = "a string " + ToString(i);
            TBlockItem b2 = reader2->GetItem(*TArrowBlock::From(unpackedColumns[1]).GetDatum().array(), i - offset);
            if (i % 2) {
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(b2.AsStringRef()), a);
            } else {
                UNIT_ASSERT(!b2);
            }

            TBlockItem b3 = reader3->GetItem(*TArrowBlock::From(unpackedColumns[legacyStruct ? 4 : 3]).GetDatum().array(), i - offset);
            if (i % 7) {
                auto elements = b3.GetElements();
                if (i % 2) {
                    UNIT_ASSERT_VALUES_EQUAL(elements[0].As<ui32>(), i);
                } else {
                    UNIT_ASSERT(!elements[0]);
                }
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(elements[1].AsStringRef()), a);
            } else {
                UNIT_ASSERT(!b3);
            }

            TBlockItem b4 = reader4->GetItem(*TArrowBlock::From(unpackedColumns[legacyStruct ? 5 : 4]).GetDatum().array(), i - offset);
            UNIT_ASSERT(b4.Get<ui16>() == i);
            UNIT_ASSERT(b4.GetTimezoneId() == (i % 100));
            TBlockItem b5 = reader5->GetItem(*TArrowBlock::From(unpackedColumns[legacyStruct ? 6 : 5]).GetDatum().array(), i - offset);
            UNIT_ASSERT(!b5);
            TBlockItem b6 = reader6->GetItem(*TArrowBlock::From(unpackedColumns[legacyStruct ? 7 : 6]).GetDatum().array(), i - offset);
            switch (i % 3) {
                case 0:
                    UNIT_ASSERT_EQUAL(b6.Get<ui64>(), i);
                    break;
                case 1:
                    UNIT_ASSERT(!b6.GetOptionalValue());
                    break;
                case 2:
                    UNIT_ASSERT(!b6);
                    break;
            }
        }
    }

    void TestBlockPackingCases(TBlockTestArgs& args, std::vector<typename IArgsDispatcher::TPtr> dispatchers = {}) {
        ui64 numberCases = 1;
        for (const auto& dispatcher : dispatchers) {
            numberCases *= dispatcher->GetSize();
        }
        for (ui64 i = 0; i < numberCases; ++i) {
            ui64 caseId = i;
            for (const auto& dispatcher : dispatchers) {
                dispatcher->Set(caseId % dispatcher->GetSize());
                caseId /= dispatcher->GetSize();
            }
            Cerr << "Run block packing test case: " << args.ToString() << "\n";
            DoTestBlockPacking(args);
        }
    }

    void TestBlockPacking() {
        TBlockTestArgs args;
        TestBlockPackingCases(args, {MakeIntrusive<TArgsDispatcher<TBlockTestArgs>>(args, std::vector<TBlockTestArgs>{
                                                                                              {.Offset = 0, .Len = 3},
                                                                                              {.Offset = 0, .Len = 1000},
                                                                                              {.Offset = 19, .Len = 623}}),
                                     MakeIntrusive<TArgsDispatcher<bool>>(args.LegacyStruct, std::vector<bool>{false, true}),
                                     MakeIntrusive<TArgsDispatcher<bool>>(args.TrimBlock, std::vector<bool>{false, true}),
                                     MakeIntrusive<TArgsDispatcher<TMaybe<ui8>>>(args.MinFillPercentage, std::vector<TMaybe<ui8>>{Nothing(), 90}),
                                     MakeIntrusive<TArgsDispatcher<EValuePackerVersion>>(args.PackerVersion, std::vector<EValuePackerVersion>{EValuePackerVersion::V0, EValuePackerVersion::V1})});
    }

    void TestBlockPackingSerializeUi32Impl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* ui32Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);

        auto blockUi32Type = PgmBuilder_.NewBlockType(ui32Type, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockUi32Type, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), ui32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(ui32Type)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem(1));
        builder->Add(NYql::NUdf::TBlockItem(2));
        builder->Add(NYql::NUdf::TBlockItem(3));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());
        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeUi32() {
        TestBlockPackingSerializeUi32Impl(EValuePackerVersion::V0, 12952134869926058643U);
        TestBlockPackingSerializeUi32Impl(EValuePackerVersion::V0, 12952134869926058643U);
    }

    void TestBlockPackingSerializeOptionalUi32Impl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* ui32Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* optUi32Type = PgmBuilder_.NewOptionalType(ui32Type);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptUi32Type = PgmBuilder_.NewBlockType(optUi32Type, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptUi32Type, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optUi32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optUi32Type)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem(ui32(1)));
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem(ui32(3)));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());
        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalUi32() {
        TestBlockPackingSerializeOptionalUi32Impl(EValuePackerVersion::V0, 2597716339394977815U);
        TestBlockPackingSerializeOptionalUi32Impl(EValuePackerVersion::V1, 13473152532547735594U);
    }

    void TestBlockPackingSerializeStringImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockStrType = PgmBuilder_.NewBlockType(strType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockStrType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), strType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(strType)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("str1")));
        builder->Add(NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("str2")));
        builder->Add(NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("str3")));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());
        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeString() {
        TestBlockPackingSerializeStringImpl(EValuePackerVersion::V0, 2530494095874447064U);
        TestBlockPackingSerializeStringImpl(EValuePackerVersion::V1, 10045743204568850498U);
    }

    void TestBlockPackingSerializeOptionalStringImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* optStrType = PgmBuilder_.NewOptionalType(strType);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptStrType = PgmBuilder_.NewBlockType(optStrType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptStrType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optStrType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optStrType)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("str1")));
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("str3")));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());
        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalString() {
        TestBlockPackingSerializeOptionalStringImpl(EValuePackerVersion::V0, 15589836522266239526U);
        TestBlockPackingSerializeOptionalStringImpl(EValuePackerVersion::V1, 7371349434798600169U);
    }

    void TestBlockPackingSerializeOptionalOptionalUi32Impl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* ui32Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui32>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* optUi32Type = PgmBuilder_.NewOptionalType(ui32Type);
        auto* optOptUi32Type = PgmBuilder_.NewOptionalType(optUi32Type);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptOptUi32Type = PgmBuilder_.NewBlockType(optOptUi32Type, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptOptUi32Type, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optOptUi32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optOptUi32Type)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem(ui32(1)));
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem(ui32(3)));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalOptionalUi32() {
        TestBlockPackingSerializeOptionalOptionalUi32Impl(EValuePackerVersion::V0, 8466504593757236838U);
        TestBlockPackingSerializeOptionalOptionalUi32Impl(EValuePackerVersion::V1, 7467770574734535989U);
    }

    void TestBlockPackingSerializeSingularTypeImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* nullType = PgmBuilder_.NewNullType();

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockNullType = PgmBuilder_.NewBlockType(nullType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockNullType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), nullType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(nullType)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem());
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeSingularType() {
        TestBlockPackingSerializeSingularTypeImpl(EValuePackerVersion::V0, 2342694087331559075U);
        TestBlockPackingSerializeSingularTypeImpl(EValuePackerVersion::V1, 814315123753111618U);
    }

    void TestBlockPackingSerializeOptionalSingularTypeImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* nullType = PgmBuilder_.NewNullType();
        auto* optNullType = PgmBuilder_.NewOptionalType(nullType);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptNullType = PgmBuilder_.NewBlockType(optNullType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptNullType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optNullType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optNullType)), nullptr);
        builder->Add(NYql::NUdf::TBlockItem().MakeOptional());
        builder->Add(NYql::NUdf::TBlockItem());
        builder->Add(NYql::NUdf::TBlockItem().MakeOptional());
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalSingularType() {
        TestBlockPackingSerializeOptionalSingularTypeImpl(EValuePackerVersion::V0, 7314801330449120945U);
        TestBlockPackingSerializeOptionalSingularTypeImpl(EValuePackerVersion::V1, 13506722731499795140U);
    }

    void TestBlockPackingSerializeTTzDateImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* tzDateType = PgmBuilder_.NewDataType(NUdf::EDataSlot::TzDate);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockTzDateType = PgmBuilder_.NewBlockType(tzDateType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockTzDateType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), tzDateType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(tzDateType)), nullptr);
        NYql::NUdf::TBlockItem tzDate1{ui16(1)};
        tzDate1.SetTimezoneId(100);
        builder->Add(tzDate1);
        NYql::NUdf::TBlockItem tzDate2{ui16(2)};
        tzDate2.SetTimezoneId(200);
        builder->Add(tzDate2);
        NYql::NUdf::TBlockItem tzDate3{ui16(3)};
        tzDate3.SetTimezoneId(300);
        builder->Add(tzDate3);
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeTTzDate() {
        TestBlockPackingSerializeTTzDateImpl(EValuePackerVersion::V0, 17903070954188109601U);
        TestBlockPackingSerializeTTzDateImpl(EValuePackerVersion::V1, 2802122333798714691);
    }

    void TestBlockPackingSerializeOptionalTTzDateImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* tzDateType = PgmBuilder_.NewDataType(NUdf::EDataSlot::TzDate);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* optTzDateType = PgmBuilder_.NewOptionalType(tzDateType);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptTzDateType = PgmBuilder_.NewBlockType(optTzDateType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptTzDateType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optTzDateType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optTzDateType)), nullptr);
        NYql::NUdf::TBlockItem tzDate1{ui16(1)};
        tzDate1.SetTimezoneId(100);
        builder->Add(tzDate1);
        builder->Add(NYql::NUdf::TBlockItem());
        NYql::NUdf::TBlockItem tzDate3{ui16(3)};
        tzDate3.SetTimezoneId(300);
        builder->Add(tzDate3);
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalTTzDate() {
        TestBlockPackingSerializeOptionalTTzDateImpl(EValuePackerVersion::V0, 18400739887390114022U);
        TestBlockPackingSerializeOptionalTTzDateImpl(EValuePackerVersion::V1, 11315052524575174297U);
    }

    void TestBlockPackingSerializeTupleInt32StringImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* i32Type = PgmBuilder_.NewDataType(NUdf::TDataType<i32>::Id);
        auto* strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* tupleType = PgmBuilder_.NewTupleType({i32Type, strType});

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockTupleType = PgmBuilder_.NewBlockType(tupleType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockTupleType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), tupleType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(tupleType)), nullptr);
        auto tuple1Items = std::to_array<NYql::NUdf::TBlockItem>({NYql::NUdf::TBlockItem(i32(1)), NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("a"))});
        builder->Add(NYql::NUdf::TBlockItem(tuple1Items.data()));
        auto tuple2Items = std::to_array<NYql::NUdf::TBlockItem>({NYql::NUdf::TBlockItem(i32(2)), NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("b"))});
        builder->Add(NYql::NUdf::TBlockItem(tuple2Items.data()));
        auto tuple3Items = std::to_array<NYql::NUdf::TBlockItem>({NYql::NUdf::TBlockItem(i32(3)), NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("c"))});
        builder->Add(NYql::NUdf::TBlockItem(tuple3Items.data()));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeTupleInt32String() {
        TestBlockPackingSerializeTupleInt32StringImpl(EValuePackerVersion::V0, 6542528838520568576U);
        TestBlockPackingSerializeTupleInt32StringImpl(EValuePackerVersion::V1, 13820338994079843620U);
    }

    void TestBlockPackingSerializeOptionalTupleInt32StringImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash) {
        auto* i32Type = PgmBuilder_.NewDataType(NUdf::TDataType<i32>::Id);
        auto* strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* tupleType = PgmBuilder_.NewTupleType({i32Type, strType});
        auto* optTupleType = PgmBuilder_.NewOptionalType(tupleType);

        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
        auto blockOptTupleType = PgmBuilder_.NewBlockType(optTupleType, TBlockType::EShape::Many);
        auto* rowType = PgmBuilder_.NewMultiType({blockOptTupleType, scalarUi64Type});

        auto builder = MakeArrayBuilder(TTypeInfoHelper(), optTupleType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optTupleType)), nullptr);
        auto tuple1Items = std::to_array<NYql::NUdf::TBlockItem>({NYql::NUdf::TBlockItem(i32(1)), NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("a"))});
        builder->Add(NYql::NUdf::TBlockItem(tuple1Items.data()));
        builder->Add(NYql::NUdf::TBlockItem());
        auto tuple3Items = std::to_array<NYql::NUdf::TBlockItem>({NYql::NUdf::TBlockItem(i32(3)), NYql::NUdf::TBlockItem(NYql::NUdf::TStringRef("c"))});
        builder->Add(NYql::NUdf::TBlockItem(tuple3Items.data()));
        TUnboxedValueVector columns;
        columns.emplace_back(HolderFactory_.CreateArrowBlock(builder->Build(/*finish=*/true)));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(3))));
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);
    }

    void TestBlockPackingSerializeOptionalTupleInt32String() {
        TestBlockPackingSerializeOptionalTupleInt32StringImpl(EValuePackerVersion::V0, 15729296940258124779U);
        TestBlockPackingSerializeOptionalTupleInt32StringImpl(EValuePackerVersion::V1, 8613360264621805090U);
    }

    void TestBlockPackingSerializeTupleShiftedOffsetImpl(EValuePackerVersion valuePackerVersion, size_t expectedHash, bool expectedToFail) {
        auto* i32Type = PgmBuilder_.NewDataType(NUdf::TDataType<i32>::Id);
        auto* strType = PgmBuilder_.NewDataType(NUdf::TDataType<char*>::Id);
        auto* ui64Type = PgmBuilder_.NewDataType(NUdf::TDataType<ui64>::Id);
        auto* tupleType = PgmBuilder_.NewTupleType({i32Type, strType});
        auto* blockTupleType = PgmBuilder_.NewBlockType(tupleType, TBlockType::EShape::Many);
        auto scalarUi64Type = PgmBuilder_.NewBlockType(ui64Type, TBlockType::EShape::Scalar);

        auto* rowType = PgmBuilder_.NewMultiType({blockTupleType, scalarUi64Type});

        auto bigString = (TString("Very long string") * 1000000).substr(0, 2000000);
        auto builder = MakeArrayBuilder(TTypeInfoHelper(), tupleType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(tupleType)), nullptr);
        {
            auto tuple = std::to_array<TBlockItem>({TBlockItem(1), TBlockItem(NYql::NUdf::TStringRef("Short string"))});
            builder->Add(TBlockItem(tuple.data()));
        }
        {
            auto tuple = std::to_array<TBlockItem>({TBlockItem(2), TBlockItem(NYql::NUdf::TStringRef("Short string"))});
            builder->Add(TBlockItem(tuple.data()));
        }
        {
            auto tuple = std::to_array<TBlockItem>({TBlockItem(3), TBlockItem(NYql::NUdf::TStringRef(bigString))});
            builder->Add(TBlockItem(tuple.data()));
        }
        auto buildResult = builder->Build(/*finish=*/true);
        UNIT_ASSERT_EQUAL(buildResult.kind(), arrow::Datum::CHUNKED_ARRAY);
        UNIT_ASSERT_EQUAL(buildResult.chunks().size(), 2);

        auto children = buildResult.chunks()[1]->data()->child_data;
        // Check that children's offsets are different.
        // It is the main invariant for this test.
        UNIT_ASSERT_UNEQUAL(children[0]->offset, children[1]->offset);

        TUnboxedValueVector columns;
        // Here we take only second chunk with exactly one element.
        columns.emplace_back(HolderFactory_.CreateArrowBlock(buildResult.chunks()[1]));
        columns.emplace_back(HolderFactory_.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(1))));

        if (expectedToFail) {
            UNIT_ASSERT_EXCEPTION(MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish(), yexception);
            return;
        }
        TChunkedBuffer serialized = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing()).AddWideItem(columns.data(), columns.size()).Finish();
        TStringStream stream;
        serialized.CopyTo(stream);
        auto actualHash = THash<TStringBuf>{}(stream.Str());

        UNIT_ASSERT_EQUAL_C(actualHash, expectedHash, TStringBuilder() << "Actual hash: " << actualHash);

        // Test deserialization result
        TUnboxedValueBatch unpacked(rowType);
        auto receiverPacker = MakeValuePacker(false, rowType, valuePackerVersion, {}, ArrowPool_, Nothing());
        receiverPacker.UnpackBatch(std::move(serialized), HolderFactory_, unpacked);

        UNIT_ASSERT_VALUES_EQUAL(unpacked.RowCount(), 1);

        TUnboxedValueVector unpackedColumns;
        unpacked.ForEachRowWide([&](const NYql::NUdf::TUnboxedValue* values, ui32 count) {
            unpackedColumns.insert(unpackedColumns.end(), values, values + count);
        });

        UNIT_ASSERT_VALUES_EQUAL(unpackedColumns.size(), 2);

        auto tupleReader = MakeBlockReader(TTypeInfoHelper(), tupleType);
        auto tupleArray = TArrowBlock::From(unpackedColumns[0]).GetDatum().array();
        UNIT_ASSERT_VALUES_EQUAL(tupleArray->length, 1);
        TBlockItem deserializedTuple = tupleReader->GetItem(*tupleArray, 0);
        auto elements = deserializedTuple.GetElements();

        UNIT_ASSERT_VALUES_EQUAL(elements[0].As<i32>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(TString(elements[1].AsStringRef()), bigString);
        UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value, 1);
    }

    void TestBlockPackingSerializeTupleShiftedOffset() {
        TestBlockPackingSerializeTupleShiftedOffsetImpl(EValuePackerVersion::V0, 0U, /*expectedToFail=*/true);
        TestBlockPackingSerializeTupleShiftedOffsetImpl(EValuePackerVersion::V1, 4876856722251275868U, /*expectedToFail=*/false);
    }

private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry_;
    TIntrusivePtr<IRandomProvider> RandomProvider_;
    TScopedAlloc Alloc_;
    TTypeEnvironment Env_;
    TProgramBuilder PgmBuilder_;
    TMemoryUsageInfo MemInfo_;
    THolderFactory HolderFactory_;
    arrow::MemoryPool* const ArrowPool_;
};

class TMiniKQLComputationNodeGenericPackTest: public TMiniKQLComputationNodePackTest<false, false> {
    UNIT_TEST_SUITE(TMiniKQLComputationNodeGenericPackTest);
    UNIT_TEST(TestNumericTypes);
    UNIT_TEST(TestOptionalNumericTypes);
    UNIT_TEST(TestStringTypes);
    UNIT_TEST(TestUuidType);
    UNIT_TEST(TestOptionalStringTypes);
    UNIT_TEST(TestListType);
    UNIT_TEST(TestListOfOptionalsType);
    UNIT_TEST(TestTupleType);
    UNIT_TEST(TestStructType);
    UNIT_TEST(TestOptionalType);
    UNIT_TEST(TestDictType);
    UNIT_TEST(TestVariantTypeOverStruct);
    UNIT_TEST(TestVariantTypeOverTuple);
    UNIT_TEST(TestIntegerPackPerformance);
    UNIT_TEST(TestShortStringPackPerformance);
    UNIT_TEST(TestPairPackPerformance);
    UNIT_TEST(TestTuplePackPerformance);
    UNIT_TEST_SUITE_END();
};

class TMiniKQLComputationNodeGenericFastPackTest: public TMiniKQLComputationNodePackTest<true, false> {
    UNIT_TEST_SUITE(TMiniKQLComputationNodeGenericFastPackTest);
    UNIT_TEST(TestNumericTypes);
    UNIT_TEST(TestOptionalNumericTypes);
    UNIT_TEST(TestStringTypes);
    UNIT_TEST(TestUuidType);
    UNIT_TEST(TestOptionalStringTypes);
    UNIT_TEST(TestListType);
    UNIT_TEST(TestListOfOptionalsType);
    UNIT_TEST(TestTupleType);
    UNIT_TEST(TestStructType);
    UNIT_TEST(TestOptionalType);
    UNIT_TEST(TestDictType);
    UNIT_TEST(TestVariantTypeOverStruct);
    UNIT_TEST(TestVariantTypeOverTuple);
    UNIT_TEST(TestIntegerPackPerformance);
    UNIT_TEST(TestShortStringPackPerformance);
    UNIT_TEST(TestPairPackPerformance);
    UNIT_TEST(TestTuplePackPerformance);
    UNIT_TEST_SUITE_END();
};

class TMiniKQLComputationNodeTransportPackTest: public TMiniKQLComputationNodePackTest<false, true> {
    UNIT_TEST_SUITE(TMiniKQLComputationNodeTransportPackTest);
    UNIT_TEST(TestNumericTypes);
    UNIT_TEST(TestOptionalNumericTypes);
    UNIT_TEST(TestStringTypes);
    UNIT_TEST(TestUuidType);
    UNIT_TEST(TestOptionalStringTypes);
    UNIT_TEST(TestListType);
    UNIT_TEST(TestListOfOptionalsType);
    UNIT_TEST(TestTupleType);
    UNIT_TEST(TestStructType);
    UNIT_TEST(TestOptionalType);
    UNIT_TEST(TestDictType);
    UNIT_TEST(TestVariantTypeOverStruct);
    UNIT_TEST(TestVariantTypeOverTuple);
    UNIT_TEST(TestIntegerPackPerformance);
    UNIT_TEST(TestShortStringPackPerformance);
    UNIT_TEST(TestPairPackPerformance);
    UNIT_TEST(TestTuplePackPerformance);
    UNIT_TEST(TestRopeSplit);
    UNIT_TEST(TestIncrementalPacking);
    UNIT_TEST(TestBlockPacking);

    UNIT_TEST(TestBlockPackingSerializeUi32);
    UNIT_TEST(TestBlockPackingSerializeOptionalUi32);
    UNIT_TEST(TestBlockPackingSerializeString);
    UNIT_TEST(TestBlockPackingSerializeOptionalString);
    UNIT_TEST(TestBlockPackingSerializeOptionalOptionalUi32);
    UNIT_TEST(TestBlockPackingSerializeSingularType);
    UNIT_TEST(TestBlockPackingSerializeOptionalSingularType);
    UNIT_TEST(TestBlockPackingSerializeTTzDate);
    UNIT_TEST(TestBlockPackingSerializeOptionalTTzDate);
    UNIT_TEST(TestBlockPackingSerializeTupleInt32String);
    UNIT_TEST(TestBlockPackingSerializeOptionalTupleInt32String);
    UNIT_TEST(TestBlockPackingSerializeTupleShiftedOffset);
    UNIT_TEST_SUITE_END();
};

class TMiniKQLComputationNodeTransportFastPackTest: public TMiniKQLComputationNodePackTest<true, true> {
    UNIT_TEST_SUITE(TMiniKQLComputationNodeTransportFastPackTest);
    UNIT_TEST(TestNumericTypes);
    UNIT_TEST(TestOptionalNumericTypes);
    UNIT_TEST(TestStringTypes);
    UNIT_TEST(TestUuidType);
    UNIT_TEST(TestOptionalStringTypes);
    UNIT_TEST(TestListType);
    UNIT_TEST(TestListOfOptionalsType);
    UNIT_TEST(TestTupleType);
    UNIT_TEST(TestStructType);
    UNIT_TEST(TestOptionalType);
    UNIT_TEST(TestDictType);
    UNIT_TEST(TestVariantTypeOverStruct);
    UNIT_TEST(TestVariantTypeOverTuple);
    UNIT_TEST(TestIntegerPackPerformance);
    UNIT_TEST(TestShortStringPackPerformance);
    UNIT_TEST(TestPairPackPerformance);
    UNIT_TEST(TestTuplePackPerformance);
    UNIT_TEST(TestRopeSplit);
    UNIT_TEST(TestIncrementalPacking);
    UNIT_TEST(TestBlockPacking);

    UNIT_TEST(TestBlockPackingSerializeUi32);
    UNIT_TEST(TestBlockPackingSerializeOptionalUi32);
    UNIT_TEST(TestBlockPackingSerializeString);
    UNIT_TEST(TestBlockPackingSerializeOptionalString);
    UNIT_TEST(TestBlockPackingSerializeOptionalOptionalUi32);
    UNIT_TEST(TestBlockPackingSerializeSingularType);
    UNIT_TEST(TestBlockPackingSerializeOptionalSingularType);
    UNIT_TEST(TestBlockPackingSerializeTTzDate);
    UNIT_TEST(TestBlockPackingSerializeOptionalTTzDate);
    UNIT_TEST(TestBlockPackingSerializeTupleInt32String);
    UNIT_TEST(TestBlockPackingSerializeOptionalTupleInt32String);
    UNIT_TEST(TestBlockPackingSerializeTupleShiftedOffset);
    UNIT_TEST_SUITE_END();
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericFastPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportFastPackTest);
} // namespace NKikimr::NMiniKQL
