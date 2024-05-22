#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_holders.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/public/udf/arrow/util.h>

#include <library/cpp/random_provider/random_provider.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>

#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ylimits.h>
#include <util/generic/xrange.h>
#include <util/generic/maybe.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
#include <util/system/hp_timer.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NMiniKQL {

#ifdef WITH_VALGRIND
constexpr static size_t PERFORMANCE_COUNT = 0x1000;
#elif defined(NDEBUG)
constexpr static size_t PERFORMANCE_COUNT = NSan::PlainOrUnderSanitizer(0x4000000, 0x1000);
#else
constexpr static size_t PERFORMANCE_COUNT = NSan::PlainOrUnderSanitizer(0x1000000, 0x1000);
#endif

template<bool Fast, bool Transport>
struct TPackerTraits;

template<bool Fast>
struct TPackerTraits<Fast, false> {
    using TPackerType = TValuePackerGeneric<Fast>;
};

template<bool Fast>
struct TPackerTraits<Fast, true> {
    using TPackerType = TValuePackerTransport<Fast>;
};

using NDetails::TChunkedInputBuffer;

template<bool Fast, bool Transport>
class TMiniKQLComputationNodePackTest: public TTestBase {
    using TValuePackerType = typename TPackerTraits<Fast, Transport>::TPackerType;
protected:
    TMiniKQLComputationNodePackTest()
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , RandomProvider(CreateDefaultRandomProvider())
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Memory")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , ArrowPool_(arrow::default_memory_pool())
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
        for (ui32 val: xrange(0, 10)) {
            listValues = listValues.Append(NUdf::TUnboxedValuePod(val));
        }
        TType* listType = PgmBuilder.NewListType(PgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        const NUdf::TUnboxedValue value = HolderFactory.CreateDirectListHolder(std::move(listValues));
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
        for (ui32 i: xrange(0, 10)) {
            NUdf::TUnboxedValue uVal = NUdf::TUnboxedValuePod();
            if (i % 2) {
                uVal = MakeString(TString(i * 2, '0' + i));
            }
            listValues = listValues.Append(std::move(uVal));
        }
        TType* listType = PgmBuilder.NewListType(PgmBuilder.NewOptionalType(PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)));
        const NUdf::TUnboxedValue value = HolderFactory.CreateDirectListHolder(std::move(listValues));
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
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[3]));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[3]));
        TType* tupleType = PgmBuilder.NewTupleType(tupleElemenTypes);

        TUnboxedValueVector tupleElemens;
        tupleElemens.push_back(MakeString("01234567890123456789"));
        tupleElemens.push_back(MakeString("01234567890"));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        tupleElemens.push_back(NUdf::TUnboxedValuePod());
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));

        const NUdf::TUnboxedValue value = HolderFactory.VectorAsArray(tupleElemens);
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
            {"a", PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)},
            {"b", PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"c", PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"d", PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)},
            {"e", PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id, true)},
            {"f", PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id, true)}
        };
        TType* structType = PgmBuilder.NewStructType(structElemenTypes);

        TUnboxedValueVector structElemens;
        structElemens.push_back(MakeString("01234567890123456789"));
        structElemens.push_back(MakeString("01234567890"));
        structElemens.push_back(NUdf::TUnboxedValuePod());
        structElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));
        structElemens.push_back(NUdf::TUnboxedValuePod());
        structElemens.push_back(NUdf::TUnboxedValuePod(ui64(12345)));

        const NUdf::TUnboxedValue value = HolderFactory.VectorAsArray(structElemens);
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
        TType* type = PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
        type = PgmBuilder.NewOptionalType(type);
        type = PgmBuilder.NewOptionalType(type);
        type = PgmBuilder.NewOptionalType(type);

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
        TType* keyType = PgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
        TType* payloadType = PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TType* dictType = PgmBuilder.NewDictType(keyType, payloadType, false);

        TValuesDictHashSingleFixedMap<ui32> map;
        map[4] = NUdf::TUnboxedValuePod::Embedded("4");
        map[10] = NUdf::TUnboxedValuePod::Embedded("10");
        map[1] = NUdf::TUnboxedValuePod::Embedded("1");
        const NUdf::TUnboxedValue value = HolderFactory.CreateDirectHashedSingleFixedMapHolder<ui32, false>(std::move(map), std::nullopt);

        const auto uValue = TestPackUnpack(dictType, value, "Type:Dict");

        UNIT_ASSERT_VALUES_EQUAL(uValue.GetDictLength(), 3);
        const auto it = uValue.GetDictIterator();
        for (NUdf::TUnboxedValue key, payload; it.NextPair(key, payload);) {
            UNIT_ASSERT_VALUES_EQUAL(ToString(key.template Get<ui32>()), std::string_view(payload.AsStringRef()));
        }
    }

    void TestVariantTypeOverStruct() {
        const std::vector<std::pair<std::string_view, TType*>> structElemenTypes = {
            {"a", PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id)},
            {"b", PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, true)},
            {"d", PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)}
        };
        TType* structType = PgmBuilder.NewStructType(structElemenTypes);
        TestVariantTypeImpl(PgmBuilder.NewVariantType(structType));
    }

    void TestVariantTypeOverTuple() {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
        TType* tupleType = PgmBuilder.NewTupleType(tupleElemenTypes);
        TestVariantTypeImpl(PgmBuilder.NewVariantType(tupleType));
    }

    void ValidateEmbeddedLength(TRope buf, const TString& info) {
        size_t size = buf.GetSize();
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
        TValuePackerType packer(false, type);
        const THPTimer timer;
        for (size_t i = 0U; i < PERFORMANCE_COUNT; ++i)
            packer.Pack(uValue);
        Cerr << timer.Passed() << Endl;
    }

    NUdf::TUnboxedValue TestPackUnpack(TValuePackerType& packer, const NUdf::TUnboxedValuePod& uValue,
        const TString& additionalMsg, const std::optional<ui32>& expectedLength = {})
    {
        auto packedValue = packer.Pack(uValue);
        if constexpr (Transport) {
            if (expectedLength) {
                UNIT_ASSERT_VALUES_EQUAL_C(packedValue.size(), *expectedLength, additionalMsg);
            }
            ValidateEmbeddedLength(packedValue, additionalMsg);
            return packer.Unpack(std::move(packedValue), HolderFactory);
        } else {
            if (expectedLength) {
                UNIT_ASSERT_VALUES_EQUAL_C(packedValue.Size(), *expectedLength, additionalMsg);
            }
            ValidateEmbeddedLength(packedValue, additionalMsg);
            return packer.Unpack(packedValue, HolderFactory);
        }
    }

    NUdf::TUnboxedValue TestPackUnpack(TType* type, const NUdf::TUnboxedValuePod& uValue, const TString& additionalMsg,
        const std::optional<ui32>& expectedLength = {})
    {
        TValuePackerType packer(false, type);
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
        TValuePackerType packer(false, PgmBuilder.NewDataType(schemeType));

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
        TValuePackerType packer(false, PgmBuilder.NewOptionalType(PgmBuilder.NewDataType(schemeType)));
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
        TValuePackerType packer(false, PgmBuilder.NewDataType(schemeType));
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
        TValuePackerType packer(false, PgmBuilder.NewDataType(schemeType));
        TestStringValue("0123456789abcdef", packer, typeDesc, Fast ? 16 : (16 + 4));
    }

    void TestOptionalStringValue(std::optional<std::string_view> value, TValuePackerType& packer, const TString& typeDesc, ui32 expectedLength) {
        TString additionalMsg = TStringBuilder() << typeDesc << "), Value:" << (value ? *value : TString("null"));
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
        TValuePackerType packer(false, PgmBuilder.NewOptionalType(PgmBuilder.NewDataType(schemeType)));
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
            const NUdf::TUnboxedValue value = HolderFactory.CreateVariantHolder(MakeString("01234567890123456789"), 0);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 0);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT_VALUES_EQUAL(std::string_view(e.AsStringRef()), "01234567890123456789");
        }
        {
            const NUdf::TUnboxedValue value = HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(), 1);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 1);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT(!e);
        }
        {
            const NUdf::TUnboxedValue value = HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(ui64(12345)), 2);
            const auto uValue = TestPackUnpack(variantType, value, descr);

            UNIT_ASSERT_VALUES_EQUAL(uValue.GetVariantIndex(), 2);
            auto e = uValue.GetVariantItem();
            UNIT_ASSERT_VALUES_EQUAL(e.template Get<ui64>(), 12345ull);
        }
    }

    NUdf::TUnboxedValue MakeTupleValue(TType*& tupleType, bool forPerf = false) {
        std::vector<TType*> tupleElemenTypes;
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[0]));
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[3]));
        tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(tupleElemenTypes[3]));
        if (!forPerf) {
            tupleElemenTypes.push_back(PgmBuilder.NewDecimalType(16, 8));
            tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(PgmBuilder.NewDecimalType(22, 3)));
            tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(PgmBuilder.NewDecimalType(35, 2)));
            tupleElemenTypes.push_back(PgmBuilder.NewOptionalType(PgmBuilder.NewDecimalType(29, 0)));
        }
        tupleType = PgmBuilder.NewTupleType(tupleElemenTypes);

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

        return HolderFactory.VectorAsArray(tupleElemens);
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
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        tupleElemenTypes.push_back(PgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id));
        TType* tupleType = PgmBuilder.NewTupleType(tupleElemenTypes);

        TUnboxedValueVector tupleElemens;
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui32(12345)));
        tupleElemens.push_back(NUdf::TUnboxedValuePod(ui32(67890)));

        const NUdf::TUnboxedValue value = HolderFactory.VectorAsArray(tupleElemens);
        TestPackPerformance(tupleType, value);
    }

    void TestShortStringPackPerformance() {
        const auto v = NUdf::TUnboxedValuePod::Embedded("01234");
        TType* type = PgmBuilder.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id);
        TestPackPerformance(type, v);
    }

    void TestIntegerPackPerformance() {
        const auto& v = NUdf::TUnboxedValuePod(ui64("123456789ULL"));
        TType* type = PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
        TestPackPerformance(type, v);
    }

    void TestRopeSplit() {
        if constexpr (Transport) {
            TType* tupleType;
            const auto value = MakeTupleValue(tupleType);

            TValuePackerType packer(false, tupleType);

            auto buffer = packer.Pack(value);

            TString packed = buffer.ConvertToString();

            if constexpr (Fast) {
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 73);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(packed.size(), 54);
            }

            for (size_t chunk = 1; chunk < packed.size(); ++chunk) {
                TString first = packed.substr(0, chunk);
                TString second = packed.substr(chunk);

                TRope result(std::move(first));
                result.Insert(result.End(), TRope(std::move(second)));

                UNIT_ASSERT_VALUES_EQUAL(result.size(), packed.size());
                UNIT_ASSERT(!result.IsContiguous());

                ValidateTupleValue(packer.Unpack(std::move(result), HolderFactory));
            }
        }
    }

    void TestIncrementalPacking() {
        if constexpr (Transport) {
            auto itemType = PgmBuilder.NewDataType(NUdf::TDataType<char *>::Id);
            auto listType = PgmBuilder.NewListType(itemType);
            TValuePackerType packer(false, itemType);
            TValuePackerType listPacker(false, listType);

            TStringBuf str = "01234567890ABCDEFG";

            size_t count = 500000;

            for (size_t i = 0; i < count; ++i) {
                NUdf::TUnboxedValue item(MakeString(str));
                packer.AddItem(item);
            }

            auto serialized = packer.Finish();

            auto listObj = listPacker.Unpack(TRope(serialized), HolderFactory);
            UNIT_ASSERT_VALUES_EQUAL(listObj.GetListLength(), count);
            const auto iter = listObj.GetListIterator();
            for (NUdf::TUnboxedValue uVal; iter.Next(uVal);) {
                UNIT_ASSERT(uVal);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(uVal.AsStringRef()), str);
            }

            TUnboxedValueBatch items;
            packer.UnpackBatch(std::move(serialized), HolderFactory, items);
            UNIT_ASSERT_VALUES_EQUAL(items.RowCount(), count);
            items.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                UNIT_ASSERT(value);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(value.AsStringRef()), str);
            });
        }
    }

    void DoTestBlockPacking(ui64 offset, ui64 len, bool legacyStruct) {
        if constexpr (Transport) {
            auto strType = PgmBuilder.NewDataType(NUdf::TDataType<char*>::Id);
            auto ui32Type = PgmBuilder.NewDataType(NUdf::TDataType<ui32>::Id);
            auto ui64Type = PgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id);
            auto optStrType = PgmBuilder.NewOptionalType(strType);
            auto optUi32Type = PgmBuilder.NewOptionalType(ui32Type);

            auto tupleOptUi32StrType = PgmBuilder.NewTupleType({ optUi32Type, strType });
            auto optTupleOptUi32StrType = PgmBuilder.NewOptionalType(tupleOptUi32StrType);

            auto blockUi32Type = PgmBuilder.NewBlockType(ui32Type, TBlockType::EShape::Many);
            auto blockOptStrType = PgmBuilder.NewBlockType(optStrType, TBlockType::EShape::Many);
            auto scalarOptStrType = PgmBuilder.NewBlockType(optStrType, TBlockType::EShape::Scalar);
            auto blockOptTupleOptUi32StrType = PgmBuilder.NewBlockType(optTupleOptUi32StrType, TBlockType::EShape::Many);
            auto scalarUi64Type = PgmBuilder.NewBlockType(ui64Type, TBlockType::EShape::Scalar);
            
            auto tzDateType = PgmBuilder.NewDataType(NUdf::EDataSlot::TzDate);
            auto blockTzDateType = PgmBuilder.NewBlockType(tzDateType, TBlockType::EShape::Many);

            auto rowType =
                legacyStruct
                    ? PgmBuilder.NewStructType({
                          {"A", blockUi32Type},
                          {"B", blockOptStrType},
                          {"_yql_block_length", scalarUi64Type},
                          {"a", scalarOptStrType},
                          {"b", blockOptTupleOptUi32StrType},
                          {"c", blockTzDateType}
                      })
                    : PgmBuilder.NewMultiType(
                          {blockUi32Type, blockOptStrType, scalarOptStrType,
                           blockOptTupleOptUi32StrType, blockTzDateType, scalarUi64Type});

            ui64 blockLen = 1000;
            UNIT_ASSERT_LE(offset + len, blockLen);

            auto builder1 = MakeArrayBuilder(TTypeInfoHelper(), ui32Type, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(ui32Type)), nullptr);
            auto builder2 = MakeArrayBuilder(TTypeInfoHelper(), optStrType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optStrType)), nullptr);
            auto builder3 = MakeArrayBuilder(TTypeInfoHelper(), optTupleOptUi32StrType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(optTupleOptUi32StrType)), nullptr);
            auto builder4 = MakeArrayBuilder(TTypeInfoHelper(), tzDateType, *ArrowPool_, CalcBlockLen(CalcMaxBlockItemSize(tzDateType)), nullptr);

            for (ui32 i = 0; i < blockLen; ++i) {
                TBlockItem b1(i);
                builder1->Add(b1);

                TString a = "a string " + ToString(i);
                TBlockItem b2 = (i % 2) ? TBlockItem(a) : TBlockItem();
                builder2->Add(b2);

                TBlockItem b3items[] = { (i % 2) ? TBlockItem(i) : TBlockItem(), TBlockItem(a) };
                TBlockItem b3 = (i % 7) ? TBlockItem(b3items) : TBlockItem();
                builder3->Add(b3);
                
                TBlockItem tzDate {i};
                tzDate.SetTimezoneId(i % 100);
                builder4->Add(tzDate);
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
            } else {
                datums.emplace_back(builder1->Build(true));
                datums.emplace_back(builder2->Build(true));
                datums.emplace_back(arrow::Datum(std::make_shared<arrow::BinaryScalar>(strbuf)));
                datums.emplace_back(builder3->Build(true));
                datums.emplace_back(builder4->Build(true));
                datums.emplace_back(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(blockLen)));
            }

            if (offset != 0 || len != blockLen) {
                for (auto& datum : datums) {
                    if (datum.is_array()) {
                        datum = NYql::NUdf::DeepSlice(datum.array(), offset, len);
                    }
                }
            }
            TUnboxedValueVector columns;
            for (auto& datum : datums) {
                columns.emplace_back(HolderFactory.CreateArrowBlock(std::move(datum)));
            }

            TValuePackerType packer(false, rowType, ArrowPool_);
            if (legacyStruct) {
                TUnboxedValueVector columnsCopy = columns;
                NUdf::TUnboxedValue row = HolderFactory.VectorAsArray(columnsCopy);
                packer.AddItem(row);
            } else {
                packer.AddWideItem(columns.data(), columns.size());
            }
            TRope packed = packer.Finish();

            TUnboxedValueBatch unpacked(rowType);
            packer.UnpackBatch(std::move(packed), HolderFactory, unpacked);

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
                UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[2]).GetDatum().scalar_as<arrow::UInt64Scalar>().value, blockLen);
                UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[3]).GetDatum().scalar_as<arrow::BinaryScalar>().value->ToString(), testScalarString);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value, blockLen);
                UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(unpackedColumns[2]).GetDatum().scalar_as<arrow::BinaryScalar>().value->ToString(), testScalarString);
            }


            auto reader1 = MakeBlockReader(TTypeInfoHelper(), ui32Type);
            auto reader2 = MakeBlockReader(TTypeInfoHelper(), optStrType);
            auto reader3 = MakeBlockReader(TTypeInfoHelper(), optTupleOptUi32StrType);
            auto reader4 = MakeBlockReader(TTypeInfoHelper(), tzDateType);

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
            }
        }
    }

    void TestBlockPacking() {
        DoTestBlockPacking(0, 1000, false);
    }

    void TestBlockPackingSliced() {
        DoTestBlockPacking(19, 623, false);
    }

    void TestLegacyBlockPacking() {
        DoTestBlockPacking(0, 1000, true);
    }

    void TestLegacyBlockPackingSliced() {
        DoTestBlockPacking(19, 623, true);
    }
private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TProgramBuilder PgmBuilder;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
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
        UNIT_TEST(TestBlockPackingSliced);
        UNIT_TEST(TestLegacyBlockPacking);
        UNIT_TEST(TestLegacyBlockPackingSliced);
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
        UNIT_TEST(TestBlockPackingSliced);
        UNIT_TEST(TestLegacyBlockPacking);
        UNIT_TEST(TestLegacyBlockPackingSliced);
    UNIT_TEST_SUITE_END();
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericFastPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportFastPackTest);
}
}
