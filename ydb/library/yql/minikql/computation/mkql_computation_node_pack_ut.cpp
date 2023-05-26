#include "mkql_computation_node_pack.h"
#include "mkql_computation_node_holders.h"
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

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
#elifdef NDEBUG
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

    void ValidateEmbeddedLength(const TStringBuf& buf, const TString& info) {
        if constexpr (!Fast) {
            if (buf.size() > 8) {
                UNIT_ASSERT_VALUES_EQUAL_C(*(const ui32*)buf.data() + 4, buf.size(), info);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(((ui32(*buf.data()) & 0x0f) >> 1) + 1, buf.size(), info);
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
        const auto& packedValue = packer.Pack(uValue);
        if (expectedLength) {
            UNIT_ASSERT_VALUES_EQUAL_C(packedValue.Size(), *expectedLength, additionalMsg);
        }
        if constexpr (Transport) {
            TString str;
            packedValue.CopyTo(str);
            ValidateEmbeddedLength(str, additionalMsg);
            return packer.Unpack(str, HolderFactory);
        } else {
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

    void TestTuplePackPerformance() {
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

    void TestIncrementalPacking() {
        if constexpr (Transport) {
            auto itemType = PgmBuilder.NewDataType(NUdf::TDataType<char *>::Id);
            auto listType = PgmBuilder.NewListType(itemType);
            TValuePackerType packer(false, itemType);
            TValuePackerType listPacker(false, listType);

            TStringBuf str = "01234567890ABCDEF";

            size_t count = 50000;

            for (size_t i = 0; i < count; ++i) {
                NUdf::TUnboxedValue item(MakeString(str));
                packer.AddItem(item);
            }

            TString serializedStr;
            packer.Finish().CopyTo(serializedStr);

            auto listObj = listPacker.Unpack(serializedStr, HolderFactory);
            UNIT_ASSERT_VALUES_EQUAL(listObj.GetListLength(), count);
            ui32 i = 0;
            const auto iter = listObj.GetListIterator();
            for (NUdf::TUnboxedValue uVal; iter.Next(uVal); ++i) {
                UNIT_ASSERT(uVal);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(uVal.AsStringRef()), str);
            }

            TUnboxedValueVector items;
            packer.UnpackBatch(serializedStr, HolderFactory, items);
            UNIT_ASSERT_VALUES_EQUAL(items.size(), count);
            for (auto& uVal : items) {
                UNIT_ASSERT(uVal);
                UNIT_ASSERT_VALUES_EQUAL(std::string_view(uVal.AsStringRef()), str);
            }
        }
    }

private:
    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TProgramBuilder PgmBuilder;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
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
        UNIT_TEST(TestIncrementalPacking);
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
        UNIT_TEST(TestIncrementalPacking);
    UNIT_TEST_SUITE_END();
};

UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeGenericFastPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportPackTest);
UNIT_TEST_SUITE_REGISTRATION(TMiniKQLComputationNodeTransportFastPackTest);
}
}
