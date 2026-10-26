#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/generic/guid.h>
#include <util/generic/serialized_enum.h>

#include <cstring>

#include "scalar_layout_converter_test_enums.h"
#include "scalar_layout_converter_utils.h"
using namespace NYql::NUdf;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

constexpr int TestSize = 128;

namespace {

constexpr ui8 DecimalPrecision = 22;
constexpr ui8 DecimalScale = 9;

NKikimr::NMiniKQL::TType* MakeFixedSizeScalarType(TProgramBuilder& builder, EFixedSizeScalarType type, bool optional) {
    NKikimr::NMiniKQL::TType* dataType = nullptr;
    switch (type) {
        case EFixedSizeScalarType::INT64:
            dataType = builder.NewDataType(NUdf::EDataSlot::Int64, false);
            break;
        case EFixedSizeScalarType::UUID:
            dataType = builder.NewDataType(NUdf::EDataSlot::Uuid, false);
            break;
        case EFixedSizeScalarType::DECIMAL:
            dataType = builder.NewDecimalType(DecimalPrecision, DecimalScale);
            break;
    }

    return optional ? builder.NewOptionalType(dataType) : dataType;
}

TGUID MakeTestGuid(ui8 byte) {
    TGUID guid;
    std::memset(&guid, byte, sizeof(guid));
    return guid;
}

NYql::NUdf::TUnboxedValue MakeFixedSizeTestValue(EFixedSizeScalarType type, size_t index) {
    switch (type) {
        case EFixedSizeScalarType::INT64:
            return NYql::NUdf::TUnboxedValuePod(static_cast<i64>(index));
        case EFixedSizeScalarType::UUID: {
            const auto guid = MakeTestGuid(static_cast<ui8>(index));
            return MakeString(NUdf::TStringRef(reinterpret_cast<const char*>(&guid), sizeof(TGUID)));
        }
        case EFixedSizeScalarType::DECIMAL:
            return NYql::NUdf::TUnboxedValuePod(static_cast<NYql::NDecimal::TInt128>(index));
    }
}

void AssertFixedSizeValuesEqual(EFixedSizeScalarType type, const NYql::NUdf::TUnboxedValue& actual, const NYql::NUdf::TUnboxedValue& expected) {
    switch (type) {
        case EFixedSizeScalarType::INT64:
            UNIT_ASSERT(actual.Get<i64>() == expected.Get<i64>());
            return;
        case EFixedSizeScalarType::UUID: {
            const auto actualRef = actual.AsStringRef();
            const auto expectedRef = expected.AsStringRef();
            UNIT_ASSERT(actualRef.Size() == sizeof(TGUID));
            UNIT_ASSERT(expectedRef.Size() == sizeof(TGUID));
            UNIT_ASSERT(std::memcmp(actualRef.Data(), expectedRef.Data(), sizeof(TGUID)) == 0);
            return;
        }
        case EFixedSizeScalarType::DECIMAL:
            UNIT_ASSERT(actual.GetInt128() == expected.GetInt128());
            return;
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TScalarLayoutConverterTest) {
    Y_UNIT_TEST(TestFixedSize) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].Get<i64>(), testValues[i].Get<i64>(), 
                "Expected the same value after pack/unpack");
        }
    }

    Y_UNIT_TEST(TestMultipleFixedSize) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, int64Type, int64Type, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        TVector<TVector<NYql::NUdf::TUnboxedValue>> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            TVector<NYql::NUdf::TUnboxedValue> tuple;
            for (size_t j = 0; j < types.size(); j++) {
                tuple.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i + j * 1000)));
            }
            testValues.push_back(tuple);
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues[i].data(), packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[4];
            converter->Unpack(packRes, i, unpacked);
            for (size_t j = 0; j < types.size(); j++) {
                UNIT_ASSERT_VALUES_EQUAL_C(unpacked[j].Get<i64>(), testValues[i][j].Get<i64>(), 
                    "Expected the same value after pack/unpack");
            }
        }
    }

    Y_UNIT_TEST(TestString) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        TVector<TString> testStrings;
        for (size_t i = 0; i < TestSize; i++) {
            TString str;
            for (size_t j = 0; j <= i; j++) {
                str += static_cast<char>('a' + (j % 26));
            }
            testStrings.push_back(str);
            testValues.push_back(MakeString(NUdf::TStringRef(str)));
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            auto unpackedStr = unpacked[0].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), testStrings[i], 
                "Expected the same string after pack/unpack");
        }
    }

    Y_UNIT_TEST(TestMixedTypes) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, stringType, int64Type, stringType};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        TVector<TVector<NYql::NUdf::TUnboxedValue>> testValues;
        TVector<TVector<TString>> testStrings;
        for (size_t i = 0; i < TestSize; i++) {
            TVector<NYql::NUdf::TUnboxedValue> tuple;
            TVector<TString> strings;
            
            tuple.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
            
            TString str1;
            for (size_t k = 0; k <= i % 20; k++) {
                str1 += static_cast<char>('a' + (k % 26));
            }
            strings.push_back(str1);
            tuple.push_back(MakeString(NUdf::TStringRef(str1)));
            
            tuple.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i * 2)));
            
            TString str2;
            for (size_t k = 0; k <= i % 15; k++) {
                str2 += static_cast<char>('A' + (k % 26));
            }
            strings.push_back(str2);
            tuple.push_back(MakeString(NUdf::TStringRef(str2)));
            
            testStrings.push_back(strings);
            testValues.push_back(tuple);
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues[i].data(), packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[4];
            converter->Unpack(packRes, i, unpacked);
            
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].Get<i64>(), testValues[i][0].Get<i64>(), 
                "Expected the same int value after pack/unpack");
            
            auto unpackedStr1 = unpacked[1].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr1), testStrings[i][0], 
                "Expected the same string after pack/unpack");
            
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[2].Get<i64>(), testValues[i][2].Get<i64>(), 
                "Expected the same int value after pack/unpack");
            
            auto unpackedStr2 = unpacked[3].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr2), testStrings[i][1], 
                "Expected the same string after pack/unpack");
        }
    }

    Y_UNIT_TEST(TestOptional) {
        TScalarLayoutConverterTestData data;

        const auto optionalInt64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true);
        TVector<NKikimr::NMiniKQL::TType*> types{optionalInt64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data with nulls
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            if (i % 2 == 0) {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
            } else {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod()); // null
            }
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            
            bool expectedHasValue = (i % 2 == 0);
            bool actualHasValue = unpacked[0].HasValue();
            UNIT_ASSERT_VALUES_EQUAL_C(actualHasValue, expectedHasValue, 
                "Expected the same optionality after pack/unpack");
            
            if (expectedHasValue) {
                UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].Get<i64>(), testValues[i].Get<i64>(), 
                    "Expected the same value after pack/unpack");
            }
        }
    }

    Y_UNIT_TEST(TestOptionalString) {
        TScalarLayoutConverterTestData data;

        const auto optionalStringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, true);
        TVector<NKikimr::NMiniKQL::TType*> types{optionalStringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        TVector<TMaybe<TString>> testStrings;
        for (size_t i = 0; i < TestSize; i++) {
            if (i % 3 == 0) {
                testStrings.push_back(Nothing());
                testValues.push_back(NYql::NUdf::TUnboxedValuePod()); // null
            } else {
                TString str;
                for (size_t j = 0; j <= i % 10; j++) {
                    str += static_cast<char>('a' + (j % 26));
                }
                testStrings.push_back(str);
                testValues.push_back(MakeString(NUdf::TStringRef(str)));
            }
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            
            bool expectedHasValue = testStrings[i].Defined();
            bool actualHasValue = unpacked[0].HasValue();
            UNIT_ASSERT_VALUES_EQUAL_C(actualHasValue, expectedHasValue, 
                "Expected the same optionality after pack/unpack");
            
            if (expectedHasValue) {
                auto unpackedStr = unpacked[0].AsStringRef();
                UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), *testStrings[i], 
                    "Expected the same string after pack/unpack");
            }
        }
    }

    Y_UNIT_TEST(TestEmptyStrings) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data with empty strings
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            testValues.push_back(MakeString(NUdf::TStringRef("", 0)));
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            auto unpackedStr = unpacked[0].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(unpackedStr.Size(), 0, 
                "Expected empty string after pack/unpack");
        }
    }

    Y_UNIT_TEST(TestLargeStrings) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data with large strings (trigger overflow)
        constexpr size_t LargeSize = 256; // Larger than inline threshold
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        TVector<TString> testStrings;
        for (size_t i = 0; i < 32; i++) {
            TString str;
            str.reserve(LargeSize);
            for (size_t j = 0; j < LargeSize; j++) {
                str += static_cast<char>('a' + ((i + j) % 26));
            }
            testStrings.push_back(str);
            testValues.push_back(MakeString(NUdf::TStringRef(str)));
        }

        // Pack all values
        TPackResult packRes;
        for (size_t i = 0; i < testValues.size(); i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testValues.size(), "Expected all tuples to be packed");
        UNIT_ASSERT_C(packRes.Overflow.size() > 0, "Expected overflow buffer to be used");

        // Unpack and verify
        for (size_t i = 0; i < testValues.size(); i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            auto unpackedStr = unpacked[0].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), testStrings[i], 
                "Expected the same large string after pack/unpack");
        }
    }

    Y_UNIT_TEST(TestBatchPackFixedSize) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data as flat array
        constexpr ui32 numTuples = TestSize;
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(numTuples);
        for (size_t i = 0; i < numTuples; i++) {
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
        }

        // Pack all values using batch method
        TPackResult packRes;
        converter->PackBatch(testValues.data(), numTuples, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, numTuples, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < numTuples; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].Get<i64>(), testValues[i].Get<i64>(), 
                "Expected the same value after batch pack/unpack");
        }
    }

    Y_UNIT_TEST(TestBatchPackMultipleColumns) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, int64Type, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, 
            NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data as flat array
        constexpr ui32 numTuples = 64;
        const ui32 numColumns = types.size();
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(numTuples * numColumns);
        
        for (ui32 i = 0; i < numTuples; i++) {
            for (ui32 j = 0; j < numColumns; j++) {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i * 100 + j)));
            }
        }

        // Pack using batch method
        TPackResult packRes;
        converter->PackBatch(testValues.data(), numTuples, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, numTuples, "Expected all tuples to be packed");

        // Unpack and verify
        for (ui32 i = 0; i < numTuples; i++) {
            NYql::NUdf::TUnboxedValue unpacked[numColumns];
            converter->Unpack(packRes, i, unpacked);
            for (ui32 j = 0; j < numColumns; j++) {
                UNIT_ASSERT_VALUES_EQUAL_C(unpacked[j].Get<i64>(), 
                    testValues[i * numColumns + j].Get<i64>(), 
                    "Expected the same value after batch pack/unpack");
            }
        }
    }

    Y_UNIT_TEST(TestBatchPackStrings) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        constexpr ui32 numTuples = 64;
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        TVector<TString> testStrings;
        testValues.reserve(numTuples);
        testStrings.reserve(numTuples);
        
        for (size_t i = 0; i < numTuples; i++) {
            TString str;
            for (size_t j = 0; j <= i % 15; j++) {
                str += static_cast<char>('a' + (j % 26));
            }
            testStrings.push_back(str);
            testValues.push_back(MakeString(NUdf::TStringRef(str)));
        }

        // Pack using batch method
        TPackResult packRes;
        converter->PackBatch(testValues.data(), numTuples, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, numTuples, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < numTuples; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            auto unpackedStr = unpacked[0].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), testStrings[i], 
                "Expected the same string after batch pack/unpack");
        }
    }

    Y_UNIT_TEST(TestBatchPackMixedTypes) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, stringType, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key, 
            NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        constexpr ui32 numTuples = 64;
        const ui32 numColumns = types.size();
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        TVector<TString> testStrings;
        testValues.reserve(numTuples * numColumns);
        testStrings.reserve(numTuples);
        
        for (ui32 i = 0; i < numTuples; i++) {
            // Column 0: int64
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
            
            // Column 1: string
            TString str;
            for (size_t j = 0; j <= i % 10; j++) {
                str += static_cast<char>('a' + (j % 26));
            }
            testStrings.push_back(str);
            testValues.push_back(MakeString(NUdf::TStringRef(str)));
            
            // Column 2: int64
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i * 2)));
        }

        // Pack using batch method
        TPackResult packRes;
        converter->PackBatch(testValues.data(), numTuples, packRes);
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, numTuples, "Expected all tuples to be packed");

        // Unpack and verify
        for (ui32 i = 0; i < numTuples; i++) {
            NYql::NUdf::TUnboxedValue unpacked[numColumns];
            converter->Unpack(packRes, i, unpacked);
            
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].Get<i64>(), 
                testValues[i * numColumns].Get<i64>(), 
                "Expected the same int value after batch pack/unpack");
            
            auto unpackedStr = unpacked[1].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), testStrings[i], 
                "Expected the same string after batch pack/unpack");
            
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[2].Get<i64>(), 
                testValues[i * numColumns + 2].Get<i64>(), 
                    "Expected the same int value after batch pack/unpack");
        }
    }

    Y_UNIT_TEST(TestBatchPackMatchesSinglePack) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, true);
        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, true);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, stringType};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        constexpr ui32 numTuples = 17;
        const ui32 numColumns = types.size();
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(numTuples * numColumns);
        for (ui32 i = 0; i < numTuples; ++i) {
            if (i % 3 == 0) {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod());
            } else {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
            }

            if (i % 4 == 0) {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod());
            } else if (i % 4 == 1) {
                testValues.push_back(MakeString(NUdf::TStringRef("", 0)));
            } else if (i % 4 == 2) {
                TString str = "row-";
                str.push_back(static_cast<char>('a' + (i % 26)));
                testValues.push_back(MakeString(NUdf::TStringRef(str)));
            } else {
                const TString str(300, static_cast<char>('a' + (i % 26)));
                testValues.push_back(MakeString(NUdf::TStringRef(str)));
            }
        }

        auto assertSamePack = [](const TPackResult& rowPack, const TPackResult& batchPack) {
            UNIT_ASSERT_VALUES_EQUAL(rowPack.NTuples, batchPack.NTuples);
            UNIT_ASSERT_VALUES_EQUAL(rowPack.PackedTuples.size(), batchPack.PackedTuples.size());
            UNIT_ASSERT_VALUES_EQUAL(rowPack.Overflow.size(), batchPack.Overflow.size());
            UNIT_ASSERT(std::memcmp(rowPack.PackedTuples.data(), batchPack.PackedTuples.data(), rowPack.PackedTuples.size()) == 0);
            if (!rowPack.Overflow.empty()) {
                UNIT_ASSERT(std::memcmp(rowPack.Overflow.data(), batchPack.Overflow.data(), rowPack.Overflow.size()) == 0);
            }
        };

        for (ui32 count : {ui32(1), ui32(7), ui32(8), ui32(9), numTuples}) {
            TPackResult rowPack;
            TPackResult batchPack;
            for (ui32 i = 0; i < count; ++i) {
                converter->Pack(testValues.data() + i * numColumns, rowPack);
            }
            converter->PackBatch(testValues.data(), count, batchPack);
            assertSamePack(rowPack, batchPack);

            for (ui32 i = 0; i < count; ++i) {
                converter->Pack(testValues.data() + i * numColumns, rowPack);
            }
            converter->PackBatch(testValues.data(), count, batchPack);
            assertSamePack(rowPack, batchPack);
        }
    }

    Y_UNIT_TEST(TestUnpackBatchMatchesSingleUnpack) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        const auto optInt32Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int32, true);
        const auto optUint64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Uint64, true);
        const auto optDoubleType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Double, true);
        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType, optInt32Type, optUint64Type, optDoubleType, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key,
            NPackedTuple::EColumnRole::Payload,
            NPackedTuple::EColumnRole::Payload,
            NPackedTuple::EColumnRole::Payload,
            NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        constexpr ui32 maxTuples = 17;
        const ui32 numColumns = types.size();
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(maxTuples * numColumns);
        for (ui32 i = 0; i < maxTuples; ++i) {
            testValues.push_back(MakeString(NUdf::TStringRef(TString(i * 5, static_cast<char>('a' + i)))));
            testValues.push_back(i % 3 == 0 ? NYql::NUdf::TUnboxedValuePod() : NYql::NUdf::TUnboxedValuePod(static_cast<i32>(i) - 8));
            testValues.push_back(i % 4 == 1 ? NYql::NUdf::TUnboxedValuePod() : NYql::NUdf::TUnboxedValuePod(static_cast<ui64>(i) << 40));
            testValues.push_back(i % 5 == 2 ? NYql::NUdf::TUnboxedValuePod() : NYql::NUdf::TUnboxedValuePod(static_cast<double>(i) / 4));
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i) * 7));
        }

        TVector<NYql::NUdf::TUnboxedValue> batchOut(maxTuples * numColumns);
        for (ui32 round = 0; round < 2; ++round) {
            for (ui32 count : {ui32(0), ui32(1), ui32(7), ui32(8), ui32(9), maxTuples}) {
                TPackResult packed;
                converter->PackBatch(testValues.data(), count, packed);
                converter->UnpackBatch(packed, batchOut.data());

                for (ui32 row = 0; row < count; ++row) {
                    NYql::NUdf::TUnboxedValue single[5];
                    converter->Unpack(packed, row, single);
                    for (ui32 col = 0; col < numColumns; ++col) {
                        const auto& actual = batchOut[row * numColumns + col];
                        const auto& expected = testValues[row * numColumns + col];
                        UNIT_ASSERT_VALUES_EQUAL(actual.HasValue(), expected.HasValue());
                        UNIT_ASSERT_VALUES_EQUAL(actual.HasValue(), single[col].HasValue());
                        if (!expected.HasValue()) {
                            continue;
                        }
                        switch (col) {
                            case 0:
                                UNIT_ASSERT_VALUES_EQUAL(TString(actual.AsStringRef()), TString(expected.AsStringRef()));
                                UNIT_ASSERT_VALUES_EQUAL(TString(actual.AsStringRef()), TString(single[col].AsStringRef()));
                                break;
                            case 1:
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<i32>(), expected.Get<i32>());
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<i32>(), single[col].Get<i32>());
                                break;
                            case 2:
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<ui64>(), expected.Get<ui64>());
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<ui64>(), single[col].Get<ui64>());
                                break;
                            case 3:
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<double>(), expected.Get<double>());
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<double>(), single[col].Get<double>());
                                break;
                            default:
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<i64>(), expected.Get<i64>());
                                UNIT_ASSERT_VALUES_EQUAL(actual.Get<i64>(), single[col].Get<i64>());
                                break;
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(TestBucketPack) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, int64Type};
        TVector<NPackedTuple::EColumnRole> roles{
            NPackedTuple::EColumnRole::Key,  // First column is key - will be used for bucketing
            NPackedTuple::EColumnRole::Payload};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        // Create test data
        constexpr ui32 numTuples = 128;
        const ui32 numColumns = types.size();
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(numTuples * numColumns);
        
        for (ui32 i = 0; i < numTuples; i++) {
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));      // Key column
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i * 10))); // Payload column
        }

        // Pack into buckets
        constexpr ui32 bucketsLogNum = 4;  // 16 buckets
        constexpr ui32 numBuckets = 1u << bucketsLogNum;
        std::array<TPackResult, numBuckets> packResults;
        
        converter->BucketPack(testValues.data(), numTuples, packResults.data(), bucketsLogNum);

        // Verify that all tuples are distributed across buckets
        ui32 totalTuples = 0;
        for (ui32 bucketIdx = 0; bucketIdx < numBuckets; ++bucketIdx) {
            totalTuples += packResults[bucketIdx].NTuples;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(totalTuples, numTuples, "All tuples should be distributed across buckets");

        // Verify each bucket separately
        for (ui32 bucketIdx = 0; bucketIdx < numBuckets; ++bucketIdx) {
            const auto& packRes = packResults[bucketIdx];
            
            // Unpack all tuples from this bucket
            for (ui32 tupleIdx = 0; tupleIdx < static_cast<ui32>(packRes.NTuples); ++tupleIdx) {
                NYql::NUdf::TUnboxedValue unpacked[2];
                converter->Unpack(packRes, tupleIdx, unpacked);
                
                // Verify values are valid (not garbage)
                i64 key = unpacked[0].Get<i64>();
                i64 payload = unpacked[1].Get<i64>();
                
                UNIT_ASSERT_C(key >= 0 && key < static_cast<i64>(numTuples),
                    "Bucket " << bucketIdx << ", tuple " << tupleIdx << ": key=" << key << " out of range");
                
                // Verify payload matches key relationship
                i64 expectedPayload = key * 10;
                UNIT_ASSERT_VALUES_EQUAL_C(payload, expectedPayload,
                    "Bucket " << bucketIdx << ", tuple " << tupleIdx << ": payload mismatch");
            }
        }
    }

    Y_UNIT_TEST(TestFixedSizePackUnpack, EFixedSizeScalarType, EFixedSizePackMode) {
        const auto scalarType = Arg<0>();
        const auto packMode = Arg<1>();

        TScalarLayoutConverterTestData data;
        const auto mkqlType = MakeFixedSizeScalarType(data.PgmBuilder, scalarType, false);
        TVector<NKikimr::NMiniKQL::TType*> types{mkqlType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        TVector<NYql::NUdf::TUnboxedValue> testValues;
        testValues.reserve(TestSize);
        for (size_t i = 0; i < TestSize; ++i) {
            testValues.push_back(MakeFixedSizeTestValue(scalarType, i));
        }

        TPackResult packRes;
        if (packMode == EFixedSizePackMode::SINGLE) {
            for (size_t i = 0; i < TestSize; ++i) {
                converter->Pack(testValues.data() + i, packRes);
            }
        } else {
            converter->PackBatch(testValues.data(), TestSize, packRes);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        for (size_t i = 0; i < TestSize; ++i) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);
            AssertFixedSizeValuesEqual(scalarType, unpacked[0], testValues[i]);
        }
    }

    Y_UNIT_TEST(TestFixedSizeOptional, EFixedSizeScalarType) {
        const auto scalarType = Arg<0>();

        TScalarLayoutConverterTestData data;
        const auto mkqlType = MakeFixedSizeScalarType(data.PgmBuilder, scalarType, true);
        TVector<NKikimr::NMiniKQL::TType*> types{mkqlType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; ++i) {
            if (i % 2 == 0) {
                testValues.push_back(MakeFixedSizeTestValue(scalarType, i));
            } else {
                testValues.push_back(NYql::NUdf::TUnboxedValuePod());
            }
        }

        TPackResult packRes;
        for (size_t i = 0; i < TestSize; ++i) {
            converter->Pack(testValues.data() + i, packRes);
        }

        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        for (size_t i = 0; i < TestSize; ++i) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked);

            const bool expectedHasValue = (i % 2 == 0);
            UNIT_ASSERT_VALUES_EQUAL_C(unpacked[0].HasValue(), expectedHasValue,
                "Expected the same optionality after pack/unpack");

            if (expectedHasValue) {
                AssertFixedSizeValuesEqual(scalarType, unpacked[0], testValues[i]);
            }
        }
    }

    Y_UNIT_TEST(TestSingularNullKeyPackedAsNull) {
        TScalarLayoutConverterTestData data;

        auto* const nullType = data.Env.GetTypeOfNullLazy();
        TVector<NKikimr::NMiniKQL::TType*> types{nullType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        constexpr size_t testSize = 17;
        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles, data.HolderFactory);

        NYql::NUdf::TUnboxedValue nullValue;
        TPackResult packRes;
        for (size_t i = 0; i < testSize; ++i) {
            converter->Pack(&nullValue, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL(packRes.NTuples, testSize);

        const auto* layout = converter->GetTupleLayout();
        UNIT_ASSERT_VALUES_EQUAL(layout->KeyColumnsNum, 1u);
        UNIT_ASSERT_VALUES_EQUAL(layout->Columns[0].DataSize, 0u);

        for (size_t row = 0; row < testSize; ++row) {
            const ui8* tuple = packRes.PackedTuples.data() + row * layout->TotalRowSize;
            const ui8 bit = (tuple[layout->BitmaskOffset] >> 0) & 1u;
            UNIT_ASSERT_VALUES_EQUAL_C(bit, 0u, "Null key column must be packed as NULL");

            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, row, unpacked);
            UNIT_ASSERT_C(!unpacked[0].HasValue(), "Unpack must restore Null, not Void");
        }

        const ui8* row0 = packRes.PackedTuples.data();
        const ui8* row1 = packRes.PackedTuples.data() + layout->TotalRowSize;
        UNIT_ASSERT_C(
            !layout->KeysEqual(row0, packRes.Overflow.data(), row1, packRes.Overflow.data()),
            "Null keys must not compare equal");
    }
}
