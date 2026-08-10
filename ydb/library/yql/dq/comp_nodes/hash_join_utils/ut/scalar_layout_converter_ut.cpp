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
#include <util/system/unaligned_mem.h>

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0, 1};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0, 1};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0, 1};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0, 1};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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

    Y_UNIT_TEST(TestDuplicateKeyColumns) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, stringType};
        TVector<ui32> keyColumns{0, 0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);
        const auto* layout = converter->GetTupleLayout();
        UNIT_ASSERT_VALUES_EQUAL(layout->KeyColumnsNum, 2u);
        UNIT_ASSERT_VALUES_EQUAL(layout->MaterializedColumnsNum, 2u);

        constexpr ui32 numTuples = 128;
        TVector<TVector<NYql::NUdf::TUnboxedValue>> rows;
        for (ui32 i = 0; i < numTuples; ++i) {
            TVector<NYql::NUdf::TUnboxedValue> row;
            row.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i + 7)));
            const char ch = static_cast<char>('a' + (i % 26));
            row.push_back(MakeString(NUdf::TStringRef(&ch, 1)));
            rows.push_back(std::move(row));
        }

        TPackResult packRes;
        for (ui32 i = 0; i < numTuples; ++i) {
            converter->Pack(rows[i].data(), packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL(packRes.NTuples, numTuples);

        for (ui32 i = 0; i < numTuples; ++i) {
            const ui8* row = packRes.PackedTuples.data() + i * layout->TotalRowSize;
            UNIT_ASSERT_VALUES_EQUAL(ReadUnaligned<ui64>(row + layout->KeyColumns[0].Offset),
                                     static_cast<ui64>(i + 7));
            UNIT_ASSERT_VALUES_EQUAL(ReadUnaligned<ui64>(row + layout->KeyColumns[1].Offset),
                                     static_cast<ui64>(i + 7));

            NYql::NUdf::TUnboxedValue unpacked[2];
            converter->Unpack(packRes, i, unpacked);
            UNIT_ASSERT_VALUES_EQUAL(unpacked[0].Get<i64>(), rows[i][0].Get<i64>());
            UNIT_ASSERT_VALUES_EQUAL(TString(unpacked[1].AsStringRef()), TString(rows[i][1].AsStringRef()));
        }
    }

    Y_UNIT_TEST(TestDuplicateStringKeyColumns) {
        TScalarLayoutConverterTestData data;

        const auto stringType = data.PgmBuilder.NewDataType(NUdf::EDataSlot::String, false);
        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{stringType, int64Type};
        TVector<ui32> keyColumns{0, 0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);
        UNIT_ASSERT_VALUES_EQUAL(converter->GetTupleLayout()->KeyColumnsNum, 2u);
        UNIT_ASSERT_VALUES_EQUAL(converter->GetTupleLayout()->MaterializedVariableColumns.size(), 1u);

        constexpr ui32 numTuples = 64;
        TVector<TString> keys;
        TPackResult packRes;
        for (ui32 i = 0; i < numTuples; ++i) {
            keys.push_back(TString(20 + (i % 30), static_cast<char>('A' + (i % 26))));
            NYql::NUdf::TUnboxedValue row[2];
            row[0] = MakeString(NUdf::TStringRef(keys.back()));
            row[1] = NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i));
            converter->Pack(row, packRes);
        }

        for (ui32 i = 0; i < numTuples; ++i) {
            NYql::NUdf::TUnboxedValue unpacked[2];
            converter->Unpack(packRes, i, unpacked);
            UNIT_ASSERT_VALUES_EQUAL(TString(unpacked[0].AsStringRef()), keys[i]);
            UNIT_ASSERT_VALUES_EQUAL(unpacked[1].Get<i64>(), static_cast<i64>(i));
        }
    }

    Y_UNIT_TEST(TestBucketPack) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type, int64Type};
        TVector<ui32> keyColumns{0}; // First column is key - will be used for bucketing

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
        TVector<ui32> keyColumns{0};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, keyColumns, data.HolderFactory);

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
}
