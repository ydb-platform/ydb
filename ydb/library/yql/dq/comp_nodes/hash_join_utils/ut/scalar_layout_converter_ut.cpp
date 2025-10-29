#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

using namespace NYql::NUdf;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

struct TScalarLayoutConverterTestData {
    TScalarLayoutConverterTestData()
        : FunctionRegistry(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , PgmBuilder(Env, *FunctionRegistry)
        , MemInfo("Memory")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
    {
    }

    TIntrusivePtr<NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NMiniKQL::TScopedAlloc Alloc;
    NMiniKQL::TTypeEnvironment Env;
    NMiniKQL::TProgramBuilder PgmBuilder;
    NMiniKQL::TMemoryUsageInfo MemInfo;
    NMiniKQL::THolderFactory HolderFactory;
};

constexpr int TestSize = 128;

Y_UNIT_TEST_SUITE(TScalarLayoutConverterTest) {
    Y_UNIT_TEST(TestFixedSize) {
        TScalarLayoutConverterTestData data;

        const auto int64Type = data.PgmBuilder.NewDataType(NUdf::EDataSlot::Int64, false);
        TVector<NKikimr::NMiniKQL::TType*> types{int64Type};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key};

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

        // Create test data
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            testValues.push_back(NYql::NUdf::TUnboxedValuePod(static_cast<i64>(i)));
        }

        // Pack all values
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues[i].data(), packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[4];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues[i].data(), packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[4];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
            
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
            
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
            
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

        // Create test data with empty strings
        TVector<NYql::NUdf::TUnboxedValue> testValues;
        for (size_t i = 0; i < TestSize; i++) {
            testValues.push_back(MakeString(NUdf::TStringRef("", 0)));
        }

        // Pack all values
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < TestSize; i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, TestSize, "Expected all tuples to be packed");

        // Unpack and verify
        for (size_t i = 0; i < TestSize; i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
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

        auto converter = MakeScalarLayoutConverter(NMiniKQL::TTypeInfoHelper(), types, roles);

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
        IScalarLayoutConverter::TPackResult packRes;
        for (size_t i = 0; i < testValues.size(); i++) {
            converter->Pack(testValues.data() + i, packRes);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(packRes.NTuples, testValues.size(), "Expected all tuples to be packed");
        UNIT_ASSERT_C(packRes.Overflow.size() > 0, "Expected overflow buffer to be used");

        // Unpack and verify
        for (size_t i = 0; i < testValues.size(); i++) {
            NYql::NUdf::TUnboxedValue unpacked[1];
            converter->Unpack(packRes, i, unpacked, data.HolderFactory);
            auto unpackedStr = unpacked[0].AsStringRef();
            UNIT_ASSERT_VALUES_EQUAL_C(TString(unpackedStr), testStrings[i], 
                "Expected the same large string after pack/unpack");
        }
    }
}
