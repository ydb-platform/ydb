#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/scalar_layout_converter.h>
#include <library/cpp/testing/unittest/registar.h>
#include "scalar_layout_converter_utils.h"
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/print_unboxed_value.h>
using NYql::NUdf::TUnboxedValuePod;
using NYql::NUdf::TUnboxedValue;
using namespace NKikimr::NMiniKQL;
Y_UNIT_TEST_SUITE(TPackedTupleDeepCopy) {
    Y_UNIT_TEST(TestAppend) {
        TScalarLayoutConverterTestData data;
        NKikimr::NMiniKQL::TType* intType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::Int64);
        NKikimr::NMiniKQL::TType* stringType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::String);
        TVector<TType *> types{intType, intType, stringType, stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload};
        auto converter = MakeScalarLayoutConverter(TTypeInfoHelper{}, types, roles, data.HolderFactory);

        const auto intEq = MakeEquateImpl(intType);
        const auto stringEq = MakeEquateImpl(intType);
        std::vector<NYql::NUdf::IEquate::TPtr> eqs;
        std::vector<IPrint::TPtr> prints;
        for(auto* type: types) {
            eqs.push_back(MakeEquateImpl(type));
            prints.push_back(MakePrinter(type));
        }

        TPackResult first;
        TPackResult second;
        std::vector<NYql::NUdf::TUnboxedValue> values{};
        for(int index = 0; index < 2; ++index ){
            values.push_back(TUnboxedValuePod(123+index));
            values.push_back(TUnboxedValuePod(12930));
            values.push_back(MakeString("adsloaml"));
            values.push_back(MakeString(std::string(123+index, 'a')));
        }
        converter->Pack(values.data(), first);
        converter->Pack(values.data() + 4, second);
        std::vector<NYql::NUdf::TUnboxedValue> expectedOut(8);

        converter->Unpack(first, 0, expectedOut.data());
        converter->Unpack(second, 0, expectedOut.data()+4);

        std::vector<NYql::NUdf::TUnboxedValue> concatOut(8);
        first.AppendTuple({.PackedData = second.PackedTuples.data(), .OverflowBegin = second.Overflow.data()}, converter->GetTupleLayout());

        for (int index = 0; index < 2; ++index) {
            converter->Unpack(first, index, concatOut.data() + 4*index);
        }
        for(int index = 0; index < 8; ++index ){
            auto eq = eqs[index%types.size()];
            auto print = prints[index%types.size()];
            UNIT_ASSERT_C(eq->Equals(concatOut[index], expectedOut[index]), std::format("concat != expected: {} vs {}", print->Stringify(concatOut[index]), print->Stringify(expectedOut[index])));
        }

    }
    Y_UNIT_TEST(TestAppendBigString) {
        TScalarLayoutConverterTestData data;
        NKikimr::NMiniKQL::TType* intType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::Int64);
        NKikimr::NMiniKQL::TType* stringType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::String);
        TVector<TType*> types{ intType, intType, stringType, stringType };
        TVector<NPackedTuple::EColumnRole> roles{ NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload,
            NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload };
        auto converter = MakeScalarLayoutConverter(TTypeInfoHelper{}, types, roles, data.HolderFactory);

        const auto intEq = MakeEquateImpl(intType);
        const auto stringEq = MakeEquateImpl(intType);
        std::vector<NYql::NUdf::IEquate::TPtr> eqs;
        std::vector<IPrint::TPtr> prints;
        for (auto* type : types) {
            eqs.push_back(MakeEquateImpl(type));
            prints.push_back(MakePrinter(type));
        }

        TPackResult first;
        TPackResult second;
        std::vector<NYql::NUdf::TUnboxedValue> values{};
        int total_values = 10;
        for (int index = 0; index < total_values; ++index) {
            values.push_back(TUnboxedValuePod(123 + index));
            values.push_back(TUnboxedValuePod(12930));
            auto smallstr = std::string(123 + index, 'b');
            smallstr[index] = 'e';
            smallstr[index * 2] = 'e';
            values.push_back(MakeString(smallstr));
            values.push_back(MakeString(std::string(1230 + index, 'a')));
        }
        converter->PackBatch(values.data(), total_values / 2, first);
        converter->PackBatch(values.data() + total_values / 2 * 4, total_values / 2, second);
        std::vector<NYql::NUdf::TUnboxedValue> expectedOut(values.size() * 4);

        for (int index = 0; index < total_values / 2; ++index) {
            converter->Unpack(first, index, expectedOut.data() + index * 4);
        }
        for (int index = 0; index < total_values / 2; ++index) {
            converter->Unpack(second, index, expectedOut.data() + index * 4 + total_values / 2 * 4);
        }

        std::vector<NYql::NUdf::TUnboxedValue> concatOut(values.size() * 4);
        for (int index = 0; index < total_values / 2; ++index) {
            first.AppendTuple({ .PackedData = second.PackedTuples.data() + index * converter->GetTupleLayout()->TotalRowSize,
                                  .OverflowBegin = second.Overflow.data() }, converter->GetTupleLayout());
        }

        for (int index = 0; index < total_values; ++index) {
            converter->Unpack(first, index, concatOut.data() + 4 * index);
        }
        for (int index = 0; index < total_values * 4; ++index) {
            auto eq = eqs[index % types.size()];
            auto print = prints[index % types.size()];
            UNIT_ASSERT_C(eq->Equals(concatOut[index], expectedOut[index]),
                std::format("concat != expected: {} vs {}", print->Stringify(concatOut[index]), print->Stringify(expectedOut[index])));
        }
    }

    Y_UNIT_TEST(TestFlatten) {
        TScalarLayoutConverterTestData data;
        NKikimr::NMiniKQL::TType* intType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::Int64);
        NKikimr::NMiniKQL::TType* stringType = data.PgmBuilder.NewDataType(NYql::NUdf::EDataSlot::String);
        TVector<TType *> types{intType, intType, stringType, stringType};
        TVector<NPackedTuple::EColumnRole> roles{NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload, NPackedTuple::EColumnRole::Key, NPackedTuple::EColumnRole::Payload};
        auto converter = MakeScalarLayoutConverter(TTypeInfoHelper{}, types, roles, data.HolderFactory);

        const auto intEq = MakeEquateImpl(intType);
        const auto stringEq = MakeEquateImpl(intType);
        std::vector<NYql::NUdf::IEquate::TPtr> eqs;
        std::vector<IPrint::TPtr> prints;
        for(auto* type: types) {
            eqs.push_back(MakeEquateImpl(type));
            prints.push_back(MakePrinter(type));
        }

        TPackResult first;
        TPackResult second;
        std::vector<NYql::NUdf::TUnboxedValue> values{};
        for(int index = 0; index < 2; ++index ){
            values.push_back(TUnboxedValuePod(123+index));
            values.push_back(TUnboxedValuePod(12930));
            values.push_back(MakeString("adsloaml"));
            values.push_back(MakeString(std::string(123+index, 'a')));
        }
        converter->Pack(values.data(), first);
        converter->Pack(values.data() + 4, second);
        std::vector<NYql::NUdf::TUnboxedValue> expectedOut(8);

        converter->Unpack(first, 0, expectedOut.data());
        converter->Unpack(second, 0, expectedOut.data()+4);

        std::vector<NYql::NUdf::TUnboxedValue> concatOut(8);
        std::vector<TPackResult> res;
        res.push_back(std::move(first));
        res.push_back(std::move(second));
        first = converter->GetTupleLayout()->Flatten(res);

        for (int index = 0; index < 2; ++index) {
            converter->Unpack(first, index, concatOut.data() + 4*index);
        }
        for(int index = 0; index < 8; ++index ){
            auto eq = eqs[index%types.size()];
            auto print = prints[index%types.size()];
            UNIT_ASSERT_C(eq->Equals(concatOut[index], expectedOut[index]), std::format("concat != expected: {} vs {}", print->Stringify(concatOut[index]), print->Stringify(expectedOut[index])));
        }

    }

    
}