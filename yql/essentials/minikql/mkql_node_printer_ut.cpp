#include "mkql_node_printer.h"
#include "mkql_node_builder.h"
#include "mkql_node_serialization.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
    TNode* BuildGraph(const TTypeEnvironment& env) {
        TStructLiteralBuilder structBuilder(env);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        TDataType* dtype2 = TDataType::Create(NUdf::TDataType<char*>::Id, env);
        TVector<TType*> callableTypes;
        callableTypes.push_back(env.GetVoidLazy()->GetGenericType());
        TCallableType* ctype1 = TCallableType::Create("ret data", dtype1, callableTypes.size(), callableTypes.data(), nullptr, env);
        TListType* ltype1 = TListType::Create(dtype1, env);
        TOptionalType* otype1 = TOptionalType::Create(dtype1, env);
        structBuilder.Add("01", TRuntimeNode(env.GetTypeOfTypeLazy(), true));
        structBuilder.Add("02", TRuntimeNode(env.GetTypeOfVoidLazy(), true));
        structBuilder.Add("03", TRuntimeNode(dtype1, true));
        structBuilder.Add("04", TRuntimeNode(TDataType::Create(NUdf::TDataType<ui32>::Id, env), true));
        structBuilder.Add("12", TRuntimeNode(env.GetEmptyStructLazy()->GetType(), true));
        TVector<std::pair<TString, TType*>> smallMembers;
        smallMembers.push_back(std::make_pair("Embedded member", env.GetVoidLazy()->GetGenericType()));
        structBuilder.Add("13", TRuntimeNode(TStructType::Create(smallMembers.data(), smallMembers.size(), env), true));
        structBuilder.Add("14", TRuntimeNode(TListType::Create(dtype1, env), true));
        structBuilder.Add("15", TRuntimeNode(TOptionalType::Create(dtype2, env), true));
        structBuilder.Add("16", TRuntimeNode(TDictType::Create(dtype1, dtype2, env), true));
        TVector<TType*> smallTypes;
        smallTypes.push_back(dtype1);
        structBuilder.Add("17", TRuntimeNode(TCallableType::Create("My callable",
            dtype2, smallTypes.size(), smallTypes.data(), nullptr, env), true));
        structBuilder.Add("18", TRuntimeNode(env.GetVoidLazy(), true));
        ui32 u = 345;
        structBuilder.Add("19", TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod(u), dtype1, env), true));
        auto v = TRuntimeNode(env.GetVoidLazy(), true);
        structBuilder.Add("26", TRuntimeNode(TListLiteral::Create(nullptr, 0, ltype1, env), true));
        TVector<TRuntimeNode> litems;
        litems.push_back(TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod(u), dtype1, env), true));
        u = 789;
        litems.push_back(TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod(u), dtype1, env), true));
        structBuilder.Add("27", TRuntimeNode(TListLiteral::Create(litems.data(), litems.size(), ltype1, env), true));
        structBuilder.Add("28", TRuntimeNode(TOptionalLiteral::Create(otype1, env), true));
        structBuilder.Add("29", TRuntimeNode(TOptionalLiteral::Create(litems[0], otype1, env), true));

        auto ditype1 = TDictType::Create(dtype1, dtype2, env);
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> ditems;
        ditems.push_back(std::make_pair(TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod((ui32)456), dtype1, env), true),
            TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod::Embedded("aaaa"), dtype2, env), true)));
        structBuilder.Add("30", TRuntimeNode(TDictLiteral::Create(ditems.size(), ditems.data(), ditype1, env), true));
        TVector<TRuntimeNode> callableArgs;
        callableArgs.push_back(TRuntimeNode(env.GetVoidLazy(), true));
        TCallable* callable = TCallable::Create(callableArgs.size(), callableArgs.data(), ctype1, env);
        callable->SetResult(TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod(u), dtype1, env), true), env);
        structBuilder.Add("31", TRuntimeNode(callable, true));
        structBuilder.Add("32", TRuntimeNode(env.GetAnyTypeLazy(), true));
        structBuilder.Add("33", TRuntimeNode(TAny::Create(env), true));
        auto anyWithData = TAny::Create(env);
        anyWithData->SetItem(TRuntimeNode(env.GetVoidLazy(), true));
        structBuilder.Add("34", TRuntimeNode(anyWithData, true));
        structBuilder.Add("35", TRuntimeNode(TCallableType::Create("My callable 2",
            dtype2, callableArgs.size(), smallTypes.data(), env.GetVoidLazy(), env), true));

        TVector<TType*> tupleTypes;
        tupleTypes.push_back(dtype1);
        TVector<TRuntimeNode> tupleValues;
        tupleValues.push_back(TRuntimeNode(TDataLiteral::Create(NUdf::TUnboxedValuePod((ui32)456), dtype1, env), true));
        auto tupleType = TTupleType::Create(tupleTypes.size(), tupleTypes.data(), env);
        structBuilder.Add("36", TRuntimeNode(tupleType, true));
        structBuilder.Add("37", TRuntimeNode(TTupleLiteral::Create(tupleValues.size(), tupleValues.data(), tupleType, env), true));
        structBuilder.Add("38", TRuntimeNode(TResourceType::Create("myres", env), true));
        structBuilder.Add("39", TRuntimeNode(TStreamType::Create(dtype1, env), true));
        structBuilder.Add("40", TRuntimeNode(env.GetNullLazy(), true));
        structBuilder.Add("41", TRuntimeNode(env.GetEmptyListLazy(), true));
        structBuilder.Add("42", TRuntimeNode(env.GetEmptyDictLazy(), true));
        structBuilder.Add("43", TRuntimeNode(TTaggedType::Create(dtype1, "mytag", env), true));
        structBuilder.Add("44", TRuntimeNode(TBlockType::Create(dtype1, TBlockType::EShape::Scalar, env), true));
        structBuilder.Add("45", TRuntimeNode(TBlockType::Create(dtype2, TBlockType::EShape::Many, env), true));
        structBuilder.Add("46", TRuntimeNode(TPgType::Create(23, env), true)); // int4 type
        return structBuilder.Build();
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLNodePrinterTest) {
    Y_UNIT_TEST(TestPrintWithoutSchema) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto node = BuildGraph(env);
        TString s = PrintNode(node);
        //Cout << s << Endl;

        auto serialized = SerializeNode(node, env);
        TNode* node2 = DeserializeNode(serialized, env);
        TString s2 = PrintNode(node2, false);
        UNIT_ASSERT_EQUAL(s2, s);
    }

    Y_UNIT_TEST(RuntimeNodeSerialization) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TDataType* dtype1 = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
        TCallableType* ctype1 = TCallableType::Create("ctype1", dtype1, 0, nullptr, nullptr, env);
        TCallable* c1 = TCallable::Create(0, nullptr, ctype1, env);
        TRuntimeNode r1(c1, false);
        auto s1 = SerializeRuntimeNode(r1, env);
        TRuntimeNode r2 = DeserializeRuntimeNode(s1, env);
        UNIT_ASSERT(r1 == r2);

        TRuntimeNode r3(c1, true);
        auto s2 = SerializeRuntimeNode(r3, env);
        TRuntimeNode r4 = DeserializeRuntimeNode(s2, env);
        UNIT_ASSERT(r3 == r4);

        UNIT_ASSERT(r1 != r3);
    }
}

}
}
