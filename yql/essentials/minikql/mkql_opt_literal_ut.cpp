#include "mkql_opt_literal.h"
#include "mkql_program_builder.h"
#include "mkql_node_printer.h"
#include "mkql_function_registry.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

TRuntimeNode MakeVoidCallable(TProgramBuilder& pgmBuilder) {
    TCallableBuilder callableBuilder(pgmBuilder.GetTypeEnvironment(), "Effect", pgmBuilder.NewVoid().GetStaticType());
    return TRuntimeNode(callableBuilder.Build(), false);
}

Y_UNIT_TEST_SUITE(TMiniKQLLPOTest) {
    Y_UNIT_TEST(TestIfPredicateTrue) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.AsList(pgmBuilder.If(pgmBuilder.NewDataLiteral(true), MakeVoidCallable(pgmBuilder), pgmBuilder.NewVoid()));

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        const auto& list = static_cast<const TListLiteral&>(*pgm);
        UNIT_ASSERT_EQUAL(list.GetItemsCount(), 1);
        auto item = list.GetItems()[0];
        UNIT_ASSERT(!item.IsImmediate());
        UNIT_ASSERT_EQUAL(item.GetNode()->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(static_cast<TCallableType&>(*item.GetNode()->GetType()).GetName(), "Effect");
    }

    Y_UNIT_TEST(TestIfPredicateSame) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.AsList(pgmBuilder.If(pgmBuilder.NewDataLiteral(true), MakeVoidCallable(pgmBuilder), MakeVoidCallable(pgmBuilder)));

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        const auto& list = static_cast<const TListLiteral&>(*pgm);
        UNIT_ASSERT_EQUAL(list.GetItemsCount(), 1);
        auto item = list.GetItems()[0];
        UNIT_ASSERT(!item.IsImmediate());
        UNIT_ASSERT_EQUAL(item.GetNode()->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(static_cast<TCallableType&>(*item.GetNode()->GetType()).GetName(), "Effect");
    }

     Y_UNIT_TEST(TestIfPredicateFalse) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.AsList(pgmBuilder.If(pgmBuilder.NewDataLiteral(false), pgmBuilder.NewVoid(), MakeVoidCallable(pgmBuilder)));

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        const auto& list = static_cast<const TListLiteral&>(*pgm);
        UNIT_ASSERT_EQUAL(list.GetItemsCount(), 1);
        auto item = list.GetItems()[0];
        UNIT_ASSERT(!item.IsImmediate());
        UNIT_ASSERT_EQUAL(item.GetNode()->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(static_cast<TCallableType&>(*item.GetNode()->GetType()).GetName(), "Effect");
    }

    Y_UNIT_TEST(TestSize) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.Size(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataType&>(*pgm->GetType()).GetSchemeType(), NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<ui32>(), 3);
    }

    Y_UNIT_TEST(TestLength) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto list = pgmBuilder.AsList(pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        auto pgmReturn = pgmBuilder.Length(list);
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataType&>(*pgm->GetType()).GetSchemeType(), NUdf::TDataType<ui64>::Id);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<ui64>(), 1);
    }

    Y_UNIT_TEST(TestAddMember) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto struct1 = pgmBuilder.NewEmptyStruct();
        struct1 = pgmBuilder.AddMember(struct1, "x", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        auto pgm = LiteralPropagationOptimization(struct1, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(static_cast<TStructLiteral&>(*pgm).GetValuesCount(), 1);

        auto struct2 = pgmBuilder.NewEmptyStruct();
        struct2 = pgmBuilder.AddMember(struct2, "x", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        struct2 = pgmBuilder.AddMember(struct2, "z", pgmBuilder.NewDataLiteral<ui32>(11));
        pgm = LiteralPropagationOptimization(struct2, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(static_cast<TStructLiteral&>(*pgm).GetValuesCount(), 2);

        auto struct3 = pgmBuilder.NewEmptyStruct();
        struct3 = pgmBuilder.AddMember(struct3, "x", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        struct3 = pgmBuilder.AddMember(struct3, "z", pgmBuilder.NewDataLiteral<ui32>(11));
        struct3 = pgmBuilder.AddMember(struct3, "y", pgmBuilder.NewDataLiteral<ui64>(777));
        pgm = LiteralPropagationOptimization(struct3, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Struct);
        UNIT_ASSERT_EQUAL(static_cast<TStructLiteral&>(*pgm).GetValuesCount(), 3);
    }

    Y_UNIT_TEST(TestMember) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto struct1 = pgmBuilder.NewEmptyStruct();
        struct1 = pgmBuilder.AddMember(struct1, "x", pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("abc"));
        struct1 = pgmBuilder.AddMember(struct1, "z", MakeVoidCallable(pgmBuilder));
        struct1 = pgmBuilder.AddMember(struct1, "y", pgmBuilder.NewVoid());
        auto pgm = LiteralPropagationOptimization(pgmBuilder.Member(struct1, "y"), env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Void);
    }

    Y_UNIT_TEST(TestFilterPredicateTrue) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> items;
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(1));
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(2));
        auto list = pgmBuilder.AsList(items);

        auto pgmReturn = pgmBuilder.Filter(list,
            [&](TRuntimeNode item) {
                Y_UNUSED(item);
                return pgmBuilder.NewDataLiteral(true);
            });

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(static_cast<TListLiteral&>(*pgm).GetItemsCount(), 2);
    }

    Y_UNIT_TEST(TestFilterPredicateFalse) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> items;
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(1));
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(2));
        auto list = pgmBuilder.AsList(items);

        auto pgmReturn = pgmBuilder.Filter(list,
            [&](TRuntimeNode item) {
                Y_UNUSED(item);
                return pgmBuilder.NewDataLiteral(false);
            });

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(static_cast<TListLiteral&>(*pgm).GetItemsCount(), 0);
    }

    Y_UNIT_TEST(TestMapToVoid) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> items;
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(1));
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(2));
        auto list = pgmBuilder.AsList(items);

        auto pgmReturn = pgmBuilder.Map(list,
            [&](TRuntimeNode item) {
                Y_UNUSED(item);
                return pgmBuilder.NewVoid();
            });

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(static_cast<TListLiteral&>(*pgm).GetItemsCount(), 0);
    }

    Y_UNIT_TEST(TestFlatMapToLiteralListOfVoid) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> items;
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(1));
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(2));
        auto list = pgmBuilder.AsList(items);

        auto pgmReturn = pgmBuilder.FlatMap(list,
            [&](TRuntimeNode item) {
                Y_UNUSED(item);
                return pgmBuilder.NewEmptyListOfVoid();
            });

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(static_cast<TListLiteral&>(*pgm).GetItemsCount(), 0);
    }

    Y_UNIT_TEST(TestFlatMapToLiteralListOfVoid_Optional) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> items;
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(1));
        items.push_back(pgmBuilder.NewDataLiteral<ui32>(2));
        auto list = pgmBuilder.AsList(items);

        auto pgmReturn = pgmBuilder.FlatMap(list,
            [&](TRuntimeNode item) {
                Y_UNUSED(item);
                return pgmBuilder.NewEmptyOptional(pgmBuilder.NewOptionalType(env.GetTypeOfVoidLazy()));
            });

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(static_cast<TListLiteral&>(*pgm).GetItemsCount(), 0);
    }

    Y_UNIT_TEST(TestLiteralCoalesceExist) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.Coalesce(pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<ui32>(1)),
            pgmBuilder.NewDataLiteral<ui32>(2));
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<ui32>(), 1);
    }

    Y_UNIT_TEST(TestLiteralCoalesceNotExist) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.Coalesce(pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id),
            pgmBuilder.NewDataLiteral<ui32>(2));
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<ui32>(), 2);
    }

    Y_UNIT_TEST(TestLiteralExists) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.Exists(pgmBuilder.NewOptional(pgmBuilder.NewDataLiteral<ui32>(1)));
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<bool>(), true);
    }

    Y_UNIT_TEST(TestNth) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        TVector<TRuntimeNode> elements(2);
        elements[0] = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>("34");
        elements[1] = pgmBuilder.NewDataLiteral<ui32>(56);
        auto tuple = pgmBuilder.NewTuple(elements);
        auto pgmReturn = pgmBuilder.Nth(tuple, 1);
        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(static_cast<TDataType&>(*pgm->GetType()).GetSchemeType(), NUdf::TDataType<ui32>::Id);
        UNIT_ASSERT_EQUAL(static_cast<TDataLiteral&>(*pgm).AsValue().Get<ui32>(), 56);
    }

    Y_UNIT_TEST(TestExtend) {
        auto functionRegistry = CreateFunctionRegistry(IBuiltinFunctionRegistry::TPtr());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto pgmReturn = pgmBuilder.Extend({
            pgmBuilder.NewEmptyListOfVoid(),
            pgmBuilder.NewEmptyListOfVoid(),
            pgmBuilder.NewEmptyListOfVoid()});

        auto pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        const auto& result1 = static_cast<const TListLiteral&>(*pgm);
        UNIT_ASSERT_EQUAL(result1.GetItemsCount(), 0);

        pgmReturn = pgmBuilder.Extend({
            pgmBuilder.NewEmptyListOfVoid(),
            pgmBuilder.Extend({
                pgmBuilder.NewEmptyListOfVoid(),
                pgmBuilder.NewEmptyListOfVoid(),
            }),
            pgmBuilder.NewEmptyListOfVoid()});

        pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::List);
        const auto& result2 = static_cast<const TListLiteral&>(*pgm);
        UNIT_ASSERT_EQUAL(result2.GetItemsCount(), 0);

        pgmReturn = pgmBuilder.Extend({
            pgmBuilder.NewEmptyListOfVoid(),
            pgmBuilder.AsList(MakeVoidCallable(pgmBuilder)),
            pgmBuilder.NewEmptyListOfVoid()});

        pgm = LiteralPropagationOptimization(pgmReturn, env, true).GetNode();
        UNIT_ASSERT_EQUAL(pgm->GetType()->GetKind(), TType::EKind::Callable);
    }
}

}
}
