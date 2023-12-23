#include "mkql_node_builder.h"
#include "mkql_node_printer.h"
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include "mkql_type_builder.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLNodeBuilderTest) {
    Y_UNIT_TEST(TestDataBuild) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto d2 = BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)456), NUdf::EDataSlot::Uint32, env);
        UNIT_ASSERT_EQUAL(d2->GetType()->GetSchemeType(), NUdf::TDataType<ui32>::Id);
    }

    Y_UNIT_TEST(TestTupleBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto builder = TTupleLiteralBuilder(env);
        const auto tuple = builder
            .Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(true), NUdf::EDataSlot::Bool, env), true))
            .Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(132), NUdf::EDataSlot::Uint32, env), true))
            .Build();
        UNIT_ASSERT_EQUAL(tuple->GetType()->GetKind(), TType::EKind::Tuple);
        UNIT_ASSERT_VALUES_EQUAL(tuple->GetValuesCount(), 2);
        builder.Clear();
        UNIT_ASSERT_VALUES_EQUAL(builder.Build()->GetValuesCount(), 0);
    }

    Y_UNIT_TEST(TestStructTypeBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto type = TStructTypeBuilder(env)
            .Add("Field1", env.GetVoidLazy()->GetGenericType())
            .Add("Field2", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Build();
        UNIT_ASSERT_EQUAL(type->GetKind(), TType::EKind::Struct);
    }

    Y_UNIT_TEST(TestStructLiteralBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto structObj = TStructLiteralBuilder(env)
            .Add("Field1", TRuntimeNode(env.GetVoidLazy(), true))
            .Add("Field2", TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)234), NUdf::EDataSlot::Uint32, env), true))
            .Build();
        UNIT_ASSERT_EQUAL(structObj->GetType()->GetKind(), TType::EKind::Struct);

        auto structObj2 = TStructLiteralBuilder(env)
            .Add("Field2", TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)234), NUdf::EDataSlot::Uint32, env), true))
            .Add("Field1", TRuntimeNode(env.GetVoidLazy(), true))
            .Build();
        UNIT_ASSERT(structObj->GetType()->IsSameType(*structObj2->GetType()));
    }

    Y_UNIT_TEST(TestListLiteralBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto list = TListLiteralBuilder(env, TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)132), NUdf::EDataSlot::Uint32, env), true))
            .Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)234), NUdf::EDataSlot::Uint32, env), true))
            .Build();
        UNIT_ASSERT_EQUAL(list->GetType()->GetKind(), TType::EKind::List);
        UNIT_ASSERT_EQUAL(list->GetItemsCount(), 2);
    }

    Y_UNIT_TEST(TestCallableTypeBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto callableType = TCallableTypeBuilder(env, "func", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(env.GetVoidLazy()->GetType())
            .Add(env.GetTypeOfVoidLazy())
            .Build();

        UNIT_ASSERT_EQUAL(callableType->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(callableType->GetName(), "func");
        UNIT_ASSERT_EQUAL(callableType->GetArgumentsCount(), 2);
    }

    Y_UNIT_TEST(TestCallableTypeBuilderWithNamesAndFlags) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto callableType = TCallableTypeBuilder(env, "func", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(env.GetVoidLazy()->GetType())
            .Add(env.GetTypeOfVoidLazy())
            .SetArgumentName("Arg2")
            .SetArgumentFlags(NUdf::ICallablePayload::TArgumentFlags::AutoMap)
            .Add(env.GetListOfVoidLazy()->GetType())
            .SetArgumentName("Arg3")
            .Build();

        UNIT_ASSERT_EQUAL(callableType->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(callableType->GetName(), "func");
        UNIT_ASSERT_EQUAL(callableType->GetArgumentsCount(), 3);

        TTypeInfoHelper typeHelper;
        NUdf::TCallableTypeInspector callableInspector(typeHelper, callableType);
        UNIT_ASSERT(callableInspector);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgsCount(), 3);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(0), NUdf::TStringRef());
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(1), NUdf::TStringRef("Arg2"));
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(2), NUdf::TStringRef("Arg3"));

        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(0), 0);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(1), NUdf::ICallablePayload::TArgumentFlags::AutoMap);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(2), 0);
    }

    Y_UNIT_TEST(TestCallableTypeBuilderBadOrderArgNames) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        UNIT_ASSERT_EXCEPTION(TCallableTypeBuilder(env, "func", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(env.GetVoidLazy()->GetType())
            .Add(env.GetTypeOfVoidLazy())
            .SetArgumentName("Arg2")
            .SetArgumentFlags(NUdf::ICallablePayload::TArgumentFlags::AutoMap)
            .Add(env.GetListOfVoidLazy()->GetType())
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestCallableTypeBuilderDuplicateArgNames) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        UNIT_ASSERT_EXCEPTION(TCallableTypeBuilder(env, "func", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(env.GetVoidLazy()->GetType())
            .Add(env.GetTypeOfVoidLazy())
            .SetArgumentName("Arg2")
            .SetArgumentFlags(NUdf::ICallablePayload::TArgumentFlags::AutoMap)
            .Add(env.GetListOfVoidLazy()->GetType())
            .SetArgumentName("Arg2")
            .Build(), yexception);
    }

    Y_UNIT_TEST(TestDictLiteralBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto dict = TDictLiteralBuilder(env, TDataType::Create(NUdf::TDataType<ui32>::Id, env),
            TDataType::Create(NUdf::TDataType<char*>::Id, env)).Add(
                TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)132), NUdf::EDataSlot::Uint32, env), true),
                TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod::Embedded("abc"), NUdf::TDataType<char*>::Id, env), true)
            ).Add(
                TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod((ui32)234), NUdf::EDataSlot::Uint32, env), true),
                TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod::Embedded("def"), NUdf::TDataType<char*>::Id, env), true)
            ).Build();

        UNIT_ASSERT_EQUAL(dict->GetType()->GetKind(), TType::EKind::Dict);
        UNIT_ASSERT_EQUAL(dict->GetItemsCount(), 2);
    }

    Y_UNIT_TEST(TestCallableBuilder) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto callable1 = TCallableBuilder(env, "func1", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(TRuntimeNode(env.GetEmptyStructLazy(), true))
            .Add(TRuntimeNode(env.GetVoidLazy(), true))
            .Build();

        UNIT_ASSERT_EQUAL(callable1->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(callable1->GetInputsCount(), 2);

        auto callable2 = TCallableBuilder(env, "func2", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(TRuntimeNode(callable1, false))
            .Add(TRuntimeNode(env.GetVoidLazy(), true))
            .Build();

        UNIT_ASSERT_EQUAL(callable2->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(callable2->GetType()->GetArgumentsCount(), 2);
        UNIT_ASSERT_EQUAL(callable2->GetType()->GetArgumentType(0)->GetKind(), TType::EKind::Data);
        UNIT_ASSERT_EQUAL(callable2->GetType()->GetArgumentType(1)->GetKind(), TType::EKind::Void);
        UNIT_ASSERT_EQUAL(callable2->GetInputsCount(), 2);
    }

    Y_UNIT_TEST(TestCallableBuilderWithNamesAndFlags) {
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        auto callable1 = TCallableBuilder(env, "func1", TDataType::Create(NUdf::TDataType<ui32>::Id, env))
            .Add(TRuntimeNode(env.GetEmptyStructLazy(), true))
            .Add(TRuntimeNode(env.GetVoidLazy(), true))
            .SetArgumentName("Arg2")
            .SetArgumentFlags(NUdf::ICallablePayload::TArgumentFlags::AutoMap)
            .Add(TRuntimeNode(env.GetEmptyTupleLazy(), true))
            .SetArgumentName("Arg3")
            .Build();

        UNIT_ASSERT_EQUAL(callable1->GetType()->GetKind(), TType::EKind::Callable);
        UNIT_ASSERT_EQUAL(callable1->GetInputsCount(), 3);

        TTypeInfoHelper typeHelper;
        NUdf::TCallableTypeInspector callableInspector(typeHelper, callable1->GetType());
        UNIT_ASSERT(callableInspector);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgsCount(), 3);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(0), NUdf::TStringRef());
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(1), NUdf::TStringRef("Arg2"));
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentName(2), NUdf::TStringRef("Arg3"));

        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(0), 0);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(1), NUdf::ICallablePayload::TArgumentFlags::AutoMap);
        UNIT_ASSERT_EQUAL(callableInspector.GetArgumentFlags(2), 0);
    }
}

}
}
