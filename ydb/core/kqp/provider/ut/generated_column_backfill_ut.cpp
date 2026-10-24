#include <ydb/core/kqp/provider/generated_column_backfill.h>
#include <ydb/core/kqp/provider/generated_column.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_user_data_storage.h>
#include <yql/essentials/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <ydb/core/engine/generated_columns/row_program.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>
#include <atomic>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

namespace NYql {
namespace {

std::atomic<ui64> CounterCalls = 0;

SIMPLE_UDF(TCounter, ui64()) {
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    return NUdf::TUnboxedValuePod(++CounterCalls);
}
SIMPLE_UDF(TCheckedInput, i64(i64)) {
    Y_UNUSED(valueBuilder);
    if (args[0].Get<i64>() < 0) {
        UdfTerminate("Generated backfill test UDF rejected its input");
    }
    return NUdf::TUnboxedValuePod(args[0].Get<i64>() + 1);
}
SIMPLE_MODULE(TGeneratedBackfillModule, TCounter, TCheckedInput);
REGISTER_MODULES(TGeneratedBackfillModule)

NKikimr::NMiniKQL::IFunctionRegistry::TPtr FullRegistry() {
    using namespace NKikimr::NMiniKQL;
    auto registry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    FillStaticModules(*registry);
    return registry;
}

NKikimrIndexBuilder::TGeneratedColumnBuild CompileSql(const TString& expression,
    const NKikimr::NMiniKQL::IFunctionRegistry& registry, EDataSlot inputSlot = EDataSlot::Int64, bool input = true,
    const NKikimr::NMiniKQL::IFunctionRegistry* compilerRegistry = nullptr, bool expectSuccess = true, TString* issues = nullptr,
    bool optionalInput = false) {
    TExprContext ctx;
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    types->RuntimeSettings = MakeRuntimeSettings();
    types->UdfResolver = NCommon::CreateSimpleUdfResolver(&registry);
    types->UserDataStorage = MakeIntrusive<TUserDataStorage>(nullptr, TUserDataTable(), nullptr, nullptr);
    const auto sql = AssembleGeneratedQuery(expression);
    NKikimr::NKqp::TKqpTranslationSettingsBuilder settings(EKikimrQueryType::Query, "/Root", sql,
        NSQLTranslation::EBindingsMode::DISABLED, nullptr);
    auto lambda = CompileGeneratedExpr(sql, "g", ctx, settings, {});
    UNIT_ASSERT_C(lambda, ctx.IssueManager.GetIssues().ToString());
    TVector<const TItemExprType*> members;
    if (input) {
        const TTypeAnnotationNode* annotation = inputSlot == EDataSlot::Decimal
            ? static_cast<const TTypeAnnotationNode*>(ctx.MakeType<TDataExprParamsType>(inputSlot, "22", "9"))
            : ctx.MakeType<TDataExprType>(inputSlot);
        if (optionalInput) {
            annotation = ctx.MakeType<TOptionalExprType>(annotation);
        }
        members.push_back(ctx.MakeType<TItemExprType>("a", annotation));
    }
    const auto* rowType = ctx.MakeType<TStructExprType>(members);
    UNIT_ASSERT(UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx));
    UNIT_ASSERT_C(InstantAnnotateTypes(lambda, ctx, false, *types), ctx.IssueManager.GetIssues().ToString());
    NKikimrIndexBuilder::TGeneratedColumnBuild descriptor;
    if (input) {
        auto* dependency = descriptor.AddDependencies();
        dependency->SetName("a");
        dependency->SetColumnId(7);
        auto* dependencyType = dependency->MutableType();
        if (optionalInput) {
            dependencyType = dependencyType->mutable_optional_type()->mutable_item();
        }
        if (inputSlot == EDataSlot::Decimal) {
            dependencyType->mutable_decimal_type()->set_precision(22);
            dependencyType->mutable_decimal_type()->set_scale(9);
        } else {
            dependencyType->set_type_id(static_cast<Ydb::Type::PrimitiveTypeId>(NUdf::GetDataTypeInfo(inputSlot).TypeId));
        }
    }
    const bool success = CompileGeneratedColumnBackfill(lambda, ctx, *types, compilerRegistry ? *compilerRegistry : registry, descriptor);
    UNIT_ASSERT_C(success == expectSuccess, ctx.IssueManager.GetIssues().ToString());
    if (issues) {
        *issues = ctx.IssueManager.GetIssues().ToString();
    }
    return descriptor;
}

NKikimrIndexBuilder::TGeneratedColumnBuild MakeProgram(bool optional, bool constant = false) {
    using namespace NKikimr::NMiniKQL;
    const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TProgramBuilder builder(env, *registry);
    auto type = builder.NewDataType(NUdf::TDataType<i64>::Id);
    TType* inputType = optional ? builder.NewOptionalType(type) : type;
    TStructTypeBuilder rowType(env);
    rowType.Add("a", inputType);
    TCallableBuilder input(env, "GeneratedColumnInput", rowType.Build());
    auto argument = TRuntimeNode(input.Build(), false);
    auto value = constant ? builder.NewDataLiteral<i64>(42) : builder.Add(builder.Member(argument, "a"), builder.NewDataLiteral<i64>(1));
    NKikimrIndexBuilder::TGeneratedColumnBuild descriptor;
    descriptor.SetProgram(SerializeRuntimeNode(value, env));
    descriptor.SetProgramFormatVersion(1);
    descriptor.SetMiniKqlRuntimeVersion(RuntimeVersion.Value());
    descriptor.SetLanguageVersion(MinLangVersion);
    descriptor.SetRuntimeSettings(SerializeRuntimeSettingsToString(*MakeRuntimeSettings()));
    auto* output = descriptor.MutableOutputType();
    if (optional && !constant) {
        output = output->mutable_optional_type()->mutable_item();
    }
    output->set_type_id(Ydb::Type::INT64);
    if (!constant) {
        auto* dependency = descriptor.AddDependencies();
        dependency->SetName("a");
        dependency->SetColumnId(7);
        auto* dependencyType = dependency->MutableType();
        if (optional) {
            dependencyType = dependencyType->mutable_optional_type()->mutable_item();
        }
        dependencyType->set_type_id(Ydb::Type::INT64);
    }
    return descriptor;
}

} // namespace

Y_UNIT_TEST_SUITE(GeneratedColumnBackfillProgram) {
    Y_UNIT_TEST(ScalarRoundTrip) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(CompileSql("a + 1", *registry), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        i64 input = 3;
        NKikimr::TSerializedCellVec output;
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make(input)}, output, error), error);
        UNIT_ASSERT_VALUES_EQUAL(output.GetCells().at(0).AsValue<i64>(), 4);
    }

    Y_UNIT_TEST(RepeatedRowsInvalidateState) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(MakeProgram(false), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec first, second;
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make<i64>(3)}, first, error), error);
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make<i64>(9)}, second, error), error);
        UNIT_ASSERT_VALUES_EQUAL(first.GetCells().at(0).AsValue<i64>(), 4);
        UNIT_ASSERT_VALUES_EQUAL(second.GetCells().at(0).AsValue<i64>(), 10);
    }

    Y_UNIT_TEST(NullAndNotNull) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(MakeProgram(true), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell()}, result, error), error);
        UNIT_ASSERT(result.GetCells().at(0).IsNull());
        runtime = NKikimr::NGeneratedColumns::CreateRowProgram(MakeProgram(false), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        UNIT_ASSERT(!runtime->Evaluate({NKikimr::TCell()}, result, error));
    }

    Y_UNIT_TEST(ConstantWithoutInput) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(MakeProgram(false, true), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        UNIT_ASSERT_C(runtime->Evaluate({}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsValue<i64>(), 42);
    }

    Y_UNIT_TEST(NativeUdfAndNestedLambda) {
        const auto registry = FullRegistry();
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        const TString text = "ABCЖ";
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(text.data(), text.size())}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "abcж");
        runtime = NKikimr::NGeneratedColumns::CreateRowProgram(CompileSql("COALESCE(ListSum(ListMap(AsList(a, a + 1), ($x) -> { RETURN $x * 2; })), 0)", *registry), *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make<i64>(2)}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsValue<i64>(), 10);
    }

    Y_UNIT_TEST(RepeatedNativeUdfRowsRemainBounded) {
        const auto registry = FullRegistry();
        const auto descriptor = CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8);
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 1 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec previous;
        const TString input = "ABCЖ";
        for (ui32 row = 0; row < 10000; ++row) {
            NKikimr::TSerializedCellVec result;
            UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(input.data(), input.size())}, result, error), error);
            UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "abcж");
            previous = std::move(result);
        }
        UNIT_ASSERT_VALUES_EQUAL(previous.GetCells().at(0).AsBuf(), "abcж");
    }

    Y_UNIT_TEST(OptionalNativeUdfAutoMap) {
        const auto registry = FullRegistry();
        const auto descriptor = CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8,
            true, nullptr, true, nullptr, /*optionalInput=*/true);
        UNIT_ASSERT(descriptor.GetOutputType().has_optional_type());
        UNIT_ASSERT(descriptor.GetDependencies(0).GetType().has_optional_type());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        const TString text = "ABCЖ";
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(text.data(), text.size())}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "abcж");
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell()}, result, error), error);
        UNIT_ASSERT(result.GetCells().at(0).IsNull());
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(text.data(), text.size())}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "abcж");
    }

    Y_UNIT_TEST(ExplicitOutputConversion) {
        const auto registry = FullRegistry();
        TString error;
        const auto descriptor = CompileSql("CAST(a AS Int32)", *registry);
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make<i64>(42)}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsValue<i32>(), 42);
    }

    Y_UNIT_TEST(ConstantNondeterministicState) {
        const auto registry = FullRegistry();
        TString error;
        CounterCalls = 0;
        const auto descriptor = CompileSql("GeneratedBackfill::Counter()", *registry, EDataSlot::Int64, false);
        UNIT_ASSERT_VALUES_EQUAL(CounterCalls.load(), 0);
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        UNIT_ASSERT_VALUES_EQUAL(CounterCalls.load(), 0);
        NKikimr::TSerializedCellVec first, second;
        UNIT_ASSERT_C(runtime->Evaluate({}, first, error), error);
        UNIT_ASSERT_C(runtime->Evaluate({}, second, error), error);
        UNIT_ASSERT_VALUES_EQUAL(first.GetCells().at(0).AsValue<ui64>() + 1, second.GetCells().at(0).AsValue<ui64>());
    }

    Y_UNIT_TEST(UnsupportedRuntime) {
        const auto registry = FullRegistry();
        const auto descriptor = CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8);
        const auto missingModuleRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *missingModuleRegistry, 64 << 20, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Unicode");
        TString issues;
        CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8, true, missingModuleRegistry.Get(), false, &issues);
        UNIT_ASSERT_STRING_CONTAINS(issues, "Unicode");
        using namespace NKikimr::NMiniKQL;
        for (const TString callable : {"ScriptUdf", "GeneratedUnsupportedRuntime"}) {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            TProgramBuilder builder(env, *registry);
            auto invalid = MakeProgram(false, true);
            TCallableBuilder root(env, callable, builder.NewDataType(NUdf::TDataType<i64>::Id));
            invalid.SetProgram(SerializeRuntimeNode(TRuntimeNode(root.Build(), false), env));
            UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(invalid, *registry, 64 << 20, error));
            UNIT_ASSERT_STRING_CONTAINS(error, callable);
        }
    }

    Y_UNIT_TEST(UnsupportedCompiler) {
        const auto registry = FullRegistry();
        for (const TString callable : {"ScriptUdf", "GeneratedUnsupportedRuntime"}) {
            TExprContext ctx;
            auto types = MakeIntrusive<TTypeAnnotationContext>();
            types->RuntimeSettings = MakeRuntimeSettings();
            auto argument = ctx.NewArgument(TPositionHandle(), "row");
            argument->SetTypeAnn(ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}));
            auto body = ctx.NewCallable(TPositionHandle(), callable, {});
            body->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Int64));
            auto lambda = ctx.NewLambda(TPositionHandle(), ctx.NewArguments(TPositionHandle(), {argument}), std::move(body));
            NKikimrIndexBuilder::TGeneratedColumnBuild descriptor;
            UNIT_ASSERT(!CompileGeneratedColumnBackfill(lambda, ctx, *types, *registry, descriptor));
            UNIT_ASSERT(!ctx.IssueManager.GetIssues().Empty());
        }
    }

    Y_UNIT_TEST(WasmRuntimeRejected) {
        const auto registry = FullRegistry();
        auto descriptor = CompileSql("Unicode::ToLower(a)", *registry, EDataSlot::Utf8);
        auto artifact = std::make_shared<NKikimr::NUdfStore::NWasm::TWasmModuleArtifact>();
        artifact->ModuleName = "Unicode";
        auto& catalog = NKikimr::NUdfStore::NWasm::GetWasmModuleCatalog();
        catalog.Register(artifact);
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        catalog.Unregister("Unicode");
        UNIT_ASSERT(!runtime);
        UNIT_ASSERT_STRING_CONTAINS(error, "WASM");
    }

    Y_UNIT_TEST(DecimalRoundTrip) {
        const auto registry = FullRegistry();
        auto descriptor = CompileSql("a", *registry, EDataSlot::Decimal);
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        const NDecimal::TInt128 decimal = 1230000000;
        NKikimr::TSerializedCellVec result;
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(reinterpret_cast<const char*>(&decimal), sizeof(decimal))}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).Size(), sizeof(decimal));
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), TStringBuf(reinterpret_cast<const char*>(&decimal), sizeof(decimal)));
    }

    Y_UNIT_TEST(PgTypeModifierRoundTrip) {
        const auto registry = FullRegistry();
        TExprContext ctx;
        auto types = MakeIntrusive<TTypeAnnotationContext>();
        const auto* pg = NKikimr::NPg::TypeDescFromPgTypeName("pgvarchar");
        UNIT_ASSERT(pg);
        const auto* pgType = ctx.MakeType<TPgExprType>(NKikimr::NPg::PgTypeIdFromTypeDesc(pg));
        auto row = ctx.NewArgument(TPositionHandle(), "row");
        auto body = ctx.NewCallable(TPositionHandle(), "Member", {row, ctx.NewAtom(TPositionHandle(), "a")});
        auto lambda = ctx.NewLambda(TPositionHandle(), ctx.NewArguments(TPositionHandle(), {row}), std::move(body));
        const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("a", pgType)});
        UNIT_ASSERT(UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx));
        UNIT_ASSERT_C(InstantAnnotateTypes(lambda, ctx, false, *types), ctx.IssueManager.GetIssues().ToString());
        NKikimrIndexBuilder::TGeneratedColumnBuild descriptor;
        auto* dependency = descriptor.AddDependencies();
        dependency->SetName("a");
        dependency->SetColumnId(7);
        NKikimr::NScheme::ProtoFromTypeInfo(NKikimr::NScheme::TTypeInfo(pg), *dependency->MutableType());
        dependency->MutableType()->mutable_pg_type()->set_type_modifier("5");
        descriptor.MutableOutputType()->CopyFrom(dependency->GetType());
        descriptor.MutableOutputType()->mutable_pg_type()->set_type_modifier("3");
        UNIT_ASSERT_C(CompileGeneratedColumnBackfill(lambda, ctx, *types, *registry, descriptor), ctx.IssueManager.GetIssues().ToString());
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        const TString tooLong = "ABCD";
        UNIT_ASSERT(!runtime->Evaluate({NKikimr::TCell(tooLong.data(), tooLong.size())}, result, error));
        const TString valid = "ABC";
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(valid.data(), valid.size())}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "ABC");
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell()}, result, error), error);
        UNIT_ASSERT(result.GetCells().at(0).IsNull());
        descriptor.MutableOutputType()->mutable_pg_type()->set_type_modifier("invalid");
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error));
    }

    Y_UNIT_TEST(NativeUdfFailureDoesNotPoisonNextRow) {
        const auto registry = FullRegistry();
        const auto descriptor = CompileSql("GeneratedBackfill::CheckedInput(a)", *registry);
        TString error;
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        UNIT_ASSERT(!runtime->Evaluate({NKikimr::TCell::Make<i64>(-1)}, result, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "rejected its input");
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell::Make<i64>(9)}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsValue<i64>(), 10);
    }

    Y_UNIT_TEST(InvalidProgram) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto descriptor = MakeProgram(false);
        descriptor.SetProgram("invalid");
        TString error;
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error));
        UNIT_ASSERT(!error.empty());
        descriptor = MakeProgram(false);
        descriptor.SetProgramFormatVersion(999);
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error));
        descriptor = MakeProgram(false);
        descriptor.AddDependencies()->CopyFrom(descriptor.GetDependencies(0));
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 64 << 20, error));
        UNIT_ASSERT_STRING_CONTAINS(error, "Duplicate");
    }

    Y_UNIT_TEST(ProgramMemoryLimit) {
        const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TString error;
        UNIT_ASSERT(!NKikimr::NGeneratedColumns::CreateRowProgram(MakeProgram(false), *registry, 1, error));
        UNIT_ASSERT(!error.empty());
        auto descriptor = CompileSql("a || a || a || a || a || a || a || a", *registry, EDataSlot::String);
        auto runtime = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, *registry, 1 << 20, error);
        UNIT_ASSERT_C(runtime, error);
        NKikimr::TSerializedCellVec result;
        const TString large(256 << 10, 'a');
        UNIT_ASSERT(!runtime->Evaluate({NKikimr::TCell(large.data(), large.size())}, result, error));
        const TString small = "x";
        UNIT_ASSERT_C(runtime->Evaluate({NKikimr::TCell(small.data(), small.size())}, result, error), error);
        UNIT_ASSERT_VALUES_EQUAL(result.GetCells().at(0).AsBuf(), "xxxxxxxx");
    }
}
} // namespace NYql
