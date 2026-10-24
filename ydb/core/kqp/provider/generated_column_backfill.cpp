#include "generated_column_backfill.h"
#include "generated_column.h"
#include "yql_kikimr_provider.h"
#include "yql_kikimr_results.h"
#include "yql_kikimr_settings.h"

#include <ydb/core/engine/generated_columns/row_program.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NYql {
using namespace NKikimr::NMiniKQL;

TExprNode::TPtr PrepareGeneratedColumnBackfill(const TString& expression, const TString& columnName,
    const TTypeAnnotationNode& outputType, const TKikimrTableMetadata& table,
    TKikimrSessionContext& sessionCtx, TExprContext& ctx, TTypeAnnotationContext& typeCtx,
    NKikimrIndexBuilder::TGeneratedColumnBuild* descriptor)
{
    if (descriptor) {
        descriptor->ClearDependencies();
    }
    const auto sql = AssembleGeneratedQuery(expression);
    NKikimr::NKqp::TKqpTranslationSettingsBuilder settings(sessionCtx.Query().Type, table.Cluster, sql,
        sessionCtx.Config().GetYqlBindingsMode(), nullptr);
    settings.SetFromConfig(sessionCtx.Config());
    auto lambda = CompileGeneratedExpr(sql, columnName, ctx, settings, typeCtx.Modules);
    if (!lambda) {
        return {};
    }
    THashSet<TString> refs;
    VisitExpr(lambda->Tail(), [&](const TExprNode& node) {
        if (node.IsCallable("Member") && node.ChildrenSize() == 2
            && node.Child(0) == &lambda->Head().Head() && node.Child(1)->IsAtom()) {
            refs.insert(TString(node.Child(1)->Content()));
        }
        return true;
    });
    TVector<TString> dependencies(refs.begin(), refs.end());
    Sort(dependencies);
    TVector<const TItemExprType*> items;
    for (const auto& name : dependencies) {
        const auto* column = table.Columns.FindPtr(name);
        TString error;
        if (name == columnName) {
            error = TStringBuilder() << "Generated column " << columnName << " can not reference itself";
        } else if (!column || column->IsBuildInProgress) {
            error = TStringBuilder() << "Generated column " << columnName << " references an unknown column: " << name;
        } else if (column->IsDefaultFromExpression()) {
            error = TStringBuilder() << "Generated column " << columnName << " references another generated column: " << name;
        }
        if (!error.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), error));
            return {};
        }
        Ydb::Type type;
        NKikimr::NScheme::ProtoFromTypeInfo(column->TypeInfo, type, column->NotNull);
        if (type.has_pg_type()) {
            type.mutable_pg_type()->set_type_modifier(column->TypeMod);
        }
        const auto* annotation = ParseTypeFromYdbType(type, ctx);
        if (!annotation) {
            return {};
        }
        items.push_back(ctx.MakeType<TItemExprType>(name, annotation));
        if (descriptor) {
            auto* dependency = descriptor->AddDependencies();
            dependency->SetName(name);
            dependency->SetColumnId(column->Id);
            dependency->MutableType()->CopyFrom(type);
        }
    }
    const auto* rowType = ctx.MakeType<TStructExprType>(items);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx)
        || !InstantAnnotateTypes(lambda, ctx, false, typeCtx) || !lambda->GetTypeAnn()) {
        return {};
    }
    const TTypeAnnotationNode* expressionType = lambda->GetTypeAnn();
    if (RemoveOptionality(outputType).GetKind() != RemoveOptionality(*expressionType).GetKind()) {
        ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder()
            << "Generated column " << columnName << " expression type mismatch, expected: " << outputType
            << ", actual: " << *expressionType));
        return {};
    }
    if (outputType.GetKind() != ETypeAnnotationKind::Pg && !outputType.IsOptionalOrNull()
        && expressionType->IsOptionalOrNull()) {
        ctx.AddError(TIssue(ctx.GetPosition(lambda->Pos()), TStringBuilder()
            << "Generated column " << columnName << " is declared NOT NULL, but its expression can evaluate to NULL"));
        return {};
    }
    TExprNode::TPtr converted = lambda->TailPtr();
    if (TryConvertTo(converted, outputType, ctx, typeCtx) == IGraphTransformer::TStatus::Error) {
        return {};
    }
    if (converted != lambda->TailPtr()) {
        lambda = ctx.NewLambda(lambda->Pos(), lambda->HeadPtr(), std::move(converted));
        if (!InstantAnnotateTypes(lambda, ctx, false, typeCtx)) {
            return {};
        }
    }
    if (descriptor) {
        descriptor->SetExprText(expression);
        descriptor->SetSourceOwnerId(table.PathId.OwnerId());
        descriptor->SetSourcePathId(table.PathId.TableId());
        descriptor->SetSourceSchemaVersion(table.SchemaVersion);
    }
    return lambda;
}

bool CompileGeneratedColumnBackfill(const TExprNode::TPtr& typedLambda, TExprContext& ctx,
    TTypeAnnotationContext& typeCtx, const IFunctionRegistry& registry,
    NKikimrIndexBuilder::TGeneratedColumnBuild& descriptor)
{
    const auto position = typedLambda ? typedLambda->Pos() : TPositionHandle();
    try {
        Y_ENSURE(typedLambda && typedLambda->IsLambda() && typedLambda->Head().ChildrenSize() == 1,
            "Generated column backfill requires a typed row lambda");
        bool supported = true;
        VisitExpr(*typedLambda, [&](const TExprNode& node) {
            if (node.IsCallable("ScriptUdf")) {
                ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "ScriptUdf is unsupported in generated column backfills"));
                supported = false;
            } else if (node.IsCallable("Udf")) {
                const TString function(node.Head().Content());
                const TString module = function.substr(0, function.find('.'));
                if (NKikimr::NUdfStore::NWasm::GetWasmModuleCatalog().FindByModuleName(module)) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                        << "WASM UDF is unsupported in generated column backfills: " << function));
                    supported = false;
                }
            }
            return true;
        });
        if (!supported) {
            return false;
        }
        const TTypeAnnotationNode* inputType = typedLambda->Head().Head().GetTypeAnn();
        Y_ENSURE(inputType, "Generated column backfill requires a typed row argument");
        const TTypeAnnotationNode* outputType = typedLambda->GetTypeAnn();
        Y_ENSURE(outputType, "Generated column backfill requires a typed result");
        // Give the standalone lambda a typed parent, as in an ordinary program.
        // Besides restoring cloned argument types and constraints, a callable
        // root lets peephole finish its physical phase on restart. A bare lambda
        // propagates that restart to logical optimization, which can reverse a
        // physical Map rewrite back into FlatMap indefinitely.
        const auto* callableType = ctx.MakeType<TCallableExprType>(outputType,
            TVector<TCallableExprType::TArgumentInfo>{{inputType, {}, 0}}, 0, TStringBuf());
        auto program = ctx.NewCallable(position, "Callable",
            {ExpandType(position, *callableType, ctx), typedLambda});
        TTypeAnnotationContextPtr types(&typeCtx);
        TTransformationPipeline pipeline(types);
        pipeline.AddTypeAnnotationTransformerWithMode(TIssuesIds::CORE_TYPE_ANN, ETypeCheckMode::Repeat)
            .AddPostTypeAnnotation(/*forSubGraph=*/true)
            .AddCommonOptimization().AddFinalCommonOptimization();
        pipeline.Add(MakePeepholeOptimization(types), "GeneratedColumnPeephole");
        auto optimizer = pipeline.Build();
        if (SyncTransform(*optimizer, program, ctx) != IGraphTransformer::TStatus::Ok) {
            return false;
        }
        Y_ENSURE(program->IsCallable("Callable") && program->ChildrenSize() == 2 && program->Tail().IsLambda(),
            "Generated column backfill lost its row lambda");
        const auto lambda = program->TailPtr();
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder builder(env, registry, false, typeCtx.LangVer, typeCtx.RuntimeSettings);
        NCommon::TMkqlCommonCallableCompiler compiler;
        auto* rowType = NCommon::BuildType(lambda->Head().Head(), *lambda->Head().Head().GetTypeAnn(), builder);
        TCallableBuilder input(env, "GeneratedColumnInput", rowType);
        NCommon::TMkqlBuildContext::TArgumentsMap arguments;
        arguments.emplace(&lambda->Head().Head(), TRuntimeNode(input.Build(), false));
        NCommon::TMkqlBuildContext build(compiler, builder, ctx, lambda->UniqueId(), std::move(arguments));
        auto root = NCommon::MkqlBuildExpr(lambda->Tail(), build);
        descriptor.SetProgram(SerializeRuntimeNode(root, env));
        descriptor.SetProgramFormatVersion(1);
        descriptor.SetMiniKqlRuntimeVersion(RuntimeVersion.Value());
        descriptor.SetLanguageVersion(typeCtx.LangVer);
        const auto runtimeSettings = typeCtx.RuntimeSettings ? typeCtx.RuntimeSettings : MakeRuntimeSettings();
        descriptor.SetRuntimeSettings(SerializeRuntimeSettingsToString(*runtimeSettings));
        if (!descriptor.HasOutputType()) {
            ExportTypeToProto(root.GetStaticType(), *descriptor.MutableOutputType());
        }
        descriptor.ClearRequiredUdfNames();
        THashSet<TString> udfs;
        TExploringNodeVisitor explorer;
        explorer.Walk(root.GetNode(), env.GetNodeStack());
        for (auto* node : explorer.GetNodes()) {
            if (node->GetType()->GetKind() == TType::EKind::Callable) {
                auto* callable = static_cast<TCallable*>(node);
                if (callable->GetType()->GetName() == "Udf") {
                    udfs.insert(TString(AS_VALUE(TDataLiteral, callable->GetInput(0))->AsValue().AsStringRef()));
                }
            }
        }
        TVector<TString> functions(udfs.begin(), udfs.end());
        Sort(functions);
        for (const auto& name : functions) {
            descriptor.AddRequiredUdfNames(name);
        }
        TString error;
        const NKikimrIndexBuilder::TIndexBuildScanSettings scanSettings;
        auto preflight = NKikimr::NGeneratedColumns::CreateRowProgram(descriptor, registry,
            scanSettings.GetMaxProgramMemoryBytes(), error);
        if (!preflight) {
            ctx.AddError(TIssue(ctx.GetPosition(position), error));
            return false;
        }
        return true;
    } catch (...) {
        ctx.AddError(TIssue(ctx.GetPosition(position), TStringBuilder()
            << "Cannot compile generated column backfill: " << CurrentExceptionMessage()));
        return false;
    }
}
} // namespace NYql
