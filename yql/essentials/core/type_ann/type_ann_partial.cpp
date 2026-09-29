#include "type_ann_partial.h"

#include "type_ann_expr.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/core/poly_args/yql_poly_args.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/algorithm.h>

namespace NYql {

namespace {

class TPartialCallableTypeAnnotationTransformer final: public IGraphTransformer {
public:
    TPartialCallableTypeAnnotationTransformer(
        TAutoPtr<IGraphTransformer> inner,
        TTypeAnnotationContext& types)
        : Inner_(inner)
        , Types_(types)
    {
    }

    TStatus Transform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (input->IsCallable("Configure!") && input->Child(1)->Head().Content() == ConfigProviderName) {
            auto ptr = Types_.DataSourceMap.FindPtr(ConfigProviderName);
            YQL_ENSURE(ptr);
            auto status = (*ptr)->GetConfigurationTransformer().Transform(input, output, ctx);
            if (status == TStatus::Ok) {
                input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
            }

            return status;
        }

        if (input->IsCallable({"Commit!", "CommitAll!", "Write!", "Configure!"})) {
            input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
            return TStatus::Ok;
        }

        if (input->IsCallable({"MrTableConcat", "MrTableRange",
                               "MrTableConcatStrict", "MrTableRangeStrict", "TempTable", "MrFolder",
                               "MrTableEach", "MrTableEachStrict", "MrPartitions", "MrPartitionsStrict",
                               "MrPartitionList", "MrPartitionListStrict", "MrWalkFolders"})) {
            input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
            return TStatus::Ok;
        }

        if (input->IsCallable("Materialize!")) {
            if (!EnsureMinArgsCount(*input, 3, ctx)) {
                return TStatus::Error;
            }
            TTypeAnnotationNode::TListType children;
            children.push_back(ctx.MakeType<TWorldExprType>());
            children.push_back(input->Child(2)->GetTypeAnn());
            input->SetTypeAnn(ctx.MakeType<TTupleExprType>(children));
            return TStatus::Ok;
        }

        if (input->IsCallable("Read!")) {
            TTypeAnnotationNode::TListType children;
            children.push_back(ctx.MakeType<TWorldExprType>());
            children.push_back(ctx.MakeType<TListExprType>(ctx.MakeType<TUniversalStructExprType>()));
            input->SetTypeAnn(ctx.MakeType<TTupleExprType>(children));
            return TStatus::Ok;
        }

        if (input->IsCallable({"Udf", "ScriptUdf"}) && !Types_.UdfResolver) {
            input->SetTypeAnn(ctx.MakeType<TUniversalExprType>());
            return TStatus::Ok;
        }

        if (input->IsCallable({"EvaluateAtom",
                               "EvaluateExpr", "EvaluateType", "EvaluateCode", "QuoteCode", "Parameter",
                               "SubqueryOrderBy", "SubqueryAssumeOrderBy", "SubqueryExtendFor", "SubqueryUnionAllFor",
                               "SubqueryMergeFor", "SubqueryUnionMergeFor",
                               "SubqueryExtend", "SubqueryUnionAll", "SubqueryMerge", "SubqueryUnionMerge",
                               "EvaluateFor!", "EvaluateParallelFor!", "EvaluateIf!"})) {
            input->SetTypeAnn(ctx.MakeType<TUniversalExprType>());
            return TStatus::Ok;
        }

        if (input->IsCallable({"FileContent", "FilePath", "FolderPath", "TableName",
                               "SecureParam", "TablePath"})) {
            input->SetTypeAnn(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String));
            return TStatus::Ok;
        }

        return Inner_->Transform(input, output, ctx);
    }

    NThreading::TFuture<void> GetAsyncFuture(const TExprNode& input) final {
        return Inner_->GetAsyncFuture(input);
    }

    TStatus ApplyAsyncChanges(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        return Inner_->ApplyAsyncChanges(input, output, ctx);
    }

    void Rewind() final {
        Inner_->Rewind();
    }

    TStatistics GetStatistics() const final {
        return Inner_->GetStatistics();
    }

private:
    TAutoPtr<IGraphTransformer> Inner_;
    TTypeAnnotationContext& Types_;
};

class TFakeArrowResolver final: public IArrowResolver {
public:
    EStatus LoadFunctionMetadata(const TPosition& pos, TStringBuf name, const TVector<const TTypeAnnotationNode*>& argTypes,
                                 const TTypeAnnotationNode* returnType, TExprContext& ctx) const override {
        Y_UNUSED(pos);
        Y_UNUSED(name);
        Y_UNUSED(argTypes);
        Y_UNUSED(returnType);
        Y_UNUSED(ctx);
        return EStatus::OK;
    }

    EStatus HasCast(const TPosition& pos, const TTypeAnnotationNode* from, const TTypeAnnotationNode* to, TExprContext& ctx) const override {
        Y_UNUSED(pos);
        Y_UNUSED(from);
        Y_UNUSED(to);
        Y_UNUSED(ctx);
        return EStatus::OK;
    }

    EStatus AreTypesSupported(const TPosition& pos, const TVector<const TTypeAnnotationNode*>& types, TExprContext& ctx,
                              const TUnsupportedTypeCallback& onUnsupported = {}) const final {
        Y_UNUSED(pos);
        Y_UNUSED(types);
        Y_UNUSED(ctx);
        Y_UNUSED(onUnsupported);
        return EStatus::OK;
    }
};

class TFakeLayersRegistry final: public NLayers::ILayersRegistry {
public:
    TMaybe<TVector<NLayers::TKey>> ResolveLogicalLayers(const TVector<NLayers::TLayerOrder>& orders, TExprContext& ctx) const final {
        Y_UNUSED(orders);
        Y_UNUSED(ctx);
        return Nothing();
    }

    TMaybe<NLayers::TLocations> ResolveLayers(const TVector<NLayers::TKey>& order, const TString& system, const TString& cluster, TExprContext& ctx) const final {
        Y_UNUSED(order);
        Y_UNUSED(system);
        Y_UNUSED(cluster);
        Y_UNUSED(ctx);
        return Nothing();
    }

    bool HasLayer(const NLayers::TKey& key) const override {
        Y_UNUSED(key);
        return false;
    }

    bool AddLayer(const TString& name, const TMaybe<TString>& parent, const TMaybe<TString>& url, TExprContext& ctx) override {
        Y_UNUSED(name);
        Y_UNUSED(parent);
        Y_UNUSED(url);
        Y_UNUSED(ctx);
        return true;
    }

    bool AddLayerFromJson(TStringBuf json, TExprContext& ctx) final {
        Y_UNUSED(json);
        Y_UNUSED(ctx);
        return true;
    }

    void ClearLayers() final {
    }
};

class TPartialUdfResolver final: public IUdfResolver {
public:
    TPartialUdfResolver(
        const IUdfMeta* udfMeta,
        TTypeParser typeParser,
        TTypeWriter typeWriter)
        : UdfMeta_(udfMeta)
        , TypeParser_(std::move(typeParser))
        , TypeWriter_(std::move(typeWriter))
    {
    }

    TMaybe<TFilePathWithMd5> GetSystemModulePath(const TStringBuf& moduleName) const final {
        Y_UNUSED(moduleName);
        ythrow yexception() << "Not supported";
    }

    bool LoadMetadata(const TVector<TImport*>& imports,
                      const TVector<TFunction*>& functions, TExprContext& ctx, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const final {
        Y_UNUSED(imports);
        Y_UNUSED(logLevel);
        Y_UNUSED(storage);
        Y_UNUSED(ctx);
        for (auto f : functions) {
            auto lowered = to_lower(f->Name);
            TStringBuf moduleName;
            TStringBuf funcName;
            if (!SplitUdfName(lowered, moduleName, funcName)) {
                ctx.AddError(TIssue(f->Pos, TStringBuilder() << "Invalid function name: " << f->Name));
                return false;
            }

            if (moduleName == "yson2" || moduleName == "datetime2") {
                moduleName = moduleName.substr(0, moduleName.size() - 1);
            }

            auto meta = UdfMeta_->GetMetadata(moduleName, funcName);
            if (!meta) {
                continue;
            }

            f->NormalizedName = f->Name;
            if (!meta->IsTypeAwareness) {
                if (meta->CallableType && meta->CallableType != "__truncated__") {
                    f->CallableType = TypeParser_(meta->CallableType, ctx);
                    if (!f->CallableType) {
                        return false;
                    }
                }

                if (meta->RunConfigType) {
                    f->RunConfigType = TypeParser_(meta->RunConfigType, ctx);
                    if (!f->RunConfigType) {
                        return false;
                    }
                }

                f->IsStrict = meta->IsStrict;
                f->SupportsBlocks = meta->SupportsBlocks;
                f->MinLangVer = meta->MinLangVer;
                f->MaxLangVer = meta->MaxLangVer;
                continue;
            }

            if (!meta->PolyArgs) {
                continue;
            }

            IPolyArgs::TArgs args;
            if (f->UserType && f->UserType->GetKind() == ETypeAnnotationKind::Tuple) {
                auto topTupleType = f->UserType->Cast<TTupleExprType>();
                if (topTupleType->GetSize() >= 1 && topTupleType->GetItems()[0]->GetKind() == ETypeAnnotationKind::Tuple) {
                    auto argsTupleType = topTupleType->GetItems()[0]->Cast<TTupleExprType>();
                    if (argsTupleType->HasUniversal()) {
                        continue;
                    }

                    for (ui32 i = 0; i < argsTupleType->GetSize(); ++i) {
                        args["T" + ToString(i)] = NYT::NodeFromYsonString(TypeWriter_(argsTupleType->GetItems()[i]));
                    }
                }
            }

            auto polyArgs = ParsePolyArgs(NYT::NodeFromYsonString(meta->PolyArgs));
            auto result = polyArgs->Match(args, f->LangVer);
            if (result.Error) {
                TStringBuilder builder;
                builder << "Can't resolve function: " << f->Name;
                if (f->UserType) {
                    builder << ", userType: " << *f->UserType;
                }

                builder << ", reason: " << *result.Error;
                ctx.AddError(TIssue(f->Pos, builder));
                return false;
            }

            NYT::TNode callableTypeNode;
            if (result.CallableType) {
                callableTypeNode = *result.CallableType;
            } else {
                auto resolvedCallableTypesNode = NYT::NodeFromYsonString(meta->ResolvedCallableTypes);
                YQL_ENSURE(resolvedCallableTypesNode.IsList());
                YQL_ENSURE(result.Index < resolvedCallableTypesNode.AsList().size());
                callableTypeNode = resolvedCallableTypesNode.AsList()[result.Index];
            }

            f->CallableType = TypeParser_(NYT::NodeToYsonString(callableTypeNode), ctx);
            if (!f->CallableType) {
                return false;
            }

            if (result.RunConfigType) {
                f->RunConfigType = TypeParser_(NYT::NodeToYsonString(*result.RunConfigType), ctx);
                if (!f->RunConfigType) {
                    return false;
                }
            }
        }

        return true;
    }

    TResolveResult LoadRichMetadata(const TVector<TImport>& imports, NUdf::ELogLevel logLevel, THoldingFileStorage& storage) const final {
        Y_UNUSED(imports);
        Y_UNUSED(logLevel);
        Y_UNUSED(storage);
        ythrow yexception() << "Not supported";
    }

    bool ContainsModule(const TStringBuf& moduleName) const final {
        Y_UNUSED(moduleName);
        ythrow yexception() << "Not supported";
    }

    bool IsPartial() const final {
        return true;
    }

private:
    const IUdfMeta* UdfMeta_;
    const TTypeParser TypeParser_;
    const TTypeWriter TypeWriter_;
};

bool RecoverResourceLimitError(TIssue& issue) {
    const auto code = issue.GetCode();
    if (code != TIssuesIds::CORE_GC_NODES_LIMIT_EXCEEDED &&
        code != TIssuesIds::CORE_GC_STRINGS_LIMIT_EXCEEDED &&
        code != TIssuesIds::CORE_REPEAT_TRANSFORM_LIMIT_EXCEEDED)
    {
        return false;
    }

    issue.Severity = TSeverityIds::S_WARNING;
    return true;
}

bool AddPartialAnnotationIssues(const TIssues& completed, TIssues& issues) {
    for (const auto& issue : completed) {
        auto recoveredIssue = MakeIntrusive<TIssue>(issue);
        if (RecoverResourceLimitError(*recoveredIssue)) {
            TIssue warning("Partial type annotation: resource limit exceeded, type inference precision is lowered");
            warning.Severity = TSeverityIds::S_WARNING;
            warning.AddSubIssue(std::move(recoveredIssue));
            issues.AddIssue(std::move(warning));
        } else {
            issues.AddIssue(*recoveredIssue);
        }
    }

    return !AnyOf(issues, [](const auto& issue) {
        return issue.GetSeverity() <= TSeverityIds::S_ERROR;
    });
}

TAutoPtr<IGraphTransformer> CreatePartialTypeAnnotationTransformer(
    TAutoPtr<IGraphTransformer> callableTransformer,
    TTypeAnnotationContext& types)
{
    return CreateTypeAnnotationTransformer(
        new TPartialCallableTypeAnnotationTransformer(callableTransformer, types),
        types,
        ETypeCheckMode::Initial);
}

} // namespace

bool PartiallyAnnotateTypes(
    TAstNode* astRoot,
    TIssues& issues,
    const TPartialAnnotationConfig& config)
{
    YQL_ENSURE(astRoot, "AST root is null");
    YQL_ENSURE(config.ConfigProviderFactory, "Config provider factory is not set");
    YQL_ENSURE(
        config.LimitStrictnessFactor >= 1 && config.LimitStrictnessFactor <= 1000,
        "Limit strictness factor must be in range [1, 1000]");
    YQL_ENSURE(
        !config.UdfMeta || (config.TypeParser && config.TypeWriter),
        "Type parser and writer must be set when UDF metadata is provided");

    TExprContext ctx;
    ctx.NodesAllocationLimit /= config.LimitStrictnessFactor;
    ctx.StringsAllocationLimit /= config.LimitStrictnessFactor;
    ctx.RepeatTransformLimit /= config.LimitStrictnessFactor;

    TExprNode::TPtr exprRoot;
    TLibraryCohesion cohesion;
    bool compiled;
    if (config.IsLibrary) {
        compiled = CompileExpr(*astRoot, cohesion, ctx, /*syntaxVersion=*/1);
    } else {
        compiled = CompileExpr(*astRoot, exprRoot, ctx, /*resolver=*/nullptr, /*urlListerManager=*/nullptr,
                               /*hasAnnotations=*/false, /*typeAnnotationIndex=*/Max<ui32>(), /*syntaxVersion=*/1);
    }

    if (!compiled) {
        return AddPartialAnnotationIssues(ctx.IssueManager.GetCompletedIssues(), issues);
    }

    if (config.IsLibrary) {
        TExprNode::TListType exports;
        for (const auto& [name, node] : cohesion.Exports.Symbols()) {
            Y_UNUSED(name);
            exports.push_back(node);
        }

        if (exports.empty()) {
            return true;
        }

        exprRoot = ctx.NewCallable(TPosition(), "LibraryExports", std::move(exports));
    }

    TTypeAnnotationContext typeCtx;
    typeCtx.LangVer = config.LangVer;
    if (config.UdfMeta) {
        typeCtx.UdfResolver = new TPartialUdfResolver(config.UdfMeta, config.TypeParser, config.TypeWriter);
    }

    typeCtx.ArrowResolver = new TFakeArrowResolver;
    typeCtx.LayersRegistry = new TFakeLayersRegistry;
    typeCtx.UserDataStorage = new TUserDataStorage(nullptr, {}, nullptr, new TUdfIndex);
    auto configProvider = config.ConfigProviderFactory(typeCtx);
    typeCtx.AddDataSource(ConfigProviderName, configProvider);
    auto callableTypeAnnTransformer = CreateExtCallableTypeAnnotationTransformer(typeCtx);
    TVector<TTransformStage> transformers;

    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(&ExpandApply),
        "ExpandApply",
        TIssuesIds::CORE_PRE_TYPE_ANN));

    transformers.push_back(TTransformStage(
        CreateFunctorTransformer(
            [&typeCtx](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
                TOptimizeExprSettings settings(&typeCtx);
                return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext& ctx) {
                    Y_UNUSED(ctx);

                    if (node->IsCallable("Apply") && node->ChildrenSize() > 0 && IsUniversalLiteral(node->HeadPtr())) {
                        return node->HeadPtr();
                    }

                    return node;
                }, ctx, settings);
            }),
        "ExpandApplyUniversal",
        TIssuesIds::CORE_PRE_TYPE_ANN));

    transformers.push_back(TTransformStage(
        CreateFunctorTransformer([&typeCtx](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            TOptimizeExprSettings settings(&typeCtx);
            return OptimizeExpr(input, output, [](const TExprNode::TPtr& node, TExprContext& ctx) {
                if (node->IsCallable("FormatCode")) {
                    // clang-format off
                    return ctx.Builder(node->Pos())
                        .Callable("String")
                            .Atom(0, "")
                        .Seal()
                        .Build();
                    // clang-format on
                }

                return node;
            }, ctx, settings);
        }),
        "RewriteEvaluation",
        TIssuesIds::CORE_PRE_TYPE_ANN));

    transformers.push_back(TTransformStage(
        CreatePartialTypeAnnotationTransformer(callableTypeAnnTransformer, typeCtx),
        "PartialTypeAnn",
        TIssuesIds::CORE_PARTIAL_TYPE_ANN));

    auto transformer = CreateCompositeGraphTransformer(transformers, /*useIssueScopes=*/true);
    InstantTransform(*transformer, exprRoot, ctx);
    return AddPartialAnnotationIssues(ctx.IssueManager.GetCompletedIssues(), issues);
}

} // namespace NYql
