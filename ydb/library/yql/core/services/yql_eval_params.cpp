#include "yql_eval_params.h"
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <util/string/builder.h>

namespace NYql {

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

bool BuildParameterValuesAsNodes(const THashMap<TStringBuf, const TTypeAnnotationNode*>& paramTypes,
    const NYT::TNode& paramData, TExprContext& ctx, const IFunctionRegistry& functionRegistry,
    THashMap<TStringBuf, TExprNode::TPtr>& paramValues) {

    if (!paramData.IsMap()) {
        ctx.AddError(TIssue({}, TStringBuilder() << "ParamData is not a map"));
        return false;
    }

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TMemoryUsageInfo memInfo("Parameters");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    bool isOk = true;
    auto& paramDataMap = paramData.AsMap();
    for (auto& p : paramTypes) {
        auto name = p.first;

        TStringStream err;
        TProgramBuilder pgmBuilder(env, functionRegistry);
        TType* mkqlType = NCommon::BuildType(*p.second, pgmBuilder, err);
        if (!mkqlType) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Failed to process type for parameter: " << name << ", reason: " << err.Str()));
            isOk = false;
            continue;
        }

        const auto parameterItem = paramDataMap.FindPtr(name);
        if (parameterItem && (!parameterItem->IsMap() || !parameterItem->HasKey("Data"))) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Parameter '" << name << "' value should be a map with key 'Data'"));
            isOk = false;
            continue;
        }

        if (!parameterItem && p.second->GetKind() != ETypeAnnotationKind::Optional && p.second->GetKind() != ETypeAnnotationKind::Null) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Missing value for parameter: " << name));
            isOk = false;
            continue;
        }

        auto value = parameterItem ? NCommon::ParseYsonNodeInResultFormat(holderFactory, (*parameterItem)["Data"], mkqlType, &err) : MakeMaybe(NUdf::TUnboxedValue());
        if (!value) {
            ctx.AddError(TIssue({}, TStringBuilder() << "Failed to parse data for parameter: " << name << ", reason: " << err.Str()));
            isOk = false;
            continue;
        }

        paramValues[name] = NCommon::ValueToExprLiteral(p.second, *value, ctx);
    }

    return isOk;
}

bool ExtractParameterTypes(const TExprNode::TPtr& input, TTypeAnnotationContext& types,
    TExprContext& ctx, THashMap<TStringBuf, const TTypeAnnotationNode*>& paramTypes) {

    auto callableTransformer = CreateExtCallableTypeAnnotationTransformer(types);
    auto typeTransformer = CreateTypeAnnotationTransformer(callableTransformer, types);
    TVector<TTransformStage> transformers;
    const auto issueCode = TIssuesIds::CORE_TYPE_ANN;
    transformers.push_back(TTransformStage(typeTransformer, "TypeAnnotation", issueCode));
    auto fullTransformer = CreateCompositeGraphTransformer(transformers, false);

    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    TExprNode::TPtr output = input;
    auto status1 = OptimizeExpr(input, output, [&](const TExprNode::TPtr& node, TExprContext& ctx)->TExprNode::TPtr {
        if (!node->IsCallable("Parameter")) {
            return node;
        }

        auto param = node;
        fullTransformer->Rewind();
        auto status = InstantTransform(*fullTransformer, param, ctx);
        if (status.Level == IGraphTransformer::TStatus::Error) {
            return nullptr;
        }

        auto name = param->Child(0)->Content();
        if (!param->GetTypeAnn()) {
            ctx.AddError(TIssue(ctx.GetPosition(param->Pos()), TStringBuilder() << "Failed to type check parameter: " << name));
            return nullptr;
        }

        auto& type = paramTypes[name];
        if (!type) {
            type = param->GetTypeAnn();
        } else if (!IsSameAnnotation(*type, *param->GetTypeAnn())) {
            ctx.AddError(TIssue(ctx.GetPosition(param->Pos()), TStringBuilder() << "Mismatch of types: " << *type << " != " << *param->GetTypeAnn()
                << " for parameter: " << name));
            return nullptr;
        }

        return param;
    }, ctx, settings);

    return status1.Level == IGraphTransformer::TStatus::Ok;
}

}

bool ExtractParametersMetaAsYson(const TExprNode::TPtr& input, TTypeAnnotationContext& types,
    TExprContext& ctx, NYT::TNode& paramsMetaMap) {

    THashMap<TStringBuf, const TTypeAnnotationNode*> params;
    if (!ExtractParameterTypes(input, types, ctx, params)) {
        return false;
    }

    for (auto& p : params) {
        paramsMetaMap[p.first] = NCommon::TypeToYsonNode(p.second);
    }
    return true;
}

IGraphTransformer::TStatus EvaluateParameters(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TTypeAnnotationContext& types, TExprContext& ctx, const IFunctionRegistry& functionRegistry) {
    output = input;
    if (ctx.Step.IsDone(TExprStep::Params)) {
        return IGraphTransformer::TStatus::Ok;
    }

    THashMap<TStringBuf, const TTypeAnnotationNode*> paramTypes;
    if (!ExtractParameterTypes(input, types, ctx, paramTypes)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!paramTypes) {
        // no params - just exit
        return IGraphTransformer::TStatus::Ok;
    }

    const auto emptyMapNode = NYT::TNode::CreateMap();
    const auto& paramData = types.OperationOptions.ParametersYson.GetOrElse(emptyMapNode);
    THashMap<TStringBuf, TExprNode::TPtr> paramValues;
    if (!BuildParameterValuesAsNodes(paramTypes, paramData, ctx, functionRegistry, paramValues)) {
        return IGraphTransformer::TStatus::Error;
    }

    // inject param values into graph
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = true;
    auto status = OptimizeExpr(output, output, [&](const TExprNode::TPtr& node, TExprContext& ctx)->TExprNode::TPtr {
        if (!node->IsCallable("Parameter")) {
            return node;
        }

        auto name = node->Child(0)->Content();
        auto evaluated = paramValues.FindPtr(name);
        YQL_ENSURE(evaluated, "Missing parameter value: " << name);
        return ctx.ShallowCopyWithPosition(**evaluated, node->Pos());
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return status;
    }

    ctx.Step.Done(TExprStep::Params);
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql
