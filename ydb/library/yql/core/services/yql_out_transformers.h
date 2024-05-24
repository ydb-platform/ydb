#pragma once

#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/services/yql_plan.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>

#include <library/cpp/yson/public.h>

#include <util/stream/output.h>
#include <util/generic/ptr.h>

namespace NYql {

class TExprOutputTransformer {
public:
    TExprOutputTransformer(const TExprNode::TPtr& exprRoot, IOutputStream* directOut, bool withTypes)
        : ExprRoot_(exprRoot), DirectOut_(directOut), WithTypes_(withTypes)
    {
    }

    IGraphTransformer::TStatus operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

    static TAutoPtr<IGraphTransformer> Sync(
        const TExprNode::TPtr& exprRoot,
        IOutputStream* directOut,
        bool withTypes = false)
    {
        return directOut ? CreateFunctorTransformer(TExprOutputTransformer(exprRoot, directOut, withTypes)) : nullptr;
    }

private:
    const TExprNode::TPtr &ExprRoot_;
    IOutputStream *DirectOut_;
    bool WithTypes_;
};

class TPlanOutputTransformer {
public:
    TPlanOutputTransformer(
        IOutputStream* directOut,
        IPlanBuilder& builder,
        NYson::EYsonFormat outputFormat,
        TPlanSettings&& settings = {})
        : DirectOut_(directOut)
        , Builder_(builder)
        , OutputFormat_(outputFormat)
        , PlanSettings_(std::move(settings))
    {
    }

    IGraphTransformer::TStatus operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

    static TAutoPtr <IGraphTransformer> Sync(
            IOutputStream* directOut,
            IPlanBuilder& builder,
            NYson::EYsonFormat outputFormat)
    {
        return CreateFunctorTransformer(TPlanOutputTransformer(directOut, builder, outputFormat));
    }

private:
    IOutputStream* DirectOut_;
    IPlanBuilder& Builder_;
    NYson::EYsonFormat OutputFormat_;
    TPlanSettings PlanSettings_;
};

class TExprLogTransformer {
public:
    TExprLogTransformer(const TString& description, NYql::NLog::EComponent component, NYql::NLog::ELevel level)
        : Description(description)
        , Component(component)
        , Level(level) {}

    NYql::IGraphTransformer::TStatus operator()(const NYql::TExprNode::TPtr& input, NYql::TExprNode::TPtr& output,
        NYql::TExprContext& ctx);

    static TAutoPtr<NYql::IGraphTransformer> Sync(const TString& description,
        NYql::NLog::EComponent component = NYql::NLog::EComponent::Core,
        NYql::NLog::ELevel level = NYql::NLog::ELevel::TRACE)
    {
        return CreateFunctorTransformer(TExprLogTransformer(description, component, level));
    }

private:
    TString Description;
    NYql::NLog::EComponent Component;
    NYql::NLog::ELevel Level;
};

} // namespace NYql
