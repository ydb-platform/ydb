#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>

namespace NYql {

using namespace NNodes;

namespace {

TExprNode::TPtr MakeWideLambda(const TExprNode& lambda, ui32 limit, TExprContext& ctx) {
    if (lambda.IsCallable("Void"))
        return {};

    if (const auto inStructType = dynamic_cast<const TStructExprType*>(GetSeqItemType(lambda.Head().Head().GetTypeAnn())), outStructType = dynamic_cast<const TStructExprType*>(GetSeqItemType(lambda.GetTypeAnn()));
        inStructType && outStructType && limit > std::max(inStructType->GetSize(), outStructType->GetSize()) && 0U < std::min(inStructType->GetSize(), outStructType->GetSize())) {

        return ctx.Builder(lambda.Pos())
            .Lambda()
                .Param("wide")
                .Callable("ExpandMap")
                    .Apply(0, lambda)
                        .With(0)
                            .Callable("NarrowMap")
                                .Arg(0, "wide")
                                .Lambda(1)
                                    .Params("fields", inStructType->GetSize())
                                    .Callable("AsStruct")
                                        .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                            ui32 i = 0U;
                                            for (const auto& item : inStructType->GetItems()) {
                                                parent.List(i)
                                                    .Atom(0, item->GetName())
                                                    .Arg(1, "fields", i)
                                                .Seal();
                                                ++i;
                                            }
                                            return parent;
                                        })
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Done()
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : outStructType->GetItems()) {
                                lambda.Callable(i++, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                .Seal();
                            }
                            return lambda;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
    } else if (inStructType && limit > inStructType->GetSize() && 0U < inStructType->GetSize()) {
        return ctx.Builder(lambda.Pos())
            .Lambda()
                .Param("wide")
                .Apply(lambda)
                    .With(0)
                        .Callable("NarrowMap")
                            .Arg(0, "wide")
                            .Lambda(1)
                                .Params("fields", inStructType->GetSize())
                                .Callable("AsStruct")
                                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                                        ui32 i = 0U;
                                        for (const auto& item : inStructType->GetItems()) {
                                            parent.List(i)
                                                .Atom(0, item->GetName())
                                                .Arg(1, "fields", i)
                                            .Seal();
                                            ++i;
                                        }
                                        return parent;
                                    })
                                .Seal()
                            .Seal()
                        .Seal()
                    .Done()
                .Seal()
            .Seal().Build();
    } else if (outStructType && limit > outStructType->GetSize() && 0U < outStructType->GetSize()) {
        return ctx.Builder(lambda.Pos())
            .Lambda()
                .Param("flow")
                .Callable("ExpandMap")
                    .Apply(0, lambda)
                        .With(0, "flow")
                    .Seal()
                    .Lambda(1)
                        .Param("item")
                        .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : outStructType->GetItems()) {
                                lambda.Callable(i++, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                .Seal();
                            }
                            return lambda;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return {};
}

TExprNode::TPtr MakeWideLambdaNoArg(const TExprNode& lambda, ui32 limit, TExprContext& ctx) {
    if (const auto outStructType = dynamic_cast<const TStructExprType*>(GetSeqItemType(lambda.GetTypeAnn()));
        outStructType && limit > outStructType->GetSize() && 0U < outStructType->GetSize()) {
        return ctx.Builder(lambda.Pos())
            .Lambda()
                .Callable("ExpandMap")
                    .Add(0, lambda.TailPtr())
                    .Lambda(1)
                        .Param("item")
                        .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                            ui32 i = 0U;
                            for (const auto& item : outStructType->GetItems()) {
                                lambda.Callable(i++, "Member")
                                    .Arg(0, "item")
                                    .Atom(1, item->GetName())
                                .Seal();
                            }
                            return lambda;
                        })
                    .Seal()
                .Seal()
            .Seal().Build();
    }

    return {};
}

class TYtWideFlowTransformer : public TOptimizeTransformerBase {
public:
    TYtWideFlowTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(state ? state->Types : nullptr, NLog::EComponent::ProviderYt, {})
        , Limit_(state ? state->Configuration->WideFlowLimit.Get().GetOrElse(DEFAULT_WIDE_FLOW_LIMIT) : DEFAULT_WIDE_FLOW_LIMIT)
    {
        if (Limit_) {
#define HNDL(name) "WideFlow-"#name, Hndl(&TYtWideFlowTransformer::name)
            AddHandler(0, &TYtFill::Match,      HNDL(OptimizeFill));
            AddHandler(0, &TYtMap::Match,       HNDL(OptimizeMap));
            AddHandler(0, &TYtReduce::Match,    HNDL(OptimizeReduce));
            AddHandler(0, &TYtMapReduce::Match, HNDL(OptimizeMapReduce));
#undef HNDL
        }
    }

    TMaybeNode<TExprBase> OptimizeFill(TExprBase node, TExprContext& ctx) {
        if (const auto fill = node.Cast<TYtFill>(); auto wideContent = MakeWideLambdaNoArg(fill.Content().Ref(), Limit_, ctx)) {
            if (auto settings = UpdateSettingValue(fill.Settings().Ref(), EYtSettingType::Flow, ctx.NewAtom(fill.Pos(), ToString(Limit_), TNodeFlags::Default), ctx)) {
                const auto wideFill = Build<TYtFill>(ctx, fill.Pos())
                        .InitFrom(fill)
                        .Content(std::move(wideContent))
                        .Settings(std::move(settings))
                    .Done();
                return wideFill.Ptr();
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> OptimizeMap(TExprBase node, TExprContext& ctx) {
        if (const auto map = node.Cast<TYtMap>(); auto wideMapper = MakeWideLambda(map.Mapper().Ref(), Limit_, ctx)) {
            if (auto settings = UpdateSettingValue(map.Settings().Ref(), EYtSettingType::Flow, ctx.NewAtom(map.Pos(), ToString(Limit_), TNodeFlags::Default), ctx)) {
                const auto wideMap = Build<TYtMap>(ctx, map.Pos())
                        .InitFrom(map)
                        .Mapper(std::move(wideMapper))
                        .Settings(std::move(settings))
                    .Done();
                return wideMap.Ptr();
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> OptimizeReduce(TExprBase node, TExprContext& ctx) {
        if (const auto reduce = node.Cast<TYtReduce>(); auto wideReducer = MakeWideLambda(reduce.Reducer().Ref(), Limit_, ctx)) {
            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, reduce.Pos()).InitFrom(reduce.Settings())
                .Add()
                    .Name().Build(ToString(EYtSettingType::ReduceInputType))
                    .Value(TExprBase(ExpandType(reduce.Pos(), GetSeqItemType(*reduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()), ctx)))
                .Build();

            if (auto settings = UpdateSettingValue(settingsBuilder.Done().Ref(), EYtSettingType::Flow, ctx.NewAtom(reduce.Pos(), ToString(Limit_), TNodeFlags::Default), ctx)) {
                const auto wideReduce = Build<TYtReduce>(ctx, reduce.Pos())
                        .InitFrom(reduce)
                        .Reducer(std::move(wideReducer))
                        .Settings(std::move(settings))
                    .Done();
                return wideReduce.Ptr();
            }
        }

        return node;
    }

    TMaybeNode<TExprBase> OptimizeMapReduce(TExprBase node, TExprContext& ctx) {
        const auto mapReduce = node.Cast<TYtMapReduce>();
        if (auto wideMapper = MakeWideLambda(mapReduce.Mapper().Ref(), Limit_, ctx), wideReducer = MakeWideLambda(mapReduce.Reducer().Ref(), Limit_, ctx); wideMapper || wideReducer) {

            auto settingsBuilder = Build<TCoNameValueTupleList>(ctx, mapReduce.Pos()).InitFrom(mapReduce.Settings())
                .Add()
                    .Name().Build(ToString(EYtSettingType::ReduceInputType))
                    .Value(TExprBase(ExpandType(mapReduce.Pos(), GetSeqItemType(*mapReduce.Reducer().Args().Arg(0).Ref().GetTypeAnn()), ctx)))
                .Build();

            if (wideMapper) {
                settingsBuilder
                    .Add()
                        .Name().Build(ToString(EYtSettingType::MapOutputType))
                        .Value(TExprBase(ExpandType(mapReduce.Pos(), GetSeqItemType(*mapReduce.Mapper().Ref().GetTypeAnn()), ctx)))
                    .Build();
            }

            if (auto settings = UpdateSettingValue(settingsBuilder.Done().Ref(), EYtSettingType::Flow, ctx.NewAtom(mapReduce.Pos(), ToString(Limit_), TNodeFlags::Default), ctx)) {
                const auto wideMapReduce = Build<TYtMapReduce>(ctx, mapReduce.Pos())
                        .InitFrom(mapReduce)
                        .Mapper(wideMapper ? std::move(wideMapper) : mapReduce.Mapper().Ptr())
                        .Reducer(wideReducer ? std::move(wideReducer) : mapReduce.Reducer().Ptr())
                        .Settings(std::move(settings))
                    .Done();
                return wideMapReduce.Ptr();
            }
        }

        return node;
    }
private:
    const ui16 Limit_;
};

}

THolder<IGraphTransformer> CreateYtWideFlowTransformer(TYtState::TPtr state) {
    return MakeHolder<TYtWideFlowTransformer>(std::move(state));
}

} // namespace NYql
