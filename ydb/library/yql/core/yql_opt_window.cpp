#include "yql_opt_window.h"
#include "yql_opt_utils.h"
#include "yql_expr_type_annotation.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

const TStringBuf SessionStartMemberName = "_yql_window_session_start";
const TStringBuf SessionParamsMemberName = "_yql_window_session_params";

enum class EFrameBoundsType : ui8 {
    EMPTY,
    LAGGING,
    CURRENT,
    LEADING,
    FULL,
    GENERIC,
};

EFrameBoundsType FrameBoundsType(const TWindowFrameSettings& settings) {
    auto first = settings.GetFirstOffset();
    auto last = settings.GetLastOffset();

    if (first.Defined() && last.Defined() && first > last) {
        return EFrameBoundsType::EMPTY;
    }

    if (!first.Defined()) {
        if (!last.Defined()) {
            return EFrameBoundsType::FULL;
        }
        if (*last < 0) {
            return EFrameBoundsType::LAGGING;
        }

        return *last > 0 ? EFrameBoundsType::LEADING : EFrameBoundsType::CURRENT;
    }

    return EFrameBoundsType::GENERIC;
}

TExprNode::TPtr ReplaceLastLambdaArgWithUnsignedLiteral(const TExprNode& lambda, ui32 literal, TExprContext& ctx) {
    YQL_ENSURE(lambda.IsLambda());
    TExprNodeList args = lambda.ChildPtr(0)->ChildrenList();
    YQL_ENSURE(!args.empty());

    auto literalNode = ctx.Builder(lambda.Pos())
        .Callable("Uint32")
            .Atom(0, literal)
        .Seal()
        .Build();
    auto newBody = ctx.ReplaceNodes(lambda.ChildPtr(1), {{args.back().Get(), literalNode}});
    args.pop_back();
    return ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Pos(), std::move(args)), std::move(newBody));
}

TExprNode::TPtr ReplaceFirstLambdaArgWithCastStruct(const TExprNode& lambda, const TTypeAnnotationNode& targetType, TExprContext& ctx) {
    YQL_ENSURE(lambda.IsLambda());
    YQL_ENSURE(targetType.GetKind() == ETypeAnnotationKind::Struct);
    TExprNodeList args = lambda.ChildPtr(0)->ChildrenList();
    YQL_ENSURE(!args.empty());

    auto newArg = ctx.NewArgument(lambda.Pos(), "row");

    auto cast = ctx.Builder(lambda.Pos())
        .Callable("MatchType")
            .Add(0, newArg)
            .Atom(1, "Optional", TNodeFlags::Default)
            .Lambda(2)
                .Param("row")
                .Callable("Map")
                    .Arg(0, "row")
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("CastStruct")
                            .Arg(0, "unwrapped")
                            .Add(1, ExpandType(lambda.Pos(), targetType, ctx))
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("row")
                .Callable("CastStruct")
                    .Arg(0, "row")
                    .Add(1, ExpandType(lambda.Pos(), targetType, ctx))
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newBody = ctx.ReplaceNodes(lambda.ChildPtr(1), {{args.front().Get(), cast}});
    args[0] = newArg;
    return ctx.NewLambda(lambda.Pos(), ctx.NewArguments(lambda.Pos(), std::move(args)), std::move(newBody));
}

TExprNode::TPtr AddOptionalIfNotAlreadyOptionalOrNull(const TExprNode::TPtr& lambda, TExprContext& ctx) {
    YQL_ENSURE(lambda->IsLambda());
    YQL_ENSURE(lambda->ChildPtr(0)->ChildrenSize() == 1);

    auto identity = MakeIdentityLambda(lambda->Pos(), ctx);
    return ctx.Builder(lambda->Pos())
        .Lambda()
            .Param("arg")
            .Callable("MatchType")
                .Apply(0, lambda)
                    .With(0, "arg")
                .Seal()
                .Atom(1, "Optional", TNodeFlags::Default)
                .Add(2, identity)
                .Atom(3, "Null", TNodeFlags::Default)
                .Add(4, identity)
                .Atom(5, "Pg", TNodeFlags::Default)
                .Add(6, identity)
                .Lambda(7)
                    .Param("result")
                    .Callable("Just")
                        .Arg(0, "result")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

struct TRawTrait {
    TPositionHandle Pos;

    // Init/Update/Default are set only for aggregations
    TExprNode::TPtr InitLambda;
    TExprNode::TPtr UpdateLambda;
    TExprNode::TPtr DefaultValue;

    TExprNode::TPtr CalculateLambda;
    TMaybe<i64> CalculateLambdaLead; // lead/lag for input to CalculateLambda;
    TVector<TExprNode::TPtr> Params; // NTile

    const TTypeAnnotationNode* OutputType = nullptr;

    TWindowFrameSettings FrameSettings;
};

struct TCalcOverWindowTraits {
    TMap<TStringBuf, TRawTrait> RawTraits;
    ui64 MaxDataOutpace = 0;
    ui64 MaxDataLag = 0;
    ui64 MaxUnboundedPrecedingLag = 0;
    const TTypeAnnotationNode* LagQueueItemType = nullptr;
};

TCalcOverWindowTraits ExtractCalcOverWindowTraits(const TExprNode::TPtr& frames, TExprContext& ctx) {
    TCalcOverWindowTraits result;

    auto& maxDataOutpace = result.MaxDataOutpace;
    auto& maxDataLag = result.MaxDataLag;
    auto& maxUnboundedPrecedingLag = result.MaxUnboundedPrecedingLag;

    TVector<const TItemExprType*> lagQueueStructItems;
    for (auto& winOn : frames->ChildrenList()) {
        TWindowFrameSettings frameSettings = TWindowFrameSettings::Parse(*winOn, ctx);

        ui64 frameOutpace = 0;
        ui64 frameLag = 0;

        const EFrameType ft = frameSettings.GetFrameType();
        if (ft == EFrameType::FrameByRows) {
            const EFrameBoundsType frameType = FrameBoundsType(frameSettings);
            const auto frameFirst = frameSettings.GetFirstOffset();
            const auto frameLast = frameSettings.GetLastOffset();

            if (frameType != EFrameBoundsType::EMPTY) {
                if (!frameLast.Defined() || *frameLast > 0) {
                    frameOutpace = frameLast.Defined() ? ui64(*frameLast) : Max<ui64>();
                }

                if (frameFirst.Defined() && *frameFirst < 0) {
                    frameLag = ui64(0 - *frameFirst);
                }
            }
        } else {
            // The only frame we currently support
            YQL_ENSURE(ft == EFrameType::FrameByRange);
            YQL_ENSURE(IsUnbounded(frameSettings.GetFirst()));
            YQL_ENSURE(IsCurrentRow(frameSettings.GetLast()));
        }
        const auto& winOnChildren = winOn->ChildrenList();
        YQL_ENSURE(winOnChildren.size() > 1);
        for (size_t i = 1; i < winOnChildren.size(); ++i) {
            auto item = winOnChildren[i];
            YQL_ENSURE(item->IsList());

            auto nameNode = item->Child(0);
            YQL_ENSURE(nameNode->IsAtom());

            TStringBuf name = nameNode->Content();

            YQL_ENSURE(!result.RawTraits.contains(name));

            auto traits = item->Child(1);

            auto& rawTraits = result.RawTraits[name];
            rawTraits.FrameSettings = frameSettings;
            rawTraits.Pos = traits->Pos();

            YQL_ENSURE(traits->IsCallable({"WindowTraits","CumeDist"}) || ft == EFrameType::FrameByRows, "Non-canonical frame for window functions");
            if (traits->IsCallable("WindowTraits")) {
                maxDataOutpace = Max(maxDataOutpace, frameOutpace);
                maxDataLag = Max(maxDataLag, frameLag);

                auto initLambda = traits->ChildPtr(1);
                auto updateLambda = traits->ChildPtr(2);
                auto calculateLambda = traits->ChildPtr(4);

                rawTraits.OutputType = calculateLambda->GetTypeAnn();
                YQL_ENSURE(rawTraits.OutputType);

                if (initLambda->Child(0)->ChildrenSize() == 2) {
                    initLambda = ReplaceLastLambdaArgWithUnsignedLiteral(*initLambda, i, ctx);
                }

                if (updateLambda->Child(0)->ChildrenSize() == 3) {
                    updateLambda = ReplaceLastLambdaArgWithUnsignedLiteral(*updateLambda, i, ctx);
                }

                auto lambdaInputType = traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                initLambda = ReplaceFirstLambdaArgWithCastStruct(*initLambda, *lambdaInputType, ctx);
                updateLambda = ReplaceFirstLambdaArgWithCastStruct(*updateLambda, *lambdaInputType, ctx);

                rawTraits.InitLambda = initLambda;
                rawTraits.UpdateLambda = updateLambda;
                rawTraits.CalculateLambda = calculateLambda;
                rawTraits.DefaultValue = traits->ChildPtr(5);

                if (ft == EFrameType::FrameByRows) {
                    const EFrameBoundsType frameType = FrameBoundsType(frameSettings);
                    const auto frameLast = frameSettings.GetLastOffset();
                    if (frameType == EFrameBoundsType::LAGGING) {
                        maxUnboundedPrecedingLag = Max(maxUnboundedPrecedingLag, ui64(abs(*frameLast)));
                        lagQueueStructItems.push_back(ctx.MakeType<TItemExprType>(name, rawTraits.OutputType));
                    }
                }
            } else if (traits->IsCallable({"Lead", "Lag"})) {
                i64 lead = 1;
                if (traits->ChildrenSize() == 3) {
                    YQL_ENSURE(traits->Child(2)->IsCallable("Int64"));
                    lead = FromString<i64>(traits->Child(2)->Child(0)->Content());
                }

                if (traits->IsCallable("Lag")) {
                    lead = -lead;
                }

                if (lead < 0) {
                    maxDataLag = Max(maxDataLag, ui64(abs(lead)));
                } else {
                    maxDataOutpace = Max<ui64>(maxDataOutpace, lead);
                }

                auto lambdaInputType =
                    traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();

                rawTraits.CalculateLambda = ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), *lambdaInputType, ctx);
                rawTraits.CalculateLambdaLead = lead;
                rawTraits.OutputType = traits->Child(1)->GetTypeAnn();
                YQL_ENSURE(rawTraits.OutputType);
            } else if (traits->IsCallable({"Rank", "DenseRank", "PercentRank"})) {
                rawTraits.OutputType = traits->Child(1)->GetTypeAnn();
                auto lambdaInputType =
                    traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TListExprType>()->GetItemType();
                auto lambda = ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), *lambdaInputType, ctx);
                rawTraits.CalculateLambda = ctx.ChangeChild(*traits, 1, std::move(lambda));
            } else {
                YQL_ENSURE(traits->IsCallable({"RowNumber","CumeDist","NTile"}));
                rawTraits.CalculateLambda = traits;
                rawTraits.OutputType = traits->GetTypeAnn();
                for (ui32 i = 1; i < traits->ChildrenSize(); ++i) {
                    rawTraits.Params.push_back(traits->ChildPtr(i));
                }
            }
        }
    }

    result.LagQueueItemType = ctx.MakeType<TStructExprType>(lagQueueStructItems);

    return result;
}

TExprNode::TPtr BuildUint64(TPositionHandle pos, ui64 value, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Uint64")
            .Atom(0, ToString(value))
        .Seal()
        .Build();
}

TExprNode::TPtr BuildDouble(TPositionHandle pos, double value, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Double")
            .Atom(0, ToString(value))
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueuePeek(TPositionHandle pos, const TExprNode::TPtr& queue, ui64 index, const TExprNode::TPtr& dependsOn,
    TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("QueuePeek")
            .Add(0, queue)
            .Add(1, BuildUint64(pos, index, ctx))
            .Callable(2, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueueRange(TPositionHandle pos, const TExprNode::TPtr& queue, ui64 begin, ui64 end,
                                const TExprNode::TPtr& dependsOn, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Callable("FlatMap")
            .Callable(0, "QueueRange")
                .Add(0, queue)
                .Add(1, BuildUint64(pos, begin, ctx))
                .Add(2, BuildUint64(pos, end, ctx))
                .Callable(3, "DependsOn")
                    .Add(0, dependsOn)
                .Seal()
            .Seal()
            .Lambda(1)
                .Param("item")
                .Arg("item")
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildQueue(TPositionHandle pos, const TTypeAnnotationNode& itemType, ui64 queueSize, ui64 initSize,
    const TExprNode::TPtr& dependsOn, TExprContext& ctx)
{
    TExprNode::TPtr size;
    if (queueSize == Max<ui64>()) {
        size = ctx.NewCallable(pos, "Void", {});
    } else {
        size = BuildUint64(pos, queueSize, ctx);
    }

    return ctx.Builder(pos)
        .Callable("QueueCreate")
            .Add(0, ExpandType(pos, itemType, ctx))
            .Add(1, size)
            .Add(2, BuildUint64(pos, initSize, ctx))
            .Callable(3, "DependsOn")
                .Add(0, dependsOn)
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr CoalesceQueueOutput(TPositionHandle pos, const TExprNode::TPtr& output, bool rawOutputIsOptional,
    const TExprNode::TPtr& defaultValue, TExprContext& ctx)
{
    // output is has type Optional<RawOutputType>
    if (!rawOutputIsOptional) {
        return ctx.Builder(pos)
            .Callable("Coalesce")
                .Add(0, output)
                .Add(1, defaultValue)
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable("IfPresent")
            .Add(0, output)
            .Lambda(1)
                .Param("item")
                .Callable("Coalesce")
                    .Arg(0, "item")
                    .Add(1, defaultValue)
                .Seal()
            .Seal()
            .Add(2, defaultValue)
        .Seal()
        .Build();
}

TExprNode::TPtr WrapWithWinContext(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (HasContextFuncs(*input)) {
        return ctx.Builder(input->Pos())
            .Callable("WithContext")
                .Add(0, input)
                .Atom(1, "WinAgg", TNodeFlags::Default)
            .Seal()
            .Build();
    }
    return input;
}

TExprNode::TPtr BuildInitLambdaForChain1Map(TPositionHandle pos, const TExprNode::TPtr& initStateLambda,
    const TExprNode::TPtr& calculateLambda, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .List()
                .Do([&](TExprNodeBuilder& parent)->TExprNodeBuilder& {
                    if (calculateLambda->Head().ChildrenSize() == 1) {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(initStateLambda)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                        .Seal();
                    } else {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(initStateLambda)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                            .With(1, "row")
                        .Seal();
                    }

                    return parent;
                })
                .Apply(1, initStateLambda)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildUpdateLambdaForChain1Map(TPositionHandle pos, const TExprNode::TPtr& updateStateLambda,
    const TExprNode::TPtr& calculateLambda, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Param("state")
            .List()
                .Do([&](TExprNodeBuilder& parent)->TExprNodeBuilder& {
                    if (calculateLambda->Head().ChildrenSize() == 1) {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(updateStateLambda)
                                    .With(0, "row")
                                    .With(1, "state")
                                .Seal()
                            .Done()
                        .Seal();
                    } else {
                        parent.Apply(0, calculateLambda)
                            .With(0)
                                .Apply(updateStateLambda)
                                    .With(0, "row")
                                    .With(1, "state")
                                .Seal()
                            .Done()
                            .With(1, "row")
                        .Seal();
                    }

                    return parent;
                })
                .Apply(1, updateStateLambda)
                    .With(0, "row")
                    .With(1, "state")
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

class TChain1MapTraits : public TThrRefBase, public TNonCopyable {
public:
    using TPtr = TIntrusivePtr<TChain1MapTraits>;

    TChain1MapTraits(TStringBuf name, TPositionHandle pos)
      : Name(name)
      , Pos(pos)
    {
    }

    TStringBuf GetName() const {
        return Name;
    }

    TPositionHandle GetPos() const {
        return Pos;
    }

    // Lambda(row) -> AsTuple(output, state)
    virtual TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const = 0;

    // Lambda(row, state) -> AsTuple(output, state)
    virtual TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const = 0;

    virtual TExprNode::TPtr ExtractLaggingOutput(const TExprNode::TPtr& lagQueue,
        const TExprNode::TPtr& dependsOn, TExprContext& ctx) const
    {
        Y_UNUSED(lagQueue);
        Y_UNUSED(dependsOn);
        Y_UNUSED(ctx);
        return {};
    }

    virtual ~TChain1MapTraits() = default;
private:
    const TStringBuf Name;
    const TPositionHandle Pos;
};

class TChain1MapTraitsLagLead : public TChain1MapTraits {
public:
    TChain1MapTraitsLagLead(TStringBuf name, const TRawTrait& raw, TMaybe<ui64> queueOffset)
        : TChain1MapTraits(name, raw.Pos)
        , QueueOffset(queueOffset)
        , LeadLagLambda(raw.CalculateLambda)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Apply(0, CalculateOutputLambda(dataQueue, ctx))
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Void")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Apply(0, CalculateOutputLambda(dataQueue, ctx))
                        .With(0, "row")
                    .Seal()
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    TExprNode::TPtr CalculateOutputLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const {
        if (!QueueOffset.Defined()) {
            return AddOptionalIfNotAlreadyOptionalOrNull(LeadLagLambda, ctx);
        }

        YQL_ENSURE(dataQueue);

        auto rowArg = ctx.NewArgument(GetPos(), "row");

        auto body = ctx.Builder(GetPos())
            .Callable("IfPresent")
                .Add(0, BuildQueuePeek(GetPos(), dataQueue, *QueueOffset, rowArg, ctx))
                .Add(1, AddOptionalIfNotAlreadyOptionalOrNull(LeadLagLambda, ctx))
                .Callable(2, "Null")
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(body));
    }

    const TMaybe<ui64> QueueOffset;
    const TExprNode::TPtr LeadLagLambda;
};

class TChain1MapTraitsRowNumber : public TChain1MapTraits {
public:
    TChain1MapTraitsRowNumber(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Inc")
                        .Arg(0, "state")
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }
};

class TChain1MapTraitsCumeDist : public TChain1MapTraits {
public:
    TChain1MapTraitsCumeDist(TStringBuf name, const TRawTrait& raw, const TString& partitionRowsColumn)
        : TChain1MapTraits(name, raw.Pos)
        , PartitionRowsColumn(partitionRowsColumn)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Callable(0, "/")
                        .Add(0, BuildDouble(GetPos(), 1.0, ctx))
                        .Callable(1, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn)
                        .Seal()
                    .Seal()
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "/")
                        .Callable(0, "SafeCast")
                            .Callable(0, "Inc")
                                .Arg(0, "state")
                            .Seal()
                            .Atom(1, "Double")
                        .Seal()
                        .Callable(1, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn)
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

private:
    const TString PartitionRowsColumn;
};

class TChain1MapTraitsNTile : public TChain1MapTraits {
public:
    TChain1MapTraitsNTile(TStringBuf name, const TRawTrait& raw, const TString& partitionRowsColumn)
        : TChain1MapTraits(name, raw.Pos)
        , PartitionRowsColumn(partitionRowsColumn)
    {
        YQL_ENSURE(raw.Params.size() == 1);
        Param = raw.Params[0];
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Add(1, BuildUint64(GetPos(), 1, ctx))
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Inc")
                        .Callable(0, "Unwrap")
                            .Callable(0, "/")
                                .Callable(0, "*")
                                    .Callable(0, "SafeCast")
                                        .Add(0, Param)
                                        .Atom(1, "Uint64")
                                    .Seal()
                                    .Arg(1, "state")
                                .Seal()
                                .Callable(1, "Member")
                                    .Arg(0, "row")
                                    .Atom(1, PartitionRowsColumn)
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Arg(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

private:
    const TString PartitionRowsColumn;
    TExprNode::TPtr Param;
};

class TChain1MapTraitsRankBase : public TChain1MapTraits {
public:
    TChain1MapTraitsRankBase(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
        , ExtractForCompareLambda(raw.CalculateLambda->ChildPtr(1))
        , Ansi(HasSetting(*raw.CalculateLambda->Child(2), "ansi"))
        , KeyType(raw.OutputType)
    {
    }

    virtual TExprNode::TPtr BuildCalculateLambda(TExprContext& ctx) const {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("state")
                .Callable("Nth")
                    .Arg(0, "state")
                    .Atom(1, "0")
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const final {
        Y_UNUSED(dataQueue);


        auto initKeyLambda = BuildRawInitLambda(ctx);
        if (!Ansi && KeyType->GetKind() == ETypeAnnotationKind::Optional) {
            auto stateType = GetStateType(KeyType->Cast<TOptionalExprType>()->GetItemType(), ctx);
            initKeyLambda = BuildOptKeyInitLambda(initKeyLambda, stateType, ctx);
        }

        auto initRowLambda = ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Apply(initKeyLambda)
                    .With(0)
                        .Apply(ExtractForCompareLambda)
                            .With(0, "row")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();

        return BuildInitLambdaForChain1Map(GetPos(), initRowLambda, BuildCalculateLambda(ctx), ctx);
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const final {
        Y_UNUSED(dataQueue);

        bool useAggrEquals = Ansi;
        auto updateKeyLambda = BuildRawUpdateLambda(useAggrEquals, ctx);

        if (!Ansi && KeyType->GetKind() == ETypeAnnotationKind::Optional) {
            auto stateType = GetStateType(KeyType->Cast<TOptionalExprType>()->GetItemType(), ctx);
            updateKeyLambda = ctx.Builder(GetPos())
                .Lambda()
                    .Param("key")
                    .Param("state")
                    .Callable("IfPresent")
                        .Arg(0, "state")
                        .Lambda(1)
                            .Param("unwrappedState")
                            .Callable("IfPresent")
                                .Arg(0, "key")
                                .Lambda(1)
                                    .Param("unwrappedKey")
                                    .Callable("Just")
                                        .Apply(0, updateKeyLambda)
                                            .With(0, "unwrappedKey")
                                            .With(1, "unwrappedState")
                                        .Seal()
                                    .Seal()
                                .Seal()
                                .Callable(2, "Just")
                                    .Arg(0, "unwrappedState")
                                .Seal()
                            .Seal()
                        .Seal()
                        .Apply(2, BuildOptKeyInitLambda(BuildRawInitLambda(ctx), stateType, ctx))
                            .With(0, "key")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }

        auto updateRowLambda = ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .Apply(updateKeyLambda)
                    .With(0)
                        .Apply(ExtractForCompareLambda)
                            .With(0, "row")
                        .Seal()
                    .Done()
                    .With(1, "state")
                .Seal()
            .Seal()
            .Build();

        return BuildUpdateLambdaForChain1Map(GetPos(), updateRowLambda, BuildCalculateLambda(ctx), ctx);
    }

    virtual TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const = 0;
    virtual TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const = 0;
    virtual const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const = 0;

private:
    TExprNode::TPtr BuildOptKeyInitLambda(const TExprNode::TPtr& rawInitKeyLambda,
        const TTypeAnnotationNode* stateType, TExprContext& ctx) const
    {
        auto optStateType = ctx.MakeType<TOptionalExprType>(stateType);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Callable("IfPresent")
                    .Arg(0, "key")
                    .Lambda(1)
                        .Param("unwrapped")
                        .Callable("Just")
                            .Apply(0, rawInitKeyLambda)
                                .With(0, "unwrapped")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(2, "Nothing")
                        .Add(0, ExpandType(GetPos(), *optStateType, ctx))
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    const TExprNode::TPtr ExtractForCompareLambda;
    const bool Ansi;
    const TTypeAnnotationNode* const KeyType;
};

class TChain1MapTraitsRank : public TChain1MapTraitsRankBase {
public:
    TChain1MapTraitsRank(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsRankBase(name, raw)
    {
    }

    TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const final {
        auto one = BuildUint64(GetPos(), 1, ctx);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .List()
                    .Add(0, one)
                    .Add(1, one)
                    .Arg(2, "key")
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Param("state")
                .List()
                    .Callable(0, "If")
                        .Callable(0, useAggrEquals ? "AggrEquals" : "==")
                            .Arg(0, "key")
                            .Callable(1, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "2")
                            .Seal()
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(2, "Inc")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "1")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Callable(1, "Inc")
                        .Callable(0, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "1")
                        .Seal()
                    .Seal()
                    .Arg(2, "key")
                .Seal()
            .Seal()
            .Build();
    }

    const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const final {
        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            keyType
        });
    }
};

class TChain1MapTraitsPercentRank : public TChain1MapTraitsRank {
public:
    TChain1MapTraitsPercentRank(TStringBuf name, const TRawTrait& raw, const TString& partitionRowsColumn)
        : TChain1MapTraitsRank(name, raw)
        , PartitionRowsColumn(partitionRowsColumn)
    {
    }

  virtual TExprNode::TPtr BuildCalculateLambda(TExprContext& ctx) const {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("state")
                .Param("row")
                .Callable("/")
                    .Callable(0, "SafeCast")
                        .Callable(0, "Dec")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "0")
                            .Seal()
                        .Seal()
                        .Atom(1, "Double")
                    .Seal()
                    .Callable(1, "Dec")
                        .Callable(0, "Member")
                            .Arg(0, "row")
                            .Atom(1, PartitionRowsColumn)
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

private:
    const TString PartitionRowsColumn;
};

class TChain1MapTraitsDenseRank : public TChain1MapTraitsRankBase {
public:
    TChain1MapTraitsDenseRank(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsRankBase(name, raw)
    {
    }

    TExprNode::TPtr BuildRawInitLambda(TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .List()
                    .Add(0, BuildUint64(GetPos(), 1, ctx))
                    .Arg(1, "key")
                .Seal()
            .Seal()
            .Build();
    }

    TExprNode::TPtr BuildRawUpdateLambda(bool useAggrEquals, TExprContext& ctx) const final {
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("key")
                .Param("state")
                .List()
                    .Callable(0, "If")
                        .Callable(0, useAggrEquals ? "AggrEquals" : "==")
                            .Arg(0, "key")
                            .Callable(1, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "1")
                            .Seal()
                        .Seal()
                        .Callable(1, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "0")
                        .Seal()
                        .Callable(2, "Inc")
                            .Callable(0, "Nth")
                                .Arg(0, "state")
                                .Atom(1, "0")
                            .Seal()
                        .Seal()
                    .Seal()
                    .Arg(1, "key")
                .Seal()
            .Seal()
            .Build();
    }

    const TTypeAnnotationNode* GetStateType(const TTypeAnnotationNode* keyType, TExprContext& ctx) const final {
        return ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TDataExprType>(EDataSlot::Uint64),
            keyType
        });
    }
};

class TChain1MapTraitsStateBase : public TChain1MapTraits {
public:
    TChain1MapTraitsStateBase(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraits(name, raw.Pos)
        , FrameNeverEmpty(raw.FrameSettings.IsNonEmpty())
        , InitLambda(raw.InitLambda)
        , UpdateLambda(raw.UpdateLambda)
        , CalculateLambda(raw.CalculateLambda)
        , DefaultValue(raw.DefaultValue)
    {
    }

protected:
    TExprNode::TPtr GetInitLambda() const {
        return InitLambda;
    }

    TExprNode::TPtr GetUpdateLambda() const {
        return UpdateLambda;
    }

    TExprNode::TPtr GetCalculateLambda() const {
        return CalculateLambda;
    }

    TExprNode::TPtr GetDefaultValue() const {
        return DefaultValue;
    }

    const bool FrameNeverEmpty;

private:
    const TExprNode::TPtr InitLambda;
    const TExprNode::TPtr UpdateLambda;
    const TExprNode::TPtr CalculateLambda;
    const TExprNode::TPtr DefaultValue;
};

class TChain1MapTraitsCurrentOrLagging : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsCurrentOrLagging(TStringBuf name, const TRawTrait& raw, TMaybe<ui64> lagQueueIndex)
        : TChain1MapTraitsStateBase(name, raw)
        , LaggingQueueIndex(lagQueueIndex)
        , OutputIsOptional(raw.OutputType->IsOptionalOrNull())
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildInitLambdaForChain1Map(GetPos(), GetInitLambda(), GetCalculateLambda(), ctx);
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return BuildUpdateLambdaForChain1Map(GetPos(), GetUpdateLambda(), GetCalculateLambda(), ctx);
    }

    TExprNode::TPtr ExtractLaggingOutput(const TExprNode::TPtr& lagQueue,
        const TExprNode::TPtr& dependsOn, TExprContext& ctx) const override
    {
        if (!LaggingQueueIndex.Defined()) {
            return {};
        }

        YQL_ENSURE(!FrameNeverEmpty);
        auto output = ctx.Builder(GetPos())
            .Callable("Map")
                .Add(0, BuildQueuePeek(GetPos(), lagQueue, *LaggingQueueIndex, dependsOn, ctx))
                .Lambda(1)
                    .Param("struct")
                    .Callable("Member")
                        .Arg(0, "struct")
                        .Atom(1, GetName())
                    .Seal()
                .Seal()
            .Seal()
            .Build();
        return CoalesceQueueOutput(GetPos(), output, OutputIsOptional, GetDefaultValue(), ctx);
    }

private:
    const TMaybe<ui64> LaggingQueueIndex;
    const bool OutputIsOptional;
};

class TChain1MapTraitsLeading : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsLeading(TStringBuf name, const TRawTrait& raw, ui64 currentRowIndex, ui64 lastRowIndex)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueBegin(currentRowIndex + 1)
        , QueueEnd(lastRowIndex + 1)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin, QueueEnd, rowArg, ctx))
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto initBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(initBody));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto stateArg = ctx.NewArgument(GetPos(), "state");

        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin, QueueEnd, rowArg, ctx))
                .Apply(1, originalUpdate)
                    .With(0, rowArg)
                    .With(1, stateArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto updateBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, originalUpdate)
                    .With(0, rowArg)
                    .With(1, stateArg)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg, stateArg}), std::move(updateBody));
    }

private:
    const ui64 QueueBegin;
    const ui64 QueueEnd;
};

class TChain1MapTraitsFull : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsFull(TStringBuf name, const TRawTrait& raw, ui64 currentRowIndex)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueBegin(currentRowIndex + 1)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    // state == output
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto state = ctx.Builder(GetPos())
            .Callable("Fold")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin, Max<ui64>(), rowArg, ctx))
                .Apply(1, originalInit)
                    .With(0, rowArg)
                .Seal()
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        state = WrapWithWinContext(state, ctx);

        auto initBody = ctx.Builder(GetPos())
            .List()
                .Apply(0, calculate)
                    .With(0, state)
                .Seal()
                .Apply(1, calculate)
                    .With(0, state)
                .Seal()
            .Seal()
            .Build();

        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(initBody));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Arg(0, "state")
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    const ui64 QueueBegin;
};

class TChain1MapTraitsGeneric : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsGeneric(TStringBuf name, const TRawTrait& raw, ui64 queueBegin, ui64 queueEnd)
        : TChain1MapTraitsStateBase(name, raw)
        , QueueBegin(queueBegin)
        , QueueEnd(queueEnd)
        , OutputIsOptional(raw.OutputType->IsOptionalOrNull())
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto body = ctx.Builder(GetPos())
            .List()
                .Add(0, BuildFinalOutput(rowArg, dataQueue, ctx))
                .Callable(1, "Void")
                .Seal()
            .Seal()
            .Build();
        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg}), std::move(body));
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        auto rowArg = ctx.NewArgument(GetPos(), "row");
        auto stateArg = ctx.NewArgument(GetPos(), "state");
        auto body = ctx.Builder(GetPos())
            .List()
                .Add(0, BuildFinalOutput(rowArg, dataQueue, ctx))
                .Add(1, stateArg)
            .Seal()
            .Build();
        return ctx.NewLambda(GetPos(), ctx.NewArguments(GetPos(), {rowArg, stateArg}), std::move(body));
    }

private:
    TExprNode::TPtr BuildFinalOutput(const TExprNode::TPtr& rowArg, const TExprNode::TPtr& dataQueue, TExprContext& ctx) const {
        YQL_ENSURE(dataQueue);
        auto originalInit = GetInitLambda();
        auto originalUpdate = GetUpdateLambda();
        auto calculate = GetCalculateLambda();

        auto fold1 = ctx.Builder(GetPos())
            .Callable("Fold1")
                .Add(0, BuildQueueRange(GetPos(), dataQueue, QueueBegin, QueueEnd, rowArg, ctx))
                .Add(1, ctx.DeepCopyLambda(*originalInit))
                .Add(2, ctx.DeepCopyLambda(*originalUpdate))
            .Seal()
            .Build();

        fold1 = WrapWithWinContext(fold1, ctx);

        auto output = ctx.Builder(GetPos())
            .Callable("Map")
                .Add(0, fold1)
                .Add(1, ctx.DeepCopyLambda(*calculate))
            .Seal()
            .Build();

        if (FrameNeverEmpty) {
            // output is always non-empty optional in this case
            // we do IfPresent with some fake output value to remove optional
            // this will have exactly the same result as Unwrap(output)
            return ctx.Builder(GetPos())
                .Callable("IfPresent")
                    .Add(0, output)
                    .Lambda(1)
                        .Param("unwrapped")
                        .Arg("unwrapped")
                    .Seal()
                    .Apply(2, calculate)
                        .With(0)
                            .Apply(originalInit)
                                .With(0, rowArg)
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build();
        }

        return CoalesceQueueOutput(GetPos(), output, OutputIsOptional, GetDefaultValue(), ctx);
    }

    const ui64 QueueBegin;
    const ui64 QueueEnd;
    const bool OutputIsOptional;
};

class TChain1MapTraitsEmpty : public TChain1MapTraitsStateBase {
public:
    TChain1MapTraitsEmpty(TStringBuf name, const TRawTrait& raw)
        : TChain1MapTraitsStateBase(name, raw)
        , RawOutputType(raw.OutputType)
    {
    }

    // Lambda(row) -> AsTuple(output, state)
    TExprNode::TPtr BuildInitLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .List()
                    .Add(0, BuildFinalOutput(ctx))
                    .Callable(1, "Void")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    // Lambda(row, state) -> AsTuple(output, state)
    TExprNode::TPtr BuildUpdateLambda(const TExprNode::TPtr& dataQueue, TExprContext& ctx) const override {
        Y_UNUSED(dataQueue);
        return ctx.Builder(GetPos())
            .Lambda()
                .Param("row")
                .Param("state")
                .List()
                    .Add(0, BuildFinalOutput(ctx))
                    .Arg(1, "state")
                .Seal()
            .Seal()
            .Build();
    }

private:
    TExprNode::TPtr BuildFinalOutput(TExprContext& ctx) const {
        const auto defaultValue = GetDefaultValue();
        YQL_ENSURE(!FrameNeverEmpty);

        if (defaultValue->IsCallable("Null")) {
            auto resultingType = RawOutputType;
            if (!resultingType->IsOptionalOrNull()) {
                resultingType = ctx.MakeType<TOptionalExprType>(resultingType);
            }

            return ctx.Builder(GetPos())
                .Callable("Nothing")
                    .Add(0, ExpandType(GetPos(), *resultingType, ctx))
                .Seal()
                .Build();
        }
        return defaultValue;
    }

    const TTypeAnnotationNode* const RawOutputType;
};

struct TQueueParams {
    ui64 DataOutpace = 0;
    ui64 DataLag = 0;
    bool DataQueueNeeded = false;
    ui64 LagQueueSize = 0;
    const TTypeAnnotationNode* LagQueueItemType = nullptr;
};

TVector<TChain1MapTraits::TPtr> BuildFoldMapTraits(TQueueParams& queueParams, const TExprNode::TPtr& frames,
    const TMaybe<TString>& partitionRowsColumn, TExprContext& ctx) {
    queueParams = {};

    TVector<TChain1MapTraits::TPtr> result;

    TCalcOverWindowTraits traits = ExtractCalcOverWindowTraits(frames, ctx);

    if (traits.LagQueueItemType->Cast<TStructExprType>()->GetSize()) {
        YQL_ENSURE(traits.MaxUnboundedPrecedingLag > 0);
        queueParams.LagQueueSize = traits.MaxUnboundedPrecedingLag;
        queueParams.LagQueueItemType = traits.LagQueueItemType;
    }

    ui64 currentRowIndex = 0;
    if (traits.MaxDataOutpace || traits.MaxDataLag) {
        queueParams.DataOutpace = traits.MaxDataOutpace;
        queueParams.DataLag = traits.MaxDataLag;
        currentRowIndex = queueParams.DataLag;
        queueParams.DataQueueNeeded = true;
    }

    for (const auto& item : traits.RawTraits) {
        TStringBuf name = item.first;
        const TRawTrait& trait = item.second;

        if (!trait.InitLambda) {
            YQL_ENSURE(!trait.UpdateLambda);
            YQL_ENSURE(!trait.DefaultValue);

            if (trait.CalculateLambdaLead.Defined()) {
                TMaybe<ui64> queueOffset;
                if (*trait.CalculateLambdaLead) {
                    queueOffset = currentRowIndex + *trait.CalculateLambdaLead;
                }

                result.push_back(new TChain1MapTraitsLagLead(name, trait, queueOffset));
            } else if (trait.CalculateLambda->IsCallable("RowNumber")) {
                result.push_back(new TChain1MapTraitsRowNumber(name, trait));
            } else if (trait.CalculateLambda->IsCallable("Rank")) {
                result.push_back(new TChain1MapTraitsRank(name, trait));
            } else if (trait.CalculateLambda->IsCallable("CumeDist")) {
                result.push_back(new TChain1MapTraitsCumeDist(name, trait, *partitionRowsColumn));
            } else if (trait.CalculateLambda->IsCallable("NTile")) {
                result.push_back(new TChain1MapTraitsNTile(name, trait, *partitionRowsColumn));
            } else if (trait.CalculateLambda->IsCallable("PercentRank")) {
                result.push_back(new TChain1MapTraitsPercentRank(name, trait, *partitionRowsColumn));
            } else {
                YQL_ENSURE(trait.CalculateLambda->IsCallable("DenseRank"));
                result.push_back(new TChain1MapTraitsDenseRank(name, trait));
            }

            continue;
        }

        if (trait.FrameSettings.GetFrameType() == EFrameType::FrameByRange) {
            result.push_back(new TChain1MapTraitsCurrentOrLagging(name, trait, {}));
            continue;
        }

        YQL_ENSURE(trait.FrameSettings.GetFrameType() == EFrameType::FrameByRows);
        switch(FrameBoundsType(trait.FrameSettings)) {
            case EFrameBoundsType::CURRENT:
            case EFrameBoundsType::LAGGING: {
                TMaybe<ui64> lagQueueIndex;
                auto end = *trait.FrameSettings.GetLastOffset();
                YQL_ENSURE(end <= 0);
                if (end < 0) {
                    YQL_ENSURE(queueParams.LagQueueSize >= ui64(0 - end));
                    lagQueueIndex = queueParams.LagQueueSize + end;
                }

                result.push_back(new TChain1MapTraitsCurrentOrLagging(name, trait, lagQueueIndex));
                break;
            }
            case EFrameBoundsType::LEADING: {
                auto end = *trait.FrameSettings.GetLastOffset();
                YQL_ENSURE(end > 0);
                ui64 lastRowIndex = currentRowIndex + ui64(end);
                result.push_back(new TChain1MapTraitsLeading(name, trait, currentRowIndex, lastRowIndex));
                break;
            }
            case EFrameBoundsType::FULL: {
                result.push_back(new TChain1MapTraitsFull(name, trait, currentRowIndex));
                break;
            }
            case EFrameBoundsType::GENERIC: {
                queueParams.DataQueueNeeded = true;
                auto first = trait.FrameSettings.GetFirstOffset();
                auto last = trait.FrameSettings.GetLastOffset();
                YQL_ENSURE(first.Defined());
                ui64 beginIndex = currentRowIndex + *first;
                ui64 endIndex = last.Defined() ? (currentRowIndex + *last + 1) : Max<ui64>();
                result.push_back(new TChain1MapTraitsGeneric(name, trait, beginIndex, endIndex));
                break;
            }
            case EFrameBoundsType::EMPTY: {
                result.push_back(new TChain1MapTraitsEmpty(name, trait));
                break;
            }
        }
    }

    return result;
}

TExprNode::TPtr ConvertStructOfTuplesToTupleOfStructs(TPositionHandle pos, const TExprNode::TPtr& input, TExprContext& ctx) {
    return ctx.Builder(pos)
        .List()
            .Callable(0, "StaticMap")
                .Add(0, input)
                .Lambda(1)
                    .Param("tuple")
                    .Callable("Nth")
                        .Arg(0, "tuple")
                        .Atom(1, "0")
                    .Seal()
                .Seal()
            .Seal()
            .Callable(1, "StaticMap")
                .Add(0, input)
                .Lambda(1)
                    .Param("tuple")
                    .Callable("Nth")
                        .Arg(0, "tuple")
                        .Atom(1, "1")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr AddInputMembersToOutput(TPositionHandle pos, const TExprNode::TPtr& tupleOfOutputStructAndStateStruct,
    const TExprNode::TPtr& rowArg, TExprContext& ctx)
{
    return ctx.Builder(pos)
        .List()
            .Callable(0, "FlattenMembers")
                .List(0)
                    .Atom(0, "")
                    .Callable(1, "Nth")
                        .Add(0, tupleOfOutputStructAndStateStruct)
                        .Atom(1, "0")
                    .Seal()
                .Seal()
                .List(1)
                    .Atom(0, "")
                    .Add(1, rowArg)
                .Seal()
            .Seal()
            .Callable(1, "Nth")
                .Add(0, tupleOfOutputStructAndStateStruct)
                .Atom(1, "1")
            .Seal()
        .Seal()
        .Build();
}

template<typename T>
TExprNode::TPtr SelectMembers(TPositionHandle pos, const T& members, const TExprNode::TPtr& structNode, TExprContext& ctx) {
    TExprNodeList structItems;
    for (auto& name : members) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, name)
                    .Callable(1, "Member")
                        .Add(0, structNode)
                        .Atom(1, name)
                    .Seal()
                .Seal()
                .Build()
        );
    }
    return ctx.NewCallable(pos, "AsStruct", std::move(structItems));
}

TExprNode::TPtr HandleLaggingItems(TPositionHandle pos, const TExprNode::TPtr& rowArg,
    const TExprNode::TPtr& tupleOfOutputAndState, const TVector<TChain1MapTraits::TPtr>& traits,
    const TExprNode::TPtr& lagQueue, TExprContext& ctx)
{

    TExprNodeList laggingStructItems;
    TSet<TStringBuf> laggingNames;
    TSet<TStringBuf> otherNames;
    for (auto& trait : traits) {
        auto name = trait->GetName();
        auto laggingOutput = trait->ExtractLaggingOutput(lagQueue, rowArg, ctx);
        if (laggingOutput) {
            laggingNames.insert(name);
            laggingStructItems.push_back(
                ctx.Builder(pos)
                    .List()
                        .Atom(0, name)
                        .Add(1, laggingOutput)
                    .Seal()
                    .Build()
            );
        } else {
            otherNames.insert(trait->GetName());
        }
    }

    if (laggingStructItems.empty()) {
        return tupleOfOutputAndState;
    }

    YQL_ENSURE(lagQueue);

    auto output = ctx.NewCallable(pos, "Nth", { tupleOfOutputAndState, ctx.NewAtom(pos, "0")});
    auto state  = ctx.NewCallable(pos, "Nth", { tupleOfOutputAndState, ctx.NewAtom(pos, "1")});

    auto leadingOutput = SelectMembers(pos, laggingNames, output, ctx);
    auto otherOutput = SelectMembers(pos, otherNames, output, ctx);
    auto laggingOutput = ctx.NewCallable(pos, "AsStruct", std::move(laggingStructItems));

    output = ctx.Builder(pos)
        .Callable("FlattenMembers")
            .List(0)
                .Atom(0, "")
                .Add(1, laggingOutput)
            .Seal()
            .List(1)
                .Atom(0, "")
                .Add(1, otherOutput)
            .Seal()
        .Seal()
        .Build();


    return ctx.Builder(pos)
        .List()
            .Add(0, output)
            .Callable(1, "Seq")
                .Add(0, output)
                .Add(1, state)
                .Add(2, leadingOutput)
                .List(3)
                    .Add(0, state)
                    .Callable(1, "QueuePush")
                        .Callable(0, "QueuePop")
                            .Add(0, lagQueue)
                        .Seal()
                        .Add(1, leadingOutput)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildChain1MapInitLambda(TPositionHandle pos, const TVector<TChain1MapTraits::TPtr>& traits,
    const TExprNode::TPtr& dataQueue, ui64 lagQueueSize, const TTypeAnnotationNode* lagQueueItemType, TExprContext& ctx)
{
    auto rowArg = ctx.NewArgument(pos, "row");

    TExprNode::TPtr lagQueue;
    if (lagQueueSize) {
        YQL_ENSURE(lagQueueItemType);
        lagQueue = BuildQueue(pos, *lagQueueItemType, lagQueueSize, lagQueueSize, rowArg, ctx);
    }

    TExprNodeList structItems;
    for (auto& trait : traits) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, trait->GetName())
                    .Apply(1, trait->BuildInitLambda(dataQueue, ctx))
                        .With(0, rowArg)
                    .Seal()
                .Seal()
                .Build()
        );
    }

    auto asStruct = ctx.NewCallable(pos, "AsStruct", std::move(structItems));
    auto tupleOfOutputAndState = ConvertStructOfTuplesToTupleOfStructs(pos, asStruct, ctx);

    tupleOfOutputAndState = HandleLaggingItems(pos, rowArg, tupleOfOutputAndState, traits, lagQueue, ctx);

    auto finalBody = AddInputMembersToOutput(pos, tupleOfOutputAndState, rowArg, ctx);
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg}), std::move(finalBody));
}

TExprNode::TPtr BuildChain1MapUpdateLambda(TPositionHandle pos, const TVector<TChain1MapTraits::TPtr>& traits,
    const TExprNode::TPtr& dataQueue, bool haveLagQueue, TExprContext& ctx)
{
    const auto rowArg = ctx.NewArgument(pos, "row");
    const auto stateArg = ctx.NewArgument(pos, "state");
    auto state = ctx.Builder(pos)
        .Callable("Nth")
            .Add(0, stateArg)
            .Atom(1, "1", TNodeFlags::Default)
        .Seal()
        .Build();

    TExprNode::TPtr lagQueue;
    if (haveLagQueue) {
        lagQueue = ctx.Builder(pos)
            .Callable("Nth")
                .Add(0, state)
                .Atom(1, "1", TNodeFlags::Default)
            .Seal()
            .Build();
        state = ctx.Builder(pos)
            .Callable("Nth")
                .Add(0, std::move(state))
                .Atom(1, "0", TNodeFlags::Default)
            .Seal()
            .Build();
    }

    TExprNodeList structItems;
    for (auto& trait : traits) {
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Atom(0, trait->GetName())
                    .Apply(1, trait->BuildUpdateLambda(dataQueue, ctx))
                        .With(0, rowArg)
                        .With(1)
                            .Callable("Member")
                                .Add(0, state)
                                .Atom(1, trait->GetName())
                            .Seal()
                        .Done()
                    .Seal()
                .Seal()
                .Build()
        );
    }

    auto asStruct = ctx.NewCallable(pos, "AsStruct", std::move(structItems));
    auto tupleOfOutputAndState = ConvertStructOfTuplesToTupleOfStructs(pos, asStruct, ctx);

    tupleOfOutputAndState = HandleLaggingItems(pos, rowArg, tupleOfOutputAndState, traits, lagQueue, ctx);

    auto finalBody = AddInputMembersToOutput(pos, tupleOfOutputAndState, rowArg, ctx);
    return ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg, stateArg}), std::move(finalBody));
}

bool IsNonCompactFullFrame(const TExprNode& winOnRows, TExprContext& ctx) {
    TWindowFrameSettings frameSettings = TWindowFrameSettings::Parse(winOnRows, ctx);
    return frameSettings.GetFrameType() == FrameByRows &&
        !frameSettings.IsCompact() && !frameSettings.GetFirstOffset().Defined() && !frameSettings.GetLastOffset().Defined();
}

TExprNode::TPtr DeduceCompatibleSort(const TExprNode::TPtr& traitsOne, const TExprNode::TPtr& traitsTwo) {
    YQL_ENSURE(traitsOne->IsCallable({"Void", "SortTraits"}));
    YQL_ENSURE(traitsTwo->IsCallable({"Void", "SortTraits"}));

    if (traitsOne->IsCallable("Void")) {
        return traitsTwo;
    }

    if (traitsTwo->IsCallable("Void")) {
        return traitsOne;
    }

    // TODO: need more advanced logic here
    if (traitsOne == traitsTwo) {
        return traitsOne;
    }

    return {};
}

TExprNode::TPtr BuildPartitionsByKeys(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNode::TPtr& keySelector,
    const TExprNode::TPtr& sortOrder, const TExprNode::TPtr& sortKey, const TExprNode::TPtr& streamProcessingLambda,
    const TExprNode::TPtr& sessionKey, const TExprNode::TPtr& sessionInit, const TExprNode::TPtr& sessionUpdate,
    const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNode::TPtr preprocessLambda;
    TExprNode::TPtr chopperKeySelector;
    const TExprNode::TPtr addSessionColumnsArg = ctx.NewArgument(pos, "row");
    TExprNode::TPtr addSessionColumnsBody = addSessionColumnsArg;
    if (sessionUpdate) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        preprocessLambda =
            AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx);

        chopperKeySelector = ctx.Builder(pos)
            .Lambda()
                .Param("item")
                .List()
                    .Apply(0, keySelector)
                        .With(0, "item")
                    .Seal()
                    .Callable(1, "Member")
                        .Arg(0, "item")
                        .Atom(1, SessionStartMemberName)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!sessionKey);
        preprocessLambda = MakeIdentityLambda(pos, ctx);
        chopperKeySelector = keySelector;
    }

    for (auto& column : sessionColumns->ChildrenList()) {
        addSessionColumnsBody = ctx.Builder(pos)
            .Callable("AddMember")
                .Add(0, addSessionColumnsBody)
                .Add(1, column)
                .Callable(2, "Member")
                    .Add(0, addSessionColumnsArg)
                    .Atom(1, SessionParamsMemberName)
                .Seal()
            .Seal()
            .Build();
    }

    addSessionColumnsBody = ctx.Builder(pos)
        .Callable("ForceRemoveMember")
            .Callable(0, "ForceRemoveMember")
                .Add(0, addSessionColumnsBody)
                .Atom(1, SessionStartMemberName)
            .Seal()
            .Atom(1, SessionParamsMemberName)
        .Seal()
        .Build();

    auto addSessionColumnsLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { addSessionColumnsArg }), std::move(addSessionColumnsBody));

    auto groupSwitchLambda = ctx.Builder(pos)
        .Lambda()
            .Param("prevKey")
            .Param("item")
            .Callable("AggrNotEquals")
                .Arg(0, "prevKey")
                .Apply(1, chopperKeySelector)
                    .With(0, "item")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return ctx.Builder(pos)
        .Callable("PartitionsByKeys")
            .Add(0, input)
            .Add(1, keySelector)
            .Add(2, sortOrder)
            .Add(3, sortKey)
            .Lambda(4)
                .Param("partitionedStream")
                .Callable("ForwardList")
                    .Callable(0, "Chopper")
                        .Callable(0, "ToStream")
                            .Apply(0, preprocessLambda)
                                .With(0, "partitionedStream")
                            .Seal()
                        .Seal()
                        .Add(1, chopperKeySelector)
                        .Add(2, groupSwitchLambda)
                        .Lambda(3)
                            .Param("key")
                            .Param("singlePartition")
                            .Callable("Map")
                                .Apply(0, streamProcessingLambda)
                                    .With(0, "singlePartition")
                                .Seal()
                                .Add(1, addSessionColumnsLambda)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

enum EFold1LambdaKind {
    INIT,
    UPDATE,
    CALCULATE,
};

TExprNode::TPtr BuildFold1Lambda(TPositionHandle pos, const TExprNode::TPtr& frames, EFold1LambdaKind kind,
    const TExprNodeList& keyColumns, TExprContext& ctx)
{
    TExprNode::TPtr arg1 = ctx.NewArgument(pos, "arg1");
    TExprNodeList args = { arg1 };

    TExprNode::TPtr arg2;
    if (kind == EFold1LambdaKind::UPDATE) {
        arg2 = ctx.NewArgument(pos, "arg2");
        args.push_back(arg2);
    }

    TExprNodeList structItems;
    for (auto& winOn : frames->ChildrenList()) {
        YQL_ENSURE(IsNonCompactFullFrame(*winOn, ctx));
        for (ui32 i = 1; i < winOn->ChildrenSize(); ++i) {
            YQL_ENSURE(winOn->Child(i)->IsList());
            YQL_ENSURE(winOn->Child(i)->Child(0)->IsAtom());
            YQL_ENSURE(winOn->Child(i)->Child(1)->IsCallable("WindowTraits"));

            auto column = winOn->Child(i)->ChildPtr(0);
            auto traits = winOn->Child(i)->ChildPtr(1);
            auto rowInputType = traits->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

            TExprNode::TPtr applied;

            switch (kind) {
                case EFold1LambdaKind::INIT: {
                    auto lambda = traits->ChildPtr(1);
                    if (lambda->Child(0)->ChildrenSize() == 2) {
                        lambda = ReplaceLastLambdaArgWithUnsignedLiteral(*lambda, i, ctx);
                    }
                    lambda = ReplaceFirstLambdaArgWithCastStruct(*lambda, *rowInputType, ctx);
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 1);
                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0, arg1)
                        .Seal()
                        .Build();
                    break;
                }
                case EFold1LambdaKind::CALCULATE: {
                    auto lambda = traits->ChildPtr(4);
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 1);
                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0)
                                .Callable("Member")
                                    .Add(0, arg1)
                                    .Add(1, column)
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                    break;
                }
                case EFold1LambdaKind::UPDATE: {
                    auto lambda = traits->ChildPtr(2);
                    if (lambda->Child(0)->ChildrenSize() == 3) {
                        lambda = ReplaceLastLambdaArgWithUnsignedLiteral(*lambda, i, ctx);
                    }
                    lambda = ReplaceFirstLambdaArgWithCastStruct(*lambda, *rowInputType, ctx);
                    YQL_ENSURE(lambda->Child(0)->ChildrenSize() == 2);
                    applied = ctx.Builder(pos)
                        .Apply(lambda)
                            .With(0, arg1)
                            .With(1)
                                .Callable("Member")
                                    .Add(0, arg2)
                                    .Add(1, column)
                                .Seal()
                            .Done()
                        .Seal()
                        .Build();
                    break;
                }
            }

            structItems.push_back(ctx.NewList(pos, {column, applied}));
        }
    }

    // pass key columns as-is
    for (auto& keyColumn : keyColumns) {
        YQL_ENSURE(keyColumn->IsAtom());
        structItems.push_back(
            ctx.Builder(pos)
                .List()
                    .Add(0, keyColumn)
                    .Callable(1, "Member")
                        .Add(0, arg1)
                        .Add(1, keyColumn)
                    .Seal()
                .Seal()
                .Build()
        );
    }
    return ctx.NewLambda(pos, ctx.NewArguments(pos, std::move(args)), ctx.NewCallable(pos, "AsStruct", std::move(structItems)));
}

TExprNode::TPtr ExpandNonCompactFullFrames(TPositionHandle pos, const TExprNode::TPtr& inputList,
    const TExprNode::TPtr& originalKeyColumns, const TExprNode::TPtr& sortTraits, const TExprNode::TPtr& frames,
    const TExprNode::TPtr& sessionTraits, const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNode::TPtr sessionKey;
    TExprNode::TPtr sessionInit;
    TExprNode::TPtr sessionUpdate;
    TExprNode::TPtr sessionSortTraits;
    const TTypeAnnotationNode* sessionKeyType = nullptr;
    const TTypeAnnotationNode* sessionParamsType = nullptr;
    ExtractSessionWindowParams(pos, sessionTraits, sessionKey, sessionKeyType, sessionParamsType, sessionSortTraits, sessionInit, sessionUpdate, ctx);

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    TExprNode::TPtr input = inputList;
    if (input->IsCallable("ForwardList")) {
        // full frame strategy uses input 2 times (for grouping and join)
        // TODO: better way to detect "single use input"
        input = ctx.NewCallable(pos, "Collect", { input });
    }

    const auto rowType = inputList->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> rowItems = rowType->GetItems();
    TExprNodeList originalKeysWithSession = originalKeyColumns->ChildrenList();

    TExprNodeList addedColumns;
    const auto commonSortTraits = DeduceCompatibleSort(sortTraits, sessionSortTraits);
    ExtractSortKeyAndOrder(pos, commonSortTraits ? commonSortTraits : sortTraits, sortKey, sortOrder, ctx);
    if (!commonSortTraits) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionUpdate);
        TExprNode::TPtr sessionSortKey;
        TExprNode::TPtr sessionSortOrder;
        ExtractSortKeyAndOrder(pos, sessionSortTraits, sessionSortKey, sessionSortOrder, ctx);
        const auto keySelector = BuildKeySelector(pos, *rowType, originalKeyColumns, ctx);
        input = ctx.Builder(pos)
            .Callable("PartitionsByKeys")
                .Add(0, input)
                .Add(1, keySelector)
                .Add(2, sessionSortOrder)
                .Add(3, sessionSortKey)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx))
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));
        addedColumns.push_back(ctx.NewAtom(pos, SessionParamsMemberName));

        originalKeysWithSession.push_back(ctx.NewAtom(pos, SessionStartMemberName));
        addedColumns.push_back(originalKeysWithSession.back());
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        sessionKey = sessionInit = sessionUpdate = {};
    }

    TExprNodeList keyColumns;

    auto rowArg = ctx.NewArgument(pos, "row");
    auto addMembersBody = rowArg;

    static const TStringBuf keyColumnNamePrefix = "_yql_CalcOverWindowJoinKey";

    const TStructExprType* rowTypeWithSession = ctx.MakeType<TStructExprType>(rowItems);
    for (auto& keyColumn : originalKeysWithSession) {
        YQL_ENSURE(keyColumn->IsAtom());
        auto columnName = keyColumn->Content();
        const TTypeAnnotationNode* columnType =
            rowTypeWithSession->GetItems()[*rowTypeWithSession->FindItem(columnName)]->GetItemType();
        if (columnType->HasOptionalOrNull()) {
            addedColumns.push_back(ctx.NewAtom(pos, TStringBuilder() << keyColumnNamePrefix << addedColumns.size()));
            keyColumns.push_back(addedColumns.back());

            TStringBuf newName = addedColumns.back()->Content();
            const TTypeAnnotationNode* newType = ctx.MakeType<TDataExprType>(EDataSlot::String);
            rowItems.push_back(ctx.MakeType<TItemExprType>(newName, newType));

            addMembersBody = ctx.Builder(pos)
                .Callable("AddMember")
                    .Add(0, addMembersBody)
                    .Atom(1, newName)
                    .Callable(2, "StablePickle")
                        .Callable(0, "Member")
                            .Add(0, rowArg)
                            .Add(1, keyColumn)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        } else {
            keyColumns.push_back(keyColumn);
        }
    }

    input = ctx.Builder(pos)
        .Callable("Map")
            .Add(0, input)
            .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, { rowArg }), std::move(addMembersBody)))
        .Seal()
        .Build();

    auto keySelector = BuildKeySelector(pos, *ctx.MakeType<TStructExprType>(rowItems),
        ctx.NewList(pos, TExprNodeList{keyColumns}), ctx);

    TExprNode::TPtr preprocessLambda;
    TExprNode::TPtr groupKeySelector;
    TExprNode::TPtr condenseSwitch;
    if (sessionUpdate) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionKeyType);
        YQL_ENSURE(commonSortTraits);

        preprocessLambda =
            AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx);
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));

        addedColumns.push_back(ctx.NewAtom(pos, SessionStartMemberName));
        addedColumns.push_back(ctx.NewAtom(pos, SessionParamsMemberName));

        if (sessionKeyType->HasOptionalOrNull()) {
            addedColumns.push_back(ctx.NewAtom(pos, TStringBuilder() << keyColumnNamePrefix << addedColumns.size()));
            preprocessLambda = ctx.Builder(pos)
                .Lambda()
                    .Param("stream")
                    .Callable("OrderedMap")
                        .Apply(0, preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                        .Lambda(1)
                            .Param("item")
                            .Callable("AddMember")
                                .Arg(0, "item")
                                .Add(1, addedColumns.back())
                                .Callable(2, "StablePickle")
                                    .Callable(0, "Member")
                                        .Arg(0, "item")
                                        .Atom(1, SessionStartMemberName)
                                    .Seal()
                                .Seal()
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
                .Build();

            TStringBuf newName = addedColumns.back()->Content();
            const TTypeAnnotationNode* newType = ctx.MakeType<TDataExprType>(EDataSlot::String);
            rowItems.push_back(ctx.MakeType<TItemExprType>(newName, newType));
        }

        keyColumns.push_back(addedColumns.back());

        auto groupKeySelector = BuildKeySelector(pos, *ctx.MakeType<TStructExprType>(rowItems),
            ctx.NewList(pos, TExprNodeList{keyColumns}), ctx);

        condenseSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("AggrNotEquals")
                    .Apply(0, groupKeySelector)
                        .With(0, "row")
                    .Seal()
                    .Apply(1, groupKeySelector)
                        .With(0, "state")
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    } else {
        YQL_ENSURE(!sessionKey);
        preprocessLambda = MakeIdentityLambda(pos, ctx);
        auto groupKeySelector = keySelector;

        condenseSwitch = ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("state")
                .Callable("IsKeySwitch")
                    .Arg(0, "row")
                    .Arg(1, "state")
                    .Add(2, groupKeySelector)
                    .Add(3, groupKeySelector)
                .Seal()
            .Seal()
            .Build();
    }

    auto partitionByKeysLambda = ctx.Builder(pos)
        .Lambda()
            .Param("stream")
            .Callable("Map")
                .Callable(0, "Condense1")
                    .Apply(0, preprocessLambda)
                        .With(0, "stream")
                    .Seal()
                    .Add(1, BuildFold1Lambda(pos, frames, EFold1LambdaKind::INIT, keyColumns, ctx))
                    .Add(2, condenseSwitch)
                    .Add(3, BuildFold1Lambda(pos, frames, EFold1LambdaKind::UPDATE, keyColumns, ctx))
                .Seal()
                .Add(1, BuildFold1Lambda(pos, frames, EFold1LambdaKind::CALCULATE, keyColumns, ctx))
            .Seal()
        .Seal()
        .Build();

    if (HasContextFuncs(*partitionByKeysLambda)) {
        partitionByKeysLambda = ctx.Builder(pos)
            .Lambda()
                .Param("stream")
                .Callable("WithContext")
                    .Apply(0, partitionByKeysLambda)
                        .With(0, "stream")
                    .Seal()
                    .Atom(1, "WinAgg", TNodeFlags::Default)
                .Seal()
            .Seal()
            .Build();
    }

    auto aggregated = ctx.Builder(pos)
        .Callable("PartitionsByKeys")
            .Add(0, input)
            .Add(1, keySelector)
            .Add(2, sortOrder)
            .Add(3, sortKey)
            .Add(4, partitionByKeysLambda)
        .Seal().Build();

    if (sessionUpdate) {
        // preprocess input without aggregation
        input = ctx.Builder(pos)
                .Callable("PartitionsByKeys")
                    .Add(0, input)
                    .Add(1, ctx.DeepCopyLambda(*keySelector))
                    .Add(2, sortOrder)
                    .Add(3, ctx.DeepCopyLambda(*sortKey))
                    .Lambda(4)
                        .Param("stream")
                        .Apply(preprocessLambda)
                            .With(0, "stream")
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
    }

    TExprNode::TPtr joined;
    if (!keyColumns.empty()) {
        // SELECT * FROM input AS a JOIN aggregated AS b USING(keyColumns)
        auto buildJoinKeysTuple = [&](TStringBuf side) {
            TExprNodeList items;
            for (const auto& keyColumn : keyColumns) {
                items.push_back(ctx.NewAtom(pos, side));
                items.push_back(keyColumn);
            }
            return ctx.NewList(pos, std::move(items));
        };

        joined = ctx.Builder(pos)
            .Callable("EquiJoin")
                .List(0)
                    .Add(0, input)
                    .Atom(1, "a", TNodeFlags::Default)
                .Seal()
                .List(1)
                    .Add(0, aggregated)
                    .Atom(1, "b", TNodeFlags::Default)
                .Seal()
                .List(2)
                    .Atom(0, "Inner", TNodeFlags::Default)
                    .Atom(1, "a", TNodeFlags::Default)
                    .Atom(2, "b", TNodeFlags::Default)
                    .Add(3, buildJoinKeysTuple("a"))
                    .Add(4, buildJoinKeysTuple("b"))
                    .List(5)
                        .List(0)
                            .Atom(0, "right", TNodeFlags::Default)
                            .Atom(1, "any", TNodeFlags::Default)
                        .Seal()
                    .Seal()
                .Seal()
                .List(3).Seal()
            .Seal()
            .Build();

        // remove b.keys*
        auto rowArg = ctx.NewArgument(pos, "row");

        TExprNode::TPtr removed = rowArg;

        auto removeSide = [&](const TString& side, const TExprNodeList& keys) {
            for (const auto& keyColumn : keys) {
                YQL_ENSURE(keyColumn->IsAtom());
                TString toRemove = side + keyColumn->Content();
                removed = ctx.Builder(pos)
                    .Callable("RemoveMember")
                        .Add(0, removed)
                        .Atom(1, toRemove)
                    .Seal()
                    .Build();
            }
        };

        removeSide("b.", keyColumns);

        // add session columns
        for (auto column : sessionColumns->ChildrenList()) {
            removed = ctx.Builder(pos)
                .Callable("AddMember")
                    .Add(0, removed)
                    .Add(1, column)
                    .Callable(2, "Member")
                        .Add(0, rowArg)
                        .Atom(1, TString("a.") + SessionParamsMemberName)
                    .Seal()
                .Seal()
                .Build();
        }

        removeSide("a.", addedColumns);

        joined = ctx.Builder(pos)
            .Callable("Map")
                .Add(0, joined)
                .Add(1, ctx.NewLambda(pos, ctx.NewArguments(pos, {rowArg}), std::move(removed)))
            .Seal()
            .Build();
    } else {
        // SELECT * FROM input AS a CROSS JOIN aggregated AS b
        joined = ctx.Builder(pos)
            .Callable("EquiJoin")
                .List(0)
                    .Add(0, input)
                    .Atom(1, "a", TNodeFlags::Default)
                .Seal()
                .List(1)
                    .Add(0, aggregated)
                    .Atom(1, "b", TNodeFlags::Default)
                .Seal()
                .List(2)
                    .Atom(0, "Cross", TNodeFlags::Default)
                    .Atom(1, "a", TNodeFlags::Default)
                    .Atom(2, "b", TNodeFlags::Default)
                    .List(3).Seal()
                    .List(4).Seal()
                    .List(5).Seal()
                .Seal()
                .List(3).Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, joined)
            .Lambda(1)
                .Param("row")
                .Callable("DivePrefixMembers")
                    .Arg(0, "row")
                    .List(1)
                        .Atom(0, "a.")
                        .Atom(1, "b.")
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr TryExpandNonCompactFullFrames(TPositionHandle pos, const TExprNode::TPtr& inputList, const TExprNode::TPtr& keyColumns,
    const TExprNode::TPtr& sortTraits, const TExprNode::TPtr& frames, const TExprNode::TPtr& sessionTraits,
    const TExprNode::TPtr& sessionColumns, TExprContext& ctx)
{
    TExprNodeList nonCompactAggregatingFullFrames;
    TExprNodeList otherFrames;

    for (auto& winOn : frames->ChildrenList()) {
        if (!IsNonCompactFullFrame(*winOn, ctx)) {
            otherFrames.push_back(winOn);
            continue;
        }

        YQL_ENSURE(TCoWinOnBase::Match(winOn.Get()));

        TExprNodeList nonAggregates = { winOn->ChildPtr(0) };
        TExprNodeList aggregates = { winOn->ChildPtr(0) };

        for (ui32 i = 1; i < winOn->ChildrenSize(); ++i) {
            auto item = winOn->Child(i)->Child(1);
            if (item->IsCallable("WindowTraits")) {
                aggregates.push_back(winOn->ChildPtr(i));
            } else {
                nonAggregates.push_back(winOn->ChildPtr(i));
            }
        }

        if (aggregates.size() == 1) {
            otherFrames.push_back(winOn);
            continue;
        }

        nonCompactAggregatingFullFrames.push_back(ctx.ChangeChildren(*winOn, std::move(aggregates)));
        if (nonAggregates.size() > 1) {
            otherFrames.push_back(ctx.ChangeChildren(*winOn, std::move(nonAggregates)));
        }
    }

    if (nonCompactAggregatingFullFrames.empty()) {
        return {};
    }

    auto fullFrames = ctx.NewList(pos, std::move(nonCompactAggregatingFullFrames));
    auto nonFullFrames = ctx.NewList(pos, std::move(otherFrames));
    auto expanded = ExpandNonCompactFullFrames(pos, inputList, keyColumns, sortTraits, fullFrames, sessionTraits, sessionColumns, ctx);

    if (sessionTraits && !sessionTraits->IsCallable("Void")) {
        return Build<TCoCalcOverSessionWindow>(ctx, pos)
            .Input(expanded)
            .Keys(keyColumns)
            .SortSpec(sortTraits)
            .Frames(nonFullFrames)
            .SessionSpec(sessionTraits)
            .SessionColumns(sessionColumns)
            .Done().Ptr();
    }
    YQL_ENSURE(sessionColumns->ChildrenSize() == 0);
    return Build<TCoCalcOverWindow>(ctx, pos)
        .Input(expanded)
        .Keys(keyColumns)
        .SortSpec(sortTraits)
        .Frames(nonFullFrames)
        .Done().Ptr();
}

void SplitFramesByType(const TExprNode::TPtr& frames, TExprNode::TPtr& rowFrames, TExprNode::TPtr& rangeFrames, TExprNode::TPtr& groupFrames, TExprContext& ctx) {
    TExprNodeList rows;
    TExprNodeList range;
    TExprNodeList groups;
    for (auto& winOn : frames->ChildrenList()) {
        if (TCoWinOnRows::Match(winOn.Get())) {
            rows.push_back(std::move(winOn));
        } else if (TCoWinOnRange::Match(winOn.Get())) {
            range.push_back(std::move(winOn));
        } else {
            YQL_ENSURE(TCoWinOnGroups::Match(winOn.Get()));
            groups.push_back(std::move(winOn));
        }
    }

    rowFrames = ctx.NewList(frames->Pos(), std::move(rows));
    rangeFrames = ctx.NewList(frames->Pos(), std::move(range));
    groupFrames = ctx.NewList(frames->Pos(), std::move(groups));
}

const TStructExprType* ApplyFramesToType(const TStructExprType& inputType, const TStructExprType& finalOutputType, const TExprNode& frames, TExprContext& ctx) {
    TVector<const TItemExprType*> resultItems = inputType.GetItems();
    for (auto& frame : frames.ChildrenList()) {
        YQL_ENSURE(TCoWinOnBase::Match(frame.Get()));
        for (size_t i = 1; i < frame->ChildrenSize(); ++i) {
            YQL_ENSURE(frame->Child(i)->IsList());
            YQL_ENSURE(frame->Child(i)->Head().IsAtom());
            TStringBuf column = frame->Child(i)->Head().Content();

            const TTypeAnnotationNode* type = finalOutputType.FindItemType(column);
            YQL_ENSURE(type);

            resultItems.push_back(ctx.MakeType<TItemExprType>(column, type));
        }
    }

    return ctx.MakeType<TStructExprType>(resultItems);
}

bool NeedPartitionRows(const TExprNode::TPtr& frames, TExprContext& ctx) {
    if (frames->ChildrenSize() == 0) {
        return false;
    }

    TCalcOverWindowTraits traits = ExtractCalcOverWindowTraits(frames, ctx);
    for (const auto& item : traits.RawTraits) {
        const TRawTrait& trait = item.second;
        if (trait.CalculateLambda->IsCallable({"CumeDist","NTile","PercentRank"})) {
            return true;
        }
    }
    
    return false;
}

TString AllocatePartitionRowsColumn(const TStructExprType& rowType) {
    ui64 index = 0;
    for (;;) {
        auto name = "_yql_partition_rows_" + ToString(index);
        if (!rowType.FindItemType(name)) {
            return name;
        }

        ++index;
    }
}

TExprNode::TPtr AddPartitionRowsColumn(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNode::TPtr& keyColumns, 
    const TString& columnName, TExprContext& ctx, TTypeAnnotationContext& types) {
    auto exportsPtr = types.Modules->GetModule("/lib/yql/window.yql");
    YQL_ENSURE(exportsPtr);
    const auto& exports = exportsPtr->Symbols();
    const auto ex = exports.find("count_traits_factory");
    YQL_ENSURE(exports.cend() != ex);
    TNodeOnNodeOwnedMap deepClones;    
    auto lambda = ctx.DeepCopy(*ex->second, exportsPtr->ExprCtx(), deepClones, true, false);
    auto listTypeNode = ctx.NewCallable(pos, "TypeOf", {input});
    auto extractor = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Callable("Void")
            .Seal()
        .Seal()
        .Build();

    auto traits = ctx.ReplaceNodes(lambda->TailPtr(), {
        {lambda->Head().Child(0), listTypeNode},
        {lambda->Head().Child(1), extractor}
    });

    ctx.Step.Repeat(TExprStep::ExpandApplyForLambdas);
    auto status = ExpandApply(traits, traits, ctx);
    YQL_ENSURE(status != IGraphTransformer::TStatus::Error);

    return ctx.Builder(pos)
        .Callable("CalcOverWindow")
            .Add(0, input)
            .Add(1, keyColumns)
            .Callable(2, "Void")
            .Seal()
            .List(3)
                .Callable(0, "WinOnRows")
                    .List(0)
                        .List(0)
                            .Atom(0, "begin")
                            .Callable(1, "Void")
                            .Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, "end")
                            .Callable(1, "Void")
                            .Seal()
                        .Seal()
                    .Seal()
                    .List(1)
                        .Atom(0, columnName)
                        .Add(1, traits)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr RemovePartitionRowsColumn(TPositionHandle pos, const TExprNode::TPtr& input, const TString& columnName, TExprContext& ctx) {
    return ctx.Builder(pos)
        .Callable("Map")
            .Add(0, input)
            .Lambda(1)
                .Param("row")
                .Callable("RemoveMember")
                    .Arg(0, "row")
                    .Atom(1, columnName)
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr ProcessRowsFrames(TPositionHandle pos, const TExprNode::TPtr& input, const TStructExprType& rowType, const TExprNode::TPtr& dependsOn,
                                  const TExprNode::TPtr& frames, const TMaybe<TString>& partitionRowsColumn, TExprContext& ctx)
{
    if (frames->ChildrenSize() == 0) {
        return input;
    }
    TExprNode::TPtr processed = input;
    TExprNode::TPtr dataQueue;
    TQueueParams queueParams;
    TVector<TChain1MapTraits::TPtr> traits = BuildFoldMapTraits(queueParams, frames, partitionRowsColumn, ctx);
    if (queueParams.DataQueueNeeded) {
        ui64 queueSize = (queueParams.DataOutpace == Max<ui64>()) ? Max<ui64>() : (queueParams.DataOutpace + queueParams.DataLag + 2);
        dataQueue = BuildQueue(pos, rowType, queueSize, queueParams.DataLag, dependsOn, ctx);
        processed = ctx.Builder(pos)
            .Callable("PreserveStream")
                .Add(0, processed)
                .Add(1, dataQueue)
                .Add(2, BuildUint64(pos, queueParams.DataOutpace, ctx))
            .Seal()
            .Build();
    }

    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Callable(0, "Chain1Map")
                .Add(0, std::move(processed))
                .Add(1, BuildChain1MapInitLambda(pos, traits, dataQueue, queueParams.LagQueueSize, queueParams.LagQueueItemType, ctx))
                .Add(2, BuildChain1MapUpdateLambda(pos, traits, dataQueue, queueParams.LagQueueSize != 0, ctx))
            .Seal()
            .Lambda(1)
                .Param("pair")
                .Callable("Nth")
                    .Arg(0, "pair")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return WrapWithWinContext(processed, ctx);
}

TExprNode::TPtr ProcessRangeFrames(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNode::TPtr& sortKey, const TExprNode::TPtr& frames,
    const TMaybe<TString>& partitionRowsColumn, TExprContext& ctx) {
    if (frames->ChildrenSize() == 0) {
        return input;
    }

    TExprNode::TPtr processed = input;
    TQueueParams queueParams;
    TVector<TChain1MapTraits::TPtr> traits = BuildFoldMapTraits(queueParams, frames, partitionRowsColumn, ctx);
    YQL_ENSURE(!queueParams.DataQueueNeeded);
    YQL_ENSURE(queueParams.LagQueueSize == 0);
    YQL_ENSURE(queueParams.LagQueueItemType == nullptr);

    // same processing as in WinOnRows
    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Callable(0, "Chain1Map")
                .Add(0, std::move(processed))
                .Add(1, BuildChain1MapInitLambda(pos, traits, nullptr, 0, nullptr, ctx))
                .Add(2, BuildChain1MapUpdateLambda(pos, traits, nullptr, false, ctx))
            .Seal()
            .Lambda(1)
                .Param("pair")
                .Callable("Nth")
                    .Arg(0, "pair")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();
    processed = WrapWithWinContext(processed, ctx);

    TExprNode::TPtr sortKeyLambda = sortKey;
    if (sortKey->IsCallable("Void")) {
        sortKeyLambda = ctx.Builder(sortKey->Pos())
            .Lambda()
                .Param("row")
                .Callable("Void")
                .Seal()
            .Seal()
            .Build();
    }

    auto processedItemType = ctx.Builder(pos)
        .Callable("StreamItemType")
            .Callable(0, "TypeOf")
                .Add(0, processed)
            .Seal()
        .Seal()
        .Build();

    auto variantType = ctx.Builder(pos)
        .Callable("VariantType")
            .Callable(0, "StructType")
                .List(0)
                    .Atom(0, "singleRow", TNodeFlags::Default)
                    .Add(1, processedItemType)
                .Seal()
                .List(1)
                    .Atom(0, "group", TNodeFlags::Default)
                    .Callable(1, "ListType")
                        .Add(0, processedItemType)
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    // split rows by groups with equal sortKey
    processed = ctx.Builder(pos)
        .Callable("Condense1")
            .Add(0, processed)
            .Lambda(1)
                .Param("row")
                .List()
                    .Apply(0, sortKeyLambda)
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Variant")
                        .Arg(0, "row")
                        .Atom(1, "singleRow", TNodeFlags::Default)
                        .Add(2, variantType)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(2)
                .Param("row")
                .Param("state")
                .Callable(0, "AggrNotEquals")
                    .Apply(0, sortKeyLambda)
                        .With(0, "row")
                    .Seal()
                    .Callable(1, "Nth")
                        .Arg(0, "state")
                        .Atom(1, "0", TNodeFlags::Default)
                    .Seal()
                .Seal()
            .Seal()
            .Lambda(3)
                .Param("row")
                .Param("state")
                .List()
                    .Callable(0, "Nth")
                        .Arg(0, "state")
                        .Atom(1, "0", TNodeFlags::Default)
                    .Seal()
                    .Callable(1, "Visit")
                        .Callable(0, "Nth")
                            .Arg(0, "state")
                            .Atom(1, "1", TNodeFlags::Default)
                        .Seal()
                        .Atom(1, "singleRow", TNodeFlags::Default)
                        .Lambda(2)
                            .Param("singleRow")
                            .Callable(0, "Variant")
                                .Callable(0, "AsList")
                                    .Arg(0, "singleRow")
                                    .Arg(1, "row")
                                .Seal()
                                .Atom(1, "group", TNodeFlags::Default)
                                .Add(2, variantType)
                            .Seal()
                        .Seal()
                        .Atom(3, "group", TNodeFlags::Default)
                        .Lambda(4)
                            .Param("group")
                            .Callable(0, "Variant")
                                .Callable(0, "Insert")
                                    .Arg(0, "group")
                                    .Arg(1, "row")
                                .Seal()
                                .Atom(1, "group", TNodeFlags::Default)
                                .Add(2, variantType)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    processed = ctx.Builder(pos)
        .Callable("OrderedMap")
            .Add(0, processed)
            .Lambda(1)
                .Param("item")
                .Callable(0, "Nth")
                    .Arg(0, "item")
                    .Atom(1, "1", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto lastRowArg = ctx.NewArgument(pos, "lastRow");
    auto currentRowArg = ctx.NewArgument(pos, "currentRow");
    auto currentRow = currentRowArg;

    for (auto& trait : traits) {
        TStringBuf name = trait->GetName();
        currentRow = ctx.Builder(pos)
            .Callable("AddMember")
                .Callable(0, "RemoveMember")
                    .Add(0, currentRow)
                    .Atom(1, name)
                .Seal()
                .Atom(1, name)
                .Callable(2, "Member")
                    .Add(0, lastRowArg)
                    .Atom(1, name)
                .Seal()
            .Seal()
            .Build();
    }

    auto overwriteWithLastRowLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, { currentRowArg, lastRowArg }), std::move(currentRow));

    // processed is currently stream of groups (=Variant<row, List<row>>>) with equal sort keys
    processed = ctx.Builder(pos)
        .Callable("OrderedFlatMap")
            .Add(0, processed)
            .Lambda(1)
                .Param("item")
                .Callable("Visit")
                    .Arg(0, "item")
                    .Atom(1, "singleRow", TNodeFlags::Default)
                    .Lambda(2)
                        .Param("singleRow")
                        .Callable(0, "AsList")
                            .Arg(0, "singleRow")
                        .Seal()
                    .Seal()
                    .Atom(3, "group", TNodeFlags::Default)
                    .Lambda(4)
                        .Param("group")
                        .Callable("Coalesce")
                            .Callable(0, "Map")
                                .Callable(0, "Last")
                                    .Arg(0, "group")
                                .Seal()
                                .Lambda(1)
                                    .Param("lastRow")
                                    .Callable("OrderedMap")
                                        .Arg(0, "group")
                                        .Lambda(1)
                                            .Param("currentRow")
                                            .Apply(overwriteWithLastRowLambda)
                                                .With(0, "currentRow")
                                                .With(1, "lastRow")
                                            .Seal()
                                        .Seal()
                                    .Seal()
                                .Seal()
                            .Seal()
                            .Callable(1, "EmptyList")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    return processed;
}

TExprNode::TPtr ExpandSingleCalcOverWindow(TPositionHandle pos, const TExprNode::TPtr& inputList, const TExprNode::TPtr& keyColumns,
    const TExprNode::TPtr& sortTraits, const TExprNode::TPtr& frames, const TExprNode::TPtr& sessionTraits,
    const TExprNode::TPtr& sessionColumns, const TStructExprType& outputRowType, TExprContext& ctx, TTypeAnnotationContext& types)
{
    if (auto expanded = TryExpandNonCompactFullFrames(pos, inputList, keyColumns, sortTraits, frames, sessionTraits, sessionColumns, ctx)) {
        YQL_CLOG(INFO, Core) << "Expanded non-compact CalcOverWindow";
        return expanded;
    }

    TExprNode::TPtr sessionKey;
    TExprNode::TPtr sessionSortTraits;
    const TTypeAnnotationNode* sessionKeyType = nullptr;
    const TTypeAnnotationNode* sessionParamsType = nullptr;
    TExprNode::TPtr sessionInit;
    TExprNode::TPtr sessionUpdate;
    ExtractSessionWindowParams(pos, sessionTraits, sessionKey, sessionKeyType, sessionParamsType, sessionSortTraits, sessionInit, sessionUpdate, ctx);

    const auto originalRowType = inputList->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> rowItems = originalRowType->GetItems();
    if (sessionKeyType) {
        YQL_ENSURE(sessionParamsType);
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionStartMemberName, sessionKeyType));
        rowItems.push_back(ctx.MakeType<TItemExprType>(SessionParamsMemberName, sessionParamsType));
    }
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);

    auto keySelector = BuildKeySelector(pos, *rowType->Cast<TStructExprType>(), keyColumns, ctx);

    TExprNode::TPtr sortKey;
    TExprNode::TPtr sortOrder;
    ExtractSortKeyAndOrder(pos, sortTraits, sortKey, sortOrder, ctx);
    const TExprNode::TPtr originalSortKey = sortKey;
    TExprNode::TPtr input = inputList;

    const auto commonSortTraits = DeduceCompatibleSort(sortTraits, sessionSortTraits);
    ExtractSortKeyAndOrder(pos, commonSortTraits ? commonSortTraits : sortTraits, sortKey, sortOrder, ctx);
    auto fullKeyColumns = keyColumns;
    if (!commonSortTraits) {
        YQL_ENSURE(sessionKey);
        YQL_ENSURE(sessionInit);
        YQL_ENSURE(sessionUpdate);
        TExprNode::TPtr sessionSortKey;
        TExprNode::TPtr sessionSortOrder;
        ExtractSortKeyAndOrder(pos, sessionSortTraits, sessionSortKey, sessionSortOrder, ctx);
        input = ctx.Builder(pos)
            .Callable("PartitionsByKeys")
                .Add(0, input)
                .Add(1, keySelector)
                .Add(2, sessionSortOrder)
                .Add(3, sessionSortKey)
                .Lambda(4)
                    .Param("partitionedStream")
                    .Apply(AddSessionParamsMemberLambda(pos, SessionStartMemberName, SessionParamsMemberName, keySelector, sessionKey, sessionInit, sessionUpdate, ctx))
                        .With(0, "partitionedStream")
                    .Seal()
                .Seal()
            .Seal()
            .Build();

        TExprNodeList keyColumnsList = keyColumns->ChildrenList();
        keyColumnsList.push_back(ctx.NewAtom(pos, SessionStartMemberName));

        auto keyColumnsWithSessionStart = ctx.NewList(pos, std::move(keyColumnsList));
        fullKeyColumns = keyColumnsWithSessionStart;

        keySelector = BuildKeySelector(pos, *rowType, keyColumnsWithSessionStart, ctx);
        sessionKey = sessionInit = sessionUpdate = {};
    }

    TExprNode::TPtr rowsFrames;
    TExprNode::TPtr rangeFrames;
    TExprNode::TPtr groupsFrames;
    SplitFramesByType(frames, rowsFrames, rangeFrames, groupsFrames, ctx);
    YQL_ENSURE(groupsFrames->ChildrenSize() == 0);

    auto topLevelStreamArg = ctx.NewArgument(pos, "stream");
    TExprNode::TPtr processed = topLevelStreamArg;

    TMaybe<TString> partitionRowsColumn;
    if (NeedPartitionRows(frames, ctx)) {
        partitionRowsColumn = AllocatePartitionRowsColumn(outputRowType);
        input = AddPartitionRowsColumn(pos, input, fullKeyColumns, *partitionRowsColumn, ctx, types);
    }

    // All RANGE frames (even simplest RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    // will require additional memory to store TableRow()'s - so we want to start with minimum size of row
    // (i.e. process range frames first)
    processed = ProcessRangeFrames(pos, processed, originalSortKey, rangeFrames, partitionRowsColumn, ctx);
    rowType = ApplyFramesToType(*rowType, outputRowType, *rangeFrames, ctx);
    processed = ProcessRowsFrames(pos, processed, *rowType, topLevelStreamArg, rowsFrames, partitionRowsColumn, ctx);

    auto topLevelStreamProcessingLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {topLevelStreamArg}), std::move(processed));

    YQL_CLOG(INFO, Core) << "Expanded compact CalcOverWindow";
    auto res = BuildPartitionsByKeys(pos, input, keySelector, sortOrder, sortKey, topLevelStreamProcessingLambda, sessionKey,
        sessionInit, sessionUpdate, sessionColumns, ctx);
    if (partitionRowsColumn) {
        res = RemovePartitionRowsColumn(pos, res, *partitionRowsColumn, ctx);
    }

    return res;
}

} // namespace

TExprNode::TPtr ExpandCalcOverWindow(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& types) {
    YQL_ENSURE(node->IsCallable({"CalcOverWindow", "CalcOverSessionWindow", "CalcOverWindowGroup"}));

    auto input = node->ChildPtr(0);
    auto calcs = ExtractCalcsOverWindow(node, ctx);
    if (calcs.empty()) {
        return input;
    }

    TCoCalcOverWindowTuple calc(calcs.front());
    if (calc.Frames().Size() != 0 || calc.SessionColumns().Size() != 0) {
        const TStructExprType& outputRowType = *node->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        input = ExpandSingleCalcOverWindow(node->Pos(), input, calc.Keys().Ptr(), calc.SortSpec().Ptr(), calc.Frames().Ptr(),
            calc.SessionSpec().Ptr(), calc.SessionColumns().Ptr(), outputRowType, ctx, types);
    }

    calcs.erase(calcs.begin());
    return RebuildCalcOverWindowGroup(node->Pos(), input, calcs, ctx);
}

TExprNodeList ExtractCalcsOverWindow(const TExprNodePtr& node, TExprContext& ctx) {
    TExprNodeList result;
    if (auto maybeBase = TMaybeNode<TCoCalcOverWindowBase>(node)) {
        TCoCalcOverWindowBase self(maybeBase.Cast());
        TExprNode::TPtr sessionSpec;
        TExprNode::TPtr sessionColumns;
        if (auto session = TMaybeNode<TCoCalcOverSessionWindow>(node)) {
            sessionSpec = session.Cast().SessionSpec().Ptr();
            sessionColumns = session.Cast().SessionColumns().Ptr();
        } else {
            sessionSpec = ctx.NewCallable(node->Pos(), "Void", {});
            sessionColumns = ctx.NewList(node->Pos(), {});
        }
        result.emplace_back(
            Build<TCoCalcOverWindowTuple>(ctx, node->Pos())
                .Keys(self.Keys())
                .SortSpec(self.SortSpec())
                .Frames(self.Frames())
                .SessionSpec(sessionSpec)
                .SessionColumns(sessionColumns)
                .Done().Ptr()
        );
    } else {
        result = TMaybeNode<TCoCalcOverWindowGroup>(node).Cast().Calcs().Ref().ChildrenList();
    }
    return result;
}

TExprNode::TPtr RebuildCalcOverWindowGroup(TPositionHandle pos, const TExprNode::TPtr& input, const TExprNodeList& calcs, TExprContext& ctx) {
    auto inputType = ctx.Builder(input->Pos())
        .Callable("TypeOf")
            .Add(0, input)
        .Seal()
        .Build();

    auto inputItemType = ctx.Builder(input->Pos())
        .Callable("ListItemType")
            .Add(0, inputType)
        .Seal()
        .Build();

    TExprNodeList fixedCalcs;
    for (auto calcNode : calcs) {
        TCoCalcOverWindowTuple calc(calcNode);
        auto sortSpec = calc.SortSpec().Ptr();
        if (sortSpec->IsCallable("SortTraits")) {
            sortSpec = ctx.Builder(sortSpec->Pos())
                .Callable("SortTraits")
                    .Add(0, inputType)
                    .Add(1, sortSpec->ChildPtr(1))
                    .Add(2, ctx.DeepCopyLambda(*sortSpec->Child(2)))
                .Seal()
                .Build();
        } else {
            YQL_ENSURE(sortSpec->IsCallable("Void"));
        }

        auto sessionSpec = calc.SessionSpec().Ptr();
        if (sessionSpec->IsCallable("SessionWindowTraits")) {
            TCoSessionWindowTraits traits(sessionSpec);
            auto sessionSortSpec = traits.SortSpec().Ptr();
            if (auto maybeSort = TMaybeNode<TCoSortTraits>(sessionSortSpec)) {
                sessionSortSpec = Build<TCoSortTraits>(ctx, sessionSortSpec->Pos())
                    .ListType(inputType)
                    .SortDirections(maybeSort.Cast().SortDirections())
                    .SortKeySelectorLambda(ctx.DeepCopyLambda(maybeSort.Cast().SortKeySelectorLambda().Ref()))
                    .Done().Ptr();
            } else {
                YQL_ENSURE(sessionSortSpec->IsCallable("Void"));
            }

            sessionSpec = Build<TCoSessionWindowTraits>(ctx, traits.Pos())
                .ListType(inputType)
                .SortSpec(sessionSortSpec)
                .InitState(ctx.DeepCopyLambda(traits.InitState().Ref()))
                .UpdateState(ctx.DeepCopyLambda(traits.UpdateState().Ref()))
                .Calculate(ctx.DeepCopyLambda(traits.Calculate().Ref()))
                .Done().Ptr();
        } else {
            YQL_ENSURE(sessionSpec->IsCallable("Void"));
        }

        auto sessionColumns = calc.SessionColumns().Ptr();

        TExprNodeList newFrames;
        for (auto frameNode : calc.Frames().Ref().Children()) {
            YQL_ENSURE(TCoWinOnBase::Match(frameNode.Get()));
            TExprNodeList winOnArgs = { frameNode->ChildPtr(0) };
            for (ui32 i = 1; i < frameNode->ChildrenSize(); ++i) {
                auto kvTuple = frameNode->ChildPtr(i);
                YQL_ENSURE(kvTuple->IsList());
                YQL_ENSURE(kvTuple->ChildrenSize() == 2);

                auto columnName = kvTuple->ChildPtr(0);

                auto traits = kvTuple->ChildPtr(1);
                YQL_ENSURE(traits->IsCallable({"Lag", "Lead", "RowNumber", "Rank", "DenseRank", "WindowTraits"}));
                if (traits->IsCallable("WindowTraits")) {
                    YQL_ENSURE(traits->Head().GetTypeAnn());
                    const TTypeAnnotationNode& oldItemType = *traits->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                    traits = ctx.Builder(traits->Pos())
                        .Callable(traits->Content())
                            .Add(0, inputItemType)
                            .Add(1, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), oldItemType, ctx)))
                            .Add(2, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(2), oldItemType, ctx)))
                            .Add(3, ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(3), oldItemType, ctx)))
                            .Add(4, ctx.DeepCopyLambda(*traits->Child(4)))
                            .Add(5, traits->Child(5)->IsLambda() ? ctx.DeepCopyLambda(*traits->Child(5)) : traits->ChildPtr(5))
                        .Seal()
                        .Build();
                } else {
                    TExprNodeList args;
                    args.push_back(inputType);
                    if (traits->ChildrenSize() > 1) {
                        YQL_ENSURE(traits->Head().GetTypeAnn());
                        const TTypeAnnotationNode& oldItemType = *traits->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType()
                            ->Cast<TListExprType>()->GetItemType();
                        args.push_back(ctx.DeepCopyLambda(*ReplaceFirstLambdaArgWithCastStruct(*traits->Child(1), oldItemType, ctx)));
                    }
                    if (traits->ChildrenSize() > 2) {
                        args.push_back(traits->ChildPtr(2));
                    }
                    traits = ctx.NewCallable(traits->Pos(), traits->Content(), std::move(args));
                }

                winOnArgs.push_back(ctx.NewList(kvTuple->Pos(), {columnName, traits}));
            }
            newFrames.push_back(ctx.ChangeChildren(*frameNode, std::move(winOnArgs)));
        }

        fixedCalcs.push_back(
            Build<TCoCalcOverWindowTuple>(ctx, calc.Pos())
                .Keys(calc.Keys())
                .SortSpec(sortSpec)
                .Frames(ctx.NewList(calc.Frames().Pos(), std::move(newFrames)))
                .SessionSpec(sessionSpec)
                .SessionColumns(sessionColumns)
                .Done().Ptr()
        );
    }

    return Build<TCoCalcOverWindowGroup>(ctx, pos)
        .Input(input)
        .Calcs(ctx.NewList(pos, std::move(fixedCalcs)))
        .Done().Ptr();
}

bool IsUnbounded(const NNodes::TCoFrameBound& bound) {
    if (bound.Ref().ChildrenSize() < 2) {
        return false;
    }
    if (auto maybeAtom = bound.Bound().Maybe<TCoAtom>()) {
        return maybeAtom.Cast().Value() == "unbounded";
    }
    return false;
}

bool IsCurrentRow(const NNodes::TCoFrameBound& bound) {
    return bound.Setting().Value() == "currentRow";
}

TWindowFrameSettings TWindowFrameSettings::Parse(const TExprNode& node, TExprContext& ctx) {
    auto maybeSettings = TryParse(node, ctx);
    YQL_ENSURE(maybeSettings);
    return *maybeSettings;
}

TMaybe<TWindowFrameSettings> TWindowFrameSettings::TryParse(const TExprNode& node, TExprContext& ctx) {
    TWindowFrameSettings settings;

    if (node.IsCallable("WinOnRows")) {
        settings.Type = EFrameType::FrameByRows;
    } else if (node.IsCallable("WinOnRange")) {
        settings.Type = EFrameType::FrameByRange;
    } else {
        YQL_ENSURE(node.IsCallable("WinOnGroups"));
        settings.Type = EFrameType::FrameByGroups;
    }
    auto frameSpec = node.Child(0);
    if (frameSpec->Type() == TExprNode::List) {
        bool hasBegin = false;
        bool hasEnd = false;

        for (const auto& setting : frameSpec->Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return {};
            }

            if (!EnsureAtom(setting->Head(), ctx)) {
                return {};
            }

            const auto settingName = setting->Head().Content();
            if (settingName != "begin" && settingName != "end" && settingName != "compact") {
                ctx.AddError(
                    TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Invalid frame bound '" << settingName << "'"));
                return {};
            }

            if (settingName == "compact") {
                settings.Compact = true;
                continue;
            }

            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return {};
            }

            bool& hasBound = (settingName == "begin") ? hasBegin : hasEnd;
            if (hasBound) {
                ctx.AddError(
                    TIssue(ctx.GetPosition(setting->Pos()), TStringBuilder() << "Duplicate " << settingName << " frame bound detected"));
                return {};
            }

            hasBound = true;
            TMaybe<i32>& boundOffset = (settingName == "begin") ? settings.FirstOffset : settings.LastOffset;
            TExprNode::TPtr& frameBound = (settingName == "begin") ? settings.First : settings.Last;

            if (setting->Tail().IsList()) {
                TExprNode::TPtr fb = setting->TailPtr();
                if (!EnsureTupleMinSize(*fb, 1, ctx)) {
                    return {};
                }
                if (!EnsureAtom(fb->Head(), ctx)) {
                    return {};
                }

                auto type = fb->Head().Content();
                if (type == "currentRow") {
                    if (fb->ChildrenSize() == 1) {
                        if (!node.IsCallable("WinOnRange")) {
                            ctx.AddError(TIssue(ctx.GetPosition(fb->Pos()), TStringBuilder() << "currentRow should only be used for RANGE"));
                            return {};
                        }
                        frameBound = fb;
                        continue;
                    }
                    ctx.AddError(TIssue(ctx.GetPosition(fb->Pos()), TStringBuilder() << "Expecting no value for '" << type << "'"));
                    return {};
                }

                if (!(type == "preceding" || type == "following")) {
                    ctx.AddError(TIssue(ctx.GetPosition(fb->Pos()), TStringBuilder() << "Expecting preceding or following, but got '" << type << "'"));
                    return {};
                }

                if (!EnsureTupleSize(*fb, 2, ctx)) {
                    return {};
                }

                auto boundValue = fb->ChildPtr(1);
                if (boundValue->IsAtom()) {
                    if (boundValue->Content() == "unbounded") {
                        frameBound = fb;
                        continue;
                    }
                    ctx.AddError(TIssue(ctx.GetPosition(fb->Pos()), TStringBuilder() << "Expecting unbounded, but got '" << boundValue->Content() << "'"));
                    return {};
                }

                if (node.IsCallable({"WinOnRows", "WinOnGroups"})) {
                    if (!EnsureDataType(*boundValue, ctx)) {
                        return {};
                    }

                    auto slot = boundValue->GetTypeAnn()->Cast<TDataExprType>()->GetSlot();
                    bool groups = node.IsCallable("WinOnGroups");
                    if (!IsDataTypeIntegral(slot)) {
                        ctx.AddError(TIssue(ctx.GetPosition(boundValue->Pos()),
                            TStringBuilder() << "Expecting integral values for " << (groups ? "GROUPS" : "ROWS") << " but got " << *boundValue->GetTypeAnn()));
                        return {};
                    }

                    if (!groups) {
                        auto maybeIntLiteral = TMaybeNode<TCoIntegralCtor>(boundValue);
                        if (!maybeIntLiteral) {
                            // TODO: this is not strictly necessary, and only needed for current implementation via Queue
                            ctx.AddError(TIssue(ctx.GetPosition(boundValue->Pos()),
                                TStringBuilder() << "Expecting literal values for ROWS"));
                            return {};
                        }
                        auto strLiteralValue = maybeIntLiteral.Cast().Literal().Value();
                        if (strLiteralValue.StartsWith("-")) {
                            ctx.AddError(TIssue(ctx.GetPosition(boundValue->Pos()),
                                TStringBuilder() << "Expecting positive literal values for ROWS, but got " << strLiteralValue));
                            return {};
                        }

                        ui64 literalValue = FromString<ui64>(strLiteralValue);
                        if (literalValue > std::numeric_limits<i32>::max()) {
                            ctx.AddError(TIssue(ctx.GetPosition(boundValue->Pos()),
                                TStringBuilder() << "ROWS offset too big: " << strLiteralValue << ", maximum is " << std::numeric_limits<i32>::max()));
                            return {};
                        }

                        i32 castedValue = (i32)literalValue;
                        if (type == "preceding") {
                            castedValue = -castedValue;
                        }
                        boundOffset = castedValue;
                    }
                } else if (!EnsureComparableType(boundValue->Pos(), *boundValue->GetTypeAnn(), ctx)) {
                    return {};
                }
                frameBound = fb;
            } else if (setting->Tail().IsCallable("Int32")) {
                auto& valNode = setting->Tail().Head();
                YQL_ENSURE(valNode.IsAtom());
                i32 value;
                YQL_ENSURE(TryFromString(valNode.Content(), value));
                boundOffset = value;
            } else if (!setting->Tail().IsCallable("Void")) {
                const TTypeAnnotationNode* type = setting->Tail().GetTypeAnn();
                TStringBuilder errMsg;
                if (!type) {
                    errMsg << "lambda";
                } else if (setting->Tail().IsCallable()) {
                    errMsg << setting->Tail().Content() << " with type " << *type;
                } else {
                    errMsg << *type;
                }

                ctx.AddError(TIssue(ctx.GetPosition(setting->Tail().Pos()),
                    TStringBuilder() << "Invalid " << settingName << " frame bound - expecting Void or Int32 callable, but got: " << errMsg));
                return {};
            }
        }

        if (!hasBegin || !hasEnd) {
            ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()),
                TStringBuilder() << "Missing " << (!hasBegin ? "begin" : "end") << " bound in frame definition"));
            return {};
        }
    } else if (frameSpec->IsCallable("Void")) {
        settings.FirstOffset = {};
        settings.LastOffset = 0;
    } else {
        const TTypeAnnotationNode* type = frameSpec->GetTypeAnn();
        ctx.AddError(TIssue(ctx.GetPosition(frameSpec->Pos()),
            TStringBuilder() << "Invalid window frame - expecting Tuple or Void, but got: " << (type ? FormatType(type) : "lambda")));
        return {};
    }

    // frame will always contain rows if it includes current row
    if (!settings.FirstOffset) {
        settings.NeverEmpty = !settings.LastOffset.Defined() || *settings.LastOffset >= 0;
    } else if (!settings.LastOffset.Defined()) {
        settings.NeverEmpty = !settings.FirstOffset.Defined() || *settings.FirstOffset <= 0;
    } else {
        settings.NeverEmpty = *settings.FirstOffset <= *settings.LastOffset && *settings.FirstOffset <= 0 && *settings.LastOffset >= 0;
    }

    return settings;
}

TMaybe<i32> TWindowFrameSettings::GetFirstOffset() const {
    YQL_ENSURE(Type == FrameByRows);
    return FirstOffset;
}

TMaybe<i32> TWindowFrameSettings::GetLastOffset() const {
    YQL_ENSURE(Type == FrameByRows);
    return LastOffset;
}

TCoFrameBound TWindowFrameSettings::GetFirst() const {
    YQL_ENSURE(First);
    return TCoFrameBound(First);
}

TCoFrameBound TWindowFrameSettings::GetLast() const {
    YQL_ENSURE(Last);
    return TCoFrameBound(Last);
}

TExprNode::TPtr ZipWithSessionParamsLambda(TPositionHandle pos, const TExprNode::TPtr& partitionKeySelector,
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit,
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx)
{
    auto extractTupleItem = [&](ui32 idx) {
        return ctx.Builder(pos)
            .Lambda()
                .Param("tuple")
                .Callable("Nth")
                    .Arg(0, "tuple")
                    .Atom(1, ToString(idx), TNodeFlags::Default)
                .Seal()
            .Seal()
            .Build();
    };

    auto initLambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .List() // row, sessionKey, sessionState, partitionKey
                .Arg(0, "row")
                .Apply(1, sessionKeySelector)
                    .With(0, "row")
                    .With(1)
                        .Apply(sessionInit)
                            .With(0, "row")
                        .Seal()
                    .Done()
                .Seal()
                .Apply(2, sessionInit)
                    .With(0, "row")
                .Seal()
                .Apply(3, partitionKeySelector)
                    .With(0, "row")
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newPartitionLambda = ctx.Builder(pos)
        .Lambda()
            .Param("row")
            .Param("prevBigState")
            .Callable("AggrNotEquals")
                .Apply(0, partitionKeySelector)
                    .With(0, "row")
                .Seal()
                .Apply(1, partitionKeySelector)
                    .With(0)
                        .Apply(extractTupleItem(0))
                            .With(0, "prevBigState")
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
        .Seal()
        .Build();

    auto newSessionOrUpdatedStateLambda = [&](bool newSession) {
        return ctx.Builder(pos)
            .Lambda()
                .Param("row")
                .Param("prevBigState")
                .Apply(extractTupleItem(newSession ? 0 : 1))
                    .With(0)
                        .Apply(sessionUpdate)
                            .With(0, "row")
                            .With(1)
                                .Apply(extractTupleItem(2))
                                    .With(0, "prevBigState")
                                .Seal()
                            .Done()
                        .Seal()
                    .Done()
                .Seal()
            .Seal()
            .Build();
    };

    return ctx.Builder(pos)
        .Lambda()
            .Param("input")
            .Callable("Chain1Map")
                .Arg(0, "input")
                .Add(1, initLambda)
                .Lambda(2)
                    .Param("row")
                    .Param("prevBigState")
                    .Callable("If")
                        .Apply(0, newPartitionLambda)
                            .With(0, "row")
                            .With(1, "prevBigState")
                        .Seal()
                        .Apply(1, initLambda)
                            .With(0, "row")
                        .Seal()
                        .List(2)
                            .Arg(0, "row")
                            .Callable(1, "If")
                                .Apply(0, newSessionOrUpdatedStateLambda(/* newSession = */ true))
                                    .With(0, "row")
                                    .With(1, "prevBigState")
                                .Seal()
                                .Apply(1, sessionKeySelector)
                                    .With(0, "row")
                                    .With(1)
                                        .Apply(newSessionOrUpdatedStateLambda(/* newSession = */ false))
                                            .With(0, "row")
                                            .With(1, "prevBigState")
                                        .Seal()
                                    .Done()
                                .Seal()
                                .Apply(2, extractTupleItem(1))
                                    .With(0, "prevBigState")
                                .Seal()
                            .Seal()
                            .Apply(2, newSessionOrUpdatedStateLambda(/* newSession = */ false))
                                .With(0, "row")
                                .With(1, "prevBigState")
                            .Seal()
                            .Apply(3, partitionKeySelector)
                                .With(0, "row")
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos,
    TStringBuf sessionStartMemberName, const TExprNode::TPtr& partitionKeySelector,
    const TSessionWindowParams& sessionWindowParams, TExprContext& ctx)
{
    return AddSessionParamsMemberLambda(pos, sessionStartMemberName, "", partitionKeySelector,
        sessionWindowParams.Key, sessionWindowParams.Init, sessionWindowParams.Update, ctx);
}

TExprNode::TPtr AddSessionParamsMemberLambda(TPositionHandle pos,
    TStringBuf sessionStartMemberName, TStringBuf sessionParamsMemberName,
    const TExprNode::TPtr& partitionKeySelector,
    const TExprNode::TPtr& sessionKeySelector, const TExprNode::TPtr& sessionInit,
    const TExprNode::TPtr& sessionUpdate, TExprContext& ctx)
{
    YQL_ENSURE(sessionStartMemberName);
    TExprNode::TPtr addLambda = ctx.Builder(pos)
        .Lambda()
            .Param("tupleOfItemAndSessionParams")
            .Callable("AddMember")
                .Callable(0, "Nth")
                    .Arg(0, "tupleOfItemAndSessionParams")
                    .Atom(1, "0", TNodeFlags::Default)
                .Seal()
                .Atom(1, sessionStartMemberName)
                .Callable(2, "Nth")
                    .Arg(0, "tupleOfItemAndSessionParams")
                    .Atom(1, "1", TNodeFlags::Default)
                .Seal()
            .Seal()
        .Seal()
        .Build();

    if (sessionParamsMemberName) {
        addLambda = ctx.Builder(pos)
            .Lambda()
                .Param("tupleOfItemAndSessionParams")
                .Callable("AddMember")
                    .Apply(0, addLambda)
                        .With(0, "tupleOfItemAndSessionParams")
                    .Seal()
                    .Atom(1,  sessionParamsMemberName)
                    .Callable(2, "AsStruct")
                        .List(0)
                            .Atom(0, "start", TNodeFlags::Default)
                            .Callable(1, "Nth")
                                .Arg(0, "tupleOfItemAndSessionParams")
                                .Atom(1, "1", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .List(1)
                            .Atom(0, "state", TNodeFlags::Default)
                            .Callable(1, "Nth")
                                .Arg(0, "tupleOfItemAndSessionParams")
                                .Atom(1, "2", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.Builder(pos)
        .Lambda()
            .Param("input")
            .Callable("OrderedMap")
                .Apply(0, ZipWithSessionParamsLambda(pos, partitionKeySelector, sessionKeySelector, sessionInit, sessionUpdate, ctx))
                    .With(0, "input")
                .Seal()
                .Add(1, addLambda)
            .Seal()
        .Seal()
        .Build();
}

void TSessionWindowParams::Reset()
{
    Traits = {};
    Key = {};
    KeyType = nullptr;
    ParamsType = nullptr;
    Init = {};
    Update = {};
    SortTraits = {};
}

}
