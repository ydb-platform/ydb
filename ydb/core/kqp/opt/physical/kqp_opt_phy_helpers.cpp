#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

template <typename TContainer>
TCoAtomList BuildColumnsListImpl(const TContainer& columns, TPositionHandle pos, TExprContext& ctx) {
    TVector<TCoAtom> columnsList;
    for (const auto& column : columns) {
        auto atom = ctx.NewAtom(pos, column);
        columnsList.push_back(TCoAtom(atom));
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columnsList)
        .Done();
}

} // namespace

TExprBase BuildReadNode(TPositionHandle pos, TExprContext& ctx, TExprBase input, TKqpReadTableSettings& settings) {
    TCoNameValueTupleList settingsNode = settings.BuildNode(ctx, pos);

    if (input.Maybe<TKqpReadTable>().IsValid()) {
        auto dataReadTable = input.Cast<TKqpReadTable>();

        return Build<TKqpReadTable>(ctx, pos)
            .Table(dataReadTable.Table())
            .Range(dataReadTable.Range())
            .Columns(dataReadTable.Columns())
            .Settings(settingsNode)
            .Done();
    } else if (input.Maybe<TKqpReadTableRanges>().IsValid()) {
        auto readTableRanges = input.Cast<TKqpReadTableRanges>();

        return Build<TKqpReadTableRanges>(ctx, pos)
            .Table(readTableRanges.Table())
            .Ranges(readTableRanges.Ranges())
            .Columns(readTableRanges.Columns())
            .Settings(settingsNode)
            .ExplainPrompt(readTableRanges.ExplainPrompt())
            .Done();
    } else if (input.Maybe<TKqpReadOlapTableRanges>().IsValid()) {
        auto olapReadTable = input.Cast<TKqpReadOlapTableRanges>();

        return Build<TKqpReadOlapTableRanges>(ctx, pos)
            .Table(olapReadTable.Table())
            .Ranges(olapReadTable.Ranges())
            .Columns(olapReadTable.Columns())
            .Settings(settingsNode)
            .ExplainPrompt(olapReadTable.ExplainPrompt())
            .Process(olapReadTable.Process())
            .Done();
    }

    YQL_ENSURE(false, "Unknown read table operation: " << input.Ptr()->Content());
}

TCoAtom GetReadTablePath(TExprBase input, bool isReadRanges) {
    if (isReadRanges) {
        return input.Cast<TKqlReadTableRangesBase>().Table().Path();
    }

    return input.Cast<TKqpReadTable>().Table().Path();
}

TKqpReadTableSettings GetReadTableSettings(TExprBase input, bool isReadRanges) {
    if (isReadRanges) {
        return TKqpReadTableSettings::Parse(input.Cast<TKqlReadTableRangesBase>());
    }

    return TKqpReadTableSettings::Parse(input.Cast<TKqpReadTable>());
};

TCoAtomList BuildColumnsList(const THashSet<TStringBuf>& columns, TPositionHandle pos, TExprContext& ctx) {
    return BuildColumnsListImpl(columns, pos, ctx);
}

TCoAtomList BuildColumnsList(const TVector<TStringBuf>& columns, NYql::TPositionHandle pos,NYql::TExprContext& ctx) {
    return BuildColumnsListImpl(columns, pos, ctx);
}

TCoAtomList BuildColumnsList(const TVector<TString>& columns, TPositionHandle pos, TExprContext& ctx) {
    return BuildColumnsListImpl(columns, pos, ctx);
}

bool AllowFuseJoinInputs(TExprBase node) {
    if (!node.Maybe<TDqJoin>()) {
        return false;
    }
    auto join = node.Cast<TDqJoin>();
    for (auto& input : {join.LeftInput(), join.RightInput()}) {
        if (auto conn = input.Maybe<TDqConnection>()) {
            auto stage = conn.Cast().Output().Stage();
            for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                auto input = stage.Inputs().Item(i);
                if (input.Maybe<TDqSource>() && input.Cast<TDqSource>().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
                    return false;
                }
            }
        }
    }
    return true;
}

NYql::NNodes::TDqStage ReplaceStageArg(NYql::NNodes::TDqStage stage, size_t inputIndex,
    NYql::NNodes::TCoArgument replaceArg, NYql::NNodes::TExprBase bodyExpression, NYql::TExprContext& ctx)
{
    auto sourceArg = stage.Program().Args().Arg(inputIndex);

    size_t index = 0;
    TVector<TCoArgument> args;
    NYql::TNodeOnNodeOwnedMap bodyReplaces;
    for (auto arg : stage.Program().Args()) {
        if (arg.Raw() == sourceArg.Raw()) {
            args.push_back(replaceArg);
        } else {
            TCoArgument replaceArg{ctx.NewArgument(sourceArg.Pos(), TStringBuilder() << "_kqp_source_arg_" << index)};
            args.push_back(replaceArg);
            bodyReplaces[arg.Raw()] = replaceArg.Ptr();
        }
        index += 1;
    }

    bodyReplaces[sourceArg.Raw()] = bodyExpression.Ptr();

    return Build<TDqStage>(ctx, stage.Pos())
        .Settings(stage.Settings())
        .Inputs(stage.Inputs())
        .Outputs(stage.Outputs())
        .Program<TCoLambda>()
            .Args(args)
            .Body(TExprBase(ctx.ReplaceNodes(stage.Program().Body().Ptr(), bodyReplaces)))
            .Build()
        .Done();
}

NYql::NNodes::TDqStage ReplaceTableSourceSettings(NYql::NNodes::TDqStage stage, size_t inputIndex,
    NYql::NNodes::TKqpReadRangesSourceSettings settings, NYql::TExprContext& ctx)
{
    auto source = stage.Inputs().Item(inputIndex).Cast<TDqSource>();
    auto readRangesSource = source.Settings().Cast<TKqpReadRangesSourceSettings>();
    auto sourceArg = stage.Program().Args().Arg(inputIndex);

    TVector<NYql::NNodes::TExprBase> inputs;
    size_t index = 0;
    for (auto input : stage.Inputs()) {
        if (index == inputIndex) {
            inputs.push_back(
                Build<TDqSource>(ctx, input.Pos())
                    .DataSource<TCoDataSource>()
                        .Category<TCoAtom>().Value(KqpReadRangesSourceName).Build()
                        .Build()
                    .Settings(settings)
                .Done());
        } else {
            inputs.push_back(input);
        }
        index += 1;
    }

    return Build<TDqStage>(ctx, stage.Pos())
        .Settings(stage.Settings())
        .Inputs().Add(inputs).Build()
        .Outputs(stage.Outputs())
        .Program(stage.Program())
        .Done();
}

ESortDirection GetSortDirection(const NYql::NNodes::TExprBase& sortDirections) {
    auto getDirection = [] (TExprBase expr) -> ESortDirection {
        if (!expr.Maybe<TCoBool>()) {
            return ESortDirection::Unknown;
        }

        if (!FromString<bool>(expr.Cast<TCoBool>().Literal().Value())) {
            return ESortDirection::Reverse;
        }

        return ESortDirection::Forward;
    };

    auto direction = ESortDirection::None;
    if (auto maybeList = sortDirections.Maybe<TExprList>()) {
        for (const auto& expr : maybeList.Cast()) {
            direction |= getDirection(expr);
        }
    } else {
        direction |= getDirection(sortDirections);
    }
    return direction;
};

TExprNode::TPtr MakeMessage(TStringBuf message, TPositionHandle pos, TExprContext& ctx) {
    return ctx.NewCallable(pos, "Utf8", { ctx.NewAtom(pos, message) });
}

} // namespace NKikimr::NKqp::NOpt
