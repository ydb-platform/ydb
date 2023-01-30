#include <ydb/core/kqp/common/kqp_yql.h>

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

} // namespace NKikimr::NKqp::NOpt
