#include "kqp_opt_phy_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>
#include <ydb/core/tx/schemeshard/schemeshard_utils.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;


bool UseSource(const TKqpOptimizeContext& kqpCtx, const NYql::TKikimrTableDescription& tableDesc) {
    bool useSource = kqpCtx.Config->EnableKqpScanQuerySourceRead && kqpCtx.IsScanQuery();
    useSource = useSource || kqpCtx.IsDataQuery();
    useSource = useSource || kqpCtx.IsGenericQuery();
    useSource = useSource &&
        tableDesc.Metadata->Kind != EKikimrTableKind::SysView &&
        tableDesc.Metadata->Kind != EKikimrTableKind::Olap;

    return useSource;
}

TExprBase KqpRewriteReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    auto stage = node.Cast<TDqStage>();
    struct TMatchedRead {
        TExprBase Expr;
        TKqpTable Table;
        TCoAtomList Columns;
        TCoNameValueTupleList Settings;
        TExprBase RangeExpr;
        TMaybeNode<TCoNameValueTupleList> ExplainPrompt = {};
    };
    TMaybe<TMatchedRead> matched;

    VisitExpr(stage.Program().Body().Ptr(), [&](const TExprNode::TPtr& node) {
            TExprBase expr(node);
            if (auto cast = expr.Maybe<TKqpReadTable>()) {
                Y_ENSURE(!matched || matched->Expr.Raw() == node.Get());
                auto read = cast.Cast();
                matched = TMatchedRead {
                    .Expr = read,
                    .Table = read.Table(),
                    .Columns = read.Columns(),
                    .Settings = read.Settings(),
                    .RangeExpr = read.Range()
                };
            }

            if (auto cast = expr.Maybe<TKqpReadTableRanges>()) {
                Y_ENSURE(!matched || matched->Expr.Raw() == node.Get());
                auto read = cast.Cast();
                matched = TMatchedRead {
                    .Expr = read,
                    .Table = read.Table(),
                    .Columns = read.Columns(),
                    .Settings = read.Settings(),
                    .RangeExpr = read.Ranges(),
                    .ExplainPrompt = read.ExplainPrompt()
                };
            }
            return true;
        });

    if (!matched) {
        return node;
    }

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, matched->Table.Path());
    if (!UseSource(kqpCtx, tableDesc)) {
        return node;
    }

    auto settings = TKqpReadTableSettings::Parse(matched->Settings);
    auto selectColumns = matched->Columns;
    TVector<TCoAtom> skipNullColumns;
    TExprNode::TPtr limit;
    if (settings.SkipNullKeys) {
        THashSet<TString> seenColumns;
        TVector<TCoAtom> columns;

        for (size_t i = 0; i < matched->Columns.Size(); ++i) {
            auto atom = matched->Columns.Item(i);
            auto column = atom.StringValue();
            if (seenColumns.insert(column).second) {
                columns.push_back(atom);
            }
        }
        for (auto& column : settings.SkipNullKeys) {
            TCoAtom atom(ctx.NewAtom(matched->Settings.Pos(), column));
            skipNullColumns.push_back(atom);
            if (seenColumns.insert(column).second) {
                columns.push_back(atom);
            }
        }

        matched->Columns = Build<TCoAtomList>(ctx, matched->Columns.Pos()).Add(columns).Done();

        settings.SkipNullKeys.clear();
        limit = settings.ItemsLimit;
        settings.ItemsLimit = nullptr;

        matched->Settings = settings.BuildNode(ctx, matched->Settings.Pos());
    }

    TVector<TExprBase> inputs;
    TVector<TCoArgument> args;
    TNodeOnNodeOwnedMap argReplaces;
    TNodeOnNodeOwnedMap sourceReplaces;

    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        inputs.push_back(stage.Inputs().Item(i));

        TCoArgument newArg{ctx.NewArgument(stage.Pos(), TStringBuilder() << "_kqp_pc_arg_" << i)};
        args.push_back(newArg);

        TCoArgument arg = stage.Program().Args().Arg(i);

        argReplaces[arg.Raw()] = newArg.Ptr();
        sourceReplaces[arg.Raw()] = stage.Inputs().Item(i).Ptr();
    }

    TCoArgument arg{ctx.NewArgument(stage.Pos(), TStringBuilder() << "_kqp_source_arg")};
    args.insert(args.begin(), arg);

    TExprNode::TPtr replaceExpr =
        Build<TCoToFlow>(ctx, matched->Expr.Pos())
            .Input(arg)
        .Done()
            .Ptr();

    if (skipNullColumns) {
        replaceExpr =
            Build<TCoExtractMembers>(ctx, node.Pos())
                .Members(selectColumns)
                .Input<TCoSkipNullMembers>()
                    .Input(replaceExpr)
                    .Members().Add(skipNullColumns).Build()
                .Build()
            .Done().Ptr();
    }

    if (limit) {
        limit = ctx.ReplaceNodes(std::move(limit), argReplaces);
        replaceExpr =
            Build<TCoTake>(ctx, node.Pos())
                .Input(replaceExpr)
                .Count(limit)
            .Done().Ptr();
    }

    argReplaces[matched->Expr.Raw()] = replaceExpr;

    auto source =
        Build<TDqSource>(ctx, matched->Expr.Pos())
            .Settings<TKqpReadRangesSourceSettings>()
                .Table(matched->Table)
                .Columns(matched->Columns)
                .Settings(matched->Settings)
                .RangesExpr(matched->RangeExpr)
                .ExplainPrompt(matched->ExplainPrompt)
            .Build()
            .DataSource<TCoDataSource>()
                .Category<TCoAtom>().Value(KqpReadRangesSourceName).Build()
            .Build()
        .Done();
    inputs.insert(inputs.begin(), TExprBase(ctx.ReplaceNodes(source.Ptr(), sourceReplaces)));

    return Build<TDqStage>(ctx, stage.Pos())
        .Inputs().Add(inputs).Build()
        .Outputs(stage.Outputs())
        .Settings(stage.Settings())
        .Program()
            .Args(args)
            .Body(ctx.ReplaceNodes(stage.Program().Body().Ptr(), argReplaces))
            .Build()
        .Done();
}

} // namespace NKikimr::NKqp::NOpt
