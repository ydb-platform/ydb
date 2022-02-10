#include "yql_kikimr_provider_impl.h"
#include <ydb/core/kqp/provider/mkql/yql_kikimr_mkql_expr_nodes.h>

#include <ydb/library/yql/core/yql_expr_optimize.h> 
#include <ydb/library/yql/core/yql_expr_type_annotation.h> 
#include <ydb/library/yql/core/yql_join.h> 


namespace NYql {
namespace {

using namespace NNodes;

TExprNode::TPtr BuildMkqlVersionedTable(const TCoAtom& tableName, const TCoAtom& schemaVersion,
    const TCoAtom& pathId, TExprContext& ctx, TPositionHandle pos)
{
    return Build<TMkqlVersionedTable>(ctx, pos)
        .Table(tableName)
        .SchemaVersion(schemaVersion)
        .PathId(pathId)
        .Done()
        .Ptr();
}

TExprNode::TPtr MkqlRewriteCallables(TCallable callable, TExprContext& ctx, const TMaybe<TString>& rtParamName) {
    TMaybeNode<TCoParameter> readTarget;

    if (rtParamName) {
        readTarget = Build<TCoParameter>(ctx, callable.Pos())
            .Name().Build(*rtParamName)
            .Type<TCoDataType>()
                .Type().Build("Uint32")
                .Build()
            .Done();
    }

    if (auto maybeSelectRow = callable.Maybe<TKiSelectRow>()) {
        auto selectRow  = maybeSelectRow.Cast();
        auto vt = selectRow.Table();
        TExprNode::TListType children = {
            BuildMkqlVersionedTable(vt.Path(), vt.SchemaVersion(), vt.PathId(), ctx, selectRow.Pos()),
            selectRow.Key().Ptr(),
            selectRow.Select().Ptr()
        };

        if (readTarget) {
            children.push_back(readTarget.Cast().Ptr());
        }

        return ctx.NewCallable(selectRow.Pos(), "SelectRow", std::move(children));
    }

    if (auto maybeSelectRange = callable.Maybe<TKiSelectRangeBase>()) {
        auto selectRange = maybeSelectRange.Cast();
        auto vt = selectRange.Table();

        TExprNode::TListType children = {
            BuildMkqlVersionedTable(vt.Path(), vt.SchemaVersion(), vt.PathId(), ctx, selectRange.Pos()),
            selectRange.Range().Ptr(),
            selectRange.Select().Ptr(),
            selectRange.Settings().Ptr()
        };

        if (readTarget) {
            children.push_back(readTarget.Cast().Ptr());
        }

        auto selectRangeMkql = ctx.NewCallable(selectRange.Pos(), "SelectRange", std::move(children));

        if (selectRange.Maybe<TKiSelectRange>()) {
            return ctx.Builder(selectRange.Pos())
                .Callable("Member")
                    .Add(0, selectRangeMkql)
                    .Atom(1, "List")
                    .Seal()
                .Build();
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(selectRange.Pos()), TStringBuilder() << "Got unsupported callable: "
                << selectRange.CallableName()));
            return nullptr;
        }
    }

    if (auto maybeUpdateRow = callable.Maybe<TKiUpdateRow>()) {
        auto updateRow = maybeUpdateRow.Cast();
        auto vt = updateRow.Table();
        return Build<TMkqlUpdateRow>(ctx, updateRow.Pos())
            .Table(BuildMkqlVersionedTable(vt.Path(), vt.SchemaVersion(), vt.PathId(), ctx, updateRow.Pos()))
            .Key(updateRow.Key())
            .Update(updateRow.Update())
            .Done()
            .Ptr();
    }

    if (auto maybeEraseRow = callable.Maybe<TKiEraseRow>()) {
        auto eraseRow = maybeEraseRow.Cast();
        auto vt = eraseRow.Table();
        return Build<TMkqlEraseRow>(ctx, eraseRow.Pos())
                .Table(BuildMkqlVersionedTable(vt.Path(), vt.SchemaVersion(), vt.PathId(), ctx, eraseRow.Pos()))
                .Key(eraseRow.Key())
                .Done()
                .Ptr();
    }

    if (auto setResult = callable.Maybe<TKiSetResult>()) {
        return ctx.Builder(setResult.Cast().Pos())
            .Callable("SetResult")
                .Add(0, setResult.Cast().Name().Ptr())
                .Add(1, setResult.Cast().Data().Ptr())
                .Seal()
            .Build();
    }

    if (auto acquireLocks = callable.Maybe<TKiAcquireLocks>()) {
        return ctx.Builder(acquireLocks.Cast().Pos())
            .Callable("AcquireLocks")
                .Add(0, acquireLocks.Cast().LockTxId().Ptr())
                .Seal()
            .Build();
    }

    if (auto map = callable.Maybe<TKiMapParameter>()) {
        return Build<TMkqlMapParameter>(ctx, map.Cast().Pos())
                .Input(map.Cast().Input())
                .Lambda(map.Cast().Lambda())
            .Done()
            .Ptr();
    }

    if (auto map = callable.Maybe<TKiFlatMapParameter>()) {
        return ctx.Builder(map.Cast().Pos())
            .Callable("FlatMapParameter")
                .Add(0, map.Cast().Input().Ptr())
                .Add(1, map.Cast().Lambda().Ptr())
                .Seal()
            .Build();
    }
    if (auto maybePartialSort = callable.Maybe<TKiPartialSort>()) {
        return ctx.Builder(maybePartialSort.Cast().Pos())
            .Callable("PartialSort")
                .Add(0, maybePartialSort.Cast().Input().Ptr())
                .Add(1, maybePartialSort.Cast().SortDirections().Ptr())
                .Add(2, maybePartialSort.Cast().KeySelectorLambda().Ptr())
                .Seal()
            .Build();
    }
    if (auto maybePartialTake = callable.Maybe<TKiPartialTake>()) {
        return ctx.Builder(maybePartialTake.Cast().Pos())
            .Callable("PartialTake")
                .Add(0, maybePartialTake.Cast().Input().Ptr())
                .Add(1, maybePartialTake.Cast().Count().Ptr())
                .Seal()
            .Build();
    }

    return callable.Ptr();
}

} // namespace

TMaybeNode<TExprBase> TranslateToMkql(TExprBase node, TExprContext& ctx, const TMaybe<TString>& rtParamName) {
    auto maybeProgram = node.Maybe<TKiProgram>();
    YQL_ENSURE(maybeProgram);

    auto program = maybeProgram.Cast();
    bool hasResult = !program.Results().Empty();

    if (hasResult) {
        node = Build<TCoAppend>(ctx, node.Pos())
            .List(program.Effects())
            .Item<TKiSetResult>()
                .Name().Build("Result")
                .Data(program.Results())
                .Build()
            .Done();
    } else {
        node = program.Effects();
    }

    auto current = node.Ptr();
    TExprNode::TPtr output;
    TOptimizeExprSettings optSettings(nullptr); 
    optSettings.VisitChanges = true;
    auto status = OptimizeExpr(current, output,
        [rtParamName](const TExprNode::TPtr& input, TExprContext& ctx) {
            auto ret = input;
            auto node = TExprBase(input);

            if (auto call = node.Maybe<TCallable>()) {
                ret = MkqlRewriteCallables(call.Cast(), ctx, rtParamName);
                if (ret != input) {
                    return ret;
                }
            }

            return ret;
        }, ctx, optSettings);

    if (status != IGraphTransformer::TStatus::Ok) {
        return TExprNode::TPtr();
    }

    return TExprBase(output);
}

} // namespace NYql
