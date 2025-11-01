#include "kqp_opt_log_impl.h"

#include <yql/essentials/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TExprBase MakeLT(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoAnd>()
            .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
            .Add<TCoExists>().Optional(right).Build()
            .Build()
        .Add<TCoCmpLess>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

TExprBase MakeEQ(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoAnd>()
            .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
            .Add<TCoNot>().Value<TCoExists>().Optional(right).Build().Build()
            .Build()
        .Add<TCoCmpEqual>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

TExprBase MakeLE(TExprBase left, TExprBase right, TExprContext& ctx, TPositionHandle pos) {
    return Build<TCoOr>(ctx, pos)
        .Add<TCoNot>().Value<TCoExists>().Optional(left).Build().Build()
        .Add<TCoCmpLessOrEqual>()
            .Left(left)
            .Right(right)
            .Build()
        .Done();
}

TCoAtomList MakeAllColumnsList(const NYql::TKikimrTableDescription & tableDesc, TExprContext& ctx, TPositionHandle pos) {
    TVector<TCoAtom> columns;
    for (auto& [column, _] : tableDesc.Metadata->Columns) {
        columns.push_back(Build<TCoAtom>(ctx, pos).Value(column).Done());
    }
    return Build<TCoAtomList>(ctx, pos).Add(columns).Done();
};

TMaybe<TPrefixLookup> RewriteReadToPrefixLookup(TKqlReadTableBase read, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    TString lookupTable;
    TString indexName;

    TMaybeNode<TCoAtomList> lookupColumns;
    size_t prefixSize;
    TMaybeNode<TExprBase> prefixExpr;
    TMaybeNode<TCoLambda> extraFilter;
    TMaybe<TSet<TString>> usedColumns;

    if (!read.template Maybe<TKqlReadTable>() && !read.template Maybe<TKqlReadTableIndex>()) {
        return {};
    }

    const auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path().StringValue());
    if (!read.Table().SysView().Value().empty() || mainTableDesc.Metadata->Kind == EKikimrTableKind::SysView) {
        // Can't lookup in system views
        return {};
    }

    if (auto indexRead = read.template Maybe<TKqlReadTableIndex>()) {
        indexName = indexRead.Cast().Index().StringValue();
        lookupTable = GetIndexMetadata(indexRead.Cast(), *kqpCtx.Tables, kqpCtx.Cluster)->Name;
    } else {
        lookupTable = read.Table().Path().StringValue();
    }
    const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);

    auto from = read.Range().From();
    auto to = read.Range().To();

    usedColumns.ConstructInPlace();
    prefixSize = 0;
    while (prefixSize < from.ArgCount() && prefixSize < to.ArgCount()) {
        if (from.Arg(prefixSize).Raw() != to.Arg(prefixSize).Raw()) {
            break;
        }
        usedColumns->insert(rightTableDesc.Metadata->KeyColumnNames[prefixSize]);
        ++prefixSize;
    }

    lookupColumns = read.Columns();

    // we don't need to make filter for point selection
    if (!(prefixSize == from.ArgCount() &&
         prefixSize == to.ArgCount() &&
         from.template Maybe<TKqlKeyInc>() &&
         to.template Maybe<TKqlKeyInc>()))
    {
        extraFilter = MakeFilterForRange(read.Range(), ctx, read.Range().Pos(), rightTableDesc.Metadata->KeyColumnNames);
        lookupColumns = MakeAllColumnsList(mainTableDesc, ctx, read.Pos());
    }

    TVector<TExprBase> columns;
    for (size_t i = 0; i < prefixSize; ++i) {
        columns.push_back(TExprBase(from.Arg(i)));
    }

    prefixExpr = Build<TCoAsList>(ctx, read.Pos())
        .Add<TExprList>()
            .Add(columns)
            .Build()
        .Done();

    Y_ENSURE(prefixExpr.IsValid());

    return NKikimr::NKqp::NOpt::TPrefixLookup {
        .LookupColumns = lookupColumns.Cast(),
        .ResultColumns = read.Columns(),

        .Filter = extraFilter,
        .FilterUsedColumnsHint = usedColumns,

        .PrefixSize = prefixSize,
        .PrefixExpr = prefixExpr.Cast(),

        .LookupTableName = lookupTable,
        .MainTable = read.Table(),
        .IndexName = indexName,
    };
}

TMaybe<TPrefixLookup> RewriteReadToPrefixLookup(TKqlReadTableRangesBase read, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, TMaybe<size_t> maxKeys) {
    TString lookupTable;
    TString indexName;

    TMaybeNode<TCoAtomList> lookupColumns;
    size_t prefixSize;
    TMaybeNode<TExprBase> prefixExpr;
    TMaybeNode<TCoLambda> extraFilter;
    TMaybe<TSet<TString>> usedColumns;

    if (!read.template Maybe<TKqlReadTableRanges>() && !read.template Maybe<TKqlReadTableIndexRanges>()) {
        return {};
    }

    const auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path().StringValue());
    if (!read.Table().SysView().Value().empty() || mainTableDesc.Metadata->Kind == EKikimrTableKind::SysView) {
        // Can't lookup in system views
        return {};
    }

    lookupColumns = read.Columns();

    if (auto indexRead = read.template Maybe<TKqlReadTableIndexRanges>()) {
        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(indexRead.Index().Cast().Value());
        lookupTable = indexMeta->Name;
        indexName = indexRead.Cast().Index().StringValue();
    } else {
        lookupTable = read.Table().Path().StringValue();
    }

    if (TCoVoid::Match(read.Ranges().Raw())) {
        prefixSize = 0;
        prefixExpr = Build<TCoJust>(ctx, read.Pos())
            .Input<TCoAsList>().Build()
            .Done();
    } else {
        auto prompt = TKqpReadTableExplainPrompt::Parse(read);

        prefixSize = prompt.PointPrefixLen;

        const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);

        TMaybeNode<TExprBase> rowsExpr;
        TMaybeNode<TCoLambda> filter;
        TMaybeNode<TCoAtomList> usedColumnsList;
        if (read.Maybe<TKqlReadTableRanges>()) {
            rowsExpr = read.Cast<TKqlReadTableRanges>().PrefixPointsExpr();
            filter = read.Cast<TKqlReadTableRanges>().PredicateExpr();
            usedColumnsList = read.Cast<TKqlReadTableRanges>().PredicateUsedColumns();
        }
        if (read.Maybe<TKqlReadTableIndexRanges>()) {
            rowsExpr = read.Cast<TKqlReadTableIndexRanges>().PrefixPointsExpr();
            filter = read.Cast<TKqlReadTableIndexRanges>().PredicateExpr();
            usedColumnsList = read.Cast<TKqlReadTableIndexRanges>().PredicateUsedColumns();
        }

        if (!rowsExpr.IsValid()) {
            return {};
        }

        if (maxKeys && (!prompt.ExpectedMaxRanges || *prompt.ExpectedMaxRanges > *maxKeys)) {
            return {};
        }

        // we don't need to make filter for point selection
        if (prompt.PointPrefixLen != prompt.UsedKeyColumns.size()) {
            if (!filter.IsValid() || !usedColumnsList.IsValid()) {
                return {};
            }
            usedColumns.ConstructInPlace();
            for (auto&& column :  usedColumnsList.Cast()) {
                usedColumns->insert(column.StringValue());
            }
            extraFilter = filter;
            lookupColumns = MakeAllColumnsList(mainTableDesc, ctx, read.Pos());
        }

        size_t prefixLen = prompt.PointPrefixLen;
        TVector<TString> keyColumns;
        for (size_t i = 0; i < prefixLen; ++i) {
            YQL_ENSURE(i < rightTableDesc.Metadata->KeyColumnNames.size());
            keyColumns.push_back(rightTableDesc.Metadata->KeyColumnNames[i]);
        }


        auto rowArg = Build<TCoArgument>(ctx, read.Pos())
            .Name("rowArg")
            .Done();

        TVector<TExprBase> components;
        for (auto column : keyColumns) {
            TCoAtom columnAtom(ctx.NewAtom(read.Ranges().Pos(), column));
            components.push_back(
                Build<TCoMember>(ctx, read.Ranges().Pos())
                    .Struct(rowArg)
                    .Name(columnAtom)
                    .Done());
        }

        prefixExpr = Build<TCoMap>(ctx, read.Pos())
            .Input(rowsExpr.Cast())
            .Lambda()
                .Args({rowArg})
                .Body<TExprList>()
                    .Add(components)
                    .Build()
                .Build()
            .Done();
    }

    Y_ENSURE(prefixExpr.IsValid());

    return TPrefixLookup{
        .LookupColumns = lookupColumns.Cast(),
        .ResultColumns = read.Columns(),

        .Filter = extraFilter,
        .FilterUsedColumnsHint = usedColumns,

        .PrefixSize = prefixSize,
        .PrefixExpr = prefixExpr.Cast(),

        .LookupTableName = lookupTable,
        .MainTable = read.Table(),
        .IndexName = indexName,
    };
}

} // namespace

TCoLambda MakeFilterForRange(TKqlKeyRange range, TExprContext& ctx, TPositionHandle pos, TVector<TString> keyColumns) {
    size_t prefix = 0;
    auto arg = Build<TCoArgument>(ctx, pos).Name("_row_arg").Done();
    TVector<TExprBase> conds;
    while (prefix < range.From().ArgCount() && prefix < range.To().ArgCount()) {
        auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[prefix]).Done();
        if (range.From().Arg(prefix).Raw() == range.To().Arg(prefix).Raw()) {
            if (prefix + 1 == range.From().ArgCount() && range.From().Maybe<TKqlKeyExc>()) {
                break;
            }
            if (prefix + 1 == range.To().ArgCount() && range.To().Maybe<TKqlKeyExc>()) {
                break;
            }
        } else {
            break;
        }
        conds.push_back(MakeEQ(column, range.From().Arg(prefix), ctx, pos));
        prefix += 1;
    }

    {
        TMaybeNode<TExprBase> tupleComparison;
        for (ssize_t i = static_cast<ssize_t>(range.From().ArgCount()) - 1; i >= static_cast<ssize_t>(prefix); --i) {
            auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[i]).Done();
            if (tupleComparison.IsValid()) {
                tupleComparison = Build<TCoOr>(ctx, pos)
                    .Add(MakeLT(range.From().Arg(i), column, ctx, pos))
                    .Add<TCoAnd>()
                        .Add(MakeEQ(range.From().Arg(i), column, ctx, pos))
                        .Add(tupleComparison.Cast())
                        .Build()
                    .Done();
            } else {
                if (range.From().Maybe<TKqlKeyInc>()) {
                    tupleComparison = MakeLE(range.From().Arg(i), column, ctx, pos);
                } else {
                    tupleComparison = MakeLT(range.From().Arg(i), column, ctx, pos);
                }
            }
        }

        if (tupleComparison.IsValid()) {
            conds.push_back(tupleComparison.Cast());
        }
    }

    {
        TMaybeNode<TExprBase> tupleComparison;
        for (ssize_t i = static_cast<ssize_t>(range.To().ArgCount()) - 1; i >= static_cast<ssize_t>(prefix); --i) {
            auto column = Build<TCoMember>(ctx, pos).Struct(arg).Name().Build(keyColumns[i]).Done();
            if (tupleComparison.IsValid()) {
                tupleComparison = Build<TCoOr>(ctx, pos)
                    .Add(MakeLT(column, range.To().Arg(i), ctx, pos))
                    .Add<TCoAnd>()
                        .Add(MakeEQ(column, range.To().Arg(i), ctx, pos))
                        .Add(tupleComparison.Cast())
                        .Build()
                    .Done();
            } else {
                if (range.To().Maybe<TKqlKeyInc>()) {
                    tupleComparison = MakeLE(column, range.To().Arg(i), ctx, pos);
                } else {
                    tupleComparison = MakeLT(column, range.To().Arg(i), ctx, pos);
                }
            }
        }

        if (tupleComparison.IsValid()) {
            conds.push_back(tupleComparison.Cast());
        }
    }

    return Build<TCoLambda>(ctx, pos)
        .Args({arg})
        .Body<TCoOptionalIf>()
            .Predicate<TCoCoalesce>()
                .Predicate<TCoAnd>()
                    .Add(conds)
                    .Build()
                .Value<TCoBool>()
                    .Literal().Build("false")
                    .Build()
                .Build()
            .Value(arg)
            .Build()
        .Done();
}

bool ExtractUsedFields(const TExprNode::TPtr& start, const TExprNode& arg, TSet<TString>& usedFields, const TParentsMap& parentsMap, bool allowDependsOn) {
    const TTypeAnnotationNode* argType = RemoveOptionalType(arg.GetTypeAnn());
    if (argType->GetKind() != ETypeAnnotationKind::Struct) {
        return false;
    }

    if (&arg == start.Get()) {
        return true;
    }

    const auto inputStructType = argType->Cast<TStructExprType>();
    if (!IsDepended(*start, arg)) {
        return true;
    }

    TNodeSet nodes;
    VisitExpr(start, [&](const TExprNode::TPtr& node) {
        nodes.insert(node.Get());
        return true;
    });

    const auto parents = parentsMap.find(&arg);
    YQL_ENSURE(parents != parentsMap.cend());
    for (const auto& parent : parents->second) {
        if (nodes.cend() == nodes.find(parent)) {
            continue;
        }

        if (parent->IsCallable("Member")) {
            usedFields.emplace(parent->Tail().Content());
        } else if (allowDependsOn && IsDependsOnUsage(*parent, parentsMap)) {
            continue;
        } else {
            // unknown node
            for (auto&& item : inputStructType->GetItems()) {
                usedFields.emplace(item->GetName());
            }
            return true;
        }
    }

    return true;
}

TExprBase TKqpMatchReadResult::BuildProcessNodes(TExprBase input, TExprContext& ctx) const {
    auto expr = input;

    if (ExtractMembers) {
        expr = Build<TCoExtractMembers>(ctx, ExtractMembers.Cast().Pos())
            .Input(expr)
            .Members(ExtractMembers.Cast().Members())
            .Done();
    }

    if (FilterNullMembers) {
        expr = Build<TCoFilterNullMembers>(ctx, FilterNullMembers.Cast().Pos())
            .Input(expr)
            .Members(FilterNullMembers.Cast().Members())
            .Done();
    }

    if (SkipNullMembers) {
        expr = Build<TCoSkipNullMembers>(ctx, SkipNullMembers.Cast().Pos())
            .Input(expr)
            .Members(SkipNullMembers.Cast().Members())
            .Done();
    }


    if (FlatMap) {
        expr = Build<TCoFlatMap>(ctx, FlatMap.Cast().Pos())
            .Input(expr)
            .Lambda(ctx.DeepCopyLambda(FlatMap.Cast().Lambda().Ref()))
            .Done();
    }

    return expr;
}

TMaybe<TKqpMatchReadResult> MatchRead(TExprBase node, std::function<bool(TExprBase)> matchFunc) {
    auto expr = node;

    TMaybeNode<TCoFlatMap> flatmap;
    if (auto maybeNode = expr.Maybe<TCoFlatMap>()) {
        flatmap = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoSkipNullMembers> skipNullMembers;
    if (auto maybeNode = expr.Maybe<TCoSkipNullMembers>()) {
        skipNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoFilterNullMembers> filterNullMembers;
    if (auto maybeNode = expr.Maybe<TCoFilterNullMembers>()) {
        filterNullMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    TMaybeNode<TCoExtractMembers> extractMembers;
    if (auto maybeNode = expr.Maybe<TCoExtractMembers>()) {
        extractMembers = maybeNode;
        expr = maybeNode.Cast().Input();
    }

    if (!matchFunc(expr)) {
        return {};
    }

    return TKqpMatchReadResult {
        .Read = expr,
        .ExtractMembers = extractMembers,
        .FilterNullMembers = filterNullMembers,
        .SkipNullMembers = skipNullMembers,
        .FlatMap = flatmap
    };
}

TMaybe<TPrefixLookup> RewriteReadToPrefixLookup(TExprBase read, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, TMaybe<size_t> maxKeys) {
    if (maxKeys == TMaybe<size_t>(0)) {
        return {};
    }
    if (auto readTable = read.Maybe<TKqlReadTableBase>()) {
        return RewriteReadToPrefixLookup(readTable.Cast(), ctx, kqpCtx);
    } else {
        auto readRanges = read.Maybe<TKqlReadTableRangesBase>();
        YQL_ENSURE(readRanges);
        return RewriteReadToPrefixLookup(readRanges.Cast(), ctx, kqpCtx, maxKeys);
    }
}

} // namespace NKikimr::NKqp::NOpt
