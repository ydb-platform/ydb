#include "kqp_opt_log_impl.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

bool GetEquiJoinKeyTypes(TExprBase leftInput, const TString& leftColumnName, const TKikimrTableDescription& rightTable,
    const TString& rightColumnName, const TTypeAnnotationNode*& leftData, const TTypeAnnotationNode*& rightData)
{
    auto rightType = rightTable.GetColumnType(rightColumnName);
    YQL_ENSURE(rightType);
    if (rightType->GetKind() == ETypeAnnotationKind::Optional) {
        rightType = rightType->Cast<TOptionalExprType>()->GetItemType();
    }

    auto leftInputType = leftInput.Ref().GetTypeAnn();
    YQL_ENSURE(leftInputType);
    YQL_ENSURE(leftInputType->GetKind() == ETypeAnnotationKind::List);
    auto itemType = leftInputType->Cast<TListExprType>()->GetItemType();
    YQL_ENSURE(itemType->GetKind() == ETypeAnnotationKind::Struct);
    auto structType = itemType->Cast<TStructExprType>();
    auto memberIndex = structType->FindItem(leftColumnName);
    YQL_ENSURE(memberIndex, "Column '" << leftColumnName << "' not found in " << *((TTypeAnnotationNode*) structType));

    auto leftType = structType->GetItems()[*memberIndex]->GetItemType();
    if (leftType->GetKind() == ETypeAnnotationKind::Optional) {
        leftType = leftType->Cast<TOptionalExprType>()->GetItemType();
    }

    if (rightType->GetKind() != ETypeAnnotationKind::Data || leftType->GetKind() != ETypeAnnotationKind::Data) {
        Y_ENSURE(rightType->GetKind() == ETypeAnnotationKind::Pg);
        Y_ENSURE(leftType->GetKind() == ETypeAnnotationKind::Pg);
        rightData = rightType->Cast<TPgExprType>();
        leftData = leftType->Cast<TPgExprType>();
        return true;
    }

    rightData = rightType->Cast<TDataExprType>();
    leftData = leftType->Cast<TDataExprType>();
    return true;
}

TExprBase ConvertToTuples(const TSet<TString>& columns, const TCoArgument& structArg, TExprContext& ctx,
    TPositionHandle pos)
{
    TVector<TExprBase> tuples{Reserve(columns.size())};

    for (const auto& key : columns) {
        tuples.emplace_back(Build<TCoMember>(ctx, pos)
            .Struct(structArg)
            .Name().Build(key)
            .Done());
    }

    if (tuples.size() == 1) {
        return tuples[0];
    }

    return Build<TExprList>(ctx, pos)
        .Add(tuples)
        .Done();
}

TExprBase DeduplicateByMembers(const TExprBase& expr,  const TMaybeNode<TCoLambda>& filter, const TSet<TString>& members,
    TExprContext& ctx, TPositionHandle pos)
{
    TMaybeNode<TCoLambda> lambda;
    if (filter.IsValid()) {
        lambda = Build<TCoLambda>(ctx, pos)
            .Args({"tuple"})
            .Body<TCoTake>()
                .Input<TCoFilter>()
                    .Input<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("1").Build()
                        .Build()
                    .Lambda(filter.Cast())
                    .Build()
                .Count<TCoUint64>()
                    .Literal().Value("1").Build()
                    .Build()
                .Build()
            .Done();
    } else {
        lambda = Build<TCoLambda>(ctx, pos)
            .Args({"tuple"})
            .Body<TCoTake>()
                .Input<TCoNth>()
                    .Tuple("tuple")
                    .Index().Value("1").Build()
                    .Build()
                .Count<TCoUint64>()
                    .Literal().Value("1").Build()
                    .Build()
                .Build()
            .Done();
    }

    auto structArg = Build<TCoArgument>(ctx, pos)
        .Name("struct")
        .Done();

    return Build<TCoPartitionByKey>(ctx, pos)
            .Input(expr)
            .KeySelectorLambda()
                .Args(structArg)
                .Body(ConvertToTuples(members, structArg, ctx, pos))
                .Build()
            .SortDirections<TCoVoid>()
                .Build()
            .SortKeySelectorLambda<TCoVoid>()
                .Build()
            .ListHandlerLambda()
                .Args({"stream"})
                .Body<TCoFlatMap>()
                    .Input("stream")
                    .Lambda(lambda.Cast())
                    .Build()
                .Build()
            .Done();
}


[[maybe_unused]]
bool IsKqlPureExpr(const TExprBase& expr) {
    auto node = FindNode(expr.Ptr(), [](const TExprNode::TPtr& node) {
        return node->IsCallable()
            && (node->Content().StartsWith("Kql")
                || node->Content().StartsWith("Kqp")
                || node->Content().StartsWith("Dq"));
    });
    return node.Get() == nullptr;
}

TDqJoin FlipLeftSemiJoin(const TDqJoin& join, TExprContext& ctx) {
    Y_DEBUG_ABORT_UNLESS(join.JoinType().Value() == "LeftSemi");

    auto joinKeysBuilder = Build<TDqJoinKeyTupleList>(ctx, join.Pos());
    for (const auto& keys : join.JoinKeys()) {
        joinKeysBuilder.Add<TDqJoinKeyTuple>()
            .LeftLabel(keys.RightLabel())
            .LeftColumn(keys.RightColumn())
            .RightLabel(keys.LeftLabel())
            .RightColumn(keys.LeftColumn())
            .Build();
    }

    return Build<TDqJoin>(ctx, join.Pos())
        .LeftInput(join.RightInput())
        .LeftLabel(join.RightLabel())
        .RightInput(join.LeftInput())
        .RightLabel(join.LeftLabel())
        .JoinType().Build("RightSemi")
        .JoinKeys(joinKeysBuilder.Done())
        .Done();
}

TExprBase BuildLookupIndex(TExprContext& ctx, const TPositionHandle pos,
    const TKqpTable& table, const TCoAtomList& columns,
    const TExprBase& keysToLookup, const TVector<TCoAtom>& skipNullColumns, const TString& indexName,
    const TKqpOptimizeContext& kqpCtx)
{
    if (kqpCtx.IsScanQuery()) {
        YQL_ENSURE(kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin, "Stream lookup is not enabled for index lookup join");
        return Build<TKqlStreamLookupIndex>(ctx, pos)
            .Table(table)
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(skipNullColumns)
                    .Build()
                .Build()
            .Columns(columns)
            .Index()
                .Build(indexName)
            .Done();
    }

    return Build<TKqlLookupIndex>(ctx, pos)
        .Table(table)
        .LookupKeys<TCoSkipNullMembers>()
            .Input(keysToLookup)
            .Members()
                .Add(skipNullColumns)
                .Build()
            .Build()
        .Columns(columns)
        .Index()
            .Build(indexName)
        .Done();
}

TExprBase BuildLookupTable(TExprContext& ctx, const TPositionHandle pos,
    const TKqpTable& table, const TCoAtomList& columns,
    const TExprBase& keysToLookup, const TVector<TCoAtom>& skipNullColumns, const TKqpOptimizeContext& kqpCtx)
{
    if (kqpCtx.IsScanQuery()) {
        YQL_ENSURE(kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin, "Stream lookup is not enabled for index lookup join");
        return Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(table)
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(skipNullColumns)
                    .Build()
                .Build()
            .Columns(columns)
            .LookupStrategy().Build(TKqpStreamLookupStrategyName)
            .Done();
    }

    if (kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
        return Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(table)
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(skipNullColumns)
                    .Build()
                .Build()
            .Columns(columns)
            .LookupStrategy().Build(TKqpStreamLookupStrategyName)
            .Done();
    }

    return Build<TKqlLookupTable>(ctx, pos)
        .Table(table)
        .LookupKeys<TCoSkipNullMembers>()
            .Input(keysToLookup)
            .Members()
                .Add(skipNullColumns)
                .Build()
            .Build()
        .Columns(columns)
        .Done();
}

TVector<TExprBase> CreateRenames(const TMaybeNode<TCoFlatMap>& rightFlatmap, const TCoAtomList& tableColumns,
    const TCoArgument& arg, const TStringBuf& rightLabel, TPositionHandle pos, TExprContext& ctx)
{
    TVector<TExprBase> renames;
    if (rightFlatmap) {
        const auto& flatMapType = GetSeqItemType(*rightFlatmap.Ref().GetTypeAnn());
        YQL_ENSURE(flatMapType.GetKind() == ETypeAnnotationKind::Struct);
        renames.reserve(flatMapType.Cast<TStructExprType>()->GetSize());

        for (const auto& column : flatMapType.Cast<TStructExprType>()->GetItems()) {
            renames.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name<TCoAtom>()
                        .Build(Join('.', rightLabel, column->GetName()))
                    .Value<TCoMember>()
                        .Struct(arg)
                        .Name<TCoAtom>()
                            .Build(column->GetName())
                        .Build()
                    .Done());
        }
    } else {
        renames.reserve(tableColumns.Size());

        for (const auto& column : tableColumns) {
            renames.emplace_back(
                Build<TCoNameValueTuple>(ctx, pos)
                    .Name<TCoAtom>()
                        .Build(Join('.', rightLabel, column.Value()))
                    .Value<TCoMember>()
                        .Struct(arg)
                        .Name(column)
                        .Build()
                    .Done());
        }
    }
    return renames;
}

bool IsParameterToListOfStructsRepack(const TExprBase& expr) {
    // Looking for next patterns:
    //  - (FlatMap $in (lambda '($x) (Just (AsStruct '('"key" $x)))))
    //  - (FlatMap $in (lambda '($x) (Just (AsStruct '('"key" (Nth $x '0)) '('"key2" (Nth $x '1))) ...)))

    if (!expr.Maybe<TCoFlatMap>().Input().Maybe<TCoParameter>()) {
        return false;
    }
    auto lambda = expr.Cast<TCoFlatMap>().Lambda();
    if (lambda.Args().Size() != 1) {
        return false;
    }
    if (!lambda.Body().Maybe<TCoJust>().Input().Maybe<TCoAsStruct>()) {
        return false;
    }
    auto lambdaArg = lambda.Args().Arg(0).Raw();
    auto asStruct = lambda.Body().Cast<TCoJust>().Input().Cast<TCoAsStruct>();

    for (const auto& member : asStruct.Args()) {
        if (member->Child(1) == lambdaArg) {
            continue;
        }
        if (member->Child(1)->IsCallable("Nth") && member->Child(1)->Child(0) == lambdaArg) {
            continue;
        }
        return false;
    }

    return true;
}

//#define DBG(...) YQL_CLOG(DEBUG, ProviderKqp) << __VA_ARGS__
#define DBG(...)

template<typename ReadType>
TMaybeNode<TExprBase> BuildKqpStreamIndexLookupJoin(
    const TDqJoin& join,
    TExprBase leftInput,
    TCoAtomList lookupColumns,
    const TKqpMatchReadResult& rightReadMatch,
    TMaybeNode<TCoLambda> extraRightFilter,
    TExprContext& ctx)
{
    TString leftLabel = join.LeftLabel().Maybe<TCoAtom>() ? TString(join.LeftLabel().Cast<TCoAtom>().Value()) : "";
    TString rightLabel = join.RightLabel().Maybe<TCoAtom>() ? TString(join.RightLabel().Cast<TCoAtom>().Value()) : "";
    auto rightRead = rightReadMatch.Read.template Cast<ReadType>();

    TExprBase lookupJoin = Build<TKqlStreamLookupTable>(ctx, join.Pos())
        .Table(rightRead.Table())
        .LookupKeys(leftInput)
        .Columns(lookupColumns)
        .LookupStrategy().Build(TKqpStreamLookupJoinStrategyName)
        .Done();

    // Stream lookup join output: stream<tuple<left_row_struct, optional<right_row_struct>>>
    // so we should apply filters to second element of tuple for each row

    if (extraRightFilter.IsValid()) {
        lookupJoin = Build<TCoMap>(ctx, join.Pos())
            .Input(lookupJoin)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Add<TCoFlatMap>()
                        .Input<TCoNth>()
                            .Tuple("tuple")
                            .Index().Value("1").Build()
                            .Build()
                        .Lambda(extraRightFilter.Cast())
                        .Build()    
                    .Build()  
                .Build()    
            .Done();
    }

    if (rightReadMatch.ExtractMembers) {
        lookupJoin = Build<TCoMap>(ctx, join.Pos())
            .Input(lookupJoin)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Add<TCoExtractMembers>()
                        .Input<TCoNth>()
                            .Tuple("tuple")
                            .Index().Value("1").Build()
                            .Build()
                        .Members(rightReadMatch.ExtractMembers.Cast().Members())
                        .Build()    
                    .Build()
                .Build()
            .Done();
    }    

    if (rightReadMatch.FilterNullMembers) {
        lookupJoin = Build<TCoMap>(ctx, join.Pos())
            .Input(lookupJoin)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Add<TCoFilterNullMembers>()
                        .Input<TCoNth>()
                            .Tuple("tuple")
                            .Index().Value("1").Build()
                            .Build()
                        .Members(rightReadMatch.FilterNullMembers.Cast().Members())
                        .Build()    
                    .Build()
                .Build()
            .Done();
    }
    
    if (rightReadMatch.SkipNullMembers) {
        lookupJoin = Build<TCoMap>(ctx, join.Pos())
            .Input(lookupJoin)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Add<TCoSkipNullMembers>()
                        .Input<TCoNth>()
                            .Tuple("tuple")
                            .Index().Value("1").Build()
                            .Build()
                        .Members(rightReadMatch.SkipNullMembers.Cast().Members())
                        .Build()    
                    .Build()
                .Build()
            .Done();
    }

    if (rightReadMatch.FlatMap) {
        lookupJoin = Build<TCoMap>(ctx, join.Pos())
            .Input(lookupJoin)
            .Lambda()
                .Args({"tuple"})
                .Body<TExprList>()
                    .Add<TCoNth>()
                        .Tuple("tuple")
                        .Index().Value("0").Build()
                        .Build()
                    .Add<TCoFlatMap>()
                        .Input<TCoNth>()
                            .Tuple("tuple")
                            .Index().Value("1").Build()
                            .Build()
                        .Lambda(rightReadMatch.FlatMap.Cast().Lambda())
                        .Build()    
                    .Build()  
                .Build()    
            .Done();
    }

    return Build<TKqlIndexLookupJoin>(ctx, join.Pos())
        .Input(lookupJoin)
        .LeftLabel().Build(leftLabel)
        .RightLabel().Build(rightLabel)
        .JoinType(join.JoinType())
        .Done();
}

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

template<typename TFieldSet>
bool ExtractUsedFields(const TExprNode::TPtr& start, const TExprNode& arg, TFieldSet& usedFields, const TParentsMap& parentsMap, bool allowDependsOn) {
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
        } else if (allowDependsOn && parent->IsCallable("DependsOn")) {
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

void ExpandColumns(TMaybeNode<TCoLambda>& filter, const TSet<TString>& usedColumns, TMaybeNode<TCoAtomList>& lookupColumns, TExprContext& ctx) {
    TSet<TString> lookupColumnsSet;
    for (auto&& column : lookupColumns.Cast()) {
        lookupColumnsSet.insert(column.StringValue());
    }
    bool rebuildColumns = false;
    for (auto& column : usedColumns) {
        if (!lookupColumnsSet.contains(column)) {
            lookupColumnsSet.insert(column);
            rebuildColumns = true;
        }
    }
    if (rebuildColumns) {
        TVector<TCoAtom> newColumns;
        auto pos = filter.Cast().Pos();
        for (auto& column : lookupColumnsSet) {
            newColumns.push_back(Build<TCoAtom>(ctx, pos).Value(column).Done());
        }
        auto arg = Build<TCoArgument>(ctx, pos).Name("_extract_members_arg").Done();
        filter = Build<TCoLambda>(ctx, pos)
            .Args({arg})
            .Body<TCoExtractMembers>()
                .Members(lookupColumns.Cast())
                .Input<TCoFlatMap>()
                    .Lambda(filter.Cast())
                    .Input<TCoJust>().Input(arg).Build()
                    .Build()
                .Build()
            .Done();
        lookupColumns = Build<TCoAtomList>(ctx, pos)
            .Add(newColumns)
            .Done();
    }
}

template<typename ReadType>
TMaybeNode<TExprBase> KqpJoinToIndexLookupImpl(const TDqJoin& join, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap & parentsMap) {
    static_assert(std::is_same_v<ReadType, TKqlReadTableBase> || std::is_same_v<ReadType, TKqlReadTableRangesBase>, "unsupported read type");

    if (!join.RightLabel().Maybe<TCoAtom>()) {
        // Lookup only in tables
        return {};
    }

    static THashSet<TStringBuf> supportedJoinKinds = {"Inner", "Left", "LeftOnly", "LeftSemi", "RightSemi"};
    static THashSet<TStringBuf> supportedStreamJoinKinds = {"Inner", "Left", "LeftOnly"};
    if (!supportedJoinKinds.contains(join.JoinType().Value())) {
        return {};
    }

    TString lookupTable;
    TString indexName;

    auto rightReadMatch = MatchRead<ReadType>(join.RightInput());
    if (!rightReadMatch || rightReadMatch->FlatMap && !IsPassthroughFlatMap(rightReadMatch->FlatMap.Cast(), nullptr)) {
        return {};
    }

    auto rightRead = rightReadMatch->Read.template Cast<ReadType>();

    TMaybeNode<TCoAtomList> lookupColumns;
    size_t rightPrefixSize;
    TMaybeNode<TExprBase> rightPrefixExpr;
    TMaybeNode<TCoLambda> extraRightFilter;

    if constexpr (std::is_same_v<ReadType, TKqlReadTableBase>) {
        Y_ENSURE(rightRead.template Maybe<TKqlReadTable>() || rightRead.template Maybe<TKqlReadTableIndex>());
        const TKqlReadTableBase read = rightRead;
        if (!read.Table().SysView().Value().empty()) {
            // Can't lookup in system views
            return {};
        }

        if (auto indexRead = rightRead.template Maybe<TKqlReadTableIndex>()) {
            indexName = indexRead.Cast().Index().StringValue();
            lookupTable = GetIndexMetadata(indexRead.Cast(), *kqpCtx.Tables, kqpCtx.Cluster)->Name;
        } else {
            lookupTable = read.Table().Path().StringValue();
        }
        const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);

        auto from = read.Range().From();
        auto to = read.Range().To();

        TSet<TString> usedColumns;
        rightPrefixSize = 0;
        while (rightPrefixSize < from.ArgCount() && rightPrefixSize < to.ArgCount()) {
            if (from.Arg(rightPrefixSize).Raw() != to.Arg(rightPrefixSize).Raw()) {
                break;
            }
            usedColumns.insert(rightTableDesc.Metadata->KeyColumnNames[rightPrefixSize]);
            ++rightPrefixSize;
        }

        lookupColumns = read.Columns();

        if (!(rightPrefixSize == from.ArgCount() &&
             rightPrefixSize == to.ArgCount() && 
             from.Maybe<TKqlKeyInc>() &&
             to.Maybe<TKqlKeyInc>()))
        {
            extraRightFilter = MakeFilterForRange(read.Range(), ctx, read.Range().Pos(), rightTableDesc.Metadata->KeyColumnNames);
            ExpandColumns(extraRightFilter, usedColumns, lookupColumns, ctx);
        }

        TVector<TExprBase> columns;
        for (size_t i = 0; i < rightPrefixSize; ++i) {
            columns.push_back(TExprBase(from.Arg(i)));
        }

        rightPrefixExpr = Build<TCoAsList>(ctx, join.Pos())
            .Add<TExprList>()
                .Add(columns)
                .Build()
            .Done();
    } else if constexpr (std::is_same_v<ReadType, TKqlReadTableRangesBase>){
        auto read = rightReadMatch->Read.template Cast<TKqlReadTableRangesBase>();
        if (!read.Table().SysView().Value().empty()) {
            // Can't lookup in system views
            return {};
        }

        lookupColumns = read.Columns();

        if (auto indexRead = read.template Maybe<TKqlReadTableIndexRanges>()) {
            const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, read.Table().Path());
            const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(indexRead.Index().Cast().StringValue());
            lookupTable = indexMeta->Name;
            indexName = indexRead.Cast().Index().StringValue();
        } else {
            lookupTable = read.Table().Path().StringValue();
        }

        if (TCoVoid::Match(read.Ranges().Raw())) {
            rightPrefixSize = 0;
            rightPrefixExpr = Build<TCoJust>(ctx, join.Pos())
                .Input<TCoAsList>().Build()
                .Done();
        } else {
            auto prompt = TKqpReadTableExplainPrompt::Parse(read);

            rightPrefixSize = prompt.PointPrefixLen;

            const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);

            TMaybeNode<TExprBase> rowsExpr;
            TMaybeNode<TCoLambda> filter;
            if (read.template Maybe<TKqlReadTableRanges>()) {
                rowsExpr = read.template Cast<TKqlReadTableRanges>().PrefixPointsExpr();
                filter = read.template Cast<TKqlReadTableRanges>().PredicateExpr();
            }
            if (read.template Maybe<TKqlReadTableIndexRanges>()) {
                rowsExpr = read.template Cast<TKqlReadTableIndexRanges>().PrefixPointsExpr();
                filter = read.template Cast<TKqlReadTableIndexRanges>().PredicateExpr();
            }

            if (!rowsExpr.IsValid()) {
                return {};
            }

            if (!prompt.ExpectedMaxRanges || *prompt.ExpectedMaxRanges > kqpCtx.Config->IdxLookupJoinsPrefixPointLimit) {
                return {};
            }

            if (prompt.PointPrefixLen != prompt.UsedKeyColumns.size() || prompt.ExpectedMaxRanges != TMaybe<ui64>(1)) {
                if (!filter.IsValid()) {
                    return {};
                }
                TSet<TString> usedColumns;
                if (!ExtractUsedFields(
                        filter.Body().Cast().Ptr(),
                        filter.Args().Cast().Arg(0).Ref(),
                        usedColumns,
                        parentsMap,
                        true))
                {
                    return {};
                }
                extraRightFilter = ctx.DeepCopyLambda(filter.Ref());
                ExpandColumns(extraRightFilter, usedColumns, lookupColumns, ctx);
            }

            size_t prefixLen = prompt.PointPrefixLen;
            TVector<TString> keyColumns;
            for (size_t i = 0; i < prefixLen; ++i) {
                YQL_ENSURE(i < rightTableDesc.Metadata->KeyColumnNames.size());
                keyColumns.push_back(rightTableDesc.Metadata->KeyColumnNames[i]);
            }


            auto rowArg = Build<TCoArgument>(ctx, join.Pos())
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

            rightPrefixExpr = Build<TCoMap>(ctx, join.Pos())
                .Input(rowsExpr.Cast())
                .Lambda()
                    .Args({rowArg})
                    .Body<TExprList>()
                        .Add(components)
                        .Build()
                    .Build()
                .Done();
        }
    }

    Y_ENSURE(rightPrefixExpr.IsValid());

    const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);
    if (rightTableDesc.Metadata->Kind == NYql::EKikimrTableKind::Olap) {
        return {};
    }

    if (!kqpCtx.Config->PredicateExtract20 && extraRightFilter.IsValid()) {
        return {};
    }

    TMap<std::string_view, TString> rightJoinKeyToLeft;
    TVector<TCoAtom> rightKeyColumns;
    rightKeyColumns.reserve(join.JoinKeys().Size());
    TSet<TString> leftJoinKeys;
    std::map<std::string_view, std::set<TString>> equalLeftKeys;

    for (ui32 i = 0; i < join.JoinKeys().Size(); ++i) {
        const auto& keyTuple = join.JoinKeys().Item(i);

        auto leftKey = join.LeftLabel().Maybe<TCoVoid>()
            ? Join('.', keyTuple.LeftLabel().Value(), keyTuple.LeftColumn().Value())
            : keyTuple.LeftColumn().StringValue();

        rightKeyColumns.emplace_back(keyTuple.RightColumn()); // unique elements

        auto [iter, newValue] = rightJoinKeyToLeft.emplace(keyTuple.RightColumn().Value(), leftKey);
        if (!newValue) {
            equalLeftKeys[iter->second].emplace(leftKey);
        }

        leftJoinKeys.emplace(leftKey);
    }

    auto leftRowArg = Build<TCoArgument>(ctx, join.Pos())
        .Name("leftRowArg")
        .Done();

    auto prefixRowArg = Build<TCoArgument>(ctx, join.Pos())
        .Name("prefixArg")
        .Done();

    TVector<TExprBase> lookupMembers;
    TVector<TCoAtom> skipNullColumns;
    ui32 fixedPrefix = 0;
    TSet<TString> deduplicateLeftColumns;
    TVector<TExprBase> prefixFilters;
    for (auto& rightColumnName : rightTableDesc.Metadata->KeyColumnNames) {
        TExprNode::TPtr member;

        auto leftColumn = rightJoinKeyToLeft.FindPtr(rightColumnName);

        if (fixedPrefix < rightPrefixSize) {
            if (leftColumn) {
                prefixFilters.push_back(
                    Build<TCoCmpEqual>(ctx, join.Pos())
                        .Left<TCoNth>()
                            .Tuple(prefixRowArg)
                            .Index().Value(ToString(fixedPrefix)).Build()
                            .Build()
                        .Right<TCoMember>()
                            .Struct(leftRowArg)
                            .Name().Build(*leftColumn)
                            .Build()
                        .Done());
                deduplicateLeftColumns.insert(*leftColumn);
                if (!kqpCtx.Config->PredicateExtract20) {
                    return {};
                }
            }

            member = Build<TCoNth>(ctx, prefixRowArg.Pos())
                .Tuple(prefixRowArg)
                .Index().Value(ToString(fixedPrefix)).Build()
                .Done().Ptr();
            fixedPrefix++;
        } else {
            if (!leftColumn) {
                break;
            }
            deduplicateLeftColumns.insert(*leftColumn);

            member = Build<TCoMember>(ctx, join.Pos())
                .Struct(leftRowArg)
                .Name().Build(*leftColumn)
                .Done().Ptr();

            const TTypeAnnotationNode* leftType;
            const TTypeAnnotationNode* rightType;
            if (!GetEquiJoinKeyTypes(join.LeftInput(), *leftColumn, rightTableDesc, rightColumnName, leftType, rightType)) {
                return {};
            }

            if (leftType->GetKind() == ETypeAnnotationKind::Pg) {
                Y_ENSURE(rightType->GetKind() == ETypeAnnotationKind::Pg);
                auto* leftPgType = static_cast<const TPgExprType*>(leftType);
                auto* rightPgType = static_cast<const TPgExprType*>(rightType);
                if (leftPgType != rightPgType) {
                    // TODO: Emit PgCast
                    return {};
                }
            } else {
                Y_ENSURE(leftType->GetKind() == ETypeAnnotationKind::Data);
                Y_ENSURE(rightType->GetKind() == ETypeAnnotationKind::Data);
                auto* leftDataType = static_cast<const TDataExprType*>(leftType);
                auto* rightDataType = static_cast<const TDataExprType*>(rightType);
                if (leftDataType != rightDataType) {
                    bool canCast = IsDataTypeNumeric(leftDataType->GetSlot()) && IsDataTypeNumeric(rightDataType->GetSlot());
                    if (!canCast) {
                        canCast = leftDataType->GetName() == "Utf8" && rightDataType->GetName() == "String";
                    }
                    if (canCast) {
                        DBG("------ cast " << leftDataType->GetName() << " to " << rightDataType->GetName());
                        member = Build<TCoConvert>(ctx, join.Pos())
                            .Input(member)
                            .Type().Build(rightDataType->GetName())
                            .Done().Ptr();
                    } else {
                        DBG("------ can not cast " << leftDataType->GetName() << " to " << rightDataType->GetName());
                        return {};
                    }
                }
            }
        }

        lookupMembers.emplace_back(
            Build<TExprList>(ctx, join.Pos())
                .Add<TCoAtom>().Build(rightColumnName)
                .Add(member)
                .Done());

        if (leftColumn) {
            skipNullColumns.emplace_back(ctx.NewAtom(join.Pos(), rightColumnName));
        }
    }

    if (lookupMembers.size() <= fixedPrefix) {
        return {};
    }

    const bool useStreamIndexLookupJoin = (kqpCtx.IsDataQuery() || kqpCtx.IsGenericQuery())
        && kqpCtx.Config->EnableKqpDataQueryStreamIdxLookupJoin
        && supportedStreamJoinKinds.contains(join.JoinType().Value());

    bool needPrecomputeLeft = (kqpCtx.IsDataQuery() || kqpCtx.IsGenericQuery())
        && !join.LeftInput().Maybe<TCoParameter>()
        && !IsParameterToListOfStructsRepack(join.LeftInput())
        && !useStreamIndexLookupJoin;

    TExprBase leftData = needPrecomputeLeft
        ? Build<TDqPrecompute>(ctx, join.Pos())
            .Input(join.LeftInput())
            .Done()
        : join.LeftInput();

    TMaybeNode<TCoLambda> filter;

    if (!equalLeftKeys.empty()) {
        auto row = Build<TCoArgument>(ctx, join.Pos())
            .Name("row")
            .Done();

        TVector<TExprBase> conditions;

        for (auto [first, others]: equalLeftKeys) {
            auto v = Build<TCoMember>(ctx, join.Pos())
                .Struct(row)
                .Name().Build(first)
                .Done();

            for (std::string_view other: others) {
                conditions.emplace_back(
                    Build<TCoCmpEqual>(ctx, join.Pos())
                        .Left(v)
                        .Right<TCoMember>()
                            .Struct(row)
                            .Name().Build(other)
                            .Build()
                        .Done());
            }
        }

        filter = Build<TCoLambda>(ctx, join.Pos())
            .Args({row})
            .Body<TCoCoalesce>()
                .Predicate<TCoAnd>()
                    .Add(conditions)
                    .Build()
                .Value<TCoBool>()
                    .Literal().Build("false")
                    .Build()
                .Build()
            .Done();
    }

    auto wrapWithPrefixFilters = [&](TExprBase body) -> TExprBase {
        if (prefixFilters.empty()) {
            return Build<TCoJust>(ctx, body.Pos())
                .Input(body)
                .Done();
        } else {
            return Build<TCoOptionalIf>(ctx, body.Pos())
            .Predicate<TCoCoalesce>()
                .Predicate<TCoAnd>()
                    .Add(prefixFilters)
                    .Build()
                .Value<TCoBool>()
                    .Literal().Build("false")
                    .Build()
                .Build()
            .Value(body)
            .Done();
        }
    };

    if (useStreamIndexLookupJoin) {
        auto leftInput = Build<TCoFlatMap>(ctx, join.Pos())
            .Input(leftData)
            .Lambda()
                .Args({leftRowArg})
                .Body<TCoFlatMap>()
                    .Input(rightPrefixExpr.Cast())
                    .Lambda()
                        .Args({prefixRowArg})
                        .Body(wrapWithPrefixFilters(
                                Build<TExprList>(ctx, join.Pos())
                                    .Add<TCoAsStruct>()
                                        .Add(lookupMembers)
                                        .Build()
                                    .Add(leftRowArg)
                                    .Done()))
                        .Build()
                    .Build()
                .Build()
            .Done();

        return BuildKqpStreamIndexLookupJoin<ReadType>(join, leftInput, lookupColumns.Cast(), *rightReadMatch, extraRightFilter, ctx);
    }

    auto leftDataDeduplicated = DeduplicateByMembers(leftData, filter, deduplicateLeftColumns, ctx, join.Pos());
    auto keysToLookup = Build<TCoFlatMap>(ctx, join.Pos())
        .Input(leftDataDeduplicated)
        .Lambda()
            .Args({leftRowArg})
            .Body<TCoFlatMap>()
                .Input(rightPrefixExpr.Cast())
                .Lambda()
                    .Args({prefixRowArg})
                    .Body(wrapWithPrefixFilters(Build<TCoAsStruct>(ctx, join.Pos()).Add(lookupMembers).Done()))
                .Build()
            .Build()
        .Build()
        .Done();

    TExprBase lookup = indexName
        ? BuildLookupIndex(ctx, join.Pos(), rightRead.Table(), lookupColumns.Cast(), keysToLookup, skipNullColumns, indexName, kqpCtx)
        : BuildLookupTable(ctx, join.Pos(), rightRead.Table(), lookupColumns.Cast(), keysToLookup, skipNullColumns, kqpCtx);

    if (extraRightFilter.IsValid()) {
        lookup = Build<TCoFlatMap>(ctx, join.Pos())
            .Input(lookup)
            .Lambda(extraRightFilter.Cast())
            .Done();
    }

    // Skip null keys in lookup part as for equijoin semantics null != null,
    // so we can't have nulls in lookup part
    lookup = Build<TCoSkipNullMembers>(ctx, join.Pos())
        .Input(lookup)
        .Members()
            .Add(rightKeyColumns)
            .Build()
        .Done();

    TMaybeNode<TCoAtomList> rightColumns = rightRead.Columns();
    if (rightReadMatch->ExtractMembers) {
        rightColumns = rightReadMatch->ExtractMembers.Cast().Members();
    }

    lookup = rightReadMatch->BuildProcessNodes(lookup, ctx);

    if (join.JoinType().Value() == "RightSemi") {
        auto arg = TCoArgument(ctx.NewArgument(join.Pos(), "row"));
        auto rightLabel = join.RightLabel().Cast<TCoAtom>().Value();

        TVector<TExprBase> renames = CreateRenames(rightReadMatch->FlatMap, rightColumns.Cast(), arg, rightLabel,
            join.Pos(), ctx);

        lookup = Build<TCoMap>(ctx, join.Pos())
            .Input(lookup)
            .Lambda()
                .Args({arg})
                .Body<TCoAsStruct>()
                    .Add(renames)
                    .Build()
                .Build()
            .Done();

        return lookup;
    }

    return Build<TDqJoin>(ctx, join.Pos())
        .LeftInput(leftData)
        .LeftLabel(join.LeftLabel())
        .RightInput(lookup)
        .RightLabel(join.RightLabel())
        .JoinType(join.JoinType())
        .JoinKeys(join.JoinKeys())
        .Done();
}

} // anonymous namespace

TExprBase KqpJoinToIndexLookup(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx, const NYql::TParentsMap & parentsMap)
{
    if ((kqpCtx.IsScanQuery() && !kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin) || !node.Maybe<TDqJoin>()) {
        return node;
    }
    auto join = node.Cast<TDqJoin>();

    DBG("-- Join: " << KqpExprToPrettyString(join, ctx));

    // SqlIn support (preferred lookup direction)
    if (join.JoinType().Value() == "LeftSemi") {
        auto flipJoin = FlipLeftSemiJoin(join, ctx);
        DBG("-- Flip join");

        if (auto indexLookupJoin = KqpJoinToIndexLookupImpl<TKqlReadTableBase>(flipJoin, ctx, kqpCtx, parentsMap)) {
            return indexLookupJoin.Cast();
        } else if (auto indexLookupJoin = KqpJoinToIndexLookupImpl<TKqlReadTableRangesBase>(flipJoin, ctx, kqpCtx, parentsMap)) {
            return indexLookupJoin.Cast();
        }
    }

    if (auto indexLookupJoin = KqpJoinToIndexLookupImpl<TKqlReadTableBase>(join, ctx, kqpCtx, parentsMap)) {
        return indexLookupJoin.Cast();
    } else if (auto indexLookupJoin = KqpJoinToIndexLookupImpl<TKqlReadTableRangesBase>(join, ctx, kqpCtx, parentsMap)) {
        return indexLookupJoin.Cast();
    }

    return node;
}

#undef DBG

} // namespace NKikimr::NKqp::NOpt
