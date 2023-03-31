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
    const TString& rightColumnName, const TDataExprType*& leftData, const TDataExprType*& rightData)
{
    auto rightType = rightTable.GetColumnType(rightColumnName);
    YQL_ENSURE(rightType);
    if (rightType->GetKind() == ETypeAnnotationKind::Optional) {
        rightType = rightType->Cast<TOptionalExprType>()->GetItemType();
    }

    YQL_ENSURE(rightType->GetKind() == ETypeAnnotationKind::Data);
    rightData = rightType->Cast<TDataExprType>();

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

    if (leftType->GetKind() != ETypeAnnotationKind::Data) {
        return false;
    }

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
    Y_VERIFY_DEBUG(join.JoinType().Value() == "LeftSemi");

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

TMaybeNode<TKqlKeyInc> GetRightTableKeyPrefix(const TKqlKeyRange& range) {
    if (!range.From().Maybe<TKqlKeyInc>() || !range.To().Maybe<TKqlKeyInc>()) {
        return {};
    }
    auto rangeFrom = range.From().Cast<TKqlKeyInc>();
    auto rangeTo = range.To().Cast<TKqlKeyInc>();

    if (rangeFrom.ArgCount() != rangeTo.ArgCount()) {
        return {};
    }
    for (ui32 i = 0; i < rangeFrom.ArgCount(); ++i) {
        if (rangeFrom.Arg(i).Raw() != rangeTo.Arg(i).Raw()) {
            return {};
        }
    }

    return rangeFrom;
}

TExprBase BuildLookupIndex(TExprContext& ctx, const TPositionHandle pos, const TKqlReadTableBase& read,
    const TExprBase& keysToLookup, const TVector<TCoAtom>& lookupNames, const TString& indexName,
    const TKqpOptimizeContext& kqpCtx)
{
    if (kqpCtx.IsScanQuery()) {
        YQL_ENSURE(kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin, "Stream lookup is not enabled for index lookup join");
        return Build<TKqlStreamLookupIndex>(ctx, pos)
            .Table(read.Table())
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(lookupNames)
                    .Build()
                .Build()
            .Columns(read.Columns())
            .Index()
                .Build(indexName)
            .Done();
    }

    return Build<TKqlLookupIndex>(ctx, pos)
        .Table(read.Table())
        .LookupKeys<TCoSkipNullMembers>()
            .Input(keysToLookup)
            .Members()
                .Add(lookupNames)
                .Build()
            .Build()
        .Columns(read.Columns())
        .Index()
            .Build(indexName)
        .Done();
}

TExprBase BuildLookupTable(TExprContext& ctx, const TPositionHandle pos, const TKqlReadTableBase& read,
    const TExprBase& keysToLookup, const TVector<TCoAtom>& lookupNames, const TKqpOptimizeContext& kqpCtx)
{
    if (kqpCtx.IsScanQuery()) {
        YQL_ENSURE(kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin, "Stream lookup is not enabled for index lookup join");
        return Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(read.Table())
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(lookupNames)
                    .Build()
                .Build()
            .Columns(read.Columns())
            .Done();
    }

    if (kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
        return Build<TKqlStreamLookupTable>(ctx, pos)
            .Table(read.Table())
            .LookupKeys<TCoSkipNullMembers>()
                .Input(keysToLookup)
                .Members()
                    .Add(lookupNames)
                    .Build()
                .Build()
            .Columns(read.Columns())
            .Done();
    }

    return Build<TKqlLookupTable>(ctx, pos)
        .Table(read.Table())
        .LookupKeys<TCoSkipNullMembers>()
            .Input(keysToLookup)
            .Members()
                .Add(lookupNames)
                .Build()
            .Build()
        .Columns(read.Columns())
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

TMaybeNode<TExprBase> KqpJoinToIndexLookupImpl(const TDqJoin& join, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!join.RightLabel().Maybe<TCoAtom>()) {
        // Lookup only in tables
        return {};
    }

    static THashSet<TStringBuf> supportedJoinKinds = {"Inner", "Left", "LeftOnly", "LeftSemi", "RightSemi"};
    if (!supportedJoinKinds.contains(join.JoinType().Value())) {
        return {};
    }

    auto rightReadMatch = MatchRead<TKqlReadTableBase>(join.RightInput());
    if (!rightReadMatch) {
        if (auto readRangesMatch = MatchRead<TKqlReadTableRangesBase>(join.RightInput())) {
            // for now only fullscans are supported
            auto read = readRangesMatch->Read.Cast<TKqlReadTableRangesBase>();
            if (TCoVoid::Match(read.Ranges().Raw())) {
                rightReadMatch = readRangesMatch;
                rightReadMatch->Read =
                    Build<TKqlReadTable>(ctx, join.Pos())
                    .Settings(read.Settings())
                    .Table(read.Table())
                    .Columns(read.Columns())
                    .Range<TKqlKeyRange>()
                        .From<TKqlKeyInc>().Build()
                        .To<TKqlKeyInc>().Build()
                        .Build()
                    .Done();
            } else {
                return {};
            }
        } else {
            return {};
        }
    }

    if (rightReadMatch->FlatMap && !IsPassthroughFlatMap(rightReadMatch->FlatMap.Cast(), nullptr)) {
        return {};
    }

    auto rightRead = rightReadMatch->Read.Cast<TKqlReadTableBase>();

    Y_ENSURE(rightRead.Maybe<TKqlReadTable>() || rightRead.Maybe<TKqlReadTableIndex>());

    const TKqlReadTableBase read = rightRead;
    if (!read.Table().SysView().Value().empty()) {
        // Can't lookup in system views
        return {};
    }

    auto maybeRightTableKeyPrefix = GetRightTableKeyPrefix(read.Range());
    if (!maybeRightTableKeyPrefix) {
        return {};
    }
    auto rightTableKeyPrefix = maybeRightTableKeyPrefix.Cast();

    TString lookupTable;
    TString indexName;

    if (auto indexRead = rightRead.Maybe<TKqlReadTableIndex>()) {
        indexName = indexRead.Cast().Index().StringValue();
        lookupTable = GetIndexMetadata(indexRead.Cast(), *kqpCtx.Tables, kqpCtx.Cluster)->Name;
    } else {
        lookupTable = read.Table().Path().StringValue();
    }

    const auto& rightTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookupTable);

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

    TVector<TExprBase> lookupMembers;
    TVector<TCoAtom> lookupNames;
    ui32 fixedPrefix = 0;
    TSet<TString> deduplicateLeftColumns;
    for (auto& rightColumnName : rightTableDesc.Metadata->KeyColumnNames) {
        TExprNode::TPtr member;

        auto leftColumn = rightJoinKeyToLeft.FindPtr(rightColumnName);

        if (fixedPrefix < rightTableKeyPrefix.ArgCount()) {
            if (leftColumn) {
                return {};
            }

            member = rightTableKeyPrefix.Arg(fixedPrefix).Ptr();
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

            const TDataExprType* leftDataType;
            const TDataExprType* rightDataType;
            if (!GetEquiJoinKeyTypes(join.LeftInput(), *leftColumn, rightTableDesc, rightColumnName, leftDataType, rightDataType)) {
                return {};
            }

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

        lookupMembers.emplace_back(
            Build<TExprList>(ctx, join.Pos())
                .Add<TCoAtom>().Build(rightColumnName)
                .Add(member)
                .Done());
        lookupNames.emplace_back(ctx.NewAtom(join.Pos(), rightColumnName));
    }

    if (lookupMembers.size() <= fixedPrefix) {
        return {};
    }

    bool needPrecomputeLeft = kqpCtx.IsDataQuery()
        && !join.LeftInput().Maybe<TCoParameter>()
        && !IsParameterToListOfStructsRepack(join.LeftInput());

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

    auto leftDataDeduplicated = DeduplicateByMembers(leftData, filter, deduplicateLeftColumns, ctx, join.Pos());
    auto keysToLookup = Build<TCoMap>(ctx, join.Pos())
        .Input(leftDataDeduplicated)
        .Lambda()
            .Args({leftRowArg})
            .Body<TCoAsStruct>()
                .Add(lookupMembers)
                .Build()
            .Build()
        .Done();

    TExprBase lookup = indexName
        ? BuildLookupIndex(ctx, join.Pos(), read, keysToLookup, lookupNames, indexName, kqpCtx)
        : BuildLookupTable(ctx, join.Pos(), read, keysToLookup, lookupNames, kqpCtx);

    // Skip null keys in lookup part as for equijoin semantics null != null,
    // so we can't have nulls in lookup part
    lookup = Build<TCoSkipNullMembers>(ctx, join.Pos())
        .Input(lookup)
        .Members()
            .Add(rightKeyColumns)
            .Build()
        .Done();

    auto lookupColumns = read.Columns();
    if (rightReadMatch->ExtractMembers) {
        lookupColumns = rightReadMatch->ExtractMembers.Cast().Members();
    }

    lookup = rightReadMatch->BuildProcessNodes(lookup, ctx);

    if (join.JoinType().Value() == "RightSemi") {
        auto arg = TCoArgument(ctx.NewArgument(join.Pos(), "row"));
        auto rightLabel = join.RightLabel().Cast<TCoAtom>().Value();

        TVector<TExprBase> renames = CreateRenames(rightReadMatch->FlatMap, lookupColumns, arg, rightLabel,
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

TExprBase KqpJoinToIndexLookup(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
    const NYql::TKikimrConfiguration::TPtr& config)
{
    if ((kqpCtx.IsScanQuery() && !kqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin) || !node.Maybe<TDqJoin>()) {
        return node;
    }
    auto join = node.Cast<TDqJoin>();

    DBG("-- Join: " << KqpExprToPrettyString(join, ctx));

    // SqlIn support (preferred lookup direction)
    if (join.JoinType().Value() == "LeftSemi" && !config->HasOptDisableJoinReverseTableLookupLeftSemi()) {
        auto flipJoin = FlipLeftSemiJoin(join, ctx);
        DBG("-- Flip join");

        if (auto indexLookupJoin = KqpJoinToIndexLookupImpl(flipJoin, ctx, kqpCtx)) {
            return indexLookupJoin.Cast();
        }
    }

    if (auto indexLookupJoin = KqpJoinToIndexLookupImpl(join, ctx, kqpCtx)) {
        return indexLookupJoin.Cast();
    }

    return node;
}

#undef DBG

} // namespace NKikimr::NKqp::NOpt
