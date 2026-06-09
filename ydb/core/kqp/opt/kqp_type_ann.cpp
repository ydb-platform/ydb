#include "kqp_opt.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>

#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_impl.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {

bool RightJoinSideAllowed(const TStringBuf& joinType) {
    return joinType != "LeftOnly" && joinType != "LeftSemi";
}

bool RightJoinSideOptional(const TStringBuf& joinType) {
    return joinType == "Left";
}

const TTypeAnnotationNode* MakeKqpEffectType(TExprContext& ctx) {
    return ctx.MakeType<TResourceExprType>(KqpEffectTag);
}

bool CheckKeyTuple(const TKqlKeyTuple& tuple, const TKikimrTableDescription& tableDesc,
    const TKikimrTableMetadataPtr meta, TExprContext& ctx)
{
    YQL_ENSURE(meta);

    for (ui32 i = 0; i < tuple.ArgCount(); ++i) {
        auto actualType = tuple.Arg(i).Ref().GetTypeAnn();
        YQL_ENSURE(actualType);

        YQL_ENSURE(i < meta->KeyColumnNames.size());
        auto expectedType = tableDesc.GetColumnType(meta->KeyColumnNames[i]);
        YQL_ENSURE(expectedType);

        auto expectedItemType = expectedType->GetKind() == ETypeAnnotationKind::Optional ?
            expectedType->Cast<TOptionalExprType>()->GetItemType() : expectedType;

        auto actualItemType = actualType->GetKind() == ETypeAnnotationKind::Optional ?
            actualType->Cast<TOptionalExprType>()->GetItemType() : actualType;

        if (IsSameAnnotation(*expectedItemType, *actualItemType)) {
            continue;
        }

        ctx.AddError(TIssue(ctx.GetPosition(tuple.Pos()), TStringBuilder()
            << "Table key type mismatch"
            << ", column: " << meta->KeyColumnNames[i]
            << ", table: " << meta->Name
            << ", expected: " << *expectedType
            << ", actual: " << *actualType));

        return false;
    }

    return true;
}

TStatus AnnotateTable(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    if (!EnsureArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    auto* path = node->Child(TKqpTable::idx_Path);
    auto* pathId = node->Child(TKqpTable::idx_PathId);
    auto* sysView = node->Child(TKqpTable::idx_SysView);
    auto* version = node->Child(TKqpTable::idx_Version);

    if (!EnsureAtom(*path, ctx) || !EnsureAtom(*pathId, ctx) || !EnsureAtom(*sysView, ctx) || !EnsureAtom(*version, ctx)) {
        return TStatus::Error;
    }

    if (pathId->Content() == "") {
        node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    TString tablePath(path->Content());
    auto tableDesc = tablesData.EnsureTableExists(cluster, tablePath, node->Pos(), ctx);
    if (!tableDesc) {
        return TStatus::Error;
    }

    YQL_ENSURE(tableDesc->Metadata);
    auto& meta  = *tableDesc->Metadata;

    if (meta.PathId.ToString() != pathId->Content()) {
        ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::UNEXPECTED, TStringBuilder()
            << "Query compilation, unexpected table id"
            << ", table: " << meta.Name
            << ", expected: " << meta.PathId.ToString()
            << ", actual: " << pathId->Content()));
        return TStatus::Error;
    }

    if (meta.SchemaVersion != FromString<ui64>(version->Content())) {
        ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::UNEXPECTED, TStringBuilder()
            << "Query compilation, unexpected table version"
            << ", table: " << meta.Name
            << ", expected: " << meta.SchemaVersion
            << ", actual: " << version->Content()));
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}

std::pair<TString, const TKikimrTableDescription*> ResolveTable(const TExprNode* kqpTableNode, TExprContext& ctx,
    const TString& cluster, const TKikimrTablesData& tablesData)
{
    if (!EnsureCallable(*kqpTableNode, ctx)) {
        return {"", nullptr};
    }

    if (!TKqpTable::Match(kqpTableNode)) {
        ctx.AddError(TIssue(ctx.GetPosition(kqpTableNode->Pos()), TStringBuilder()
            << "Expected " << TKqpTable::CallableName()));
        return {"", nullptr};
    }

    TString tableName{kqpTableNode->Child(TKqpTable::idx_Path)->Content()};

    auto tableDesc = tablesData.EnsureTableExists(cluster, tableName, kqpTableNode->Pos(), ctx);
    return {std::move(tableName), tableDesc};
}

const TFlowExprType* GetWideRowsType(TExprContext& ctx, const TStructExprType* rowType) {
    YQL_ENSURE(rowType);

    const auto& columns = rowType->GetItems();

    TTypeAnnotationNode::TListType items;
    items.reserve(columns.size());

    for (const auto& column: columns) {
        items.push_back(column->GetItemType());
    }

    auto wideRowType = ctx.MakeType<TMultiExprType>(items);
    return ctx.MakeType<TFlowExprType>(wideRowType);
}

const TFlowExprType* GetBlockRowsType(TExprContext& ctx, const TStructExprType* rowType) {
    YQL_ENSURE(rowType);

    const auto& columns = rowType->GetItems();

    TTypeAnnotationNode::TListType items;
    items.reserve(columns.size());

    for (const auto& column: columns) {
        items.push_back(ctx.MakeType<TBlockExprType>(column->GetItemType()));
    }
    // Last item is height of block
    items.push_back(ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));

    auto blockRowType = ctx.MakeType<TMultiExprType>(items);
    return ctx.MakeType<TFlowExprType>(blockRowType);
}

bool CalcKeyColumnsCount(TExprContext& ctx, const TPositionHandle pos, const TStructExprType& structType,
    const TKikimrTableDescription& tableDesc, const TKikimrTableMetadata& metadata, ui32& keyColumnsCount)
{
    for (auto& keyColumnName : metadata.KeyColumnNames) {
        auto itemIndex = structType.FindItem(keyColumnName);
        if (!itemIndex) {
            break;
        }

        auto itemType = structType.GetItems()[*itemIndex]->GetItemType();
        auto keyColumnType = tableDesc.GetColumnType(keyColumnName);

        if (CanCompare<true>(itemType, keyColumnType) == ECompareOptions::Uncomparable) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder()
                << "Invalid column type in table lookup, column: " <<  keyColumnName
                << ", expected: " << FormatType(keyColumnType)
                << ", actual: " << FormatType(itemType)));
            return false;
        }

        ++keyColumnsCount;
    }
    return true;
}

TStatus AnnotateReturningList(
    const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqlReturningList::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlReturningList::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));

    return TStatus::Ok;
}

TStatus AnnotateReadTable(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    const bool readIndex = TKqlReadTableIndex::Match(node.Get());
    if (readIndex && !EnsureArgsCount(*node, 5, ctx)) {
        return TStatus::Error;
    }

    if (!readIndex && !EnsureArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlReadTableBase::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    YQL_ENSURE(table.second->Metadata, "Expected loaded metadata");

    TKikimrTableMetadataPtr meta;

    if (readIndex) {
        meta = table.second->Metadata->GetIndexMetadata(node->Child(TKqlReadTableIndex::idx_Index)->Content()).first;
        if (!meta) {
            return TStatus::Error;
        }
    } else {
        meta = table.second->Metadata;
    }

    const auto& columns = node->ChildPtr(TKqlReadTableBase::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    if (!TKqlKeyRange::Match(node->Child(TKqlReadTableBase::idx_Range))) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqlReadTableBase::idx_Range)->Pos()), "Expected TKqlKeyRange tuple"));
        return TStatus::Error;
    }

    TKqlKeyRange range{node->ChildPtr(TKqlReadTableBase::idx_Range)};

    if (!CheckKeyTuple(range.From(), *table.second, meta, ctx)) {
        return TStatus::Error;
    }

    if (!CheckKeyTuple(range.To(), *table.second, meta, ctx)) {
        return TStatus::Error;
    }

    if (TKqlReadTable::Match(node.Get()) || TKqlReadTableIndex::Match(node.Get())) {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
    } else if (TKqpReadTable::Match(node.Get())) {
        node->SetTypeAnn(ctx.MakeType<TFlowExprType>(rowType));
    } else if (TKqpWideReadTable::Match(node.Get())) {
        node->SetTypeAnn(GetWideRowsType(ctx, rowType->Cast<TStructExprType>()));
    } else {
        YQL_ENSURE(false, "Unexpected ReadTable callable: " << node->Content());
    }

    return TStatus::Ok;
}

const TTypeAnnotationNode* GetReadTableRowTypeFullText(TExprContext& ctx, const TKikimrTablesData& tablesData,
    const TString& cluster, const TString& table, TCoAtomList select, bool withSystemColumns)
{
    auto tableDesc = tablesData.EnsureTableExists(cluster, table, select.Pos(), ctx);
    if (!tableDesc) {
        return nullptr;
    }

    TVector<const TItemExprType*> resultItems;
    for (auto item : select) {
        if (item.Value() == NTableIndex::NFulltext::FullTextRelevanceColumn) {
            auto itemType = ctx.MakeType<TItemExprType>(TString(item.Value()), ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Double));
            resultItems.push_back(itemType);
            YQL_ENSURE(itemType->Validate(select.Pos(), ctx));
            if (!itemType->Validate(select.Pos(), ctx)) {
                return nullptr;
            }
            continue;
        }

        auto column = tableDesc->Metadata->Columns.FindPtr(item.Value());
        TString columnName;
        if (column) {
            columnName = column->Name;
        } else {
            if (withSystemColumns && IsKikimrSystemColumn(item.Value())) {
                columnName = TString(item.Value());
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(select.Pos()), TStringBuilder()
                    << "Column not found: " << item.Value()));
                return nullptr;
            }
        }

        auto type = tableDesc->GetColumnType(columnName);
        YQL_ENSURE(type, "No such column: " << columnName);

        auto itemType = ctx.MakeType<TItemExprType>(columnName, type);
        if (!itemType->Validate(select.Pos(), ctx)) {
            return nullptr;
        }
        resultItems.push_back(itemType);
    }

    auto resultType = ctx.MakeType<TStructExprType>(resultItems);
    if (!resultType->Validate(select.Pos(), ctx)) {
        return nullptr;
    }

    return resultType;
}

TStatus AnnotateReadTableFullTextIndexSourceSettings(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster, const TKikimrTablesData& tablesData) {
    if (!EnsureArgsCount(*node, 6, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqpReadTableFullTextIndexSourceSettings::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqpReadTableFullTextIndexSourceSettings::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqpReadTableFullTextIndexSourceSettings::idx_Index), ctx)) {
        return TStatus::Error;
    }

    auto index = node->Child(TKqpReadTableFullTextIndexSourceSettings::idx_Index)->Content();
    const auto& [indexMeta, indexState] = table.second->Metadata->GetIndexMetadata(index);

    if (!indexMeta) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Index not found"));
        return TStatus::Error;
    }

    if (indexState != TIndexDescription::EIndexState::Ready) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Index is not ready"));
        return TStatus::Error;
    }

    const auto& queryColumns = node->Child(TKqpReadTableFullTextIndexSourceSettings::idx_QueryColumns);
    if (!EnsureTupleOfAtoms(*queryColumns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowTypeFullText(
        ctx, tablesData, cluster, table.first, TCoAtomList(columns), false);

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));
    return TStatus::Ok;
}

TStatus AnnotateKqpSourceSettings(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    auto table = ResolveTable(node->Child(TKqpReadRangesSourceSettings::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqpReadRangesSourceSettings::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    auto ranges = node->Child(TKqpReadRangesSourceSettings::idx_RangesExpr);
    if (!TCoVoid::Match(ranges) &&
        !TCoArgument::Match(ranges) &&
        !TCoParameter::Match(ranges) &&
        !TCoRangeFinalize::Match(ranges) &&
        !TDqPhyPrecompute::Match(ranges) &&
        !TKqpTxResultBinding::Match(ranges) &&
        !TKqlKeyRange::Match(ranges))
    {
        ctx.AddError(TIssue(
            ctx.GetPosition(ranges->Pos()),
            TStringBuilder()
                << "Expected KeyRange, Void, Parameter, Argument or RangeFinalize in ranges, but got: "
                << ranges->Content()
        ));
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));
    return TStatus::Ok;
}

TStatus AnnotateSysViewSourceSettings(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    auto table = ResolveTable(node->Child(TKqpReadSysViewSourceSettings::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqpReadSysViewSourceSettings::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), false);
    if (!rowType) {
        return TStatus::Error;
    }

    auto ranges = node->Child(TKqpReadSysViewSourceSettings::idx_RangesExpr);
    if (!TCoVoid::Match(ranges) &&
        !TCoArgument::Match(ranges) &&
        !TCoParameter::Match(ranges) &&
        !TCoRangeFinalize::Match(ranges) &&
        !TDqPhyPrecompute::Match(ranges) &&
        !TKqpTxResultBinding::Match(ranges) &&
        !TKqlKeyRange::Match(ranges))
    {
        ctx.AddError(TIssue(
            ctx.GetPosition(ranges->Pos()),
            TStringBuilder()
                << "Expected KeyRange, Void, Parameter, Argument or RangeFinalize in ranges, but got: "
                << ranges->Content()
        ));
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));
    return TStatus::Ok;
}

TStatus AnnotateReadTableRanges(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    bool olapTable = TKqpReadOlapTableRangesBase::Match(node.Get());
    bool index = TKqlReadTableIndexRanges::Match(node.Get());

    size_t argCount = (olapTable || index) ? 6 : 5;

    // prefix
    if (!EnsureMinArgsCount(*node, argCount, ctx) && EnsureMaxArgsCount(*node, argCount + 3, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlReadTableRangesBase::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqlReadTableRangesBase::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    auto ranges = node->Child(TKqlReadTableRangesBase::idx_Ranges);
    if (!TCoVoid::Match(ranges) &&
        !TCoArgument::Match(ranges) &&
        !TCoParameter::Match(ranges) &&
        !TCoRangeFinalize::Match(ranges))
    {
        ctx.AddError(TIssue(
            ctx.GetPosition(ranges->Pos()),
            TStringBuilder()
                << "Expected Void, Parameter, Argument or RangeFinalize in ranges, but got: "
                << ranges->Content()
        ));
        return TStatus::Error;
    }

    if (TKqlReadTableRanges::Match(node.Get())) {
        if (node->ChildrenSize() > TKqlReadTableRanges::idx_PredicateExpr) {
            auto& lambda = node->ChildRef(TKqlReadTableRanges::idx_PredicateExpr);
            auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, node->Pos(), withSystemColumns);
            if (!rowType) {
                return TStatus::Error;
            }
            if (!UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!lambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }
        }
        node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
    } else if (TKqlReadTableIndexRanges::Match(node.Get())) {
        if (node->ChildrenSize() > TKqlReadTableIndexRanges::idx_PredicateExpr) {
            auto& lambda = node->ChildRef(TKqlReadTableIndexRanges::idx_PredicateExpr);
            auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, node->Pos(), withSystemColumns);
            if (!rowType) {
                return TStatus::Error;
            }
            if (!UpdateLambdaAllArgumentsTypes(lambda, {rowType}, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!lambda->GetTypeAnn()) {
                return IGraphTransformer::TStatus::Repeat;
            }
        }
        node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
    } else if (TKqpReadTableRanges::Match(node.Get())) {
        node->SetTypeAnn(ctx.MakeType<TFlowExprType>(rowType));
    } else if (TKqpWideReadTableRanges::Match(node.Get())) {
        node->SetTypeAnn(GetWideRowsType(ctx, rowType->Cast<TStructExprType>()));
    } else if (TKqpReadOlapTableRangesBase::Match(node.Get())) {
        if (!EnsureLambda(*node->Child(TKqpReadOlapTableRangesBase::idx_Process), ctx)) {
            return TStatus::Error;
        }

        auto& processLambda = node->ChildRef(TKqpReadOlapTableRangesBase::idx_Process);
        if (!UpdateLambdaAllArgumentsTypes(processLambda, {ctx.MakeType<TFlowExprType>(rowType)}, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!processLambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        auto processType = processLambda->GetTypeAnn();
        const TTypeAnnotationNode* processRowType;
        if (!EnsureNewSeqType<false, false, true>(node->Pos(), *processType, ctx, &processRowType)) {
            return TStatus::Error;
        }

        if (!EnsureStructType(node->Pos(), *processRowType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (TKqpReadOlapTableRanges::Match(node.Get())) {
            node->SetTypeAnn(ctx.MakeType<TFlowExprType>(processRowType));
        } else if (TKqpWideReadOlapTableRanges::Match(node.Get())) {
            node->SetTypeAnn(GetWideRowsType(ctx, processRowType->Cast<TStructExprType>()));
        } else if (TKqpBlockReadOlapTableRanges::Match(node.Get())) {
            node->SetTypeAnn(GetBlockRowsType(ctx, processRowType->Cast<TStructExprType>()));
        } else {
            YQL_ENSURE(false, "Unexpected ReadOlapTable callable." << node->Content());
        }
    } else {
        YQL_ENSURE(false, "Unexpected ReadTableRanges callable." << node->Content());
    }

    return TStatus::Ok;
}

TStatus AnnotateReadTableFullTextIndex(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster, const TKikimrTablesData& tablesData) {
    if (!EnsureArgsCount(*node, 6, ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Expected 3 arguments for FullTextMatch"));
        return TStatus::Error;
    }

    bool isPhysical = TKqpReadTableFullTextIndex::Match(node.Get());

    auto table = ResolveTable(node->Child(TKqlReadTableFullTextIndex::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqlReadTableFullTextIndex::idx_Index), ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Expected index name"));
        return TStatus::Error;
    }

    auto [indexMeta, indexState] = table.second->Metadata->GetIndexMetadata(node->Child(TKqlReadTableFullTextIndex::idx_Index)->Content());

    if (!indexMeta) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Index not found"));
        return TStatus::Error;
    }

    if (indexState != TIndexDescription::EIndexState::Ready) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Index is not ready"));
        return TStatus::Error;
    }

    const auto& columns = node->ChildPtr(TKqlReadTableFullTextIndex::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    const auto& queryColumns = node->ChildPtr(TKqlReadTableFullTextIndex::idx_QueryColumns);
    if (!EnsureTupleOfAtoms(*queryColumns, ctx)) {
        return TStatus::Error;
    }

    auto rowType = GetReadTableRowTypeFullText(
        ctx, tablesData, cluster, table.first, TCoAtomList(columns), false);

    if (isPhysical) {
        node->SetTypeAnn(ctx.MakeType<TFlowExprType>(rowType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
    }

    return TStatus::Ok;
}

TStatus AnnotateLookupTable(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    const bool isStreamLookup = TKqlStreamLookupTable::Match(node.Get()) || TKqlStreamLookupIndex::Match(node.Get());
    if (isStreamLookup && !EnsureArgsCount(*node, TKqlStreamLookupIndex::Match(node.Get()) ? 5 : 4, ctx)) {
        return TStatus::Error;
    }

    if (!isStreamLookup && !EnsureMinMaxArgsCount(*node, 3, 4, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlLookupTableBase::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqlLookupTableBase::idx_Columns), ctx)) {
        return TStatus::Error;
    }
    TCoAtomList columns{node->ChildPtr(TKqlLookupTableBase::idx_Columns)};

    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, columns, withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    bool isPhysical = TKqpLookupTable::Match(node.Get());

    const TTypeAnnotationNode* lookupType;
    if (isPhysical) {
        if (!EnsureNewSeqType<false, false, true>(*node->Child(TKqlLookupTableBase::idx_LookupKeys), ctx, &lookupType)) {
            return TStatus::Error;
        }

        if (node->Child(TKqlLookupTableBase::idx_LookupKeys)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Flow) {
            auto streamLookupKeys = Build<TCoFromFlow>(ctx, node->Pos())
                .Input(node->ChildRef(TKqlLookupTableBase::idx_LookupKeys))
                .Done();

            node->ChildRef(TKqpLookupTable::idx_LookupKeys) = streamLookupKeys.Ptr();
            return TStatus::Repeat;
        }
    } else {
        if (!EnsureNewSeqType<false, true, false>(*node->Child(TKqlLookupTableBase::idx_LookupKeys), ctx, &lookupType)) {
            return TStatus::Error;
        }
    }

    YQL_ENSURE(lookupType);

    const TStructExprType* structType = nullptr;
    if (isStreamLookup) {
        TCoNameValueTupleList settingsNode{node->ChildPtr(TKqlStreamLookupTable::Match(node.Get()) ?
            TKqlStreamLookupTable::idx_Settings : TKqlStreamLookupIndex::idx_Settings)};
        auto settings = TKqpStreamLookupSettings::Parse(settingsNode);
        if (settings.Strategy == EStreamLookupStrategyType::LookupJoinRows
            || settings.Strategy == EStreamLookupStrategyType::LookupSemiJoinRows) {

            if (settings.VectorTopColumn) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "VectorTop is not supported in Join mode"));
                return TStatus::Error;
            }

            if (!EnsureTupleType(node->Pos(), *lookupType, ctx)) {
                return TStatus::Error;
            }

            auto tupleType = lookupType->Cast<TTupleExprType>();
            if (tupleType->GetSize() < 2 || tupleType->GetSize() > 3) {
                ctx.AddError(
                    TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Table stream lookup has unexpected input tuple, expected tuple size 2 or 3, but found %s"
                        << tupleType->GetSize()));
                return TStatus::Error;
            }

            if (!EnsureOptionalType(node->Pos(), *tupleType->GetItems()[1], ctx)) {
                return TStatus::Error;
            }

            auto joinKeyType = tupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
            if (!EnsureStructType(node->Pos(), *joinKeyType, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureStructType(node->Pos(), *tupleType->GetItems()[0], ctx)) {
                return TStatus::Error;
            }

            structType = joinKeyType->Cast<TStructExprType>();
            auto leftRowType = tupleType->GetItems()[0]->Cast<TStructExprType>();

            TVector<const TTypeAnnotationNode*> outputTypes;
            outputTypes.push_back(leftRowType);
            outputTypes.push_back(ctx.MakeType<TOptionalExprType>(rowType));
            outputTypes.push_back(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64));

            rowType = ctx.MakeType<TTupleExprType>(outputTypes);
        } else {
            if (!EnsureStructType(node->Pos(), *lookupType, ctx)) {
                return TStatus::Error;
            }

            structType = lookupType->Cast<TStructExprType>();

            if (settings.VectorTopColumn || settings.VectorTopIndex || settings.VectorTopTarget || settings.VectorTopLimit) {
                if (!settings.VectorTopColumn || !settings.VectorTopIndex || !settings.VectorTopTarget || !settings.VectorTopLimit) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "VectorTop requires Column, Index, Target and Limit"));
                    return TStatus::Error;
                }
                bool found = false;
                for (const auto& item : columns) {
                    if (item.Value() == settings.VectorTopColumn) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "VectorTopColumn is not in the StreamLookup result column list"));
                    return TStatus::Error;
                }
            }
        }
    } else {
        if (!EnsureStructType(node->Pos(), *lookupType, ctx)) {
            return TStatus::Error;
        }

        structType = lookupType->Cast<TStructExprType>();
    }

    YQL_ENSURE(structType);

    ui32 keyColumnsCount = 0;
    if (TKqlStreamLookupIndex::Match(node.Get())) {
        auto index = node->Child(TKqlStreamLookupIndex::idx_Index);
        if (!EnsureAtom(*index, ctx)) {
            return TStatus::Error;
        }
        auto indexMeta = table.second->Metadata->GetIndexMetadata(index->Content()).first;

        if (!CalcKeyColumnsCount(ctx, node->Pos(), *structType, *table.second, *indexMeta, keyColumnsCount)) {
            return TStatus::Error;
        }

    } else {
        if (!CalcKeyColumnsCount(ctx, node->Pos(), *structType, *table.second, *table.second->Metadata, keyColumnsCount)) {
            return TStatus::Error;
        }
    }

    auto tableDbg = [&]() {
        return TStringBuilder() << "Lookup: " << structType->ToString()
           << ", for table: " << table.second->Metadata->Name;
    };

    if (!keyColumnsCount) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Table lookup has no key columns. " + tableDbg()));
        return TStatus::Error;
    }

    if (structType->GetSize() != keyColumnsCount) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Table lookup contains non-key columns. " + tableDbg()));
        return TStatus::Error;
    }

    if (isPhysical) {
        node->SetTypeAnn(ctx.MakeType<TFlowExprType>(rowType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
    }
    return TStatus::Ok;
}

TStatus AnnotateKeyTuple(const TExprNode::TPtr& node, TExprContext& ctx) {
    TVector<const TTypeAnnotationNode*> keyTypes;
    for (const auto& arg : node->ChildrenList()) {
        keyTypes.push_back(arg->GetTypeAnn());
    }

    auto tupleType = ctx.MakeType<TTupleExprType>(keyTypes);
    node->SetTypeAnn(tupleType);
    return TStatus::Ok;
}

TStatus AnnotateFillTable(const TExprNode::TPtr& node, TExprContext& ctx)
{
    if (!EnsureMinMaxArgsCount(*node, 4, 4, ctx)) {
        return TStatus::Error;
    }

    const auto* input = node->Child(TKqlFillTable::idx_Input);

    AFL_ENSURE(input->GetTypeAnn());

    const TTypeAnnotationNode* itemType = nullptr;
    bool isStream = false;
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Stream) {
        if (!EnsureStreamType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        isStream = true;
    } else {
        if (!EnsureListType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        isStream = false;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto effectType = MakeKqpEffectType(ctx);
    if (isStream) {
        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(effectType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(effectType));
    }

    return TStatus::Ok;
}

TStatus AnnotateUpsertRows(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    if (!EnsureMinArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlUpsertRowsBase::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqlUpsertRowsBase::idx_Columns), ctx)) {
        return TStatus::Error;
    }
    TCoAtomList columns{node->ChildPtr(TKqlUpsertRowsBase::idx_Columns)};

    const TTypeAnnotationNode* itemType = nullptr;
    bool isStream;

    auto* input = node->Child(TKqlUpsertRowsBase::idx_Input);

    if (TKqpUpsertRows::Match(node.Get())) {
        if (!EnsureStreamType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        isStream = true;
    } else {

        YQL_ENSURE(
            TKqlUpsertRows::Match(node.Get()) ||
            TKqlUpsertRowsIndex::Match(node.Get()) ||
            TKqlInsertOnConflictUpdateRows::Match(node.Get())
        );

        if (!EnsureListType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        isStream = false;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto rowType = itemType->Cast<TStructExprType>();

    const bool isStructOfRows = rowType->GetSize() == 2 && [&]() {
            THashSet<TStringBuf> names;
            for (const auto& item : rowType->GetItems()) {
                names.insert(item->GetName());
                if (item->GetItemType()->GetKind() != NYql::ETypeAnnotationKind::Struct) {
                    return false;
                }
            }
            return names.contains("new") && names.contains("old");
        }();
    if (isStructOfRows) {
        for (const auto& item : rowType->GetItems()) {
            if (item->GetName() == "new") {
                rowType = item->GetItemType()->Cast<TStructExprType>();
                break;
            }
        }
    }

    for (const auto& column : columns) {
        if (!rowType->FindItem(column.Value())) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "Missing column in input type: " << column.Value()));
            return TStatus::Error;
        }
    }

    if (TKqpUpsertRows::Match(node.Get()) && rowType->GetItems().size() != columns.Size()) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
            << "Input type contains excess columns"));
        return TStatus::Error;
    }

    for (auto& keyColumnName : table.second->Metadata->KeyColumnNames) {
        const auto& columnInfo = table.second->Metadata->Columns.at(keyColumnName);
        if (!rowType->FindItem(keyColumnName) && !columnInfo.IsDefaultKindDefined()) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Missing key column in input type: " << keyColumnName));
            return TStatus::Error;
        }
    }

    TMaybeNode<TCoNameValueTupleList> settings;
    TKqpUpsertRowsSettings upsertSettings;
    if (TKqlUpsertRows::Match(node.Get()) && node->ChildrenSize() > TKqlUpsertRows::idx_Settings) {
        settings = node->ChildPtr(TKqlUpsertRows::idx_Settings);
    }
    if (TKqlUpsertRowsIndex::Match(node.Get()) && node->ChildrenSize() > TKqlUpsertRowsIndex::idx_Settings) {
        settings = node->ChildPtr(TKqlUpsertRowsIndex::idx_Settings);
    }
    if (TKqpUpsertRows::Match(node.Get())) /* here settings are not optional*/ {
        settings = node->ChildPtr(TKqpUpsertRows::idx_Settings);
    }
    if (settings) {
        upsertSettings = TKqpUpsertRowsSettings::Parse(settings.Cast());
    }
    if (!upsertSettings.IsUpdate) {
        for (auto& [name, meta] : table.second->Metadata->Columns) {
            if (meta.NotNull && !rowType->FindItem(name)) {
                ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE, TStringBuilder()
                    << "Missing not null column in input: " << name
                    << ". All not null columns should be initialized"));
                return TStatus::Error;
            }


            if (meta.NotNull && rowType->FindItemType(name)->HasOptionalOrNull()) {
                if (rowType->FindItemType(name)->GetKind() != ETypeAnnotationKind::Pg) {
                    ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                        << "Can't set optional or NULL value to not null column: " << name
                        << ". All not null columns should be initialized"));
                    return TStatus::Error;
                }
            }
        }
    }

    if (TKqlUpsertRowsIndex::Match(node.Get())) {
        Y_ENSURE(!table.second->Metadata->ImplTables.empty());
    }

    auto effectType = MakeKqpEffectType(ctx);
    if (isStream) {
        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(effectType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(effectType));
    }
    return TStatus::Ok;
}

TStatus AnnotateInsertRows(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    if (!EnsureMinMaxArgsCount(*node, 5, 6, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlInsertRows::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqlInsertRows::idx_Columns), ctx)) {
        return TStatus::Error;
    }
    TCoAtomList columns{node->ChildPtr(TKqlInsertRows::idx_Columns)};

    auto* input = node->Child(TKqlInsertRows::idx_Input);
    if (!EnsureListType(*input, ctx)) {
        return TStatus::Error;
    }

    auto itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto rowType = itemType->Cast<TStructExprType>();
    for (const auto& column : columns) {
        if (!rowType->FindItem(column.Value())) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "Missing column in input type: " << column.Value()));
            return TStatus::Error;
        }
    }

    for (auto& keyColumnName : table.second->Metadata->KeyColumnNames) {
        if (!rowType->FindItem(keyColumnName)) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Missing key column in input type: " << keyColumnName));
            return TStatus::Error;
        }
    }

    for (auto& [name, meta] : table.second->Metadata->Columns) {
        if (meta.NotNull && !rowType->FindItem(name)) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE, TStringBuilder()
                << "Missing not null column in input: " << name
                << ". All not null columns should be initialized"));
            return TStatus::Error;
        }

        if (meta.NotNull && rowType->FindItemType(name)->HasOptionalOrNull()) {
            if (rowType->FindItemType(name)->GetKind() != ETypeAnnotationKind::Pg) {
                ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                    << "Can't set optional or NULL value to not null column: " << name
                    << ". All not null columns should be initialized"));
                return TStatus::Error;
            }
        }
    }

    if (!EnsureAtom(*node->Child(TKqlInsertRows::idx_OnConflict), ctx)) {
        return TStatus::Error;
    }

    TStringBuf onConflict = node->Child(TKqlInsertRows::idx_OnConflict)->Content();
    if (onConflict != "abort"sv && onConflict != "revert"sv) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
            << "Unsupported insert on-conflict policy `" << onConflict << "`."));
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TListExprType>(MakeKqpEffectType(ctx)));
    return TStatus::Ok;
}

TStatus AnnotateUpdateRows(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    if (!EnsureMinArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureMaxArgsCount(*node, 6, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlUpdateRows::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqlUpdateRows::idx_Columns), ctx)) {
        return TStatus::Error;
    }
    TCoAtomList columns{node->ChildPtr(TKqlUpdateRows::idx_Columns)};

    auto* input = node->Child(TKqlUpdateRows::idx_Input);
    if (!EnsureListType(*input, ctx)) {
        return TStatus::Error;
    }

    auto itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto rowType = itemType->Cast<TStructExprType>();
    for (const auto& column : columns) {
        YQL_ENSURE(rowType->FindItem(column.Value()), "Missing column in input type: " << column.Value());
    }

    for (auto& keyColumnName : table.second->Metadata->KeyColumnNames) {
        if (!rowType->FindItem(keyColumnName)) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE, TStringBuilder()
                << "Missing key column in input type: " << keyColumnName));
            return TStatus::Error;
        }
    }

    for (const auto& item : rowType->GetItems()) {
        auto column = table.second->Metadata->Columns.FindPtr(TString(item->GetName()));
        YQL_ENSURE(column);
        if (column->NotNull && item->HasOptionalOrNull()) {
            if (item->GetItemType()->GetKind() != ETypeAnnotationKind::Pg) {
                ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_BAD_COLUMN_TYPE, TStringBuilder()
                    << "Can't set optional or NULL value to not null column: " << column->Name));
                return TStatus::Error;
            }
        }
    }

    node->SetTypeAnn(ctx.MakeType<TListExprType>(MakeKqpEffectType(ctx)));
    return TStatus::Ok;
}

TStatus AnnotateDeleteRows(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    if (!EnsureMaxArgsCount(*node, 5, ctx) && !EnsureMinArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqlDeleteRowsBase::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    const TTypeAnnotationNode* itemType = nullptr;
    bool isStream = false;

    auto* input = node->Child(TKqlDeleteRowsBase::idx_Input);

    if (TKqpDeleteRows::Match(node.Get())) {
        if (!EnsureStreamType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        isStream = true;
    } else {
        YQL_ENSURE(TKqlDeleteRows::Match(node.Get()) || TKqlDeleteRowsIndex::Match(node.Get()));
        if (!EnsureListType(*input, ctx)) {
            return TStatus::Error;
        }
        itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        isStream = false;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto rowType = itemType->Cast<TStructExprType>();
    for (auto& keyColumnName : table.second->Metadata->KeyColumnNames) {
        if (!rowType->FindItem(keyColumnName)) {
            ctx.AddError(YqlIssue(ctx.GetPosition(node->Pos()), TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Missing key column in input type: " << keyColumnName));
            return TStatus::Error;
        }
    }

    auto effectType = MakeKqpEffectType(ctx);
    if (isStream) {
        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(effectType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(effectType));
    }
    return TStatus::Ok;
}

TStatus AnnotateOlapUnaryLogicOperator(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TUnitExprType>());
    return TStatus::Ok;
}

TStatus AnnotateOlapBinaryLogicOperator(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureMinArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TUnitExprType>());
    return TStatus::Ok;
}

bool ValidateOlapFilterConditions(const TExprNode* node, const TStructExprType* itemType, TExprContext& ctx) {
    if (TKqpOlapApply::Match(node)) {
        return true;
    } else if (TKqpOlapAnd::Match(node) || TKqpOlapOr::Match(node) || TKqpOlapXor::Match(node) || TKqpOlapNot::Match(node)) {
        bool res = true;
        for (auto arg : node->ChildrenList()) {
            res &= ValidateOlapFilterConditions(arg.Get(), itemType, ctx);
            if (!res) {
                break;
            }
        }
        return res;
    } else if (TKqpOlapFilterUnaryOp::Match(node)) {
        const auto op = node->Child(TKqpOlapFilterUnaryOp::idx_Operator);
        if (!EnsureAtom(*op, ctx)) {
            return false;
        }
        if (!op->IsAtom({"minus", "abs", "not", "size", "exists", "empty", "just"})) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Unexpected OLAP unary operation: " << op->Content()
            ));
            return false;
        }
        return ValidateOlapFilterConditions(node->Child(TKqpOlapFilterUnaryOp::idx_Arg), itemType, ctx);
    } else if (TKqpOlapFilterBinaryOp::Match(node)) {
        const auto op = node->Child(TKqpOlapFilterBinaryOp::idx_Operator);
        if (!EnsureAtom(*op, ctx)) {
            return false;
        }
        if (!op->IsAtom({"eq", "neq", "lt", "lte", "gt", "gte", "string_contains", "starts_with", "ends_with", "+", "-", "*", "/", "%", "??"})) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Unexpected OLAP binary operation: " << op->Content()
            ));
            return false;
        }
        return ValidateOlapFilterConditions(node->Child(TKqpOlapFilterBinaryOp::idx_Left), itemType, ctx)
            && ValidateOlapFilterConditions(node->Child(TKqpOlapFilterBinaryOp::idx_Right), itemType, ctx);
    } else if (TKqpOlapFilterTernaryOp::Match(node)) {
        const auto op = node->Child(TKqpOlapFilterTernaryOp::idx_Operator);
        if (!EnsureAtom(*op, ctx)) {
            return false;
        }
        if (!op->IsAtom("if")) { // TODO: +substring
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Unexpected OLAP ternary operation: " << op->Content()
            ));
            return false;
        }
        return ValidateOlapFilterConditions(node->Child(TKqpOlapFilterTernaryOp::idx_First), itemType, ctx)
            && ValidateOlapFilterConditions(node->Child(TKqpOlapFilterTernaryOp::idx_Second), itemType, ctx)
            && ValidateOlapFilterConditions(node->Child(TKqpOlapFilterTernaryOp::idx_Third), itemType, ctx);
    } else if (TKqpOlapFilterExists::Match(node)) {
        if (!EnsureArgsCount(*node, 1, ctx)) {
            return false;
        }
        auto column = node->Child(TKqpOlapFilterExists::idx_Column);
        if (!EnsureAtom(*column, ctx)) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Expected column in OLAP Exists filter, got: " << column->Content()
            ));
            return false;
        }
        return ValidateOlapFilterConditions(column, itemType, ctx);
    }

    // Column name, validate that it is present in Input node
    if (TCoAtom::Match(node)) {
        if (itemType->FindItem(node->Content())) {
            return true;
        }

        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
            TStringBuilder() << "Missing column in input type: " << node->Content()
        ));

        return false;
    }

    // Null argument for IS NULL/NOT NULL
    if (TCoNull::Match(node)) {
        return true;
    }

    // Incoming parameter
    if (TCoParameter::Match(node)) {
        return true;
    }

    // Any supported literal
    if (TCoDataCtor::Match(node)) {
        return true;
    }

    // SafeCast, the checks about validity should be placed in kqp_opt_phy_olap_filter.cpp
    if (TCoSafeCast::Match(node)) {
        return true;
    }

    if (TKqpOlapJsonValue::Match(node)) {
        return true;
    }

    if (TKqpOlapJsonExists::Match(node)) {
        return true;
    }

    ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
        TStringBuilder() << "Expected literal or column as OLAP filter value, got: " << node->Content()
    ));

    return false;
}

TStatus AnnotateOlapProjection(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* olapOperation = node->Child(TKqpOlapProjection::idx_OlapOperation);
    // Exptecting that type annotation is supported for olap operation.
    if (!olapOperation->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(olapOperation->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateOlapProjections(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* input = node->Child(TKqpOlapProjections::idx_Input);
    const TTypeAnnotationNode* inputType;
    if (!EnsureNewSeqType<false, false, true>(*input, ctx, &inputType)) {
        return TStatus::Error;
    }

    if (!EnsureStructType(input->Pos(), *inputType, ctx)) {
        return TStatus::Error;
    }

    // For each `Projection` we want to replace a type annotation for column
    // which associated with a `Projection`.
    // For example: JsonDocument -> UTF8.
    THashMap<TString, const TTypeAnnotationNode*> projectionsTypes;
    const auto* projections = node->Child(TKqpOlapProjections::idx_Projections);
    for (const auto& expr : TExprBase(projections).Cast<TExprList>()) {
        auto projection = TExprBase(expr).Cast<TKqpOlapProjection>();
        const auto projectionTypeAnn = projection.Ptr()->GetTypeAnn();
        // Expecting annotation for projection.
        if (!projectionTypeAnn) {
            return TStatus::Repeat;
        }
        projectionsTypes.emplace(TString(projection.ColumnName()), projectionTypeAnn);
    }

    THashSet<TString> takenColumns;
    TVector<const TItemExprType*> newItemTypes;
    const auto* originalStructType = inputType->Cast<TStructExprType>();
    for (const auto* originalItemType : originalStructType->GetItems()) {
        const auto& itemName = originalItemType->GetName();
        if (projectionsTypes.contains(itemName)) {
            newItemTypes.push_back(ctx.MakeType<TItemExprType>(itemName, projectionsTypes[itemName]));
            takenColumns.insert(TString(itemName));
        } else {
            newItemTypes.push_back(originalItemType);
        }
    }

    for (const auto &projectionType : projectionsTypes) {
        if (!takenColumns.contains(projectionType.first)) {
            newItemTypes.push_back(ctx.MakeType<TItemExprType>(projectionType.first, projectionType.second));
        }
    }

    // Create a final type (Flow(Struct{items}))
    node->SetTypeAnn(ctx.MakeType<TFlowExprType>(ctx.MakeType<TStructExprType>(newItemTypes)));
    return TStatus::Ok;
}

TStatus AnnotateOlapFilter(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* input = node->Child(TKqpOlapFilter::idx_Input);

    const TTypeAnnotationNode* itemType;
    if (!EnsureNewSeqType<false, false, true>(*input, ctx, &itemType)) {
        return TStatus::Error;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    if (!ValidateOlapFilterConditions(node->Child(TKqpOlapFilter::idx_Condition), itemType->Cast<TStructExprType>(), ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(input->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateOlapApplyColumnArg(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2U, ctx)) {
        return TStatus::Error;
    }

    const auto& row = node->Head();
    if (!EnsureType(row, ctx)) {
        return TStatus::Error;
    }
    const auto& rowType = row.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    if (!EnsureStructType(row.Pos(), *rowType, ctx)) {
        return TStatus::Error;
    }
    const auto& rowStructType = rowType->Cast<TStructExprType>();


    if (!EnsureAtom(node->Tail(), ctx)) {
        return TStatus::Error;
    }
    const auto& columnName = node->Tail().Content();
    if (const auto& columnType = rowStructType->FindItemType(columnName)) {
        node->SetTypeAnn(columnType);
        return TStatus::Ok;
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(node->Tail().Pos()),
            TStringBuilder() << "Missed column: " << columnName
        ));
        return TStatus::Error;
    }
}

TStatus AnnotateOlapApply(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 3U, ctx)) {
        return TStatus::Error;
    }

    TExprList args = TExprList(node->Child(TKqpOlapApply::idx_Args));
    std::vector<const NYql::TTypeAnnotationNode*> argTypes;

    for(const auto& arg: args) {
        argTypes.push_back(arg.Ref().GetTypeAnn());
    }

    auto& lambda = node->ChildRef(TKqpOlapApply::idx_Lambda);
    if (!EnsureLambda(*lambda, ctx)) {
        return TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, argTypes, ctx)) {
        return TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    if (!EnsureAtom(*node->Child(TKqpOlapApply::idx_KernelName), ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(lambda->GetTypeAnn());
    return TStatus::Ok;
}

bool ValidateOlapJsonOperation(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto column = node->Child(TKqpOlapJsonOperationBase::idx_Column);
    if (!EnsureAtom(*column, ctx)) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
            TStringBuilder() << "Expected column name in OLAP JSON function, got: " << column->Content()
        ));
        return false;
    }
    auto path = node->Child(TKqpOlapJsonOperationBase::idx_Path);
    auto pathTypeAnn = path->GetTypeAnn();
    if (pathTypeAnn->GetKind() != ETypeAnnotationKind::Data || pathTypeAnn->Cast<TDataExprType>()->GetSlot() != EDataSlot::Utf8) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
            TStringBuilder() << "Expected Utf8 as path in OLAP JSON function, got: " << path->Content()
        ));
        return false;
    }
    return true;
}

TStatus AnnotateOlapJsonValue(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    if (!ValidateOlapJsonOperation(node, ctx)) {
        return TStatus::Error;
    }

    auto returningTypeArg = node->Child(TKqpOlapJsonValue::idx_ReturningType);

    const auto* returningTypeAnn = returningTypeArg->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    if (!EnsureDataType(returningTypeArg->Pos(), *returningTypeAnn, ctx)) {
        return TStatus::Error;
    }
    EDataSlot resultSlot = returningTypeAnn->Cast<TDataExprType>()->GetSlot();

    if (!IsDataTypeNumeric(resultSlot)
        && !IsDataTypeDate(resultSlot)
        && resultSlot != EDataSlot::Utf8
        && resultSlot != EDataSlot::String
        && resultSlot != EDataSlot::Bool)
    {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Returning argument of KqpOlapJsonValue callable supports only Utf8, String, Bool, date and numeric types"));
        return TStatus::Error;
    }

    const TTypeAnnotationNode* resultType = ctx.MakeType<TDataExprType>(resultSlot);
    node->SetTypeAnn(ctx.MakeType<TOptionalExprType>(resultType));
    return TStatus::Ok;
}

TStatus AnnotateOlapJsonExists(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    if (!ValidateOlapJsonOperation(node, ctx)) {
        return TStatus::Error;
    }

    const TTypeAnnotationNode* resultType = ctx.MakeType<TDataExprType>(EDataSlot::Bool);
    node->SetTypeAnn(ctx.MakeType<TOptionalExprType>(resultType));
    return TStatus::Ok;
}

TStatus AnnotateOlapAgg(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    auto* input = node->Child(TKqpOlapAgg::idx_Input);

    const TTypeAnnotationNode* itemType;
    if (!EnsureNewSeqType<false, false, true>(*input, ctx, &itemType)) {
        return TStatus::Error;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto structType = itemType->Cast<TStructExprType>();

    if (!EnsureTuple(*node->Child(TKqpOlapAgg::idx_Aggregates), ctx)) {
        return TStatus::Error;
    }

    TVector<const TItemExprType*> aggTypes;
    for (auto agg : node->Child(TKqpOlapAgg::idx_Aggregates)->ChildrenList()) {
        auto aggName = agg->Child(TKqpOlapAggOperation::idx_Name);
        auto opType = agg->Child(TKqpOlapAggOperation::idx_Type);
        auto colName = agg->Child(TKqpOlapAggOperation::idx_Column);
        if (!EnsureAtom(*opType, ctx)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Expected operation type in OLAP aggregation, got: " << opType->Content()
            ));
            return TStatus::Error;
        }
        if (!EnsureAtom(*colName, ctx)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Expected column name in OLAP aggregation, got: " << colName->Content()
            ));
            return TStatus::Error;
        }
        if (!EnsureAtom(*aggName, ctx)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Expected aggregate column generated name in OLAP aggregation, got: " << aggName->Content()
            ));
            return TStatus::Error;
        }
        if (opType->Content() == "count") {
            aggTypes.push_back(ctx.MakeType<TItemExprType>(aggName->Content(), ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));
        } else if (opType->Content() == "sum") {
            auto colType = structType->FindItemType(colName->Content());
            const TTypeAnnotationNode* resultType = nullptr;
            if(!GetSumResultType(node->Pos(), *colType, resultType, ctx)) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()),
                    TStringBuilder() << "Unsupported type: " << FormatType(colType) << ". Expected Data or Optional of Data or Null."));
                return TStatus::Error;
            }
            aggTypes.push_back(ctx.MakeType<TItemExprType>(aggName->Content(), resultType));
        } else if (opType->Content() == "min" || opType->Content() == "max" || opType->Content() == "some") {
            auto colType = structType->FindItemType(colName->Content());
            aggTypes.push_back(ctx.MakeType<TItemExprType>(aggName->Content(), colType));
        } else {
            ctx.AddError(TIssue(
                ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Unsupported operation type in OLAP aggregation, got: " << opType->Content()
            ));
            return TStatus::Error;
        }
    }

    if (!EnsureTuple(*node->Child(TKqpOlapAgg::idx_KeyColumns), ctx)) {
        return TStatus::Error;
    }
    for (auto keyCol : node->Child(TKqpOlapAgg::idx_KeyColumns)->ChildrenList()) {
        if (!EnsureAtom(*keyCol, ctx)) {
            ctx.AddError(TIssue(
                ctx.GetPosition(node->Pos()),
                TStringBuilder() << "Expected column name in OLAP key columns, got: " << keyCol->Content()
            ));
            return TStatus::Error;
        }
        aggTypes.push_back(ctx.MakeType<TItemExprType>(keyCol->Content(), structType->FindItemType(keyCol->Content())));
    }

    node->SetTypeAnn(MakeSequenceType(input->GetTypeAnn()->GetKind(), *ctx.MakeType<TStructExprType>(aggTypes), ctx));
    return TStatus::Ok;
}

TStatus AnnotateOlapDistinct(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* input = node->Child(TKqpOlapDistinct::idx_Input);

    const TTypeAnnotationNode* itemType;
    if (!EnsureNewSeqType<false, false, true>(*input, ctx, &itemType)) {
        return TStatus::Error;
    }

    if (!EnsureStructType(input->Pos(), *itemType, ctx)) {
        return TStatus::Error;
    }

    auto structType = itemType->Cast<TStructExprType>();

    auto* key = node->Child(TKqpOlapDistinct::idx_Key);
    if (!EnsureAtom(*key, ctx)) {
        ctx.AddError(TIssue(
            ctx.GetPosition(node->Pos()),
            TStringBuilder() << "Expected column name atom in OLAP distinct, got: " << key->Content()
        ));
        return TStatus::Error;
    }

    const TStringBuf keyName = key->Content();
    const TTypeAnnotationNode* keyItemType = structType->FindItemType(keyName);
    if (!keyItemType) {
        const auto inputPtr = node->ChildPtr(TKqpOlapDistinct::idx_Input);
        for (const auto& projNode : FindNodes(inputPtr, [&](const TExprNode::TPtr& n) {
                if (!TKqpOlapProjection::Match(n.Get())) {
                    return false;
                }
                const auto proj = TExprBase(n).Cast<TKqpOlapProjection>();
                return TString(proj.ColumnName()) == TString(keyName);
            })) {
            if (const auto* ta = projNode->GetTypeAnn().Get()) {
                keyItemType = ta;
                break;
            }
        }
        if (!keyItemType) {
            for (const auto& projsNode : FindNodes(inputPtr, [&](const TExprNode::TPtr& n) { return TKqpOlapProjections::Match(n.Get()); })) {
                const auto* projections = projsNode->Child(TKqpOlapProjections::idx_Projections);
                for (const auto& expr : TExprBase(projections).Cast<TExprList>()) {
                    if (!TKqpOlapProjection::Match(expr.Raw())) {
                        continue;
                    }
                    const auto proj = TExprBase(expr).Cast<TKqpOlapProjection>();
                    if (TString(proj.ColumnName()) != TString(keyName)) {
                        continue;
                    }
                    if (const auto* ta = expr.Raw()->GetTypeAnn().Get()) {
                        keyItemType = ta;
                        break;
                    }
                }
                if (keyItemType) {
                    break;
                }
            }
        }
        if (!keyItemType) {
            const auto jsonVals = FindNodes(inputPtr, [&](const TExprNode::TPtr& n) { return TKqpOlapJsonValue::Match(n.Get()); });
            if (jsonVals.size() == 1) {
                if (const auto* ta = jsonVals.front()->GetTypeAnn().Get()) {
                    keyItemType = ta;
                }
            }
        }
    }
    if (!keyItemType) {
        ctx.AddError(TIssue(
            ctx.GetPosition(key->Pos()),
            TStringBuilder() << "OLAP DISTINCT key '" << keyName
                << "' is not present in the input row type and no matching OLAP projection or JSON_VALUE was found"
        ));
        return TStatus::Error;
    }

    TVector<const TItemExprType*> outFields;
    outFields.push_back(ctx.MakeType<TItemExprType>(TString{keyName}, keyItemType));

    node->SetTypeAnn(MakeSequenceType(input->GetTypeAnn()->GetKind(), *ctx.MakeType<TStructExprType>(outFields), ctx));
    return TStatus::Ok;
}


TStatus AnnotateOlapExtractMembers(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TTypeAnnotationNode* nodeItemType = nullptr;
    if (!EnsureNewSeqType<true>(node->Head(), ctx, &nodeItemType)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureStructType(node->Head().Pos(), *nodeItemType, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto structType = nodeItemType->Cast<TStructExprType>();
    TVector<const TItemExprType*> resItems;
    for (auto& x : node->Tail().Children()) {
        YQL_ENSURE(x->IsAtom());
        auto pos = NYql::NTypeAnnImpl::FindOrReportMissingMember(x->Content(), node->Head().Pos(), *structType, ctx);
        if (!pos) {
            return IGraphTransformer::TStatus::Error;
        }

        resItems.push_back(structType->GetItems()[*pos]);
    }

    const auto resItemType = ctx.MakeType<TStructExprType>(resItems);
    if (!resItemType->Validate(node->Pos(), ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    node->SetTypeAnn(MakeSequenceType(node->Head().GetTypeAnn()->GetKind(), *resItemType, ctx));
    return IGraphTransformer::TStatus::Ok;
}

TStatus AnnotateKqpTxInternalBinding(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* kind = node->Child(TKqpTxInternalBinding::idx_Kind);
    if (!EnsureAtom(*kind, ctx)) {
        return TStatus::Error;
    }

    if (kind->IsAtom("Unspecified")) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "Unspecified internal binding kind"));
        return TStatus::Error;
    }

    auto* type = node->Child(TKqpTxInternalBinding::idx_Type);
    if (!EnsureType(*type, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(type->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
    return TStatus::Ok;
}

TStatus AnnotateKqpTxResultBinding(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    auto* txIndex = node->Child(TKqpTxResultBinding::idx_TxIndex);
    auto* txResultIndex = node->Child(TKqpTxResultBinding::idx_ResultIndex);
    if (!EnsureAtom(*txIndex, ctx) || !EnsureAtom(*txResultIndex, ctx)) {
        return TStatus::Error;
    }

    auto* type = node->Child(TKqpTxResultBinding::idx_Type);
    if (!EnsureType(*type, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(type->GetTypeAnn()->Cast<TTypeExprType>()->GetType());
    return TStatus::Ok;
}

TStatus AnnotateKqpPhysicalTx(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    // TODO: ???

    node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}

TStatus AnnotateKqpPhysicalQuery(const TExprNode::TPtr& node, TExprContext& ctx, bool enableRBO) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    // We need to infer the type of physical query for RBO at this time
    if (enableRBO) {
        TKqpPhysicalQuery query(node);
        auto type = query.Results().Item(0).Ptr()->GetTypeAnn();
        node->SetTypeAnn(type);
    }
    else {
        node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    }
    return TStatus::Ok;
}

bool IsExpectedEffect(const NYql::TExprNode* effect) {
    return TKqpUpsertRows::Match(effect)
        || TKqpDeleteRows::Match(effect)
        || TKqpWriteConstraint::Match(effect);
}

TStatus AnnotateKqpEffects(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto kqpEffectType = MakeKqpEffectType(ctx);

    for (const auto& arg : node->ChildrenList()) {
        if (!EnsureCallable(*arg, ctx)) {
            return TStatus::Error;
        }

        if (!IsExpectedEffect(arg.Get())) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "Unexpected effect: " << arg->Content()));
            return TStatus::Error;
        }

        if (!EnsureStreamType(*arg, ctx)) {
            return TStatus::Error;
        }

        auto itemType = arg->GetTypeAnn()->Cast<TStreamExprType>()->GetItemType();
        if (!IsSameAnnotation(*kqpEffectType, *itemType)) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "Invalid YDB effect type, expected: " << FormatType(kqpEffectType)
                << ", actual: " << FormatType(itemType)));
            return TStatus::Error;
        }
    }

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(kqpEffectType));
    return TStatus::Ok;
}

TStatus AnnotateWriteConstraint(const TExprNode::TPtr& node, TExprContext& ctx) {
    Y_UNUSED(ctx);

    auto* input = node->Child(TKqpWriteConstraint::idx_Input);
    if (!input->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(input->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateSequencer(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData)
{
    auto& input = node->ChildRef(TKqlSequencer::idx_Input);

    const TStructExprType *rowType = nullptr;

    auto inputType = input->GetTypeAnn();
    if (inputType->GetKind() == ETypeAnnotationKind::List) {
        auto listType = inputType->Cast<TListExprType>();
        auto itemType = listType->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
          rowType = itemType->Cast<TStructExprType>();
        }
    } else if (inputType->GetKind() == ETypeAnnotationKind::Stream) {
        auto streamType = inputType->Cast<TStreamExprType>();
        auto itemType = streamType->GetItemType();
        if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
          rowType = itemType->Cast<TStructExprType>();
        }
    }

    auto resolveResult = ResolveTable(node->Child(TKqlSequencer::idx_Table), ctx, cluster, tablesData);
    if (!resolveResult.second) {
        return TStatus::Error;
    }

    auto table = resolveResult.second;
    YQL_ENSURE(rowType);
    absl::flat_hash_set<TString, THash<TString>> columnsToGenerate;
    TCoAtomList generatedOnWriteColumns (node->Child(TKqlSequencer::idx_DefaultConstraintColumns));
    for(const auto& col : generatedOnWriteColumns) {
        auto [_, inserted] = columnsToGenerate.emplace(TString(col.Value()));
        YQL_ENSURE(inserted, "unexpected duplicates in the names of columns.");
    }

    TVector<const TItemExprType *> seqRowTypeItems = rowType->GetItems();
    for (auto &column : columnsToGenerate) {
        auto columnType = table->GetColumnType(column);
        YQL_ENSURE(columnType);
        seqRowTypeItems.push_back(
            ctx.MakeType<TItemExprType>(column, columnType));
    }

    const TTypeAnnotationNode *expectedRowType =
        ctx.MakeType<TStructExprType>(seqRowTypeItems);

    auto listSeqType = ctx.MakeType<TListExprType>(expectedRowType);
    node->SetTypeAnn(listSeqType);

    return TStatus::Ok;
}

TStatus AnnotateKqpPredicateClosure(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* argsType = node->Child(TKqpPredicateClosure::idx_ArgsType);

    if (!EnsureType(*argsType, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTupleRaw = argsType->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    if (!EnsureTupleType(node->Pos(), *argTypesTupleRaw, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTuple = argTypesTupleRaw->Cast<TTupleExprType>();

    std::vector<const TTypeAnnotationNode*> argTypes;
    argTypes.reserve(argTypesTuple->GetSize());

    for (const auto& argTypeRaw : argTypesTuple->GetItems()) {
        if (!EnsureStructType(node->Pos(), *argTypeRaw, ctx)) {
            return TStatus::Error;
        }
        argTypes.push_back(argTypeRaw);
    }

    auto& lambda = node->ChildRef(TKqpPredicateClosure::idx_Lambda);
    if (!EnsureLambda(*lambda, ctx)) {
        return TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, argTypes, ctx)) {
        return TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}

TStatus AnnotateKqpProgram(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* argsType = node->Child(TKqpProgram::idx_ArgsType);

    if (!EnsureType(*argsType, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTupleRaw = argsType->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    if (!EnsureTupleType(node->Pos(), *argTypesTupleRaw, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTuple = argTypesTupleRaw->Cast<TTupleExprType>();

    std::vector<const TTypeAnnotationNode*> argTypes;
    argTypes.reserve(argTypesTuple->GetSize());

    for (const auto& argTypeRaw : argTypesTuple->GetItems()) {
        if (!EnsureStreamType(node->Pos(), *argTypeRaw, ctx)) {
            return TStatus::Error;
        }
        argTypes.push_back(argTypeRaw);
    }

    auto& lambda = node->ChildRef(TKqpProgram::idx_Lambda);
    if (!EnsureLambda(*lambda, ctx)) {
        return TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, argTypes, ctx)) {
        return TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}

TStatus AnnotateKqpEnsure(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureComputable(node->Head(), ctx)) {
        return TStatus::Error;
    }

    const TDataExprType* dataType;
    bool isOptional;
    if (!EnsureDataOrOptionalOfData(*node->Child(TKqpEnsure::idx_Predicate), isOptional, dataType, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureSpecificDataType(node->Child(TKqpEnsure::idx_Predicate)->Pos(), *dataType, EDataSlot::Bool, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqpEnsure::idx_IssueCode), ctx)) {
        return TStatus::Error;
    }

    ui64 code;
    if (!TryFromString(node->Child(TKqpEnsure::idx_IssueCode)->Content(), code)) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
            << "Issue code expected to be an integer: " << node->Child(TKqpEnsure::idx_IssueCode)->Content()));
        return TStatus::Error;
    }

    const auto* message = node->Child(TKqpEnsure::idx_Message);
    if (message->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(message->Pos()), TStringBuilder()
            << "Expected Utf8, but got: " << *message->GetTypeAnn()));
        return TStatus::Error;
    }

    if (const auto dataSlot = message->GetTypeAnn()->Cast<TDataExprType>()->GetSlot(); dataSlot != EDataSlot::Utf8) {
        ctx.AddError(TIssue(ctx.GetPosition(message->Pos()), TStringBuilder()
            << "Expected Utf8, but got: " << *message->GetTypeAnn()));
        return TStatus::Error;
    }

    node->SetTypeAnn(node->Head().GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateKqpLockAndCheck(
        const TExprNode::TPtr& node,
        TExprContext& ctx,
        const TString& cluster,
        const TKikimrTablesData& tablesData) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*node->Child(TKqpLockAndCheck::idx_Input), ctx)) {
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqpLockAndCheck::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
            << "Unknown table in KqpLockAndCheck."));
        return TStatus::Error;
    }

    const TTypeAnnotationNode* inputType = node->ChildRef(TKqpLockAndCheck::idx_Input)->GetTypeAnn();
    const TTypeAnnotationNode* itemType = inputType->Cast<TListExprType>()->GetItemType();

    for (const auto& column : itemType->Cast<TStructExprType>()->GetItems()) {
        if (!table.second->Metadata->Columns.contains(column->GetName())) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "Unknown column in KqpLockAndCheck: `" << column->GetName() << "`."));
            return TStatus::Error;
        }
    }

    auto& filterLambda = node->ChildRef(TKqpLockAndCheck::idx_Lambda);
    if (!EnsureLambda(*filterLambda, ctx)) {
        return TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(filterLambda, {itemType}, ctx)) {
        return TStatus::Error;
    }

    if (const auto filterLambdaType = filterLambda->GetTypeAnn()) {
        if (filterLambdaType->GetKind() != ETypeAnnotationKind::Data) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "KqpLockAndCheck lambda bad annotation kind."));
            return TStatus::Error;
        }
        auto dataExprType = filterLambdaType->Cast<TDataExprType>();
        if (dataExprType->GetSlot() != EDataSlot::Bool) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder()
                << "KqpLockAndCheck lambda return type is not Bool."));
            return TStatus::Error;
        }
    } else {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(inputType);
    return TStatus::Ok;
}


TStatus AnnotateKqpStreamEnumerate(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    const TTypeAnnotationNode* inputType = node->Child(0)->GetTypeAnn();
    if (!inputType) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(0)->Pos()), "KqpStreamEnumerate: input has no type"));
        return TStatus::Error;
    }

    const TTypeAnnotationNode* itemType = nullptr;
    const auto kind = inputType->GetKind();
    switch (kind) {
        case ETypeAnnotationKind::List:   itemType = inputType->Cast<TListExprType>()->GetItemType(); break;
        case ETypeAnnotationKind::Flow:   itemType = inputType->Cast<TFlowExprType>()->GetItemType(); break;
        case ETypeAnnotationKind::Stream: itemType = inputType->Cast<TStreamExprType>()->GetItemType(); break;
        default:
            ctx.AddError(TIssue(ctx.GetPosition(node->Child(0)->Pos()), TStringBuilder()
                << "KqpStreamEnumerate: expected List, Flow or Stream, but got: " << *inputType));
            return TStatus::Error;
    }

    const auto* rankType = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
    const auto* pairType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{rankType, itemType});

    const TTypeAnnotationNode* resultType = nullptr;
    switch (kind) {
        case ETypeAnnotationKind::List:   resultType = ctx.MakeType<TListExprType>(pairType); break;
        case ETypeAnnotationKind::Flow:   resultType = ctx.MakeType<TFlowExprType>(pairType); break;
        case ETypeAnnotationKind::Stream: resultType = ctx.MakeType<TStreamExprType>(pairType); break;
        default: Y_ABORT("unreachable");
    }

    node->SetTypeAnn(resultType);
    return TStatus::Ok;
}

TStatus AnnotateFulltextAnalyze(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 3, ctx)) {
        return TStatus::Error;
    }

    // First argument: text (String, Utf8, Json, or JsonDocument — same payload as string for analyzers / tokenize)
    const auto* textArg = node->Child(0);
    if (!EnsureComputable(*textArg, ctx)) {
        return TStatus::Error;
    }

    const TDataExprType* textDataType;
    bool isOptional;
    if (!EnsureDataOrOptionalOfData(*textArg, isOptional, textDataType, ctx)) {
        return TStatus::Error;
    }

    const auto textSlot = textDataType->GetSlot();
    switch (textSlot) {
        case EDataSlot::String:
        case EDataSlot::Utf8:
        case EDataSlot::Json:
        case EDataSlot::JsonDocument:
            break;
        default:
            ctx.AddError(TIssue(ctx.GetPosition(textArg->Pos()), TStringBuilder()
            << "Expected String, Utf8, Json, or JsonDocument for text argument, but got: " << *textArg->GetTypeAnn()));
            return TStatus::Error;
    }

    // Second argument: settings (should be String - serialized proto)
    const auto* settingsArg = node->Child(1);
    if (!EnsureComputable(*settingsArg, ctx)) {
        return TStatus::Error;
    }

    const TDataExprType* settingsDataType;
    if (!EnsureDataOrOptionalOfData(*settingsArg, isOptional, settingsDataType, ctx)) {
        return TStatus::Error;
    }

    if (settingsDataType->GetSlot() != EDataSlot::String) {
        ctx.AddError(TIssue(ctx.GetPosition(settingsArg->Pos()), TStringBuilder()
            << "Expected String for settings argument, but got: " << *settingsArg->GetTypeAnn()));
        return TStatus::Error;
    }

    // Third argument: mode (should be Atom - "0" (any fulltext), "1" (JI on Json) or "2" (JI on JsonDocument))
    const auto* modeArg = node->Child(2);
    if (!EnsureAtom(*modeArg, ctx)) {
        return TStatus::Error;
    }

    // Return type: List<Struct<__ydb_token:String or Utf8,__ydb_freq:Uint32>>
    const EDataSlot tokenSlot = (textSlot == EDataSlot::Json || textSlot == EDataSlot::JsonDocument) ? EDataSlot::String : textSlot;
    auto stringType = ctx.MakeType<TDataExprType>(tokenSlot);
    TVector<const TItemExprType*> rowItems;
    rowItems.push_back(ctx.MakeType<TItemExprType>(NTableIndex::NFulltext::TokenColumn, stringType));
    rowItems.push_back(ctx.MakeType<TItemExprType>(NTableIndex::NFulltext::FreqColumn, ctx.MakeType<TDataExprType>(EDataSlot::Uint32)));
    auto rowType = ctx.MakeType<TStructExprType>(rowItems);
    auto listType = ctx.MakeType<TListExprType>(rowType);
    node->SetTypeAnn(listType);

    return TStatus::Ok;
}

TStatus AnnotateSequencerConnection(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{
    if (!EnsureArgsCount(*node, 5, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*node->Child(TKqpCnSequencer::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(node->Child(TKqpCnSequencer::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TDqCnMerge::idx_Output)->Pos()),
            TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqpCnSequencer::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqpCnSequencer::idx_Columns), ctx)) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqpCnSequencer::idx_DefaultConstraintColumns), ctx)) {
        return TStatus::Error;
    }

    auto inputNodeTypeNode = node->Child(TKqpCnSequencer::idx_InputItemType);
    if (!EnsureType(*inputNodeTypeNode, ctx)) {
        return TStatus::Error;
    }

    TCoAtomList columns{node->ChildPtr(TKqpCnSequencer::idx_Columns)};
    auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, columns, withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));
    return TStatus::Ok;
}

TStatus AnnotateStreamLookupConnection(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns) {

    if (!EnsureArgsCount(*node, 5, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*node->Child(TKqpCnStreamLookup::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(node->Child(TKqpCnStreamLookup::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnStreamLookup::idx_Output)->Pos()),
            TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    auto table = ResolveTable(node->Child(TKqpCnStreamLookup::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    if (!EnsureTupleOfAtoms(*node->Child(TKqpCnStreamLookup::idx_Columns), ctx)) {
        return TStatus::Error;
    }

    TCoAtomList columns{node->ChildPtr(TKqpCnStreamLookup::idx_Columns)};
    auto inputTypeNode = node->Child(TKqpCnStreamLookup::idx_InputType);

    if (!EnsureType(*inputTypeNode, ctx)) {
        return TStatus::Error;
    }

    auto inputType = inputTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    const TTypeAnnotationNode* inputItemType;
    if (!EnsureNewSeqType<false>(node->Pos(), *inputType, ctx, &inputItemType)) {
        return TStatus::Error;
    }

    YQL_ENSURE(inputItemType);

    TCoNameValueTupleList settingsNode{node->ChildPtr(TKqpCnStreamLookup::idx_Settings)};
    auto settings = TKqpStreamLookupSettings::Parse(settingsNode);

    if (settings.Strategy == EStreamLookupStrategyType::LockAndLookupRows) {
        if (!EnsureStructType(node->Pos(), *inputItemType, ctx)) {
            return TStatus::Error;
        }

        auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, columns, withSystemColumns);
        if (!rowType) {
            return TStatus::Error;
        }

        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));
    } else if (settings.Strategy == EStreamLookupStrategyType::LookupRows
        || settings.Strategy == EStreamLookupStrategyType::LookupUniqueRows) {

        if (!EnsureStructType(node->Pos(), *inputItemType, ctx)) {
            return TStatus::Error;
        }

        const auto& lookupKeyColumns = inputItemType->Cast<TStructExprType>()->GetItems();
        for (const auto& keyColumn : lookupKeyColumns) {
            if (!table.second->GetKeyColumnIndex(TString(keyColumn->GetName()))) {
                return TStatus::Error;
            }
        }

        auto rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, columns, withSystemColumns);
        if (!rowType) {
            return TStatus::Error;
        }

        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));

    } else if (settings.Strategy == EStreamLookupStrategyType::LookupJoinRows
        || settings.Strategy == EStreamLookupStrategyType::LookupSemiJoinRows) {

        if (!EnsureTupleType(node->Pos(), *inputItemType, ctx)) {
            return TStatus::Error;
        }

        auto inputTupleType = inputItemType->Cast<TTupleExprType>();
        if (inputTupleType->GetSize() < 2 || inputTupleType->GetSize() > 3) {
            ctx.AddError(
                TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << "Table stream lookup has unexpected input tuple, expected tuple size 2 or 3, but found %s"
                    << inputTupleType->GetSize()));
            return TStatus::Error;
        }

        if (!EnsureOptionalType(node->Pos(), *inputTupleType->GetItems()[1], ctx)) {
            return TStatus::Error;
        }

        auto joinKeyType = inputTupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
        if (!EnsureStructType(node->Pos(), *joinKeyType, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureStructType(node->Pos(), *inputTupleType->GetItems()[0], ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* joinKeys = joinKeyType->Cast<TStructExprType>();
        const TStructExprType* leftRowType = inputTupleType->GetItems()[0]->Cast<TStructExprType>();

        for (const auto& inputKey : joinKeys->GetItems()) {
            if (!table.second->GetKeyColumnIndex(TString(inputKey->GetName()))) {
                return TStatus::Error;
            }
        }

        auto rightRowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, columns, withSystemColumns);
        if (!rightRowType) {
            return TStatus::Error;
        }

        TVector<const TTypeAnnotationNode*> outputTypes;
        outputTypes.push_back(leftRowType);
        outputTypes.push_back(ctx.MakeType<TOptionalExprType>(rightRowType));
        outputTypes.push_back(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));

        auto outputItemType = ctx.MakeType<TTupleExprType>(outputTypes);
        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(outputItemType));

    } else {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnStreamLookup::idx_Settings)->Pos()),
            TStringBuilder() << "Unexpected lookup strategy: " << settings.Strategy));
        return TStatus::Error;
    }

    return TStatus::Ok;
}

TStatus AnnotateVectorResolveConnection(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData) {

    if (!EnsureArgsCount(*node, 5, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureCallable(*node->Child(TKqpCnVectorResolve::idx_Output), ctx)) {
        return TStatus::Error;
    }

    if (!TDqOutput::Match(node->Child(TKqpCnVectorResolve::idx_Output))) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnVectorResolve::idx_Output)->Pos()),
            TStringBuilder() << "Expected " << TDqOutput::CallableName()));
        return TStatus::Error;
    }

    // Check table
    auto table = ResolveTable(node->Child(TKqpCnVectorResolve::idx_Table), ctx, cluster, tablesData);
    auto tableDesc = table.second;
    if (!tableDesc) {
        return TStatus::Error;
    }

    YQL_ENSURE(tableDesc->Metadata, "Expected loaded metadata");

    auto indexName = node->Child(TKqpCnVectorResolve::idx_Index)->Content();
    TIndexDescription *indexDesc = nullptr;
    for (auto& index: tableDesc->Metadata->Indexes) {
        if (index.Name == indexName) {
            indexDesc = &index;
        }
    }
    if (!indexDesc) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnVectorResolve::idx_Index)->Pos()),
            TStringBuilder() << "Index does not exist"));
        return TStatus::Error;
    }
    if (indexDesc->Type != TIndexDescription::EType::GlobalSyncVectorKMeansTree) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnVectorResolve::idx_Index)->Pos()),
            TStringBuilder() << "Index is not a vector index"));
        return TStatus::Error;
    }

    // Check transform input type
    auto inputTypeNode = node->Child(TKqpCnVectorResolve::idx_InputType);
    if (!EnsureType(*inputTypeNode, ctx)) {
        return TStatus::Error;
    }

    auto inputType = inputTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    const TTypeAnnotationNode* inputItemType;
    if (!EnsureNewSeqType<false>(node->Pos(), *inputType, ctx, &inputItemType)) {
        return TStatus::Error;
    }

    YQL_ENSURE(inputItemType);

    if (!EnsureStructType(node->Pos(), *inputItemType, ctx)) {
        return TStatus::Error;
    }

    const auto& inputColumns = inputItemType->Cast<TStructExprType>()->GetItems();
    TSet<TString> inputColSet;
    for (const auto& keyColumn : inputColumns) {
        inputColSet.insert(TString(keyColumn->GetName()));
    }

    // Input must contain PK columns and index key columns
    // Index data columns may also be requested but they're not required
    for (const auto& keyColumn : tableDesc->Metadata->KeyColumnNames) {
        if (!inputColSet.contains(keyColumn)) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnVectorResolve::idx_InputType)->Pos()),
                TStringBuilder() << "Input must contain all table PK columns"));
            return TStatus::Error;
        }
    }
    if (!inputColSet.contains(indexDesc->KeyColumns.back())) {
        ctx.AddError(TIssue(ctx.GetPosition(node->Child(TKqpCnVectorResolve::idx_InputType)->Pos()),
            TStringBuilder() << "Input must contain the embedding column: " << indexDesc->KeyColumns.back()));
        return TStatus::Error;
    }

    // Generate output type
    TVector<const TItemExprType*> rowItems;
    TSet<TString> outputColSet;

    // First cluster ID
    rowItems.push_back(ctx.MakeType<TItemExprType>(NTableIndex::NKMeans::ParentColumn, ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));

    // Then primary key columns
    for (const auto& keyColumn : tableDesc->Metadata->KeyColumnNames) {
        auto type = tableDesc->GetColumnType(keyColumn);
        YQL_ENSURE(type, "No key column: " << keyColumn);

        auto itemType = ctx.MakeType<TItemExprType>(keyColumn, type);
        if (!itemType->Validate(node->Pos(), ctx)) {
            return TStatus::Error;
        }
        rowItems.push_back(itemType);
        outputColSet.insert(keyColumn);
    }

    if (node->Child(TKqpCnVectorResolve::idx_WithData)->Content() == "true") {
        // Then index data columns which are not also part of the PK
        for (const auto& dataColumn : indexDesc->DataColumns) {
            YQL_ENSURE(inputColSet.contains(dataColumn), "No data column in input: " << dataColumn);
            if (outputColSet.contains(dataColumn)) {
                continue;
            }
            auto type = tableDesc->GetColumnType(dataColumn);
            YQL_ENSURE(type, "No data column: " << dataColumn);

            auto itemType = ctx.MakeType<TItemExprType>(dataColumn, type);
            if (!itemType->Validate(node->Pos(), ctx)) {
                return TStatus::Error;
            }
            rowItems.push_back(itemType);
            outputColSet.insert(dataColumn);
        }
    }

    auto rowType = ctx.MakeType<TStructExprType>(rowItems);
    if (!rowType->Validate(node->Pos(), ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType));

    return TStatus::Ok;
}

TStatus AnnotateIndexLookupJoin(const TExprNode::TPtr& node, TExprContext& ctx) {

    if (!EnsureArgsCount(*node, 4, ctx)) {
        return TStatus::Error;
    }

    auto inputType = node->Child(TKqlIndexLookupJoinBase::idx_Input)->GetTypeAnn();
    const TTypeAnnotationNode* inputItemType;
    if (!EnsureNewSeqType<false>(node->Pos(), *inputType, ctx, &inputItemType)) {
        return TStatus::Error;
    }

    YQL_ENSURE(inputItemType);
    if (!EnsureTupleType(node->Pos(), *inputItemType, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureTupleTypeSize(node->Pos(), inputItemType, 3, ctx)) {
        return TStatus::Error;
    }

    auto inputTupleType = inputItemType->Cast<TTupleExprType>();
    if (!EnsureStructType(node->Pos(), *inputTupleType->GetItems()[0], ctx)) {
        return TStatus::Error;
    }

    const TStructExprType* leftRowType = inputTupleType->GetItems()[0]->Cast<TStructExprType>();

    if (!EnsureOptionalType(node->Pos(), *inputTupleType->GetItems()[1], ctx)) {
        return TStatus::Error;
    }

    auto rightRowType = inputTupleType->GetItems()[1]->Cast<TOptionalExprType>()->GetItemType();
    if (!EnsureStructType(node->Pos(), *rightRowType, ctx)) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqlIndexLookupJoinBase::idx_JoinType), ctx)) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqlIndexLookupJoinBase::idx_LeftLabel), ctx)) {
        return TStatus::Error;
    }

    TCoAtom leftLabel(node->Child(TKqlIndexLookupJoinBase::idx_LeftLabel));

    if (!EnsureAtom(*node->Child(TKqlIndexLookupJoinBase::idx_RightLabel), ctx)) {
        return TStatus::Error;
    }

    TCoAtom rightLabel(node->Child(TKqlIndexLookupJoinBase::idx_RightLabel));
    TCoAtom joinType(node->Child(TKqlIndexLookupJoinBase::idx_JoinType));

    TVector<const TItemExprType*> resultStructItems;
    for (const auto& item : leftRowType->GetItems()) {
        TString itemName = leftLabel.Value().empty()
            ? TString(item->GetName())
            : TString::Join(leftLabel.Value(), ".", item->GetName());
        resultStructItems.emplace_back(ctx.MakeType<TItemExprType>(itemName, item->GetItemType()));
    }

    if (RightJoinSideAllowed(joinType.Value())) {
        for (const auto& item : rightRowType->Cast<TStructExprType>()->GetItems()) {
            const bool makeOptional = RightJoinSideOptional(joinType.Value()) && !item->GetItemType()->IsOptionalOrNull();

            const TTypeAnnotationNode* itemType = makeOptional
                ? ctx.MakeType<TOptionalExprType>(item->GetItemType())
                : item->GetItemType();

            TString itemName = rightLabel.Value().empty()
                ? TString(item->GetName())
                : TString::Join(rightLabel.Value(), ".", item->GetName());
            resultStructItems.emplace_back(ctx.MakeType<TItemExprType>(itemName, itemType));
        }
    }

    auto outputRowType = ctx.MakeType<TStructExprType>(resultStructItems);
    const bool isPhysical = TKqpIndexLookupJoin::Match(node.Get());
    if (isPhysical) {
        node->SetTypeAnn(ctx.MakeType<TStreamExprType>(outputRowType));
    } else {
        node->SetTypeAnn(ctx.MakeType<TListExprType>(outputRowType));
    }

    return TStatus::Ok;
}

TStatus AnnotateExternalEffect(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 1, ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(node->Child(TKqlExternalEffect::idx_Input)->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateKqpSinkEffect(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    if (!TDqStageBase::Match(node->Child(TKqpSinkEffect::idx_Stage))) {
        return TStatus::Error;
    }

    if (!EnsureAtom(*node->Child(TKqpSinkEffect::idx_SinkIndex), ctx)) {
        return TStatus::Error;
    }

    node->SetTypeAnn(node->Child(TKqpSinkEffect::idx_Stage)->GetTypeAnn());
    return TStatus::Ok;
}

TStatus AnnotateTableSinkSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (!EnsureMinMaxArgsCount(*input, 9, 10, ctx)) {
        return TStatus::Error;
    }
    input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}

TStatus AnnotateSublinkBase(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto subquery = node->Child(TKqpSublinkBase::idx_Subquery);
    auto itemType = subquery->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto valueType = itemType->GetItems()[0]->GetItemType();

    if (TKqpExprSublink::Match(node.Get())) {
        if (!valueType->IsOptionalOrNull()) {
            valueType = ctx.MakeType<TOptionalExprType>(valueType);
        }
        node->SetTypeAnn(valueType);
        return TStatus::Ok;
    }

    YQL_CLOG(TRACE, CoreDq) << "Checking boolean sublink";


    if (TKqpInSublink::Match(node.Get())) {
        auto& lambda = node->ChildRef(TKqpInSublink::idx_InLambda);
        auto outerTypeNode = node->Child(TKqpInSublink::idx_OuterType);
        auto outerType = outerTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
      
        
        TVector<const TItemExprType*> newItemTypes;
        // Need to modify the outer type to remove _alias prefix
        for (auto itemType : outerType->GetItems()) {
            auto oldTypeName = TString(itemType->GetName());
            if (oldTypeName.StartsWith("_alias_")) {
                auto newTypeName = oldTypeName.substr(7);
                newItemTypes.push_back(ctx.MakeType<TItemExprType>(newTypeName, itemType->GetItemType()));
            }
            newItemTypes.push_back(ctx.MakeType<TItemExprType>(oldTypeName, itemType->GetItemType()));
        }
        auto fixedOuterType = ctx.MakeType<TStructExprType>(newItemTypes);

        YQL_CLOG(TRACE, CoreDq) << "Outer type: " << *(TTypeAnnotationNode*)fixedOuterType;

        if (!UpdateLambdaAllArgumentsTypes(lambda, { fixedOuterType, valueType }, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    node->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Bool));

    return TStatus::Ok;
}

TStatus AnnotateInfuseDependents(const TExprNode::TPtr& node, TExprContext& ctx) {
    auto input = node->Child(TKqpInfuseDependents::idx_Input);
    auto itemType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> structItemTypes = itemType->GetItems();

    auto columns = node->Child(TKqpInfuseDependents::idx_Columns);
    auto types = node->Child(TKqpInfuseDependents::idx_Types);

    for (size_t i=0; i<columns->ChildrenSize(); i++) {
        auto columnType = types->Child(i)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        auto correlatedType = ctx.MakeType<TItemExprType>(columns->Child(i)->Content(), columnType);
        structItemTypes.push_back(correlatedType);
    }

    auto newStructType = ctx.MakeType<TStructExprType>(structItemTypes);
    node->SetTypeAnn(ctx.MakeType<TListExprType>(newStructType));

    return TStatus::Ok;
}

TStatus AnnotateOpRead(const TExprNode::TPtr& node, TExprContext& ctx, const TString& cluster,
    const TKikimrTablesData& tablesData, bool withSystemColumns)
{

    auto table = ResolveTable(node->Child(TKqpOpRead::idx_Table), ctx, cluster, tablesData);
    if (!table.second) {
        return TStatus::Error;
    }

    YQL_ENSURE(table.second->Metadata, "Expected loaded metadata");

    auto meta = table.second->Metadata;

    auto alias = node->Child(TKqpOpRead::idx_Alias);

    const auto& columns = node->ChildPtr(TKqpOpRead::idx_Columns);
    if (!EnsureTupleOfAtoms(*columns, ctx)) {
        return TStatus::Error;
    }

    const TTypeAnnotationNode* rowType = GetReadTableRowType(ctx, tablesData, cluster, table.first, TCoAtomList(columns), withSystemColumns);
    if (!rowType) {
        return TStatus::Error;
    }

    TVector<const TItemExprType*> structItemTypes = rowType->Cast<TStructExprType>()->GetItems();
    TVector<const TItemExprType*> newItemTypes;
    for (const auto *t : structItemTypes) {
        TString aliasName = TString(alias->Content());
        TString columnName = TString(t->GetName());
        TString fullName = aliasName != "" ? aliasName + "." + columnName : columnName;
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(fullName, t->GetItemType()));
    }

    YQL_CLOG(TRACE, CoreDq) << "Row type:" << *rowType;

    auto newStructType = ctx.MakeType<TStructExprType>(newItemTypes);
    node->SetTypeAnn(ctx.MakeType<TListExprType>(newStructType));

    return TStatus::Ok;
}

TStatus AnnotateOpEmptySource(const TExprNode::TPtr& input, TExprContext& ctx) {

    TVector<const TItemExprType*> resultItems;
    auto resultType = ctx.MakeType<TStructExprType>(resultItems);

    input->SetTypeAnn(ctx.MakeType<TListExprType>(resultType));

    return TStatus::Ok;
}

TStatus AnnotateOpMapElementLambda(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto mapElementLambda = TKqpOpMapElementLambda(input);
    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpMapElementLambda::idx_Input)->GetTypeAnn();
    const TTypeAnnotationNode* itemType = inputType->Cast<TListExprType>()->GetItemType();
    auto& lambda = input->ChildRef(TKqpOpMapElementLambda::idx_Lambda);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (mapElementLambda.ForceOptional().StringValue() == "True" && !lambdaType->IsOptionalOrNull()) {
        lambdaType = ctx.MakeType<TOptionalExprType>(lambdaType);
    }

    auto variable = input->ChildRef(TKqpOpMapElementLambda::idx_Variable);
    auto res = ctx.MakeType<TItemExprType>(variable->Content(), lambdaType);

    input->SetTypeAnn(res);
    return TStatus::Ok;
}

TStatus AnnotateOpMapElementRename(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);

    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpMapElementLambda::idx_Input)->GetTypeAnn();
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto typeItems = structType->GetItems();

    auto from = input->ChildRef(TKqpOpMapElementRename::idx_From);
    auto typeIt = std::find_if(typeItems.begin(), typeItems.end(), [&from](const TItemExprType* t){
        return from->Content() == t->GetName();
    });

    if (typeIt==typeItems.end()) {
        YQL_CLOG(TRACE, CoreDq) << "Trying to find " << from->Content() << " in " << *(TTypeAnnotationNode*)structType;
    }

    Y_ENSURE(typeIt!=typeItems.end());

    auto variable = input->ChildRef(TKqpOpMapElementRename::idx_Variable);
    auto res = ctx.MakeType<TItemExprType>(variable->Content(), (*typeIt)->GetItemType());

    input->SetTypeAnn(res);
    return TStatus::Ok;
}

TStatus AnnotateOpMap(const TExprNode::TPtr& input, TExprContext& ctx) {
    TVector<const TItemExprType*> structItemTypes;

    if (input->ChildrenSize() <= TKqpOpMap::idx_Project) {
        const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpMap::idx_Input)->GetTypeAnn();
        auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

        for (auto t : structType->GetItems()) {
            structItemTypes.push_back(t);
        }
    }

    for (size_t idx = 0; idx < input->ChildPtr(TKqpOpMap::idx_MapElements)->ChildrenSize(); idx++) {
        auto& element = input->ChildPtr(TKqpOpMap::idx_MapElements)->ChildRef(idx);
        auto type = (const TTypeAnnotationNode*)element->GetTypeAnn();
        structItemTypes.push_back((const TItemExprType*)type);
    }

    auto resultItemType = ctx.MakeType<TStructExprType>(structItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.MakeType<TListExprType>(resultItemType);

    input->SetTypeAnn(resultAnn);

    YQL_CLOG(TRACE, CoreDq) << "Type annotation for OpMap done: " << *resultAnn;

    return TStatus::Ok;
}

TStatus AnnotateOpProject(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto structType = input->ChildPtr(TKqpOpProject::idx_Input)->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> structItemTypes;
    auto typeItems = structType->GetItems();

    for (size_t i=0; i<input->ChildPtr(TKqpOpProject::idx_ProjectList)->ChildrenSize(); i++) {
        auto proj = input->ChildPtr(TKqpOpProject::idx_ProjectList)->ChildRef(i);
        auto typeItemIt = std::find_if(typeItems.begin(), typeItems.end(), [&proj](const TItemExprType* t){
            return proj->Content() == t->GetName();
        });
        if (typeItemIt == typeItems.end()) {
            continue;
        }

        structItemTypes.push_back(*typeItemIt);
    }

    auto resultItemType = ctx.MakeType<TStructExprType>(structItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.MakeType<TListExprType>(resultItemType);

    input->SetTypeAnn(resultAnn);

    YQL_CLOG(TRACE, CoreDq) << "Type annotation for OpProject done: " << *resultAnn;

    return TStatus::Ok;
}

TStatus AnnotateOpReplaceAlias(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto structType = input->ChildPtr(TKqpOpReplaceAlias::idx_Input)->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    TVector<const TItemExprType*> structItemTypes;
    auto typeItems = structType->GetItems();

    for (const auto& item: typeItems) {
        auto columnName = TString(item->GetName());
        if (auto it = columnName.find("."); it != TString::npos) {
            columnName = columnName.substr(it+1);
        }

        auto alias = TString(input->ChildPtr(TKqpOpReplaceAlias::idx_Alias)->Content());
        columnName = alias + "." + columnName;
        structItemTypes.push_back(ctx.MakeType<TItemExprType>(columnName, item->GetItemType()));
    }

    auto resultItemType = ctx.MakeType<TStructExprType>(structItemTypes);
    const TTypeAnnotationNode* resultAnn = ctx.MakeType<TListExprType>(resultItemType);
    input->SetTypeAnn(resultAnn);
    return TStatus::Ok;
}

TStatus AnnotateOpFilter(const TExprNode::TPtr& input, TExprContext& ctx) {

    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpFilter::idx_Input)->GetTypeAnn();
    YQL_CLOG(TRACE, CoreDq) << "Type annotation for OpFilter, inputType: " << *inputType;

    auto itemType = inputType->Cast<TListExprType>()->GetItemType();

    auto& lambda = input->ChildRef(TKqpOpFilter::idx_Lambda);

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return IGraphTransformer::TStatus::Repeat;
    }

    //if(!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx, false)) {
    //    return IGraphTransformer::TStatus::Error;
    //}

    input->SetTypeAnn(inputType);

    return TStatus::Ok;
}

bool IsSupportedJoinKind(const TString &joinKind) {
    return joinKind == "Left" || joinKind == "Inner" || joinKind == "Cross";
}

const TStructExprType* JoinResultType(const TTypeAnnotationNode* leftType, const TTypeAnnotationNode* rightType, TString joinKind, TExprContext& ctx) {
    auto leftItemType = leftType->Cast<TListExprType>()->GetItemType();
    auto rightItemType = rightType->Cast<TListExprType>()->GetItemType();

    const bool rightSideColumnsNeedsOptional = joinKind == "Left";
    TVector<const TItemExprType*> structItemTypes = leftItemType->Cast<TStructExprType>()->GetItems();

    for (const auto *item : rightItemType->Cast<TStructExprType>()->GetItems()) {
        if (item->GetItemType()->IsOptionalOrNull() || !rightSideColumnsNeedsOptional) {
            structItemTypes.push_back(item);
        } else {
            auto colName = item->GetName();
            const TTypeAnnotationNode* colType = item->GetItemType();
            structItemTypes.push_back(ctx.MakeType<TItemExprType>(colName, ctx.MakeType<TOptionalExprType>(colType)));
        }
    }
    auto resultStructType = ctx.MakeType<TStructExprType>(structItemTypes);
    return resultStructType;
}

TStatus AnnotateOpJoinFilter(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto leftInputType = input->ChildPtr(TKqpOpJoinFilter::idx_LeftInput)->GetTypeAnn();
    auto rightInputType = input->ChildPtr(TKqpOpJoinFilter::idx_RightInput)->GetTypeAnn();
    
    // Join filters operate on top of potenially joined tuples, so inner join is the right semantics
    auto itemType = JoinResultType(leftInputType, rightInputType, "Inner", ctx);

    auto& lambda = input->ChildRef(TKqpOpJoinFilter::idx_Lambda);

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(ctx.MakeType<TVoidExprType>());

    return TStatus::Ok;
}

TStatus AnnotateOpJoin(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto leftInputType = input->ChildPtr(TKqpOpJoin::idx_LeftInput)->GetTypeAnn();
    auto rightInputType = input->ChildPtr(TKqpOpJoin::idx_RightInput)->GetTypeAnn();
    auto opJoin = TKqpOpJoin(input);
    const TString joinKind = TString(opJoin.JoinKind().StringValue());
    Y_ENSURE(IsSupportedJoinKind(joinKind), "Unsupported join kind");

    auto resultStructType = JoinResultType(leftInputType, rightInputType, joinKind, ctx);
    const TTypeAnnotationNode* resultAnn = ctx.MakeType<TListExprType>(resultStructType);
    input->SetTypeAnn(resultAnn);

    return TStatus::Ok;
}

TStatus AnnotateOpUnionAll(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto leftInputType = input->ChildPtr(TKqpOpJoin::idx_LeftInput)->GetTypeAnn();
    auto rightInputType = input->ChildPtr(TKqpOpJoin::idx_RightInput)->GetTypeAnn();
    auto leftStructType = leftInputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto rightStructType = rightInputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto leftItems = leftStructType->GetItems();
    auto rightItems = rightStructType->GetItems();
    Y_ENSURE(leftItems.size() == rightItems.size(), "Invalid number of fields for Union all.");

    TVector<const TItemExprType*> newItemTypes;
    for (ui32 i = 0, e = leftItems.size(); i < e; ++i) {
        if (leftItems[i]->GetItemType()->IsOptionalOrNull()) {
            newItemTypes.push_back(leftItems[i]);
        } else {
            newItemTypes.push_back(rightItems[i]);
        }
    }

    auto resultType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(newItemTypes));
    input->SetTypeAnn(resultType);

    return TStatus::Ok;
}

TStatus AnnotateOpLimit(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpRoot::idx_Input)->GetTypeAnn();
    input->SetTypeAnn(inputType);
    return TStatus::Ok;
}

TStatus AnnotateOpSortElement(const TExprNode::TPtr& input, TExprContext& ctx) {
    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpSortElement::idx_Input)->GetTypeAnn();
    const TTypeAnnotationNode* itemType = inputType->Cast<TListExprType>()->GetItemType();

    auto& lambda = input->ChildRef(TKqpOpSortElement::idx_Lambda);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(lambdaType);
    return TStatus::Ok;
}

TStatus AnnotateOpSort(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpSort::idx_Input)->GetTypeAnn();
    input->SetTypeAnn(inputType);
    return TStatus::Ok;
}

TStatus AnnotateOpAggregate(const TExprNode::TPtr& input, TExprContext& ctx) {
    const auto inputType = input->ChildPtr(TKqpOpAggregate::idx_Input)->GetTypeAnn();
    const auto* structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto opAggregate = TKqpOpAggregate(input);
    const bool scalarAggregation = opAggregate.KeyColumns().Empty();
    auto pos = input->Pos();

    TVector<const TItemExprType*> newItemTypes;
    THashMap<TString, const TTypeAnnotationNode*> aggTraitsMap;
    for (const auto* itemType : structType->GetItems()) {
        const auto itemName = itemType->GetName();
        aggTraitsMap.emplace(itemName, itemType->GetItemType());
    }

    for (const auto& keyColumn : opAggregate.KeyColumns()) {
        auto it = aggTraitsMap.find(TString(keyColumn));
        Y_ENSURE(it != aggTraitsMap.end());
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(it->first, it->second));
    }

    for (const auto& traits : opAggregate.AggregationTraitsList()) {
        const auto originalColName = TString(traits.OriginalColName());
        const auto aggFunction = TString(traits.AggregationFunction());
        const auto resultColName = TString(traits.ResultColName());
        auto it = aggTraitsMap.find(originalColName);
        Y_ENSURE(it != aggTraitsMap.end());
        auto aggFieldType = it->second;

        if (aggFunction == "count") {
            aggFieldType = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        } else if (aggFunction == "sum") {
            Y_ENSURE(GetSumResultType(pos, *it->second, aggFieldType, ctx), "Unsupported type for sum aggregation function.");
        } else if (aggFunction == "avg") {
            Y_ENSURE(GetAvgResultType(pos, *it->second, aggFieldType, ctx), "Unsupported type for avg aggregation function.");
        } else if (aggFunction == "variance_1_1") {
            // I guess it's a same as avg.
            Y_ENSURE(GetAvgResultType(pos, *it->second, aggFieldType, ctx), "Unsupported type for variance aggregation function.");
        }

        // Special case for scalar aggregation (aka aggregation with empty keys).
        if (scalarAggregation && !aggFieldType->IsOptionalOrNull() &&
            (aggFunction == "min" || aggFunction == "max" || aggFunction == "sum" || aggFunction == "avg" || aggFunction == "variance_1_1")) {
            aggFieldType = ctx.MakeType<TOptionalExprType>(aggFieldType);
        }

        newItemTypes.push_back(ctx.MakeType<TItemExprType>(resultColName, aggFieldType));
    }

    auto resultType = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(newItemTypes));
    input->SetTypeAnn(resultType);
    return TStatus::Ok;
}

TStatus AnnotateOpRoot(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);
    const TTypeAnnotationNode* inputType = input->ChildPtr(TKqpOpRoot::idx_Input)->GetTypeAnn();
    input->SetTypeAnn(inputType);
    return TStatus::Ok;
}

class TKiTypeAnnotationTransformer final : public TVisitorTransformerBase {
public:
    TKiTypeAnnotationTransformer(const TString& cluster, TIntrusivePtr<TKikimrTablesData> tablesData, TKikimrConfiguration::TPtr config)
        : TVisitorTransformerBase(/* failOnUnknown */ true)
        , Cluster(cluster)
        , TablesData(std::move(tablesData))
        , Config(std::move(config))
    {
        AddHandler({TKqpTable::CallableName()}, HndlInt(&AnnotateTable));
        AddHandler({
            TKqlReadTable::CallableName(),
            TKqpReadTable::CallableName(),
            TKqlReadTableIndex::CallableName(),
            TKqpWideReadTable::CallableName(),
        }, HndlInt(&AnnotateReadTable));
        AddHandler({
            TKqlReadTableRanges::CallableName(),
            TKqpReadTableRanges::CallableName(),
            TKqpWideReadTableRanges::CallableName(),
            TKqpReadOlapTableRanges::CallableName(),
            TKqpWideReadOlapTableRanges::CallableName(),
            TKqpBlockReadOlapTableRanges::CallableName(),
            TKqlReadTableIndexRanges::CallableName(),
        }, HndlInt(&AnnotateReadTableRanges));
        AddHandler({
            TKqpReadTableFullTextIndex::CallableName(),
            TKqlReadTableFullTextIndex::CallableName(),
        }, HndlInt(&AnnotateReadTableFullTextIndex));
        AddHandler({
            TKqpLookupTable::CallableName(),
            TKqlStreamLookupTable::CallableName(),
            TKqlStreamLookupIndex::CallableName(),
        }, HndlInt(&AnnotateLookupTable));
        AddHandler({
            TKqlKeyInc::CallableName(),
            TKqlKeyExc::CallableName(),
        }, Hndl(&AnnotateKeyTuple));
        AddHandler({TKqlFillTable::CallableName()}, Hndl(&AnnotateFillTable));
        AddHandler({
            TKqlUpsertRows::CallableName(),
            TKqlInsertOnConflictUpdateRows::CallableName(),
            TKqlUpsertRowsIndex::CallableName(),
            TKqpUpsertRows::CallableName(),
        }, HndlInt(&AnnotateUpsertRows));
        AddHandler({
            TKqlInsertRows::CallableName(),
            TKqlInsertRowsIndex::CallableName(),
        }, HndlInt(&AnnotateInsertRows));
        AddHandler({
            TKqlUpdateRows::CallableName(),
            TKqlUpdateRowsIndex::CallableName(),
        }, HndlInt(&AnnotateUpdateRows));
        AddHandler({
            TKqlDeleteRows::CallableName(),
            TKqlDeleteRowsIndex::CallableName(),
            TKqpDeleteRows::CallableName(),
        }, HndlInt(&AnnotateDeleteRows));
        AddHandler({
            TKqpOlapAnd::CallableName(),
            TKqpOlapOr::CallableName(),
            TKqpOlapXor::CallableName(),
        }, Hndl(&AnnotateOlapBinaryLogicOperator));
        AddHandler({TKqpOlapNot::CallableName()}, Hndl(&AnnotateOlapUnaryLogicOperator));
        AddHandler({TKqpOlapProjection::CallableName()}, Hndl(&AnnotateOlapProjection));
        AddHandler({TKqpOlapProjections::CallableName()}, Hndl(&AnnotateOlapProjections));
        AddHandler({TKqpPredicateClosure::CallableName()}, Hndl(&AnnotateKqpPredicateClosure));
        AddHandler({TKqpOlapFilter::CallableName()}, Hndl(&AnnotateOlapFilter));
        AddHandler({TKqpOlapApplyColumnArg::CallableName()}, Hndl(&AnnotateOlapApplyColumnArg));
        AddHandler({TKqpOlapApply::CallableName()}, Hndl(&AnnotateOlapApply));
        AddHandler({TKqpOlapAgg::CallableName()}, Hndl(&AnnotateOlapAgg));
        AddHandler({TKqpOlapDistinct::CallableName()}, Hndl(&AnnotateOlapDistinct));
        AddHandler({TKqpOlapExtractMembers::CallableName()}, Hndl(&AnnotateOlapExtractMembers));
        AddHandler({TKqpOlapJsonValue::CallableName()}, Hndl(&AnnotateOlapJsonValue));
        AddHandler({TKqpOlapJsonExists::CallableName()}, Hndl(&AnnotateOlapJsonExists));
        AddHandler({TKqpCnSequencer::CallableName()}, HndlInt(&AnnotateSequencerConnection));
        AddHandler({TKqpCnStreamLookup::CallableName()}, HndlInt(&AnnotateStreamLookupConnection));
        AddHandler({TKqpCnVectorResolve::CallableName()}, HndlInt(&AnnotateVectorResolveConnection));
        AddHandler({
            TKqlIndexLookupJoin::CallableName(),
            TKqpIndexLookupJoin::CallableName(),
        }, Hndl(&AnnotateIndexLookupJoin));
        AddHandler({TKqpTxResultBinding::CallableName()}, Hndl(&AnnotateKqpTxResultBinding));
        AddHandler({TKqpTxInternalBinding::CallableName()}, Hndl(&AnnotateKqpTxInternalBinding));
        AddHandler({TKqpPhysicalTx::CallableName()}, Hndl(&AnnotateKqpPhysicalTx));
        AddHandler({TKqpPhysicalQuery::CallableName()}, HndlInt(&AnnotateKqpPhysicalQuery));
        AddHandler({TKqpEffects::CallableName()}, Hndl(&AnnotateKqpEffects));
        AddHandler({TKqpWriteConstraint::CallableName()}, Hndl(&AnnotateWriteConstraint));
        AddHandler({TKqlSequencer::CallableName()}, HndlInt(&AnnotateSequencer));
        AddHandler({TKqpProgram::CallableName()}, Hndl(&AnnotateKqpProgram));
        AddHandler({TKqpEnsure::CallableName()}, Hndl(&AnnotateKqpEnsure));
        AddHandler({TKqpLockAndCheck::CallableName()}, HndlInt(&AnnotateKqpLockAndCheck));
        AddHandler({TFulltextAnalyze::CallableName()}, Hndl(&AnnotateFulltextAnalyze));
        AddHandler({TKqpStreamEnumerate::CallableName()}, Hndl(&AnnotateKqpStreamEnumerate));
        AddHandler({TKqpReadTableFullTextIndexSourceSettings::CallableName()}, HndlInt(&AnnotateReadTableFullTextIndexSourceSettings));
        AddHandler({TKqpReadRangesSourceSettings::CallableName()}, HndlInt(&AnnotateKqpSourceSettings));
        AddHandler({TKqpReadSysViewSourceSettings::CallableName()}, HndlInt(&AnnotateSysViewSourceSettings));
        AddHandler({TKqlExternalEffect::CallableName()}, Hndl(&AnnotateExternalEffect));
        AddHandler({TKqpSinkEffect::CallableName()}, Hndl(&AnnotateKqpSinkEffect));
        AddHandler({TKqlReturningList::CallableName()}, HndlInt(&AnnotateReturningList));
        AddHandler({TKqpTableSinkSettings::CallableName()}, Hndl(&AnnotateTableSinkSettings));
        AddHandler({
            TKqpExprSublink::CallableName(),
            TKqpExistsSublink::CallableName(),
            TKqpInSublink::CallableName(),
        }, Hndl(&AnnotateSublinkBase));
        AddHandler({TKqpInfuseDependents::CallableName()}, Hndl(&AnnotateInfuseDependents));
        AddHandler({TKqpOpRead::CallableName()}, HndlInt(&AnnotateOpRead));
        AddHandler({TKqpOpEmptySource::CallableName()}, Hndl(&AnnotateOpEmptySource));
        AddHandler({TKqpOpMapElementLambda::CallableName()}, Hndl(&AnnotateOpMapElementLambda));
        AddHandler({TKqpOpMapElementRename::CallableName()}, Hndl(&AnnotateOpMapElementRename));
        AddHandler({TKqpOpMap::CallableName()}, Hndl(&AnnotateOpMap));
        AddHandler({TKqpOpProject::CallableName()}, Hndl(&AnnotateOpProject));
        AddHandler({TKqpOpFilter::CallableName()}, Hndl(&AnnotateOpFilter));
        AddHandler({TKqpOpJoinFilter::CallableName()}, Hndl(&AnnotateOpJoinFilter));
        AddHandler({TKqpOpJoin::CallableName()}, Hndl(&AnnotateOpJoin));
        AddHandler({TKqpOpUnionAll::CallableName()}, Hndl(&AnnotateOpUnionAll));
        AddHandler({TKqpOpLimit::CallableName()}, Hndl(&AnnotateOpLimit));
        AddHandler({TKqpOpSortElement::CallableName()}, Hndl(&AnnotateOpSortElement));
        AddHandler({TKqpOpSort::CallableName()}, Hndl(&AnnotateOpSort));
        AddHandler({TKqpOpReplaceAlias::CallableName()}, Hndl(&AnnotateOpReplaceAlias));
        AddHandler({TKqpOpAggregate::CallableName()}, Hndl(&AnnotateOpAggregate));
        AddHandler({TKqpOpRoot::CallableName()}, Hndl(&AnnotateOpRoot));
    }

private:
    THandler HndlInt(TStatus (*handler)(const TExprNode::TPtr&, TExprContext&, const TString& cluster, const TKikimrTablesData&)) {
        return [handler, this](TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(input, ctx, Cluster, *TablesData);
        };
    }

    THandler HndlInt(TStatus (*handler)(const TExprNode::TPtr&, TExprContext&, const TString& cluster, const TKikimrTablesData&, bool withSystemColumns)) {
        return [handler, this, withSystemColumns = Config->SystemColumnsEnabled()](TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(input, ctx, Cluster, *TablesData, withSystemColumns);
        };
    }

    THandler HndlInt(TStatus (*handler)(const TExprNode::TPtr&, TExprContext&, bool enableRBO)) {
        return [handler, enableRBO = Config->GetEnableNewRBO()](TExprNode::TPtr input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
            return handler(input, ctx, enableRBO);
        };
    }

    const TString Cluster;
    const TIntrusivePtr<TKikimrTablesData> TablesData;
    const TKikimrConfiguration::TPtr Config;
};

} // anonymous namespace

THolder<TVisitorTransformerBase> CreateKqpTypeAnnotationTransformer(const TString& cluster, TIntrusivePtr<TKikimrTablesData> tablesData, TKikimrConfiguration::TPtr config) {
    return MakeHolder<TKiTypeAnnotationTransformer>(cluster, std::move(tablesData), std::move(config));
}

TAutoPtr<IGraphTransformer> CreateKqpCheckQueryTransformer() {
    return CreateFunctorTransformer(
        [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
            output = input;

            YQL_ENSURE(TMaybeNode<TKqlQuery>(input));

            auto query = TKqlQuery(input);
            for (const auto& result : query.Results()) {
                if (!EnsureTupleSize(result.MutableRef(), 2, ctx)) {
                    return TStatus::Error;
                }
                if (!EnsureListType(result.Value().Ref(), ctx)) {
                    return TStatus::Error;
                }
            }

            return TStatus::Ok;
        });
}

} // namespace NKikimr::NKqp::NOpt
