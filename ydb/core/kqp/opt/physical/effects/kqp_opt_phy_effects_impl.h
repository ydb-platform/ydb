#pragma once

#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/physical/kqp_opt_phy_impl.h>

#include <yql/essentials/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>

namespace NKikimr::NKqp::NOpt {

using TSecondaryIndexes = TVector<std::pair<
    NYql::TExprNode::TPtr,
    const NYql::TIndexDescription*>>;

TSecondaryIndexes BuildAffectedIndexTables(const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx, const THashSet<TStringBuf>* filter = nullptr);

struct TCondenseInputResult {
    NYql::NNodes::TExprBase Stream;
    TVector<NYql::NNodes::TExprBase> StageInputs;
    TVector<NYql::NNodes::TCoArgument> StageArgs;
};

TMaybe<TCondenseInputResult> CondenseInput(const NYql::NNodes::TExprBase& input, NYql::TExprContext& ctx);

TCondenseInputResult DeduplicateInput(const TCondenseInputResult& input,
    const NYql::TKikimrTableDescription& table, NYql::TExprContext& ctx);

TMaybe<TCondenseInputResult> CondenseInputToDictByPk(const NYql::NNodes::TExprBase& input,
    const NYql::TKikimrTableDescription& table, const NYql::NNodes::TCoLambda& payloadSelector,
    NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqPhyPrecompute> PrecomputeTableLookupDict(
    const NYql::NNodes::TDqPhyPrecompute& lookupKeys, const NYql::TKikimrTableDescription& table,
    const TVector<NYql::NNodes::TExprBase>& columnsList,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx, bool fixLookupKeys);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqPhyPrecompute> PrecomputeTableLookupDict(
    const NYql::NNodes::TDqPhyPrecompute& lookupKeys, const NYql::TKikimrTableDescription& table,
    const THashSet<TString>& dataColumns, const THashSet<TString>& keyColumns, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TDqPhyPrecompute PrecomputeCondenseInputResult(const TCondenseInputResult& condenseResult,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

// Creates key selector using PK of given table
NYql::NNodes::TCoLambda MakeTableKeySelector(const NYql::TKikimrTableMetadataPtr tableMeta, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx, TMaybe<int> tupleId = {});

// Creates key selector using user provided index columns.
// It is important to note. This function looks at the _user_prvided_ set of columns.
// Example: table with columns a, b, pk(a)
//   case 1:
//     user creates index for column b, index table pk(b,a). But this fuction must create selector only for column b
//   case 2:
//     user creates index for columns b, a, index table pk is same pk(b,a). But this function must crete selector for b, a

NYql::NNodes::TCoLambda MakeIndexPrefixKeySelector(const NYql::TIndexDescription& indexDesc, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TCoLambda MakeRowsPayloadSelector(const NYql::NNodes::TCoAtomList& columns,
    const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase MakeRowsFromDict(const NYql::NNodes::TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const TVector<TStringBuf>& columns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

// Same as MakeRowsFromDict but skip rows which marked as non changed (true in second tuple)
NYql::NNodes::TExprBase MakeRowsFromTupleDict(const NYql::NNodes::TDqPhyPrecompute& dict, const TVector<TString>& dictKeys,
    const TVector<TStringBuf>& columns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TMaybeNode<NYql::NNodes::TDqCnUnionAll> MakeConditionalInsertRows(const NYql::NNodes::TExprBase& input,
    const NYql::TKikimrTableDescription& table, const TMaybe<THashSet<TStringBuf>>& inputColumn, bool abortOnError,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

enum class TKqpPhyUpsertIndexMode {
    Upsert,
    UpdateOn
};

NYql::NNodes::TMaybeNode<NYql::NNodes::TExprList> KqpPhyUpsertIndexEffectsImpl(TKqpPhyUpsertIndexMode mode,
    const NYql::NNodes::TExprBase& inputRows,
    const NYql::NNodes::TCoAtomList& inputColumns,
    const NYql::NNodes::TCoAtomList& returningColumns,
    const NYql::NNodes::TCoAtomList& columnsWithDefaults,
    const NYql::NNodes::TExprBase& tableExpr,
    const NYql::TKikimrTableDescription& table, const bool isBatch,
    const NYql::NNodes::TMaybeNode<NYql::NNodes::TCoNameValueTupleList>& settings,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx, const TKqpOptimizeContext& kqpCtx);


struct TDictAndKeysResult {
    NYql::NNodes::TDqPhyPrecompute DictPrecompute;
    NYql::NNodes::TDqPhyPrecompute KeysPrecompute;
};

TDictAndKeysResult PrecomputeDictAndKeys(const TCondenseInputResult& condenseResult, NYql::TPositionHandle pos,
    NYql::TExprContext& ctx);

NYql::NNodes::TKqpCnStreamLookup BuildStreamLookupOverPrecompute(const NYql::TKikimrTableDescription & table, NYql::NNodes::TDqPhyPrecompute& precompute,
    NYql::NNodes::TExprBase input,
    const NYql::NNodes::TKqpTable& kqpTableNode, const NYql::TPositionHandle& pos, NYql::TExprContext& ctx, const TVector<TString>& extraColumnsToRead = {});

NYql::NNodes::TDqStageBase ReadInputToStage(const NYql::NNodes::TExprBase& expr, NYql::TExprContext& ctx);

NYql::NNodes::TDqPhyPrecompute ReadInputToPrecompute(const NYql::NNodes::TExprBase& expr, const NYql::TPositionHandle& pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildVectorIndexPostingRows(const NYql::TKikimrTableDescription& table,
    const NYql::NNodes::TKqpTable& tableNode,
    const TString& indexName,
    const TVector<TStringBuf>& indexTableColumns,
    const NYql::NNodes::TExprBase& inputRows,
    bool withData,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

TVector<TStringBuf> BuildVectorIndexPostingColumns(const NYql::TKikimrTableDescription& table, const NYql::TIndexDescription* indexDesc);

NYql::NNodes::TExprBase BuildVectorIndexPrefixRows(const NYql::TKikimrTableDescription& table, const NYql::TKikimrTableDescription& prefixTable,
    bool withData, const NYql::TIndexDescription* indexDesc, const NYql::NNodes::TExprBase& inputRows,
    TVector<TStringBuf>& indexTableColumns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

std::pair<NYql::NNodes::TExprBase, NYql::NNodes::TExprBase> BuildVectorIndexPrefixRowsWithNew(
    const NYql::TKikimrTableDescription& table, const NYql::TKikimrTableDescription& prefixTable,
    const NYql::TIndexDescription* indexDesc, const NYql::NNodes::TExprBase& inputRows,
    TVector<TStringBuf>& indexTableColumns, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextIndexRows(const NYql::TKikimrTableDescription& table, const NYql::TIndexDescription* indexDesc,
    const NYql::NNodes::TExprBase& inputRows, const THashSet<TStringBuf>& inputColumns, TVector<TStringBuf>& indexTableColumns,
    bool forDelete, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextDocsRows(const NYql::TKikimrTableDescription& table, const NYql::TIndexDescription* indexDesc,
    const NYql::NNodes::TExprBase& inputRows, const THashSet<TStringBuf>& inputColumns, TVector<TStringBuf>& docsColumns,
    bool forDelete, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextDictRows(const NYql::NNodes::TExprBase& tokenRows, bool useSum, bool useStage,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase CombineFulltextDictRows(const TVector<NYql::NNodes::TExprBase>& deltas, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextPostingKeys(const NYql::TKikimrTableDescription& table, const NYql::NNodes::TExprBase& tokenRows,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextDictUpsert(const NYql::TKikimrTableDescription& dictTable,
    const NYql::NNodes::TExprBase& tokenRows, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

NYql::NNodes::TExprBase BuildFulltextStatsUpsert(const NYql::TKikimrTableDescription& statsTable,
    const NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase>& addedDocs,
    const NYql::NNodes::TMaybeNode<NYql::NNodes::TExprBase>& removedDocs,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

} // NKikimr::NKqp::NOpt
