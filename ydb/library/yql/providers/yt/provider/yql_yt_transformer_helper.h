#pragma once

#include "yql_yt_helpers.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_table.h"
#include "yql_yt_join_impl.h"
#include "yql_yt_optimize.h"
#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/core/extract_predicate/extract_predicate.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>

namespace NYql::NPrivate {

class TYtSortMembersCollection: public TSortMembersCollection {
public:
    void BuildKeyFilters(TPositionHandle pos, size_t tableCount, size_t orGroupCount, TExprNode::TListType& result, TExprContext& ctx);

private:
    void BuildKeyFiltersImpl(TPositionHandle pos, size_t tableCount, size_t orGroup, TExprNode::TListType& prefix,
        const TMemberDescrMap& members, TExprNode::TListType& result, TExprContext& ctx);
};

TMaybeNode<TCoLambda> GetLambdaWithPredicate(TCoLambda lambda);

bool IsAscending(const TExprNode& node);

bool CollectSortSet(const TExprNode& sortNode, TSet<TVector<TStringBuf>>& sortSets);

TExprNode::TPtr CollectPreferredSortsForEquiJoinOutput(TExprBase join, const TExprNode::TPtr& options,
                                                              TExprContext& ctx, const TParentsMap& parentsMap);

bool CanExtraColumnBePulledIntoEquiJoin(const TTypeAnnotationNode* type);

bool IsYtOrPlainTablePropsDependent(NNodes::TExprBase input);

bool IsLambdaSuitableForPullingIntoEquiJoin(const TCoFlatMapBase& flatMap, const TExprNode& label,
                                                   const THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap,
                                                   const TExprNode* extractedMembers);

TExprNode::TPtr BuildYtEquiJoinPremap(TExprBase list, TMaybeNode<TCoLambda> premapLambda, TExprContext& ctx);

// label -> pair(<asc sort keys>, <inputs matched by keys>)
THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> CollectTableSortKeysUsage(const TYtState::TPtr& state, const TCoEquiJoin& equiJoin);

TCoLambda FallbackLambdaInput(TCoLambda lambda, TExprContext& ctx);

TCoLambda FallbackLambdaOutput(TCoLambda lambda, TExprContext& ctx);

TYtDSink GetDataSink(TExprBase input, TExprContext& ctx);

TExprBase GetWorld(TExprBase input, TMaybeNode<TExprBase> main, TExprContext& ctx);

struct TConvertInputOpts {
    TMaybeNode<TCoNameValueTupleList> Settings_;
    TMaybeNode<TCoAtomList> CustomFields_;
    bool KeepDirecRead_;
    bool MakeUnordered_;
    bool ClearUnordered_;

    TConvertInputOpts();

    TConvertInputOpts& Settings(const TMaybeNode<TCoNameValueTupleList>& settings);

    TConvertInputOpts& CustomFields(const TMaybeNode<TCoAtomList>& customFields);

    TConvertInputOpts& ExplicitFields(const TYqlRowSpecInfo& rowSpec, TPositionHandle pos, TExprContext& ctx);

    TConvertInputOpts& ExplicitFields(const TStructExprType& type, TPositionHandle pos, TExprContext& ctx);

    TConvertInputOpts& KeepDirecRead(bool keepDirecRead = true);

    TConvertInputOpts& MakeUnordered(bool makeUnordered = true);

    TConvertInputOpts& ClearUnordered();
};

TYtSectionList ConvertInputTable(TExprBase input, TExprContext& ctx, const TConvertInputOpts& opts = {});

bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths, TVector<ui32>& currPath);

bool CollectMemberPaths(TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths);

bool CollectKeyPredicatesFromLookup(TExprBase row, TCoLookupBase lookup, TVector<TKeyFilterPredicates>& ranges,
    size_t maxTables);

bool CollectKeyPredicatesAnd(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

bool CollectKeyPredicatesOr(TExprBase row, const std::vector<TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

bool CollectKeyPredicates(TExprBase row, TExprBase predicate, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

TVector<TYtOutTable> ConvertMultiOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TMultiConstraintNode* multi = nullptr);

TVector<TYtOutTable> ConvertOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TConstraintSet* constraint = nullptr);

TVector<TYtOutTable> ConvertMultiOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

TYtOutTable ConvertSingleOutTableWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

TVector<TYtOutTable> ConvertOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

TExprBase WrapOp(TYtOutputOpBase op, TExprContext& ctx);

TCoLambda MapEmbedInputFieldsFilter(TCoLambda lambda, bool ordered, TCoAtomList fields, TExprContext& ctx);

}  // namespace NYql::NPrivate
