#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <ydb/library/yql/providers/yt/lib/key_filter/yql_key_filter.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <util/generic/strbuf.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NYql::NPrivate {

class TYtSortMembersCollection: public TSortMembersCollection {
public:
    void BuildKeyFilters(TPositionHandle pos, size_t tableCount, size_t orGroupCount, TExprNode::TListType& result, TExprContext& ctx);

private:
    void BuildKeyFiltersImpl(TPositionHandle pos, size_t tableCount, size_t orGroup, TExprNode::TListType& prefix,
        const TMemberDescrMap& members, TExprNode::TListType& result, TExprContext& ctx);
};

NNodes::TMaybeNode<NNodes::TCoLambda> GetLambdaWithPredicate(NNodes::TCoLambda lambda);

bool IsAscending(const TExprNode& node);

bool CollectSortSet(const TExprNode& sortNode, TSet<TVector<TStringBuf>>& sortSets);

TExprNode::TPtr CollectPreferredSortsForEquiJoinOutput(NNodes::TExprBase join, const TExprNode::TPtr& options,
                                                              TExprContext& ctx, const TParentsMap& parentsMap);

bool CanExtraColumnBePulledIntoEquiJoin(const TTypeAnnotationNode* type);

bool IsYtOrPlainTablePropsDependent(NNodes::TExprBase input);

bool IsLambdaSuitableForPullingIntoEquiJoin(const NNodes::TCoFlatMapBase& flatMap, const TExprNode& label,
                                                   const THashMap<TStringBuf, THashSet<TStringBuf>>& tableKeysMap,
                                                   const TExprNode* extractedMembers);

TExprNode::TPtr BuildYtEquiJoinPremap(NNodes::TExprBase list, NNodes::TMaybeNode<NNodes::TCoLambda> premapLambda, TExprContext& ctx);

// label -> pair(<asc sort keys>, <inputs matched by keys>)
THashMap<TStringBuf, std::pair<TVector<TStringBuf>, ui32>> CollectTableSortKeysUsage(const TYtState::TPtr& state, const NNodes::TCoEquiJoin& equiJoin);

NNodes::TCoLambda FallbackLambdaInput(NNodes::TCoLambda lambda, TExprContext& ctx);

NNodes::TCoLambda FallbackLambdaOutput(NNodes::TCoLambda lambda, TExprContext& ctx);

NNodes::TYtDSink GetDataSink(NNodes::TExprBase input, TExprContext& ctx);

NNodes::TExprBase GetWorld(NNodes::TExprBase input, NNodes::TMaybeNode<NNodes::TExprBase> main, TExprContext& ctx);

struct TConvertInputOpts {
    NNodes::TMaybeNode<NNodes::TCoNameValueTupleList> Settings_;
    NNodes::TMaybeNode<NNodes::TCoAtomList> CustomFields_;
    bool KeepDirecRead_;
    bool MakeUnordered_;
    bool ClearUnordered_;

    TConvertInputOpts();

    TConvertInputOpts& Settings(const NNodes::TMaybeNode<NNodes::TCoNameValueTupleList>& settings);

    TConvertInputOpts& CustomFields(const NNodes::TMaybeNode<NNodes::TCoAtomList>& customFields);

    TConvertInputOpts& ExplicitFields(const TYqlRowSpecInfo& rowSpec, TPositionHandle pos, TExprContext& ctx);

    TConvertInputOpts& ExplicitFields(const TStructExprType& type, TPositionHandle pos, TExprContext& ctx);

    TConvertInputOpts& KeepDirecRead(bool keepDirecRead = true);

    TConvertInputOpts& MakeUnordered(bool makeUnordered = true);

    TConvertInputOpts& ClearUnordered();
};

NNodes::TYtSectionList ConvertInputTable(NNodes::TExprBase input, TExprContext& ctx, const TConvertInputOpts& opts = {});

bool CollectMemberPaths(NNodes::TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths, TVector<ui32>& currPath);

bool CollectMemberPaths(NNodes::TExprBase row, const TExprNode::TPtr& lookupItem,
    TMap<TStringBuf, TVector<ui32>>& memberPaths);

bool CollectKeyPredicatesFromLookup(NNodes::TExprBase row, NNodes::TCoLookupBase lookup, TVector<TKeyFilterPredicates>& ranges,
    size_t maxTables);

bool CollectKeyPredicatesAnd(NNodes::TExprBase row, const std::vector<NNodes::TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

bool CollectKeyPredicatesOr(NNodes::TExprBase row, const std::vector<NNodes::TExprBase>& predicates, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

bool CollectKeyPredicates(NNodes::TExprBase row, NNodes::TExprBase predicate, TVector<TKeyFilterPredicates>& ranges, size_t maxTables);

TVector<NNodes::TYtOutTable> ConvertMultiOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TMultiConstraintNode* multi = nullptr);

TVector<NNodes::TYtOutTable> ConvertOutTables(TPositionHandle pos, const TTypeAnnotationNode* outItemType, TExprContext& ctx,
    const TYtState::TPtr& state, const TConstraintSet* constraint = nullptr);

TVector<NNodes::TYtOutTable> ConvertMultiOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

NNodes::TYtOutTable ConvertSingleOutTableWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

TVector<NNodes::TYtOutTable> ConvertOutTablesWithSortAware(TExprNode::TPtr& lambda, bool& ordered, TPositionHandle pos,
    const TTypeAnnotationNode* outItemType, TExprContext& ctx, const TYtState::TPtr& state, const TConstraintSet& constraints);

NNodes::TExprBase WrapOp(NNodes::TYtOutputOpBase op, TExprContext& ctx);

NNodes::TCoLambda MapEmbedInputFieldsFilter(NNodes::TCoLambda lambda, bool ordered, NNodes::TCoAtomList fields, TExprContext& ctx);

}  // namespace NYql::NPrivate
