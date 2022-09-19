#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include "yql_type_annotation.h"

namespace NYql {

class TAggregateExpander {
public:
    TAggregateExpander(bool allowPickle, const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx, bool forceCompact = false, bool compactForDistinct = false)
        : Node(node)
        , Ctx(ctx)
        , TypesCtx(typesCtx)
        , AllowPickle(allowPickle)
        , ForceCompact(forceCompact)
        , CompactForDistinct(compactForDistinct)
        , AggregatedColumns(nullptr)
        , VoidNode(ctx.NewCallable(node->Pos(), "Void", {}))
        , HaveDistinct(false)
        , EffectiveCompact(false)
        , HaveSessionSetting(false)
        , OriginalRowType(nullptr)
        , RowType(nullptr)
    {
        PreMap = Ctx.Builder(node->Pos())
            .Lambda()
                .Param("premap")
                .Callable("Just").Arg(0, "premap").Seal()
            .Seal().Build();
        SortParams = {
            .Key = VoidNode,
            .Order = VoidNode
        };
    }
    
    TExprNode::TPtr ExpandAggregate();

private:
    TExprNode::TPtr ExpandAggApply(const TExprNode::TPtr& node);
    bool CollectTraits();
    TExprNode::TPtr RebuildAggregate();
    TExprNode::TPtr GetContextLambda();
    void ProcessSessionSetting(TExprNode::TPtr sessionSetting);
    TVector<const TTypeAnnotationNode*> GetKeyItemTypes();
    bool IsNeedPickle(const TVector<const TTypeAnnotationNode*>& keyItemTypes);
    TExprNode::TPtr GetKeyExtractor(bool needPickle);
    void CollectColumnsSpecs();

    // Partial aggregate generation
    TExprNode::TPtr GeneratePartialAggregate(const TExprNode::TPtr keyExtractor, const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needPickle);
    TExprNode::TPtr GeneratePartialAggregateForNonDistinct(const TExprNode::TPtr& keyExtractor, const TExprNode::TPtr& pickleTypeNode);
    
    TExprNode::TPtr GenerateDistinctGrouper(const TExprNode::TPtr distinctField,
        const TVector<const TTypeAnnotationNode*>& keyItemTypes, bool needDistinctPickle);

    TExprNode::TPtr ReturnKeyAsIsForCombineInit(const TExprNode::TPtr& pickleTypeNode);

    // Post aggregate
    TExprNode::TPtr GeneratePostAggregate(const TExprNode::TPtr& preAgg, const TExprNode::TPtr& keyExtractor);
    TExprNode::TPtr GeneratePreprocessLambda(const TExprNode::TPtr& keyExtractor);
    TExprNode::TPtr GenerateCondenseSwitch(const TExprNode::TPtr& keyExtractor);
    TExprNode::TPtr GeneratePostAggregateInitPhase();
    TExprNode::TPtr GeneratePostAggregateSavePhase();
    TExprNode::TPtr GeneratePostAggregateMergePhase();

private:
    static constexpr TStringBuf SessionStartMemberName = "_yql_group_session_start";

    const TExprNode::TPtr Node;
    TExprContext& Ctx;
    TTypeAnnotationContext& TypesCtx;
    bool AllowPickle;
    bool ForceCompact;
    bool CompactForDistinct;

    TSessionWindowParams SessionWindowParams;
    TExprNode::TPtr AggList;
    TExprNode::TListType Traits;
    TExprNode::TPtr KeyColumns;
    TExprNode::TPtr AggregatedColumns;
    const TExprNode::TPtr VoidNode;
    TMaybe<TStringBuf> SessionOutputColumn;
    TSortParams SortParams;
    bool HaveDistinct;
    bool EffectiveCompact;
    bool HaveSessionSetting;
    const TStructExprType* OriginalRowType;
    const TStructExprType* RowType;
    TVector<const TItemExprType*> RowItems;
    TExprNode::TPtr PreMap;

    TExprNode::TListType InitialColumnNames;
    TExprNode::TListType FinalColumnNames;
    TExprNode::TListType DistinctFields;
    TExprNode::TListType NothingStates;

    using TIdxSet = std::set<ui32>;
    std::unordered_map<std::string_view, TIdxSet> Distinct2Columns;
    TIdxSet NonDistinctColumns;

    std::unordered_map<std::string_view, bool> DistinctFieldNeedsPickle;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfSetCreate;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfAddValue;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfWasChanged;
};

inline TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    TAggregateExpander aggExpander(false, node, ctx, typesCtx, true);
    return aggExpander.ExpandAggregate();
}

}