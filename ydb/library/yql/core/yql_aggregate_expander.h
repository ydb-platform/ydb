#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include "yql_type_annotation.h"

namespace NYql {

class TAggregateExpander {
public:
    TAggregateExpander(bool allowPickle, const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
        bool forceCompact = false, bool compactForDistinct = false, bool usePhases = false)
        : Node(node)
        , Ctx(ctx)
        , TypesCtx(typesCtx)
        , AllowPickle(allowPickle)
        , ForceCompact(forceCompact)
        , CompactForDistinct(compactForDistinct)
        , UsePhases(usePhases)
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
    using TIdxSet = std::set<ui32>;

    TExprNode::TPtr ExpandAggApply(const TExprNode::TPtr& node);
    bool CollectTraits();
    TExprNode::TPtr RebuildAggregate();
    TExprNode::TPtr GetContextLambda();
    void ProcessSessionSetting(TExprNode::TPtr sessionSetting);
    TVector<const TTypeAnnotationNode*> GetKeyItemTypes();
    bool IsNeedPickle(const TVector<const TTypeAnnotationNode*>& keyItemTypes);
    TExprNode::TPtr GetKeyExtractor(bool needPickle);
    void CollectColumnsSpecs();
    void BuildNothingStates();

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

    std::function<TExprNodeBuilder& (TExprNodeBuilder&)> GetPartialAggArgExtractor(ui32 i, bool deserialize);
    TExprNode::TPtr GetFinalAggStateExtractor(ui32 i);

    TExprNode::TPtr GeneratePhases();
    void GenerateInitForDistinct(TExprNodeBuilder& parent, ui32& ndx, const TIdxSet& indicies, const TExprNode::TPtr& distinctField);
    TExprNode::TPtr GenerateJustOverStates(const TExprNode::TPtr& input, const TIdxSet& indicies);
    TExprNode::TPtr TryGenerateBlockCombineAll();
    TExprNode::TPtr TryGenerateBlockCombine();

private:
    static constexpr TStringBuf SessionStartMemberName = "_yql_group_session_start";

    const TExprNode::TPtr Node;
    TExprContext& Ctx;
    TTypeAnnotationContext& TypesCtx;
    bool AllowPickle;
    bool ForceCompact;
    bool CompactForDistinct;
    bool UsePhases;
    TStringBuf Suffix;

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
