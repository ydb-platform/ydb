#pragma once
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include "yql_type_annotation.h"

namespace NYql {

class TAggregateExpander {
public:
    TAggregateExpander(bool usePartitionsByKeys, const bool useFinalizeByKeys, const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
        bool forceCompact = false, bool compactForDistinct = false, bool usePhases = false, bool allowSpilling = false)
        : Node(node)
        , Ctx(ctx)
        , TypesCtx(typesCtx)
        , UsePartitionsByKeys(usePartitionsByKeys)
        , UseFinalizeByKeys(useFinalizeByKeys)
        , ForceCompact(forceCompact)
        , CompactForDistinct(compactForDistinct)
        , UsePhases(usePhases)
        , AllowSpilling(allowSpilling)
        , AggregatedColumns(nullptr)
        , VoidNode(ctx.NewCallable(node->Pos(), "Void", {}))
        , HaveDistinct(false)
        , EffectiveCompact(false)
        , HaveSessionSetting(false)
        , OriginalRowType(nullptr)
        , RowType(nullptr)
        , UseBlocks(typesCtx.IsBlockEngineEnabled() && !allowSpilling)
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
    static TExprNode::TPtr CountAggregateRewrite(const NNodes::TCoAggregate& node, TExprContext& ctx, bool useBlocks);

private:
    using TIdxSet = std::set<ui32>;

    TExprNode::TPtr ExpandAggregateWithFullOutput();
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
    TExprNode::TPtr BuildFinalizeByKeyLambda(const TExprNode::TPtr& preprocessLambda, const TExprNode::TPtr& keyExtractor);
    TExprNode::TPtr GeneratePostAggregateInitPhase();
    TExprNode::TPtr GeneratePostAggregateFinishPhase();
    TExprNode::TPtr GeneratePostAggregateMergePhase();
    TExprNode::TPtr GeneratePostAggregateSerializePhase();

    std::function<TExprNodeBuilder& (TExprNodeBuilder&)> GetPartialAggArgExtractor(ui32 i, bool deserialize);
    TExprNode::TPtr GetFinalAggStateExtractor(ui32 i);

    TExprNode::TPtr GeneratePhases();
    void GenerateInitForDistinct(TExprNodeBuilder& parent, ui32& ndx, const TIdxSet& indicies, const TExprNode::TPtr& distinctField);
    TExprNode::TPtr GenerateJustOverStates(const TExprNode::TPtr& input, const TIdxSet& indicies);
    TExprNode::TPtr SerializeIdxSet(const TIdxSet& indicies);
    TExprNode::TPtr TryGenerateBlockCombineAllOrHashed();
    TExprNode::TPtr TryGenerateBlockMergeFinalizeHashed();
    TExprNode::TPtr TryGenerateBlockCombine();
    TExprNode::TPtr TryGenerateBlockMergeFinalize();
    TExprNode::TPtr MakeInputBlocks(const TExprNode::TPtr& stream, TExprNode::TListType& keyIdxs,
        TVector<TString>& outputColumns, TExprNode::TListType& aggs, bool overState, bool many, ui32* streamIdxColumn = nullptr);

private:
    static constexpr TStringBuf SessionStartMemberName = "_yql_group_session_start";

    const TExprNode::TPtr Node;
    TExprContext& Ctx;
    TTypeAnnotationContext& TypesCtx;
    bool UsePartitionsByKeys;
    bool UseFinalizeByKeys = false;
    bool ForceCompact;
    bool CompactForDistinct;
    bool UsePhases;
    bool AllowSpilling;
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
    bool UseBlocks;

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

inline TExprNode::TPtr ExpandAggregatePeepholeImpl(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
    const bool useFinalizeByKey, const bool useBlocks, const bool allowSpilling) {
    TAggregateExpander aggExpander(!useFinalizeByKey && !useBlocks, useFinalizeByKey, node, ctx, typesCtx,
        true, false, false, allowSpilling);
    return aggExpander.ExpandAggregate();
}

TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);

}
