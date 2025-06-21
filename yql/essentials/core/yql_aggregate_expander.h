#pragma once
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include "yql_type_annotation.h"

namespace NYql {

class TAggregateExpander {
public:
    TAggregateExpander(bool usePartitionsByKeys, const bool useFinalizeByKeys, const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
        bool forceCompact = false, bool compactForDistinct = false, bool usePhases = false, bool useBlocks = false)
        : Node_(node)
        , Ctx_(ctx)
        , TypesCtx_(typesCtx)
        , UsePartitionsByKeys_(usePartitionsByKeys)
        , UseFinalizeByKeys_(useFinalizeByKeys)
        , ForceCompact_(forceCompact)
        , CompactForDistinct_(compactForDistinct)
        , UsePhases_(usePhases)
        , AggregatedColumns_(nullptr)
        , VoidNode_(ctx.NewCallable(node->Pos(), "Void", {}))
        , HaveDistinct_(false)
        , EffectiveCompact_(false)
        , HaveSessionSetting_(false)
        , OriginalRowType_(nullptr)
        , RowType_(nullptr)
        , UseBlocks_(useBlocks)
    {
        PreMap_ = Ctx_.Builder(node->Pos())
            .Lambda()
                .Param("premap")
                .Callable("Just").Arg(0, "premap").Seal()
            .Seal().Build();
        SortParams_ = {
            .Key = VoidNode_,
            .Order = VoidNode_
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
    TExprNode::TPtr GeneratePostAggregateSavePhase();
    TExprNode::TPtr GeneratePostAggregateMergePhase();

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

    const TExprNode::TPtr Node_;
    TExprContext& Ctx_;
    TTypeAnnotationContext& TypesCtx_;
    bool UsePartitionsByKeys_;
    bool UseFinalizeByKeys_ = false;
    bool ForceCompact_;
    bool CompactForDistinct_;
    bool UsePhases_;
    TStringBuf Suffix_;

    TSessionWindowParams SessionWindowParams_;
    TExprNode::TPtr AggList_;
    TExprNode::TListType Traits_;
    TExprNode::TPtr KeyColumns_;
    TExprNode::TPtr AggregatedColumns_;
    const TExprNode::TPtr VoidNode_;
    TMaybe<TStringBuf> SessionOutputColumn_;
    TSortParams SortParams_;
    bool HaveDistinct_;
    bool EffectiveCompact_;
    bool HaveSessionSetting_;
    const TStructExprType* OriginalRowType_;
    const TStructExprType* RowType_;
    TVector<const TItemExprType*> RowItems_;
    TExprNode::TPtr PreMap_;
    bool UseBlocks_;

    TExprNode::TListType InitialColumnNames_;
    TExprNode::TListType FinalColumnNames_;
    TExprNode::TListType DistinctFields_;
    TExprNode::TListType NothingStates_;

    std::unordered_map<std::string_view, TIdxSet> Distinct2Columns_;
    TIdxSet NonDistinctColumns_;

    std::unordered_map<std::string_view, bool> DistinctFieldNeedsPickle_;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfSetCreate_;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfAddValue_;
    std::unordered_map<std::string_view, TExprNode::TPtr> UdfWasChanged_;
};

inline TExprNode::TPtr ExpandAggregatePeepholeImpl(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx,
    const bool useFinalizeByKey, const bool useBlocks, const bool allowSpilling) {
    const bool usePhases = typesCtx.PeepholeFlags.contains("useaggphases");
    TAggregateExpander aggExpander(!useFinalizeByKey && !useBlocks, useFinalizeByKey, node, ctx, typesCtx,
        !usePhases, false, usePhases, typesCtx.IsBlockEngineEnabled() && !allowSpilling);
    return aggExpander.ExpandAggregate();
}

TExprNode::TPtr ExpandAggregatePeephole(const TExprNode::TPtr& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);

}
