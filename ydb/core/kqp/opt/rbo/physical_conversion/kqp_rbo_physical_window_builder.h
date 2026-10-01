#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalWindowBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalWindowBuilder(TOpWindow& window, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names)
        : TPhysicalUnaryOpBuilder(ctx, pos, names)
        , Window(window) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;
    static bool CanBuildWindow(const TOpWindow& window);
    static bool UsesWholePartition(const TOpWindow& window);
    static bool UsesRangeCarry(const TOpWindow& window);
    static bool UsesRangePeerGroups(const TOpWindow& window);
    static bool UsesRowFrames(const TOpWindow& window);
    static bool UsesRangeFrames(const TOpWindow& window);

private:
    void Prepare(const TVector<TInfoUnitId>& inputs);
    ui32 IndexOf(TInfoUnitId column) const;
    const TTypeAnnotationNode* InputItemType(TInfoUnitId column) const;
    TExprNode::TPtr BuildOutputRowType() const;

    TVector<TExprNode::TPtr> BuildSortKeys() const;
    TExprNode::TPtr BuildKeyExtractorLambda() const;
    TExprNode::TPtr BuildGroupSwitchLambda() const;

    TExprNode::TPtr BuildChain(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildChainLambda(bool update) const;
    TExprNode::TPtr BuildAccumulator(const TOpWindowFunc& func, ui32 funcIndex, TExprNode::TPtr itemArg, TExprNode::TPtr previousState,
                                     TExprNode::TPtr sortKeyChanged, TVector<std::pair<TString, TExprNode::TPtr>>& stateMembers) const;
    TExprNode::TPtr BuildWholePartition(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildFoldLambda(bool update) const;
    TExprNode::TPtr BuildChainOutputs(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildPartitionList(TExprNode::TPtr flow) const;
    TExprNode::TPtr BuildQueue(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildFrameBounds(TExprNode::TListType rangeIncrementals, TExprNode::TListType rowIntervals,
                                     TExprNode::TListType rowIncrementals, TExprNode::TListType rangeIntervals = {}) const;
    TExprNode::TPtr BuildCollector(TExprNode::TPtr outputs, TExprNode::TPtr queue, TExprNode::TPtr bounds, bool ascending) const;
    TExprNode::TPtr BuildIncrementalCarry(TExprNode::TPtr wideFlow, TExprNode::TPtr bounds, bool isRange, bool ascending,
                                          bool mayBeEmpty) const;
    TExprNode::TPtr BuildRangeCarry(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRowIncremental(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRangeIncremental(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRowSuffix(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRangePeerGroups(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRowFrames(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildRangeFrames(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildFrameFold(TExprNode::TPtr wideFlow, TExprNode::TPtr bounds, bool isRange, bool ascending) const;
    TExprNode::TPtr BuildRowBound(EWindowFrameBound kind, ui64 value) const;
    TExprNode::TPtr BuildRangeBound(EWindowFrameBound kind, ui64 value, const TString& sortedColumn) const;
    TExprNode::TPtr BuildPartitionHandler(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildExpandFromStructs(TExprNode::TPtr list) const;
    TExprNode::TPtr BuildExpandFromChain(TExprNode::TPtr chained) const;

    TString AccumulatorName(ui32 funcIndex) const;
    TString PositionName(ui32 funcIndex) const;
    TString PeerName(ui32 sortIndex) const;

    TExprNode::TPtr Member(TExprNode::TPtr from, const TString& name) const;
    TExprNode::TPtr BuildStruct(const TVector<std::pair<TString, TExprNode::TPtr>>& members) const;
    TExprNode::TPtr BuildSumCastTarget(TInfoUnitId column) const;
    TExprNode::TPtr BuildAvgAccumulatorDataType(TInfoUnitId column) const;
    TExprNode::TPtr BuildAvgAccumulatorType(TInfoUnitId column) const;
    TExprNode::TPtr BuildResultFromAccumulator(const TOpWindowFunc& func, TExprNode::TPtr accumulator) const;
    TExprNode::TPtr MakeOptional(TExprNode::TPtr value, bool alreadyOptional) const;
    TExprNode::TPtr BuildUint64(ui64 value) const;

    TOpWindow& Window;
    TVector<TInfoUnitId> Inputs;
    TMappedIUs<ui32> Indexes;
    TVector<TInfoUnitId> Functions;
    TVector<TInfoUnitId> OutputLayout;
    bool NeedsPeerKey = false;
    bool WholePartition = false;
    bool RangeCarry = false;
    bool RangePeerGroups = false;
    bool RowFrames = false;
    bool RowIncremental = false;
    bool RowSuffix = false;
    bool RangeFrames = false;
    bool RangeIncremental = false;
};
