
#include "yql_yt_phy_opt.h"

#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>

#include <yql/essentials/utils/log/log.h>


namespace NYql {

using namespace NNodes;
using namespace NPrivate;

TYtPhysicalOptProposalTransformer::TYtPhysicalOptProposalTransformer(TYtState::TPtr state)
    : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
    , State_(state)
{
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYtPhysicalOptProposalTransformer::name)
    if (State_->Configuration->RuntimeClusterSelection.Get().GetOrElse(DEFAULT_RUNTIME_CLUSTER_SELECTION) != ERuntimeClusterSelectionMode::Disable) {
        AddHandler(0, &TYtTransientOpBase::Match, HNDL(UpdateDataSinkCluster));
        AddHandler(0, &TYtReadTable::Match, HNDL(UpdateDataSourceCluster));
    }
    AddHandler(0, &TCoMux::Match, HNDL(Mux));
    AddHandler(0, &TYtCreateTable::Match, HNDL(Create));
    AddHandler(0, &TYtWriteTable::Match, HNDL(Write));
    if (!State_->Configuration->_EnableYtDqProcessWriteConstraints.Get().GetOrElse(DEFAULT_ENABLE_DQ_WRITE_CONSTRAINTS)) {
        AddHandler(0, &TYtWriteTable::Match, HNDL(DqWrite));
    }
    AddHandler(0, Names({TCoLength::CallableName(), TCoHasItems::CallableName()}), HNDL(Length));
    AddHandler(0, &TCoSort::Match, HNDL(Sort<false>));
    AddHandler(0, &TCoTopSort::Match, HNDL(Sort<true>));
    AddHandler(0, &TCoTop::Match, HNDL(Sort<true>));
    AddHandler(0, &TYtSort::Match, HNDL(YtSortOverAlreadySorted));
    AddHandler(0, &TCoPruneKeys::Match, HNDL(PushPruneKeysIntoYtOperation));
    AddHandler(0, &TCoPruneAdjacentKeys::Match, HNDL(PushPruneKeysIntoYtOperation));
    AddHandler(0, &TCoPartitionByKeyBase::Match, HNDL(PartitionByKey));
    AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMap));
    AddHandler(0, &TCoCombineByKey::Match, HNDL(CombineByKey));
    AddHandler(0, &TCoLMap::Match, HNDL(LMap<TCoLMap>));
    AddHandler(0, &TCoOrderedLMap::Match, HNDL(LMap<TCoOrderedLMap>));
    AddHandler(0, &TCoEquiJoin::Match, HNDL(EquiJoin));
    AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkip));
    if (State_->Configuration->_EnableYtDqProcessWriteConstraints.Get().GetOrElse(DEFAULT_ENABLE_DQ_WRITE_CONSTRAINTS)) {
        AddHandler(0, &TYtMaterialize::Match, HNDL(DqMaterialize));
        AddHandler(0, &TYtMaterialize::Match, HNDL(Materialize));
        AddHandler(0, &TYtWriteTable::Match, HNDL(FillToMaterialize));
    } else {
        AddHandler(0, &TYtWriteTable::Match, HNDL(Fill));
    }
    AddHandler(0, &TResPull::Match, HNDL(ResPull));
    if (State_->Configuration->UseNewPredicateExtraction.Get().GetOrElse(DEFAULT_USE_NEW_PREDICATE_EXTRACTION)) {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRange));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(ExtractKeyRangeDqReadWrap));
    } else {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRangeLegacy));
    }
    AddHandler(0, &TCoExtendBase::Match, HNDL(Extend));
    AddHandler(0, &TCoAssumeSorted::Match, HNDL(AssumeConstraints));
    AddHandler(0, &TCoAssumeConstraints::Match, HNDL(AssumeConstraints));
    AddHandler(0, &TCoAssumeUnique::Match, HNDL(AssumeConstraints));
    AddHandler(0, &TCoAssumeDistinct::Match, HNDL(AssumeConstraints));
    AddHandler(0, &TYtDqWrite::Match, HNDL(YtDqWrite));
    AddHandler(0, &TYtDqProcessWrite::Match, HNDL(YtDqProcessWrite));
    AddHandler(0, &TYtTransientOpBase::Match, HNDL(ConvertDynamicTablesToStatic<TYtTransientOpBase>));
    AddHandler(0, &TYtReadTable::Match, HNDL(ConvertDynamicTablesToStatic<TYtReadTable>));
    AddHandler(0, &TYtEquiJoin::Match, HNDL(EarlyMergeJoin));
    AddHandler(0, &TYtEquiJoin::Match, HNDL(AddPruneKeys));
    AddHandler(0, &TYtOutputOpBase::Match, HNDL(TableContentWithSettings));
    AddHandler(0, &TYtOutputOpBase::Match, HNDL(NonOptimalTableContent));

    if (!State_->Configuration->DisableFuseOperations.Get().GetOrElse(DEFAULT_DISABLE_FUSE_OPERATIONS)) {
        AddHandler(1, &TYtMap::Match, HNDL(FuseInnerMap));
        AddHandler(1, &TYtMap::Match, HNDL(FuseOuterMap));
        if (State_->Configuration->FuseMapToMapReduce.Get().GetOrElse(DEFAULT_FUSE_MAP_TO_MAPREDUCE) == EFuseMapToMapReduceMode::Normal) {
            AddHandler(1, &TYtMapReduce::Match, HNDL(FuseMapToMapReduce));
        }
    }
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(ReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MultiMapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(MultiReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(WeakFields));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(BypassMerge));
    AddHandler(1, &TYtPublish::Match, HNDL(BypassMergeBeforePublish));
    AddHandler(1, &TCoRight::Match, HNDL(ReadWithSettings));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(PushDownKeyExtract));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(TransientOpWithSettings));
    AddHandler(1, &TYtSort::Match, HNDL(TopSort));
    AddHandler(1, &TYtWithUserJobsOpBase::Match, HNDL(EmbedLimit));
    AddHandler(1, &TYtMerge::Match, HNDL(PushMergeLimitToInput));
    if (!State_->Configuration->DisableFuseOperations.Get().GetOrElse(DEFAULT_DISABLE_FUSE_OPERATIONS)) {
        AddHandler(1, &TYtReduce::Match, HNDL(FuseReduce));
        AddHandler(1, &TYtReduce::Match, HNDL(FuseReduceWithTrivialMap));
    }

    if (State_->Configuration->UseQLFilter.Get().GetOrElse(DEFAULT_USE_QL_FILTER)) {
        // best to run after Fuse*Map and before MapToMerge
        AddHandler(2, Names({TYtMap::CallableName()}), HNDL(ExtractQLFilters));
        AddHandler(2, Names({TYtQLFilter::CallableName()}), HNDL(OptimizeQLFilterType));
    }
    AddHandler(2, &TYtEquiJoin::Match, HNDL(RuntimeEquiJoin));
    AddHandler(2, &TStatWriteTable::Match, HNDL(ReplaceStatWriteTable));
    AddHandler(2, &TYtMap::Match, HNDL(MapToMerge));
    AddHandler(2, &TYtMap::Match, HNDL(PushDownYtMapOverSortedMerge));
    AddHandler(2, &TYtMerge::Match, HNDL(ForceTransform));
    AddHandler(2, &TYtMerge::Match, HNDL(MergeToCopy));
    AddHandler(2, &TYtMap::Match, HNDL(UnessentialFilter));
#undef HNDL
}

}  // namespace NYql
