
#include "yql_yt_phy_opt.h"

#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>

#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;
using namespace NDq;
using namespace NPrivate;

TYtPhysicalOptProposalTransformer::TYtPhysicalOptProposalTransformer(TYtState::TPtr state)
    : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYt, state->Configuration->DisableOptimizers.Get().GetOrElse(TSet<TString>()))
    , State_(state)
{
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TYtPhysicalOptProposalTransformer::name)
    AddHandler(0, &TCoMux::Match, HNDL(Mux));
    AddHandler(0, &TYtWriteTable::Match, HNDL(Write));
    AddHandler(0, &TYtWriteTable::Match, HNDL(DqWrite));
    AddHandler(0, Names({TCoLength::CallableName(), TCoHasItems::CallableName()}), HNDL(Length));
    AddHandler(0, &TCoSort::Match, HNDL(Sort<false>));
    AddHandler(0, &TCoTopSort::Match, HNDL(Sort<true>));
    AddHandler(0, &TCoTop::Match, HNDL(Sort<true>));
    AddHandler(0, &TYtSort::Match, HNDL(YtSortOverAlreadySorted));
    AddHandler(0, &TCoPartitionByKeyBase::Match, HNDL(PartitionByKey));
    AddHandler(0, &TCoFlatMapBase::Match, HNDL(FlatMap));
    AddHandler(0, &TCoCombineByKey::Match, HNDL(CombineByKey));
    AddHandler(0, &TCoLMap::Match, HNDL(LMap<TCoLMap>));
    AddHandler(0, &TCoOrderedLMap::Match, HNDL(LMap<TCoOrderedLMap>));
    AddHandler(0, &TCoEquiJoin::Match, HNDL(EquiJoin));
    AddHandler(0, &TCoCountBase::Match, HNDL(TakeOrSkip));
    AddHandler(0, &TYtWriteTable::Match, HNDL(Fill));
    AddHandler(0, &TResPull::Match, HNDL(ResPull));
    if (State_->Configuration->UseNewPredicateExtraction.Get().GetOrElse(DEFAULT_USE_NEW_PREDICATE_EXTRACTION)) {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRange));
        AddHandler(0, &TCoFlatMapBase::Match, HNDL(ExtractKeyRangeDqReadWrap));
    } else {
        AddHandler(0, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(ExtractKeyRangeLegacy));
    }
    AddHandler(0, &TCoExtendBase::Match, HNDL(Extend));
    AddHandler(0, &TCoAssumeSorted::Match, HNDL(AssumeSorted));
    AddHandler(0, &TYtMapReduce::Match, HNDL(AddTrivialMapperForNativeYtTypes));
    AddHandler(0, &TYtDqWrite::Match, HNDL(YtDqWrite));
    AddHandler(0, &TYtDqProcessWrite::Match, HNDL(YtDqProcessWrite));
    AddHandler(0, &TYtEquiJoin::Match, HNDL(EarlyMergeJoin));

    if (!State_->Configuration->DisableFuseOperations.Get().GetOrElse(DEFAULT_DISABLE_FUSE_OPERATIONS)) {
        AddHandler(1, &TYtMap::Match, HNDL(FuseInnerMap));
        AddHandler(1, &TYtMap::Match, HNDL(FuseOuterMap));
    }
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(ReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(MultiMapFieldsSubset));
    AddHandler(1, Names({TYtMapReduce::CallableName(), TYtReduce::CallableName()}), HNDL(MultiReduceFieldsSubset));
    AddHandler(1, Names({TYtMap::CallableName(), TYtMapReduce::CallableName()}), HNDL(WeakFields));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(BypassMerge));
    AddHandler(1, &TYtPublish::Match, HNDL(BypassMergeBeforePublish));
    AddHandler(1, &TYtOutputOpBase::Match, HNDL(TableContentWithSettings));
    AddHandler(1, &TYtOutputOpBase::Match, HNDL(NonOptimalTableContent));
    AddHandler(1, &TCoRight::Match, HNDL(ReadWithSettings));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(PushDownKeyExtract));
    AddHandler(1, &TYtTransientOpBase::Match, HNDL(TransientOpWithSettings));
    AddHandler(1, &TYtSort::Match, HNDL(TopSort));
    AddHandler(1, &TYtWithUserJobsOpBase::Match, HNDL(EmbedLimit));
    AddHandler(1, &TYtMerge::Match, HNDL(PushMergeLimitToInput));
    if (!State_->Configuration->DisableFuseOperations.Get().GetOrElse(DEFAULT_DISABLE_FUSE_OPERATIONS)) {
        AddHandler(1, &TYtReduce::Match, HNDL(FuseReduce));
    }

    AddHandler(2, &TYtEquiJoin::Match, HNDL(RuntimeEquiJoin));
    AddHandler(2, &TStatWriteTable::Match, HNDL(ReplaceStatWriteTable));
    AddHandler(2, &TYtMap::Match, HNDL(MapToMerge));
    AddHandler(2, &TYtPublish::Match, HNDL(UnorderedPublishTarget));
    AddHandler(2, &TYtMap::Match, HNDL(PushDownYtMapOverSortedMerge));
    AddHandler(2, &TYtMerge::Match, HNDL(MergeToCopy));
#undef HNDL
}

}  // namespace NYql
