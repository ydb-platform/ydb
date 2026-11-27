#include "index.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/scheme/scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/tiering/tiering.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NOlap::NActualizer {

void TGranuleActualizationIndex::ExtractActualizationTasks(
    TTieringProcessContext& tasksContext, const NActualizer::TExternalTasksContext& externalContext, EActualizationScope scope) const {
    TInternalTasksContext internalContext;
    const bool moveDataOnly = scope == EActualizationScope::MoveDataOnly;
    for (const auto& actualizer : Actualizers) {
        const bool isMoveData = actualizer == MoveDataActualizer;
        if (moveDataOnly == isMoveData) {
            actualizer->ExtractTasks(tasksContext, externalContext, internalContext);
        }
    }
}

void TGranuleActualizationIndex::AddPortion(const std::shared_ptr<TPortionInfo>& portion, const TAddExternalContext& context) {
    for (auto&& i : Actualizers) {
        i->AddPortion(portion, context);
    }
}

void TGranuleActualizationIndex::RemovePortion(const std::shared_ptr<TPortionInfo>& portion) {
    for (auto&& i : Actualizers) {
        i->RemovePortion(portion->GetPortionId());
    }
}

void TGranuleActualizationIndex::RefreshTiering(const std::optional<TTiering>& info, const TAddExternalContext& context) {
    YDB_LOG_VERIFY(TieringActualizer);
    TieringActualizer->Refresh(info, context);
    NYDBTest::TControllers::GetColumnShardController()->OnActualizationRefreshTiering();
}

void TGranuleActualizationIndex::RefreshScheme(const TAddExternalContext& context) {
    YDB_LOG_VERIFY(SchemeActualizer);
    SchemeActualizer->Refresh(context);
    YDB_LOG_VERIFY(TieringActualizer);
    TieringActualizer->RefreshScheme(context);
    NYDBTest::TControllers::GetColumnShardController()->OnActualizationRefreshScheme();
}

bool TGranuleActualizationIndex::IsSchemeActualizationFinished() const {
    return !SchemeActualizer || SchemeActualizer->IsFinished();
}

TGranuleActualizationIndex::TGranuleActualizationIndex(
    const TInternalPathId pathId, const TVersionedIndex& versionedIndex, const std::shared_ptr<IStoragesManager>& storagesManager)
    : PathId(pathId)
    , VersionedIndex(versionedIndex)
    , StoragesManager(storagesManager)
{
    Y_UNUSED(PathId);
}

void TGranuleActualizationIndex::Start() {
    YDB_LOG_VERIFY(Actualizers.empty());
    TieringActualizer = std::make_shared<TTieringActualizer>(PathId, VersionedIndex, StoragesManager);
    SchemeActualizer = std::make_shared<TSchemeActualizer>(PathId, VersionedIndex);
    Actualizers.emplace_back(TieringActualizer);
    Actualizers.emplace_back(SchemeActualizer);
}

void TGranuleActualizationIndex::StartMoveData(
    THashSet<ui32> targetGroups, const TAddExternalContext& context, const TWrittenPortionsById& uncommitted) {
    AFL_VERIFY(!MoveDataActualizer);
    MoveDataActualizer = std::make_shared<TMoveDataActualizer>(std::move(targetGroups), VersionedIndex);
    MoveDataActualizer->InitializePortionQueue(context, uncommitted);
    Actualizers.emplace_back(MoveDataActualizer);
}

void TGranuleActualizationIndex::StopMoveData() {
    Erase(Actualizers, MoveDataActualizer);
    MoveDataActualizer.reset();
}

TMoveDataQueueSizes TGranuleActualizationIndex::UpdateMoveDataQueueSizes(
    const THashMap<ui64, TPortionInfo::TPtr>& portions, const TWrittenPortionsById& uncommitted, size_t& visitsLeft) {
    if (!MoveDataActualizer) {
        return TMoveDataQueueSizes();
    }
    visitsLeft -= MoveDataActualizer->UpdatePortionStates(portions, uncommitted, visitsLeft);
    return MoveDataActualizer->GetMoveDataQueueSizes();
}

std::vector<TCSMetadataRequest> TGranuleActualizationIndex::CollectMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions) {
    if (!TieringActualizer) {
        return {};
    }
    return TieringActualizer->BuildMetadataRequests(PathId, portions, TieringActualizer);
}

std::vector<TCSMetadataRequest> TGranuleActualizationIndex::CollectMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions,
    const TWrittenPortionsById& uncommitted, TMoveDataMetadataBudget& budget) {
    if (!MoveDataActualizer) {
        return {};
    }
    return MoveDataActualizer->BuildMoveDataMetadataRequests(portions, uncommitted, MoveDataActualizer, budget);
}

}   // namespace NKikimr::NOlap::NActualizer
