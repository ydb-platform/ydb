#include "index.h"

#include <ydb/core/tx/columnshard/engines/storage/actualizer/move/move.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/scheme/scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/tiering/tiering.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NOlap::NActualizer {

void TGranuleActualizationIndex::ExtractActualizationTasks(
    TTieringProcessContext& tasksContext, const NActualizer::TExternalTasksContext& externalContext, const bool moveDataOnly) const {
    TInternalTasksContext internalContext;
    for (auto&& i : Actualizers) {
        if (moveDataOnly && i != MoveDataActualizer) {
            continue;
        }
        i->ExtractTasks(tasksContext, externalContext, internalContext);
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
    AFL_VERIFY(TieringActualizer);
    TieringActualizer->Refresh(info, context);
    NYDBTest::TControllers::GetColumnShardController()->OnActualizationRefreshTiering();
}

void TGranuleActualizationIndex::RefreshScheme(const TAddExternalContext& context) {
    AFL_VERIFY(SchemeActualizer);
    SchemeActualizer->Refresh(context);
    NYDBTest::TControllers::GetColumnShardController()->OnActualizationRefreshScheme();
}

TGranuleActualizationIndex::TGranuleActualizationIndex(
    const TInternalPathId pathId, const TVersionedIndex& versionedIndex, const std::shared_ptr<IStoragesManager>& storagesManager)
    : PathId(pathId)
    , VersionedIndex(versionedIndex)
    , StoragesManager(storagesManager)
{
}

void TGranuleActualizationIndex::Start() {
    AFL_VERIFY(Actualizers.empty());
    TieringActualizer = std::make_shared<TTieringActualizer>(PathId, VersionedIndex, StoragesManager);
    SchemeActualizer = std::make_shared<TSchemeActualizer>(PathId, VersionedIndex);
    Actualizers.emplace_back(TieringActualizer);
    Actualizers.emplace_back(SchemeActualizer);
}

void TGranuleActualizationIndex::StartMoveData(const THashSet<ui32>& targetGroups, const TAddExternalContext& context) {
    AFL_VERIFY(!MoveDataActualizer);
    MoveDataActualizer = std::make_shared<TMoveDataActualizer>(targetGroups, VersionedIndex);
    Actualizers.emplace_back(MoveDataActualizer);
    MoveDataActualizer->Refresh(context);
}

void TGranuleActualizationIndex::StopMoveData() {
    if (!MoveDataActualizer) {
        return;
    }
    Erase(Actualizers, MoveDataActualizer);
    MoveDataActualizer.reset();
}

TMoveDataQueueSizes TGranuleActualizationIndex::GetMoveDataQueueSizes() const {
    if (!MoveDataActualizer) {
        return TMoveDataQueueSizes();
    }
    return MoveDataActualizer->GetMoveDataQueueSizes();
}

std::vector<TCSMetadataRequest> TGranuleActualizationIndex::CollectMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions) {
    if (!TieringActualizer) {
        return {};
    }
    return TieringActualizer->BuildMetadataRequests(PathId, portions, TieringActualizer);
}

std::vector<TCSMetadataRequest> TGranuleActualizationIndex::CollectMoveDataMetadataRequests(const THashMap<ui64, TPortionInfo::TPtr>& portions) {
    if (!MoveDataActualizer) {
        return {};
    }
    return MoveDataActualizer->BuildMoveDataMetadataRequests(portions, MoveDataActualizer);
}

}   // namespace NKikimr::NOlap::NActualizer
