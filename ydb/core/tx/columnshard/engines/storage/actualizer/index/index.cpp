#include "index.h"
#include <ydb/core/tx/columnshard/engines/storage/actualizer/tiering/tiering.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/scheme/scheme.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NActualizer {

void TGranuleActualizationIndex::ExtractActualizationTasks(TTieringProcessContext& tasksContext, const NActualizer::TExternalTasksContext& externalContext) const {
    TInternalTasksContext internalContext;
    for (auto&& i : Actualizers) {
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

TGranuleActualizationIndex::TGranuleActualizationIndex(const ui64 pathId, const TVersionedIndex& versionedIndex)
    : PathId(pathId)
    , VersionedIndex(versionedIndex)
{
    Y_UNUSED(PathId);
}

void TGranuleActualizationIndex::Start() {
    AFL_VERIFY(Actualizers.empty());
    TieringActualizer = std::make_shared<TTieringActualizer>(PathId, VersionedIndex);
    SchemeActualizer = std::make_shared<TSchemeActualizer>(PathId, VersionedIndex);
    Actualizers.emplace_back(TieringActualizer);
    Actualizers.emplace_back(SchemeActualizer);
}

}
