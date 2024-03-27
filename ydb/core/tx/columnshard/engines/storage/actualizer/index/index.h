#pragma once
#include <ydb/core/tx/columnshard/engines/storage/actualizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/counters/counters.h>

namespace NKikimr::NOlap {
class TVersionedIndex;
class TTiering;
}

namespace NKikimr::NOlap::NActualizer {
class TTieringActualizer;
class TSchemeActualizer;

class TGranuleActualizationIndex {
private:
    TCounters Counters;
    std::vector<std::shared_ptr<IActualizer>> Actualizers;

    std::shared_ptr<TTieringActualizer> TieringActualizer;
    std::shared_ptr<TSchemeActualizer> SchemeActualizer;

    const ui64 PathId;
    const TVersionedIndex& VersionedIndex;
public:
    void Start();
    TGranuleActualizationIndex(const ui64 pathId, const TVersionedIndex& versionedIndex);

    void ExtractActualizationTasks(TTieringProcessContext& tasksContext, const NActualizer::TExternalTasksContext& externalContext) const;

    void RefreshTiering(const std::optional<TTiering>& info, const TAddExternalContext& context);
    void RefreshScheme(const TAddExternalContext& context);

    void AddPortion(const std::shared_ptr<TPortionInfo>& portion, const TAddExternalContext& context);
    void RemovePortion(const std::shared_ptr<TPortionInfo>& portion);
};

}