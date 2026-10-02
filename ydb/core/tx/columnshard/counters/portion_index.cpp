#include "portion_index.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

TPortionIndexStats::TPortionClass::TPortionClass(const NOlap::TPortionInfo& portion) {
    Produced = portion.GetProduced();
    IsDefaultTier = portion.IsDefaultTier(NOlap::NBlobOperations::TGlobal::DefaultStorageId);
}

void TPortionIndexStats::AddPortion(const NOlap::TPortionInfo& portion) {
    TPortionClass portionClass(portion);
    TotalStats[portionClass].AddPortion(portion);
    StatsByPathId[portion.GetPathId()][portionClass].AddPortion(portion);

    const ui64 smallBlobsVolume = portion.GetSmallBlobBytesInBlobStorage(SmallBlobThresholdBytes);
    const ui64 smallBlobsCount = smallBlobsVolume ? 1 : 0;
    TotalSmallBlobs.Add(smallBlobsVolume, smallBlobsCount);
    SmallBlobsByPathId[portion.GetPathId()].Add(smallBlobsVolume, smallBlobsCount);
}

void TPortionIndexStats::RemovePortion(const NOlap::TPortionInfo& portion) {
    TPortionClass portionClass(portion);

    {
        auto findClass = TotalStats.find(portionClass);
        AFL_VERIFY(findClass != TotalStats.end())("path_id", portion.GetPathId());
        findClass->second.RemovePortion(portion);
        if (findClass->second.IsEmpty()) {
            TotalStats.erase(findClass);
        }
    }

    {
        auto findPathId = StatsByPathId.find(portion.GetPathId());
        AFL_VERIFY(findPathId != StatsByPathId.end())("path_id", portion.GetPathId());
        auto findClass = findPathId->second.find(portionClass);
        AFL_VERIFY(findClass != findPathId->second.end())("path_id", portion.GetPathId());
        findClass->second.RemovePortion(portion);
        if (findClass->second.IsEmpty()) {
            findPathId->second.erase(findClass);
            if (findPathId->second.empty()) {
                StatsByPathId.erase(findPathId);
            }
        }
    }

    const ui64 smallBlobsVolume = portion.GetSmallBlobBytesInBlobStorage(SmallBlobThresholdBytes);
    const ui64 smallBlobsCount = smallBlobsVolume ? 1 : 0;
    TotalSmallBlobs.Sub(smallBlobsVolume, smallBlobsCount);
    if (auto findPathId = SmallBlobsByPathId.find(portion.GetPathId()); findPathId != SmallBlobsByPathId.end()) {
        findPathId->second.Sub(smallBlobsVolume, smallBlobsCount);
        if (!StatsByPathId.contains(portion.GetPathId())) {
            SmallBlobsByPathId.erase(findPathId);
        }
    }
}

}   // namespace NKikimr::NColumnShard
