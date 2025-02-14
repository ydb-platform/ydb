#include "portion_index.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NColumnShard {

TPortionIndexStats::TPortionClass::TPortionClass(const NOlap::TPortionInfo& portion) {
    if (portion.HasRemoveSnapshot()) {
        Produced = NOlap::NPortion::EProduced::INACTIVE;
    } else if (portion.GetTierNameDef(NOlap::NBlobOperations::TGlobal::DefaultStorageId) != NOlap::NBlobOperations::TGlobal::DefaultStorageId) {
        Produced = NOlap::NPortion::EProduced::EVICTED;
    } else {
        Produced = portion.GetMeta().GetProduced();
    }
}

void TPortionIndexStats::AddPortion(const NOlap::TPortionInfo& portion) {
    TPortionClass portionClass(portion);
    TotalStats[portionClass].AddPortion(portion);
    StatsByPathId[portion.GetPathId()][portionClass].AddPortion(portion);
}

void TPortionIndexStats::RemovePortion(const NOlap::TPortionInfo& portion) {
    TPortionClass portionClass(portion);

    {
        auto findClass = TotalStats.find(portionClass);
        AFL_VERIFY(!findClass.IsEnd())("path_id", portion.GetPathId());
        findClass->second.RemovePortion(portion);
        if (findClass->second.IsEmpty()) {
            TotalStats.erase(findClass);
        }
    }

    {
        auto findPathId = StatsByPathId.find(portion.GetPathId());
        AFL_VERIFY(!findPathId.IsEnd())("path_id", portion.GetPathId());
        auto findClass = findPathId->second.find(portionClass);
        AFL_VERIFY(!findClass.IsEnd())("path_id", portion.GetPathId());
        findClass->second.RemovePortion(portion);
        if (findClass->second.IsEmpty()) {
            findPathId->second.erase(findClass);
            if (findPathId->second.empty()) {
                StatsByPathId.erase(findPathId);
            }
        }
    }
}

}   // namespace NKikimr::NColumnShard
