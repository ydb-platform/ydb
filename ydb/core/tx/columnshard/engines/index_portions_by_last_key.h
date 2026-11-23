#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <optional>
#include <vector>

namespace NKikimr::NOlap {

class TPortionInfo;

// Key (PathId, last key, PortionId), no values; a shard older than the table gets it from a normalizer
class TIndexPortionsByLastKey {
public:
    static bool Exists(const NTable::TDatabase& db);
    static void Create(NTable::TDatabase& db, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& pkColumns);

    // Upsert and Erase do nothing while the table does not exist
    static void Upsert(NTable::TDatabase& db, const TPortionInfo& portion);
    static void Erase(NTable::TDatabase& db, const TPortionInfo& portion);

    // Portions of pathId with last keys in [lastKey, below), greatest first (nullopt below: from the top): rowsLimit of them,
    // plus the rest of the portions sharing the last one's key. A nullopt lastKey: nothing of pathId is left below.
    // May return portions already in memory: the inserted ones loaded at start and any written after start, committed or not.
    // Returns false when pages are missing: they are requested, run it again in the next tx.
    static bool ReadBatch(NTable::TDatabase& db, const TInternalPathId pathId, const std::optional<TOwnedCellVec>& below, const ui32 rowsLimit,
        std::vector<ui64>& portionIds, std::optional<TOwnedCellVec>& lastKey);
};

}   // namespace NKikimr::NOlap
