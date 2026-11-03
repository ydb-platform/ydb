#include "index_portions_by_last_key.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

namespace {

constexpr ui32 TableId = NColumnShard::Schema::PortionsByLastKeyTableId;
constexpr ui32 PathIdTag = 1;
constexpr ui32 PortionIdTag = 2;
constexpr ui32 LastKeyFirstTag = 1000;

const NTable::TScheme::TTableInfo* GetTableInfo(const NTable::TDatabase& db) {
    return db.GetScheme().GetTableInfo(TableId);
}

TRawTypeValue ToRawValue(const std::optional<std::string_view> value, const NTable::TScheme::TTableInfo& info, const ui32 tag) {
    if (!value) {
        return TRawTypeValue();
    }
    return TRawTypeValue(value->data(), value->size(), info.Columns.at(tag).PType.GetTypeId());
}

TRawTypeValue ToRawValue(const TCell& cell, const NTable::TScheme::TTableInfo& info, const ui32 tag) {
    return ToRawValue(cell.IsNull() ? std::nullopt : std::optional<std::string_view>(cell.AsBuf()), info, tag);
}

TConstArrayRef<TCell> GetLastKey(const TConstArrayRef<TCell> row) {
    return row.Slice(1, row.size() - 2);
}

// byte equality is value equality for every key type of a column table
bool IsSameKey(const TConstArrayRef<TCell> lhs, const TConstArrayRef<TCell> rhs) {
    AFL_VERIFY(lhs.size() == rhs.size());
    for (ui32 i = 0; i < lhs.size(); ++i) {
        if (lhs[i].IsNull() != rhs[i].IsNull() || lhs[i].AsBuf() != rhs[i].AsBuf()) {
            return false;
        }
    }
    return true;
}

// The values point into the portion meta and into pathId and portionId
std::vector<TRawTypeValue> BuildKey(
    const NTable::TScheme::TTableInfo& info, const TPortionInfo& portion, const ui64& pathId, const ui64& portionId) {
    std::vector<TRawTypeValue> key;
    key.emplace_back(&pathId, sizeof(pathId), NScheme::NTypeIds::Uint64);
    AFL_VERIFY(portion.GetMeta()
                   .IndexKeyViewEnd()
                   .ForEachValue(*portion.GetMeta().GetPkSchema(),
                       [&](const std::optional<std::string_view> value) {
        key.emplace_back(ToRawValue(value, info, LastKeyFirstTag + key.size() - 1));
                       })
                   .IsSuccess());
    key.emplace_back(&portionId, sizeof(portionId), NScheme::NTypeIds::Uint64);
    AFL_VERIFY(key.size() == info.KeyColumns.size())("key", key.size())("table_key", info.KeyColumns.size());
    return key;
}

}   // namespace

bool TIndexPortionsByLastKey::Exists(const NTable::TDatabase& db) {
    return !!GetTableInfo(db);
}

void TIndexPortionsByLastKey::Create(NTable::TDatabase& db, const std::vector<std::pair<TString, NScheme::TTypeInfo>>& pkColumns) {
    AFL_VERIFY(!Exists(db));
    AFL_VERIFY(pkColumns.size())("pk_columns", pkColumns.size());
    auto& alter = db.Alter();
    alter.AddTable("IndexPortionsByLastKey", TableId);
    alter.AddColumn(TableId, "PathId", PathIdTag, NScheme::NTypeIds::Uint64, false, false);
    alter.AddColumn(TableId, "PortionId", PortionIdTag, NScheme::NTypeIds::Uint64, false, false);
    for (ui32 i = 0; i < pkColumns.size(); ++i) {
        const auto type = NScheme::ProtoColumnTypeFromTypeInfoMod(pkColumns[i].second, "");
        alter.AddColumnWithTypeInfo(TableId, "LastKey_" + pkColumns[i].first, LastKeyFirstTag + i, type.TypeId, type.TypeInfo, false, false);
    }
    alter.AddColumnToKey(TableId, PathIdTag);
    for (ui32 i = 0; i < pkColumns.size(); ++i) {
        alter.AddColumnToKey(TableId, LastKeyFirstTag + i);
    }
    alter.AddColumnToKey(TableId, PortionIdTag);
}

void TIndexPortionsByLastKey::Upsert(NTable::TDatabase& db, const TPortionInfo& portion) {
    const auto* info = GetTableInfo(db);
    if (!info) {
        return;
    }
    const ui64 pathId = portion.GetPathId().GetRawValue();
    const ui64 portionId = portion.GetPortionId();
    db.Update(TableId, NTable::ERowOp::Upsert, BuildKey(*info, portion, pathId, portionId), {});
}

void TIndexPortionsByLastKey::Erase(NTable::TDatabase& db, const TPortionInfo& portion) {
    const auto* info = GetTableInfo(db);
    if (!info) {
        return;
    }
    const ui64 pathId = portion.GetPathId().GetRawValue();
    const ui64 portionId = portion.GetPortionId();
    db.Update(TableId, NTable::ERowOp::Erase, BuildKey(*info, portion, pathId, portionId), {});
}

bool TIndexPortionsByLastKey::ReadBatch(NTable::TDatabase& db, const TInternalPathId pathId, const std::optional<TOwnedCellVec>& below,
    const ui32 rowsLimit, std::vector<ui64>& portionIds, std::optional<TOwnedCellVec>& lastKey) {
    const auto* info = GetTableInfo(db);
    AFL_VERIFY(info);
    AFL_VERIFY(rowsLimit);
    const ui64 rawPathId = pathId.GetRawValue();
    const TRawTypeValue pathIdValue(&rawPathId, sizeof(rawPathId), NScheme::NTypeIds::Uint64);

    // a short key ends with +inf in local-db, so the start of the prefix is the prefix followed by nulls
    std::vector<TRawTypeValue> minKey(info->KeyColumns.size());
    minKey[0] = pathIdValue;
    NTable::TKeyRange range;
    range.MinKey = minKey;
    std::vector<TRawTypeValue> maxKey = { pathIdValue };
    if (below) {
        for (ui32 i = 0; i < below->size(); ++i) {
            maxKey.emplace_back(ToRawValue((*below)[i], *info, LastKeyFirstTag + i));
        }
        maxKey.emplace_back();
        AFL_VERIFY(maxKey.size() == info->KeyColumns.size())("key", maxKey.size())("table_key", info->KeyColumns.size());
        // a null PortionId sorts first, so (PathId, below, null) exclusive skips every portion with the last key `below`
        range.MaxInclusive = false;
    } else {
        // the bare PathId reads as (PathId, +inf, ...), and local-db accepts such a short max key only as inclusive
        range.MaxInclusive = true;
    }
    range.MaxKey = maxKey;
    const std::array<ui32, 1> tags = { PortionIdTag };

    // requests all missing pages of the first rowsLimit rows at once; the rest of the last key's run seldom crosses a page
    if (!db.Precharge(TableId, range.MinKey, range.MaxKey, tags, 0, rowsLimit, Max<ui64>(), NTable::EDirection::Reverse).Ready) {
        return false;
    }
    std::vector<ui64> ids;
    std::optional<TOwnedCellVec> lastKeyAtLimit;
    auto it = db.IterateRangeReverse(TableId, range, tags);
    while (it->Next(NTable::ENext::Data) == NTable::EReady::Data) {
        const TConstArrayRef<TCell> row = it->GetKey().Cells();
        if (lastKeyAtLimit && !IsSameKey(*lastKeyAtLimit, GetLastKey(row))) {
            portionIds = std::move(ids);
            lastKey = std::move(lastKeyAtLimit);
            return true;
        }
        ids.emplace_back(row.back().AsValue<ui64>());
        if (ids.size() == rowsLimit) {
            lastKeyAtLimit = TOwnedCellVec::Make(GetLastKey(row));
        }
    }
    if (it->Last() == NTable::EReady::Page) {
        return false;
    }
    portionIds = std::move(ids);
    lastKey = std::nullopt;
    return true;
}

}   // namespace NKikimr::NOlap
