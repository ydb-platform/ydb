#include "manager.h"

namespace NKikimr::NSchemeShard {

void TTablesStorage::OnAddObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    for (auto&& s : object->GetColumnShards()) {
        AFL_VERIFY(TablesByShard[s].AddId(pathId));
    }
}

void TTablesStorage::OnRemoveObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    for (auto&& s : object->GetColumnShards()) {
        TablesByShard[s].RemoveId(pathId);
    }
}

TColumnTableInfo::TPtr TTablesStorage::ExtractPtr(const TPathId& id) {
    auto it = Tables.find(id);
    Y_ABORT_UNLESS(it != Tables.end());
    auto result = it->second;
    Tables.erase(it);
    return result;
}

TTablesStorage::TTableExtractedGuard TTablesStorage::TakeVerified(const TPathId& id) {
    return TTableExtractedGuard(*this, id, ExtractPtr(id), false);
}

TTablesStorage::TTableExtractedGuard TTablesStorage::TakeAlterVerified(const TPathId& id) {
    return TTableExtractedGuard(*this, id, ExtractPtr(id), true);
}

 TColumnTableInfo::TPtr TTablesStorage::GetVerifiedPtr(const TPathId& id) const {
    auto it = Tables.find(id);
    Y_ABORT_UNLESS(it != Tables.end());
    return it->second;
}

TTablesStorage::TTableReadGuard TTablesStorage::GetVerified(const TPathId& id) const {
    auto it = Tables.find(id);
    Y_ABORT_UNLESS(it != Tables.end());
    return TTableReadGuard(it->second);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id, TColumnTableInfo::TPtr object) {
    auto it = Tables.find(id);
    Y_ABORT_UNLESS(it == Tables.end());
    return TTableCreatedGuard(*this, id, object);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id) {
    auto it = Tables.find(id);
    Y_ABORT_UNLESS(it == Tables.end());
    return TTableCreatedGuard(*this, id);
}

bool TTablesStorage::Drop(const TPathId& id) {
    auto it = Tables.find(id);
    if (it == Tables.end()) {
        return false;
    } else {
        OnRemoveObject(id, it->second);
        Tables.erase(it);
        return true;
    }
}

TColumnTablesLayout TTablesStorage::GetTablesLayout(const std::vector<ui64>& tabletIds) const {
    std::vector<TColumnTablesLayout::TTablesGroup> groups;
    groups.reserve(tabletIds.size());
    for (auto&& i : tabletIds) {
        auto it = TablesByShard.find(i);
        if (it == TablesByShard.end()) {
            groups.emplace_back(&Default<TColumnTablesLayout::TTableIdsGroup>(), std::set<ui64>({i}));
        } else {
            groups.emplace_back(&it->second, std::set<ui64>({i}));
        }
    }
    std::sort(groups.begin(), groups.end());
    ui32 delta = 0;
    for (ui32 i = 0; i + delta + 1 < groups.size();) {
        if (delta) {
            groups[i + 1] = std::move(groups[i + delta + 1]);
        }
        if (groups[i].TryMerge(groups[i + 1])) {
            ++delta;
        } else {
            ++i;
        }
    }
    groups.resize(groups.size() - delta);
    return TColumnTablesLayout(std::move(groups));
}

void TTablesStorage::TTableExtractedGuard::UseAlterDataVerified() {
    Y_ABORT_UNLESS(Object);
    TColumnTableInfo::TPtr alterInfo = Object->AlterData;
    Y_ABORT_UNLESS(alterInfo);
    alterInfo->AlterBody.Clear();
    auto stats = Object->Stats;
    Object = alterInfo;
    Object->Stats = stats;
}

std::unordered_set<TPathId> TTablesStorage::GetAllPathIds() const {
    std::unordered_set<TPathId> result;
    for (const auto& [pathId, _] : Tables) {
        result.emplace(pathId);
    }
    return result;
}

}
