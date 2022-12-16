#include "schemeshard_tables_storage.h"

namespace NKikimr::NSchemeShard {

void TTablesStorage::OnAddObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    const TString& tieringId = object->Description.GetTtlSettings().GetUseTiering();
    if (!!tieringId) {
        PathesByTieringId[tieringId].emplace(pathId);
    }
}

void TTablesStorage::OnRemoveObject(const TPathId& pathId, TColumnTableInfo::TPtr object) {
    const TString& tieringId = object->Description.GetTtlSettings().GetUseTiering();
    if (!tieringId) {
        return;
    }
    auto it = PathesByTieringId.find(tieringId);
    if (PathesByTieringId.end() == it) {
        return;
    }
    it->second.erase(pathId);
    if (it->second.empty()) {
        PathesByTieringId.erase(it);
    }
}

const std::set<NKikimr::TPathId>& TTablesStorage::GetTablesWithTiering(const TString& tieringId) const {
    auto it = PathesByTieringId.find(tieringId);
    if (it != PathesByTieringId.end()) {
        return it->second;
    } else {
        return Default<std::set<TPathId>>();
    }
}

TColumnTableInfo::TPtr TTablesStorage::ExtractPtr(const TPathId& id) {
    auto it = Tables.find(id);
    Y_VERIFY(it != Tables.end());
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

TTablesStorage::TTableReadGuard TTablesStorage::GetVerified(const TPathId& id) const {
    auto it = Tables.find(id);
    Y_VERIFY(it != Tables.end());
    return TTableReadGuard(it->second);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id, TColumnTableInfo::TPtr object) {
    auto it = Tables.find(id);
    Y_VERIFY(it == Tables.end());
    return TTableCreatedGuard(*this, id, object);
}

TTablesStorage::TTableCreatedGuard TTablesStorage::BuildNew(const TPathId& id) {
    auto it = Tables.find(id);
    Y_VERIFY(it == Tables.end());
    return TTableCreatedGuard(*this, id);
}

size_t TTablesStorage::Drop(const TPathId& id) {
    auto it = Tables.find(id);
    if (it == Tables.end()) {
        return 0;
    } else {
        OnRemoveObject(id, it->second);
        return Tables.erase(id);
    }
}

void TTablesStorage::TTableExtractedGuard::UseAlterDataVerified() {
    Y_VERIFY(Object);
    TColumnTableInfo::TPtr alterInfo = Object->AlterData;
    Y_VERIFY(alterInfo);
    alterInfo->AlterBody.Clear();
    Object = alterInfo;
}

}
