#include "snapshot_scheme.h"

namespace NKikimr::NOlap {

TSnapshotSchema::TSnapshotSchema(TIndexInfo&& indexInfo, const TSnapshot& snapshot)
    : IndexInfo(std::move(indexInfo))
    , Schema(IndexInfo.ArrowSchemaWithSpecials())
    , Snapshot(snapshot)
{
}

TColumnSaver TSnapshotSchema::GetColumnSaver(const ui32 columnId) const {
    return IndexInfo.GetColumnSaver(columnId);
}

std::shared_ptr<TColumnLoader> TSnapshotSchema::GetColumnLoaderOptional(const ui32 columnId) const {
    return IndexInfo.GetColumnLoaderOptional(columnId);
}

std::optional<ui32> TSnapshotSchema::GetColumnIdOptional(const std::string& columnName) const {
    return IndexInfo.GetColumnIdOptional(columnName);
}

ui32 TSnapshotSchema::GetColumnIdVerified(const std::string& columnName) const {
    return IndexInfo.GetColumnIdVerified(columnName);
}

int TSnapshotSchema::GetFieldIndex(const ui32 columnId) const {
    return IndexInfo.GetColumnIndexOptional(columnId).value_or(-1);
}

const std::shared_ptr<NArrow::TSchemaLite>& TSnapshotSchema::GetSchema() const {
    return Schema;
}

const TIndexInfo& TSnapshotSchema::GetIndexInfo() const {
    return IndexInfo;
}

const TSnapshot& TSnapshotSchema::GetSnapshot() const {
    return Snapshot;
}

ui32 TSnapshotSchema::GetColumnsCount() const {
    return Schema->num_fields();
}

ui64 TSnapshotSchema::GetVersion() const {
    return IndexInfo.GetVersion();
}

bool TSnapshotSchema::IsReplaceableByNextVersion(const TSnapshotSchema& nextSchema) const {
    const auto& nextFields = nextSchema.GetSchema()->fields();
    const auto& curFields = Schema->fields();
    const ui32 curFieldCount = curFields.size();
    if (nextFields.size() < curFieldCount) {
        return false;
    }
    THashMap<std::string, std::pair<const arrow::Field*, ui32>> nextFieldMap;
    ui32 ind = 0;
    for (const auto& field: nextFields) {
        auto& pair = nextFieldMap[field->name()];
        pair.first = field.get();
        pair.second = ind++;
    }

    const TIndexInfo& nextIndexInfo = nextSchema.GetIndexInfo();
    for (ui32 fld = 0; fld < curFieldCount; fld++) {
        const auto* field = curFields[fld].get();
        auto iter = nextFieldMap.find(field->name());
        if (iter == nextFieldMap.end()) {
            return false;
        }
        if (!curFields[fld]->Equals(*iter->second.first, true)) {
            return false;
        }
        const std::shared_ptr<TColumnFeatures>& features = IndexInfo.GetColumnFeaturesByIndex(fld);
        const std::shared_ptr<TColumnFeatures>& nextFeatures = nextIndexInfo.GetColumnFeaturesByIndex(iter->second.second);
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD)("event", "column_features")("column_name", curFields[fld]->name())("name", features ? features->GetColumnName() : "")("next_name", nextFeatures ? nextFeatures->GetColumnName() : "")("field_id", fld);
        if (!features != !nextFeatures) {
            return false;
        }
        if (features && (*features != *nextFeatures)) {
            return false;
        }
    }
    const THashMap<ui32, NIndexes::TIndexMetaContainer>& indexes = IndexInfo.GetIndexes();
    const THashMap<ui32, NIndexes::TIndexMetaContainer>& nextIndexes = nextIndexInfo.GetIndexes();
    if (indexes.size() != nextIndexes.size()) {
        return false;
    }
    for (auto& [key, meta]: indexes) {
        THashMap<ui32, NIndexes::TIndexMetaContainer>::const_iterator it = nextIndexes.find(key);
        if (it == nextIndexes.end()) {
            return false;
        }
        if (*meta.GetObjectPtr() != *it->second.GetObjectPtr()) {
            return false;
        }
    }

    return true;
}

}
