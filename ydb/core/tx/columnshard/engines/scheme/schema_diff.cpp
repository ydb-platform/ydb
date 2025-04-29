#include "schema_diff.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void ApplyToScheme(NKikimrSchemeOp::TColumnTableSchema& current, const NKikimrSchemeOp::TColumnTableSchemaDiff& proto) {
    current.SetVersion(proto.GetVersion());
    *current.MutableDefaultCompression() = proto.GetDefaultCompression();
    *current.MutableOptions() = proto.GetOptions();

    THashMap<ui32, NKikimrSchemeOp::TOlapColumnDescription*> curIds;
    for (auto& i : *current.MutableColumns()) {
        AFL_VERIFY(curIds.emplace(i.GetId(), &i).second);
    }

    for (auto&& i : proto.GetUpsertColumns()) {
        auto it = curIds.find(i.GetId());
        if (it == curIds.end()) {
            *current.AddColumns() = i;
        } else {
            *it->second = i;
        }
    }

    THashSet<ui32> deleted;
    for (auto&& i : proto.GetDropColumns()) {
        deleted.insert(i);
    }
    if (deleted.size() > 0) {
        ui32 cur = 0;
        ui32 size = current.GetColumns().size();
        for (ui32 i = 0; i < size; i++) {
            ui32 id = current.GetColumns(i).GetId();
            if (deleted.find(id) == deleted.end()) {
                (*current.MutableColumns())[cur++] = current.GetColumns(i);
            }
        }
        current.MutableColumns()->Truncate(cur);
    }

    THashMap<ui32, NKikimrSchemeOp::TOlapIndexDescription*> curInds;
    for (auto& i : *current.MutableIndexes()) {
        AFL_VERIFY(curInds.emplace(i.GetId(), &i).second);
    }

    for (auto&& i : proto.GetUpsertIndexes()) {
        auto it = curInds.find(i.GetId());
        if (it == curInds.end()) {
            *current.AddIndexes() = i;
        } else {
            *it->second = i;
        }
    }

    deleted.clear();
    for (auto&& i : proto.GetDropIndexes()) {
        deleted.insert(i);
    }
    if (deleted.size() > 0) {
        ui32 cur = 0;
        ui32 size = current.GetIndexes().size();
        for (ui32 i = 0; i < size; i++) {
            ui32 id = current.GetIndexes(i).GetId();
            if (deleted.find(id) == deleted.end()) {
                (*current.MutableIndexes())[cur++] = current.GetIndexes(i);
            }
        }
        current.MutableIndexes()->Truncate(cur);
    }
}

NKikimrSchemeOp::TColumnTableSchemaDiff TSchemaDiffView::MakeSchemasDiff(
    const NKikimrSchemeOp::TColumnTableSchema& current, const NKikimrSchemeOp::TColumnTableSchema& next) {
    NKikimrSchemeOp::TColumnTableSchemaDiff result;
    result.SetVersion(next.GetVersion());
    *result.MutableDefaultCompression() = next.GetDefaultCompression();
    *result.MutableOptions() = next.GetOptions();

    {
        THashMap<ui32, NKikimrSchemeOp::TOlapColumnDescription> nextIds;
        for (auto&& i : next.GetColumns()) {
            AFL_VERIFY(nextIds.emplace(i.GetId(), i).second);
        }
        THashSet<ui32> currentIds;
        for (auto&& i : current.GetColumns()) {
            auto it = nextIds.find(i.GetId());
            if (it == nextIds.end()) {
                result.AddDropColumns(i.GetId());
            } else if (it->second.SerializeAsString() != i.SerializeAsString()) {
                *result.AddUpsertColumns() = it->second;
            }
            currentIds.emplace(i.GetId());
        }
        for (auto&& i : next.GetColumns()) {
            if (currentIds.contains(i.GetId())) {
                continue;
            }
            *result.AddUpsertColumns() = i;
        }
    }
    {
        THashMap<ui32, NKikimrSchemeOp::TOlapIndexDescription> nextIds;
        for (auto&& i : next.GetIndexes()) {
            AFL_VERIFY(nextIds.emplace(i.GetId(), i).second);
        }
        THashSet<ui32> currentIds;
        for (auto&& i : current.GetIndexes()) {
            auto it = nextIds.find(i.GetId());
            if (it == nextIds.end()) {
                result.AddDropIndexes(i.GetId());
            } else if (it->second.SerializeAsString() != i.SerializeAsString()) {
                *result.AddUpsertIndexes() = it->second;
            }
            currentIds.emplace(i.GetId());
        }
        for (auto&& i : next.GetIndexes()) {
            if (currentIds.contains(i.GetId())) {
                continue;
            }
            *result.AddUpsertIndexes() = i;
        }
    }
    return result;
}

TConclusionStatus TSchemaDiffView::DeserializeFromProto(const NKikimrSchemeOp::TColumnTableSchemaDiff& proto) {
    SchemaOptions = &proto.GetOptions();
    Version = proto.GetVersion();
    if (proto.HasDefaultCompression()) {
        CompressionOptions = &proto.GetDefaultCompression();
    }
    for (auto&& i : proto.GetUpsertColumns()) {
        AFL_VERIFY(ModifiedColumns.emplace(i.GetId(), &i).second);
    }
    for (auto&& i : proto.GetDropColumns()) {
        AFL_VERIFY(ModifiedColumns.emplace(i, nullptr).second);
    }
    for (auto&& i : proto.GetUpsertIndexes()) {
        AFL_VERIFY(ModifiedIndexes.emplace(i.GetId(), &i).second);
    }
    for (auto&& i : proto.GetDropIndexes()) {
        AFL_VERIFY(ModifiedIndexes.emplace(i, nullptr).second);
    }
    return TConclusionStatus::Success();
}

void TSchemaDiffView::SerializeToProto(NKikimrSchemeOp::TColumnTableSchemaDiff& proto) {
    proto.MutableOptions()->CopyFrom(*SchemaOptions);
    proto.SetVersion(Version);
    if (CompressionOptions != nullptr) {
        proto.MutableDefaultCompression()->CopyFrom(*CompressionOptions);
    }
    for (auto& [id, column]: ModifiedColumns) {
        if (column == nullptr) {
            proto.AddDropColumns(id);
        } else {
            proto.AddUpsertColumns()->CopyFrom(*column);
        }
    }
    for (auto& [id, index]: ModifiedIndexes) {
        if (index == nullptr) {
            proto.AddDropIndexes(id);
        } else {
            proto.AddUpsertIndexes()->CopyFrom(*index);
        }
    }
}

ui64 TSchemaDiffView::GetVersion() const {
    AFL_VERIFY(Version);
    return Version;
}

void TSchemaDiffView::ApplyForColumns(const std::vector<ui32>& originalColumnIds,
    const std::function<void(const ui32 originalIndex)>& addFromOriginal,
    const std::function<void(const NKikimrSchemeOp::TOlapColumnDescription& col, const std::optional<ui32> originalIndex)>& addFromDiff) const {
    auto it = ModifiedColumns.begin();
    ui32 i = 0;
    while (i < originalColumnIds.size() || it != ModifiedColumns.end()) {
        AFL_VERIFY(i != originalColumnIds.size());
        const ui32 originalColId = originalColumnIds[i];

        if (it == ModifiedColumns.end() || originalColId < it->first) {
            addFromOriginal(i);
            ++i;
        } else if (it->first == originalColId) {
            if (it->second) {
                addFromDiff(*it->second, i);
            }
            ++it;
            ++i;
        } else if (it->first < originalColId) {
            AFL_VERIFY(it->second);
            addFromDiff(*it->second, std::nullopt);
            ++it;
        }
    }
}

const NKikimrSchemeOp::TColumnTableSchemeOptions& TSchemaDiffView::GetSchemaOptions() const {
    AFL_VERIFY(SchemaOptions);
    return *SchemaOptions;
}

}   // namespace NKikimr::NOlap
