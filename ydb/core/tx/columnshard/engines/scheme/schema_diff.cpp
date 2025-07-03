#include "index_info.h"
#include "schema_diff.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {
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
    OriginalProto = &proto;
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

NKikimrSchemeOp::TColumnTableSchema TSchemaDiffView::ApplyDiff(const NKikimrSchemeOp::TColumnTableSchema& schema) const {
    NKikimrSchemeOp::TColumnTableSchema result = schema;
    AFL_VERIFY(result.GetVersion() < Version);
    result.SetVersion(Version);
    if (SchemaOptions) {
        *result.MutableOptions() = *SchemaOptions;
    }
    if (CompressionOptions) {
        *result.MutableDefaultCompression() = *CompressionOptions;
    }
    result.ClearColumns();
    result.ClearIndexes();
    {
        auto itDiff = ModifiedColumns.begin();
        auto itSchema = schema.GetColumns().begin();
        while (itDiff != ModifiedColumns.end() || itSchema != schema.GetColumns().end()) {
            if (itDiff == ModifiedColumns.end()) {
                *result.AddColumns() = *itSchema;
                ++itSchema;
            } else if (itSchema == schema.GetColumns().end()) {
                *result.AddColumns() = *itDiff->second;
                ++itDiff;
            } else if (itSchema->GetId() < itDiff->first) {
                *result.AddColumns() = *itSchema;
                ++itSchema;
            } else if (itDiff->first <= itSchema->GetId()) {
                if (itDiff->second) {
                    *result.AddColumns() = *itDiff->second;
                }
                if (itDiff->first == itSchema->GetId()) {
                    ++itSchema;
                }
                ++itDiff;
            }
        }
    }
    {
        auto itDiff = ModifiedIndexes.begin();
        auto itSchema = schema.GetIndexes().begin();
        while (itDiff != ModifiedIndexes.end() || itSchema != schema.GetIndexes().end()) {
            if (itDiff == ModifiedIndexes.end()) {
                *result.AddIndexes() = *itSchema;
                ++itSchema;
            } else if (itSchema == schema.GetIndexes().end()) {
                *result.AddIndexes() = *itDiff->second;
                ++itDiff;
            } else if (itSchema->GetId() < itDiff->first) {
                *result.AddIndexes() = *itSchema;
                ++itSchema;
            } else if (itDiff->first <= itSchema->GetId()) {
                if (itDiff->second) {
                    *result.AddIndexes() = *itDiff->second;
                }
                if (itDiff->first == itSchema->GetId()) {
                    ++itSchema;
                }
                ++itDiff;
            }
        }
    }
    return result;
}

NKikimrSchemeOp::TColumnTableSchemaDiff TSchemaDiffView::ApplyDiff(
    const NKikimrSchemeOp::TColumnTableSchemaDiff& diff0, const NKikimrSchemeOp::TColumnTableSchemaDiff& diff1) {
    AFL_VERIFY(diff0.GetVersion() < diff1.GetVersion());
    NKikimrSchemeOp::TColumnTableSchemaDiff result;
    result.SetVersion(diff1.GetVersion());
    if (diff1.HasDefaultCompression()) {
        *result.MutableDefaultCompression() = diff1.GetDefaultCompression();
    } else if (diff0.HasDefaultCompression()) {
        *result.MutableDefaultCompression() = diff0.GetDefaultCompression();
    }
    if (diff1.HasOptions()) {
        *result.MutableOptions() = diff1.GetOptions();
    } else if (diff0.HasOptions()) {
        *result.MutableOptions() = diff0.GetOptions();
    }
    TSchemaDiffView view0;
    view0.DeserializeFromProto(diff0).Validate();
    TSchemaDiffView view1;
    view1.DeserializeFromProto(diff1).Validate();
    {
        auto it0 = view0.ModifiedColumns.begin();
        auto it1 = view1.ModifiedColumns.begin();
        const auto predUpsert = [&](const ui32 entityId, const NKikimrSchemeOp::TOlapColumnDescription* data) {
            if (data) {
                *result.AddUpsertColumns() = *data;
            } else {
                result.AddDropColumns(entityId);
            }
        };
        while (it0 != view0.ModifiedColumns.end() || it1 != view1.ModifiedColumns.end()) {
            if (it1 == view1.ModifiedColumns.end()) {
                predUpsert(it0->first, it0->second);
                ++it0;
            } else if (it0 == view0.ModifiedColumns.end()) {
                predUpsert(it1->first, it1->second);
                ++it1;
            } else if (it0->first < it1->first) {
                predUpsert(it0->first, it0->second);
                ++it0;
            } else if (it1->first <= it0->first) {
                predUpsert(it1->first, it1->second);
                if (it1->first == it0->first) {
                    ++it0;
                }
                ++it1;
            }
        }
    }
    {
        auto it0 = view0.ModifiedIndexes.begin();
        auto it1 = view1.ModifiedIndexes.begin();
        const auto predUpsert = [&](const ui32 entityId, const NKikimrSchemeOp::TOlapIndexDescription* data) {
            if (data) {
                *result.AddUpsertIndexes() = *data;
            } else {
                result.AddDropIndexes(entityId);
            }
        };
        while (it0 != view0.ModifiedIndexes.end() || it1 != view1.ModifiedIndexes.end()) {
            if (it1 == view1.ModifiedIndexes.end()) {
                predUpsert(it0->first, it0->second);
                ++it0;
            } else if (it0 == view0.ModifiedIndexes.end()) {
                predUpsert(it1->first, it1->second);
                ++it1;
            } else if (it0->first < it1->first) {
                predUpsert(it0->first, it0->second);
                ++it0;
            } else if (it1->first <= it0->first) {
                predUpsert(it1->first, it1->second);
                if (it1->first == it0->first) {
                    ++it0;
                }
                ++it1;
            }
        }
    }
    return result;
}

NKikimrTxColumnShard::TSchemaPresetVersionInfo TSchemaDiffView::Merge(
    const std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo>& schemas) {
    AFL_VERIFY(schemas.size());
    std::optional<ui32> lastSchemaIdx;
    for (ui32 idx = 0; idx < schemas.size(); ++idx) {
        if (!schemas[idx].HasDiff()) {
            AFL_VERIFY(schemas[idx].HasSchema());
            lastSchemaIdx = idx;
        }
    }
    NKikimrTxColumnShard::TSchemaPresetVersionInfo result = schemas.back();
    result.ClearSchema();
    result.ClearDiff();
    AFL_VERIFY(!result.HasSchema());
    AFL_VERIFY(!result.HasDiff());
    if (lastSchemaIdx) {
        NKikimrSchemeOp::TColumnTableSchema schema = schemas[*lastSchemaIdx].GetSchema();
        for (ui32 idx = *lastSchemaIdx + 1; idx < schemas.size(); ++idx) {
            TSchemaDiffView view;
            view.DeserializeFromProto(schemas[idx].GetDiff()).Validate();
            schema = view.ApplyDiff(schema);
        }
        *result.MutableSchema() = std::move(schema);
    } else {
        *result.MutableDiff() = MergeDiffs(schemas);
    }
    return result;
}

NKikimrSchemeOp::TColumnTableSchemaDiff TSchemaDiffView::MergeDiffs(const std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo>& schemas) {
    AFL_VERIFY(schemas.size());
    AFL_VERIFY(schemas.front().HasDiff());
    NKikimrSchemeOp::TColumnTableSchemaDiff result = schemas.front().GetDiff();
    for (ui32 i = 1; i < schemas.size(); ++i) {
        AFL_VERIFY(schemas[i].HasDiff());
        const NKikimrSchemeOp::TColumnTableSchemaDiff& localDiff = schemas[i].GetDiff();
        result = ApplyDiff(result, localDiff);
    }
    return result;
}

bool TSchemaDiffView::IsCorrectToIgnorePreviouse(const TIndexInfo& indexInfo) const {
    for (auto&& i : ModifiedIndexes) {
        if (!i.second) {
            return false;
        }
        if (indexInfo.HasIndexId(i.first)) {
            return false;
        }
    }
    for (auto&& i : ModifiedColumns) {
        if (!i.second) {
            return false;
        }
        if (indexInfo.HasColumnId(i.first)) {
            return false;
        }
        if (i.second->GetNotNull()) {
            return false;
        }
    }
    if (SchemaOptions) {
        if (SchemaOptions->GetSchemeNeedActualization()) {
            return false;
        }
    }
    return true;
}

}   // namespace NKikimr::NOlap
