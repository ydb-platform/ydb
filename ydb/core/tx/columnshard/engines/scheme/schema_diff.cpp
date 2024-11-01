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

}   // namespace NKikimr::NOlap
