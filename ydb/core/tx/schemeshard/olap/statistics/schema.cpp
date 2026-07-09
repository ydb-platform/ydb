#include "schema.h"

#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <util/system/yassert.h>

namespace NKikimr::NSchemeShard {

void TOlapMultiColumnStatisticsSchema::SerializeToProto(NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto) const {
    proto.SetName(Name);
    for (const auto& columnName : ColumnNames) {
        proto.AddColumnNames(columnName);
    }
    for (const auto columnId : ColumnIds) {
        proto.AddColumnIds(columnId);
    }
    for (const auto type : Types) {
        proto.AddTypes(type);
    }
}

void TOlapMultiColumnStatisticsSchema::DeserializeFromProto(const NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto) {
    Name = proto.GetName();
    Y_ABORT_UNLESS(proto.ColumnIdsSize() == 0 || proto.ColumnIdsSize() == proto.ColumnNamesSize(),
        "MultiColumnStatistics '%s' has inconsistent ColumnNames (%d) and ColumnIds (%d) sizes",
        Name.c_str(), static_cast<int>(proto.ColumnNamesSize()), static_cast<int>(proto.ColumnIdsSize()));
    for (const auto& columnName : proto.GetColumnNames()) {
        ColumnNames.emplace_back(columnName);
    }
    for (const auto columnId : proto.GetColumnIds()) {
        ColumnIds.emplace_back(columnId);
    }
    for (const auto type : proto.GetTypes()) {
        Types.emplace_back(static_cast<NKikimrSchemeOp::EMultiColumnStatisticsType>(type));
    }
}

bool TOlapMultiColumnStatisticsSchema::ApplyUpsert(const TOlapSchema& currentSchema, const TOlapMultiColumnStatisticsUpsert& upsert, IErrorCollector& errors) {
    Name = upsert.GetName();
    if (Name.empty()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter, "MultiColumnStatistics name must be specified");
        return false;
    }
    ColumnNames.clear();
    ColumnIds.clear();
    Types.clear();

    if (upsert.GetColumnNames().empty()) {
        errors.AddError(NKikimrScheme::StatusInvalidParameter,
            TStringBuilder() << "MultiColumnStatistics '" << Name << "' must have at least one column");
        return false;
    }

    for (const auto& columnName : upsert.GetColumnNames()) {
        const auto* column = currentSchema.GetColumns().GetByName(columnName);
        if (!column) {
            errors.AddError(NKikimrScheme::StatusSchemeError,
                TStringBuilder() << "Undefined column: " << columnName);
            return false;
        }
        ColumnNames.emplace_back(columnName);
        ColumnIds.emplace_back(column->GetId());
    }

    for (const auto rawType : upsert.GetTypes()) {
        const auto type = static_cast<NKikimrSchemeOp::EMultiColumnStatisticsType>(rawType);
        switch (type) {
            case NKikimrSchemeOp::EMultiColumnStatisticsType::UNSPECIFIED:
                errors.AddError(NKikimrScheme::StatusInvalidParameter, TStringBuilder() << "MultiColumnStatistics '" << Name << "' type must be specified");
                return false;
            case NKikimrSchemeOp::EMultiColumnStatisticsType::COUNT_MIN_SKETCH:
                break;
        }
        Types.emplace_back(type);
    }

    return true;
}

bool TOlapMultiColumnStatisticsDescription::ApplyUpdate(const TOlapSchema& currentSchema, const TOlapMultiColumnStatisticsUpdate& schemaUpdate, IErrorCollector& errors) {
    for (const auto& name : schemaUpdate.GetDropMultiColumnStatistics()) {
        if (!MultiColumnStatisticsByName.contains(name)) {
            errors.AddError(NKikimrScheme::StatusSchemeError, TStringBuilder() << "Unknown statistics for drop: " << name);
            return false;
        }
        MultiColumnStatisticsByName.erase(name);
    }

    for (const auto& upsert : schemaUpdate.GetUpsertMultiColumnStatistics()) {
        TOlapMultiColumnStatisticsSchema statistics;
        if (!statistics.ApplyUpsert(currentSchema, upsert, errors)) {
            return false;
        }
        MultiColumnStatisticsByName[upsert.GetName()] = std::move(statistics);
    }

    return true;
}

void TOlapMultiColumnStatisticsDescription::Parse(const NKikimrSchemeOp::TColumnTableDescription& description) {
    for (const auto& proto : description.GetMultiColumnStatistics()) {
        TOlapMultiColumnStatisticsSchema statistics;
        statistics.DeserializeFromProto(proto);
        MultiColumnStatisticsByName.emplace(proto.GetName(), std::move(statistics));
    }
}

void TOlapMultiColumnStatisticsDescription::Serialize(NKikimrSchemeOp::TColumnTableDescription& description) const {
    description.ClearMultiColumnStatistics();
    for (const auto& [_, statistics] : MultiColumnStatisticsByName) {
        statistics.SerializeToProto(*description.AddMultiColumnStatistics());
    }
}
}
