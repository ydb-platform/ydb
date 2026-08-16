#include "update.h"

namespace NKikimr::NSchemeShard {

void TOlapMultiColumnStatisticsUpsert::DeserializeFromProto(const NKikimrSchemeOp::TMultiColumnStatisticsDescription& proto) {
    Name = proto.GetName();
    for (const auto& columnName : proto.GetColumnNames()) {
        ColumnNames.emplace_back(columnName);
    }
    for (const auto type : proto.GetTypes()) {
        Types.emplace_back(static_cast<NKikimrSchemeOp::EMultiColumnStatisticsType>(type));
    }
}

bool TOlapMultiColumnStatisticsUpdate::Parse(const NKikimrSchemeOp::TColumnTableDescription& description, IErrorCollector& errors) {
    TSet<TString> upsertNames;
    for (const auto& proto : description.GetMultiColumnStatistics()) {
        TOlapMultiColumnStatisticsUpsert statistics;
        statistics.DeserializeFromProto(proto);
        if (!upsertNames.emplace(statistics.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists,
                TStringBuilder() << "statistics '" << statistics.GetName() << "' duplication for add");
            return false;
        }
        UpsertMultiColumnStatistics.emplace_back(std::move(statistics));
    }
    return true;
}

bool TOlapMultiColumnStatisticsUpdate::Parse(const NKikimrSchemeOp::TAlterColumnTable& alterRequest, IErrorCollector& errors) {
    for (const auto& name : alterRequest.GetDropMultiColumnStatistics()) {
        if (!DropMultiColumnStatistics.emplace(name).second) {
            errors.AddError(NKikimrScheme::StatusInvalidParameter, "Duplicated statistics for drop");
            return false;
        }
    }

    TSet<TString> upsertNames;
    for (const auto& proto : alterRequest.GetUpsertMultiColumnStatistics()) {
        TOlapMultiColumnStatisticsUpsert statistics;
        statistics.DeserializeFromProto(proto);
        if (!upsertNames.emplace(statistics.GetName()).second) {
            errors.AddError(NKikimrScheme::StatusAlreadyExists,
                TStringBuilder() << "statistics '" << statistics.GetName() << "' duplication for add");
            return false;
        }
        UpsertMultiColumnStatistics.emplace_back(std::move(statistics));
    }
    return true;
}
}
