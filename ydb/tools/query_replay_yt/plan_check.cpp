#include "plan_check.h"

#include <library/cpp/json/json_reader.h>
#include <util/string/builder.h>

namespace NKikimr::NQueryReplay {

NJson::TJsonValue ParseQueryPlan(const TString& plan) {
    static NJson::TJsonReaderConfig readConfig;
    TStringInput in(plan);
    NJson::TJsonValue reply;
    NJson::ReadJsonTree(&in, &readConfig, &reply, false);
    return reply;
}

std::map<std::string, TTableStats> ExtractTableStats(
    const NJson::TJsonValue& plan,
    const TTableMetadataLookup& metadataLookup)
{
    std::map<std::string, TTableStats> result;

    if (!plan.Has("tables")) {
        return result;
    }

    for (const auto& table : plan["tables"].GetArraySafe()) {
        TString name = table["name"].GetStringSafe();
        std::vector<TTableReadAccessInfo> reads;
        std::vector<TTableWriteInfo> writes;

        if (table.Has("reads")) {
            for (const auto& read : table["reads"].GetArraySafe()) {
                std::vector<std::string> columns;
                if (read.Has("columns")) {
                    const NYql::TKikimrTableMetadataPtr* tableMetadata = metadataLookup(name);
                    Y_ENSURE(tableMetadata);
                    const auto& keyColumnNames = tableMetadata->Get()->KeyColumnNames;
                    std::unordered_set<std::string_view> keyColumns(keyColumnNames.begin(), keyColumnNames.end());
                    for (const auto& column : read["columns"].GetArraySafe()) {
                        if (!keyColumns.contains(column.GetStringSafe())) {
                            columns.push_back(column.GetStringSafe());
                        }
                    }
                    std::sort(columns.begin(), columns.end());
                }

                const auto& type = read["type"].GetStringSafe();
                i32 limit = -1;
                if (read.Has("limit") && read["limit"].IsInteger()) {
                    limit = read["limit"].GetIntegerSafe();
                }

                bool probablyLookupScan = false;
                if (read.Has("lookup_by") && read.Has("scan_by")) {
                    probablyLookupScan = true;
                }

                if (type == "Lookup") {
                    if (probablyLookupScan) {
                        reads.push_back(TTableReadAccessInfo{"Scan", limit, columns});
                    } else {
                        reads.push_back(TTableReadAccessInfo{"Lookup", limit, columns});
                    }
                } else if (type == "MultiLookup") {
                    reads.push_back(TTableReadAccessInfo{"Lookup", limit, columns});
                } else {
                    reads.push_back(TTableReadAccessInfo({type, limit, columns}));
                }
            }

            std::sort(reads.begin(), reads.end());
        }

        if (table.Has("writes")) {
            for (const auto& write : table["writes"].GetArraySafe()) {
                std::vector<std::string> columns;
                if (write.Has("columns")) {
                    const NYql::TKikimrTableMetadataPtr* tableMetadata = metadataLookup(name);
                    Y_ENSURE(tableMetadata);
                    const auto& keyColumnNames = tableMetadata->Get()->KeyColumnNames;
                    std::unordered_set<std::string_view> keyColumns(keyColumnNames.begin(), keyColumnNames.end());
                    for (const auto& column : write["columns"].GetArraySafe()) {
                        if (!keyColumns.contains(column.GetStringSafe())) {
                            columns.push_back(column.GetStringSafe());
                        }
                    }
                    std::sort(columns.begin(), columns.end());
                }

                const auto& type = write["type"].GetStringSafe();
                writes.push_back(TTableWriteInfo{type, columns});
            }

            std::sort(writes.begin(), writes.end());
        }

        result.emplace(name, TTableStats{name, std::move(reads), std::move(writes)});
    }

    return result;
}

std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> CompareTableStats(
    const TTableStats& oldEngineStats,
    const TTableStats& newEngineStats)
{
    Y_ENSURE(oldEngineStats.Name == newEngineStats.Name);
    if (oldEngineStats.Reads.size() > newEngineStats.Reads.size()) {
        return {TQueryReplayEvents::ExtraReadingOldEngine, TStringBuilder()
            << "Extra reading in old engine plan for table " << oldEngineStats.Name};
    }

    if (oldEngineStats.Reads.size() < newEngineStats.Reads.size()) {
        return {TQueryReplayEvents::ExtraReadingNewEngine, TStringBuilder()
            << "Extra reading in new engine plan for table " << newEngineStats.Name};
    }

    for (size_t i = 0; i < oldEngineStats.Reads.size(); ++i) {
        if (oldEngineStats.Reads[i].ReadType != newEngineStats.Reads[i].ReadType) {
            return {TQueryReplayEvents::ReadTypesMismatch, TStringBuilder() << "Read types mismatch, old engine: "
                << oldEngineStats.Reads[i].ReadType << ", new engine: " << newEngineStats.Reads[i].ReadType};
        }

        if (oldEngineStats.Reads[i].PushedLimit != newEngineStats.Reads[i].PushedLimit) {
            return {TQueryReplayEvents::ReadLimitsMismatch, TStringBuilder() << "Read limits mismatch, old engine: "
                << oldEngineStats.Reads[i].PushedLimit << ", new engine: " << newEngineStats.Reads[i].PushedLimit};
        }

        if (oldEngineStats.Reads[i].ReadColumns != newEngineStats.Reads[i].ReadColumns) {
            return {TQueryReplayEvents::ReadColumnsMismatch, TStringBuilder() << "Read columns mismatch"};
        }
    }

    if (oldEngineStats.Writes.size() > newEngineStats.Writes.size()) {
        return {TQueryReplayEvents::ExtraWriting, TStringBuilder()
            << "Extra write operation in old engine plan for table " << oldEngineStats.Name};
    }

    if (oldEngineStats.Writes.size() < newEngineStats.Writes.size()) {
        return {TQueryReplayEvents::ExtraWriting, TStringBuilder()
            << "Extra write operation in new engine plan for table " << newEngineStats.Name};
    }

    for (size_t i = 0; i < oldEngineStats.Writes.size(); ++i) {
        if (oldEngineStats.Writes[i].WriteType != newEngineStats.Writes[i].WriteType) {
            return {TQueryReplayEvents::WriteTypesMismatch, TStringBuilder() << "Write types mismatch, old engine: "
                << oldEngineStats.Writes[i].WriteType << ", new engine: " << newEngineStats.Writes[i].WriteType};
        }

        if (oldEngineStats.Writes[i].WriteColumns != newEngineStats.Writes[i].WriteColumns) {
            return {TQueryReplayEvents::WriteColumnsMismatch, TStringBuilder() << "Write columns mismatch"};
        }
    }

    return {TQueryReplayEvents::UncategorizedPlanMismatch, ""};
}

std::pair<TQueryReplayEvents::TCheckQueryPlanStatus, TString> CheckQueryPlans(
    const TString& oldPlanJson,
    const NJson::TJsonValue& newPlan,
    const TTableMetadataLookup& metadataLookup,
    std::map<std::string, TTableStats>* newEngineStats)
{
    const NJson::TJsonValue oldEnginePlan = ParseQueryPlan(oldPlanJson);
    const auto oldStats = ExtractTableStats(oldEnginePlan, metadataLookup);
    auto newStats = ExtractTableStats(newPlan, metadataLookup);
    if (newEngineStats) {
        *newEngineStats = newStats;
    }

    for (const auto& [table, stats] : oldStats) {
        auto it = newStats.find(table);
        if (it == newStats.end()) {
            return {TQueryReplayEvents::TableMissing,
                TStringBuilder() << "Table " << table << " not found in new engine plan"};
        }

        if (stats != it->second) {
            return CompareTableStats(stats, it->second);
        }
    }

    return {TQueryReplayEvents::Success, ""};
}

TString StatusToFailReason(TQueryReplayEvents::TCheckQueryPlanStatus status) {
    switch (status) {
        case TQueryReplayEvents::CompileError:
            return "compile_error";
        case TQueryReplayEvents::CompileTimeout:
            return "compile_timeout";
        case TQueryReplayEvents::TableMissing:
            return "table_missing";
        case TQueryReplayEvents::ExtraReadingOldEngine:
            return "extra_reading_old_engine";
        case TQueryReplayEvents::ExtraReadingNewEngine:
            return "extra_reading_new_engine";
        case TQueryReplayEvents::ReadTypesMismatch:
            return "read_types_mismatch";
        case TQueryReplayEvents::ReadLimitsMismatch:
            return "read_limits_mismatch";
        case TQueryReplayEvents::ReadColumnsMismatch:
            return "read_columns_mismatch";
        case TQueryReplayEvents::ExtraWriting:
            return "extra_writing";
        case TQueryReplayEvents::WriteColumnsMismatch:
            return "write_columns_mismatch";
        case TQueryReplayEvents::WriteTypesMismatch:
            return "write_types_mismatch";
        case TQueryReplayEvents::UncategorizedPlanMismatch:
            return "uncategorized_plan_mismatch";
        case TQueryReplayEvents::MissingTableMetadata:
            return "missing_table_metadata";
        case TQueryReplayEvents::UncategorizedFailure:
            return "uncategorized_failure";
        default:
            return "unspecified";
    }
}

} // namespace NKikimr::NQueryReplay
