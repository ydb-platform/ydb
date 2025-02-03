#include "log.h"
#include <library/cpp/json/json_value.h>
#include <library/cpp/resource/resource.h>
#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/random/normal.h>
#include <util/random/random.h>
#include <util/string/split.h>

namespace NYdbWorkload {

namespace NLog {

using TRow = TLogGenerator::TRow;


std::string TLogGenerator::GetDDLQueries() const {
    std::stringstream ss;

    ss << "--!syntax_v1\n";
    ss << "CREATE TABLE `" << Params.DbPath << "/" << Params.TableName << "` " << R"((
    -- Основные поля для идентификации и времени
    log_id          Utf8 NOT NULL,        -- Уникальный идентификатор лога
    timestamp       Timestamp NOT NULL,      -- Время создания лога
    
    -- Поля для классификации
    level           Int32 NOT NULL,          -- Уровень лога (INFO, WARNING, ERROR, etc.)
    service_name    Utf8 NOT NULL,          -- Название сервиса/приложения
    component       Utf8,                   -- Компонент системы
    
    -- Детали сообщения
    message         Utf8 NOT NULL,          -- Текст сообщения
        
    -- Контекстная информация
    request_id      Utf8,                   -- ID запроса для трейсинга

    -- Дополнительные метаданные
    metadata        JsonDocument,           -- Дополнительные данные в JSON формате

    ingested_at    Timestamp,
)";
    std::stringstream keys;
    keys << "timestamp, log_id";
    for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
        ss << "c" << i << (i < Params.IntColumnsCnt ? " Uint64" : " Utf8");
        if (i < Params.KeyColumnsCnt) {
            keys << ", c" << i;
            if (Params.GetStoreType() == TLogWorkloadParams::EStoreType::Column) {
                ss << " NOT NULL";
            }
        }
        ss << "," << std::endl;
    }
    ss << "    PRIMARY KEY (" << keys.str() << ")" << std::endl << ") WITH (" << std::endl;
    ss << "    TTL = Interval(\"PT" << Params.TimestampTtlMinutes << "M\") ON timestamp," << std::endl;
    switch (Params.GetStoreType()) {
        case TLogWorkloadParams::EStoreType::Row:
            ss << "    STORE = ROW, " << std::endl;
            break;
        case TLogWorkloadParams::EStoreType::Column:
            ss << "    STORE = COLUMN, " << std::endl;
            break;
    }
    if (Params.PartitionsByLoad) {
        ss << "    AUTO_PARTITIONING_BY_LOAD = ENABLED, ";
    }
    ss << "    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = " << Max(Params.MinPartitions, Params.MaxPartitions) << "," << std::endl;
    ss << "    AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.PartitionSizeMb << "," << std::endl;
    ss << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << Params.MinPartitions  << std::endl;
    ss << ")";
    return ss.str();
}

TQueryInfoList TLogGenerator::GetWorkload(int type) {
    switch (static_cast<EType>(type)) {
        case EType::Insert:
            return Insert(GenerateRandomRows());
        case EType::Upsert:
            return Upsert(GenerateRandomRows());
        case EType::BulkUpsert:
            return BulkUpsert(GenerateRandomRows());
        case EType::Select:
            return Select();
    }
}


TVector<IWorkloadQueryGenerator::TWorkloadType> TLogGenerator::GetSupportedWorkloadTypes() const {
    TVector<TWorkloadType> result;
    result.emplace_back(static_cast<int>(EType::Insert), "insert", "Insert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::Upsert), "upsert", "Upsert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::BulkUpsert), "bulk_upsert", "Bulk upsert random rows into table near current ts");
    result.emplace_back(static_cast<int>(EType::Select), "select", "Select some agregated queries");
    return result;
}

TQueryInfoList TLogGenerator::WriteRows(TString operation, TVector<TRow>&& rows) const {
    std::stringstream ss;

    NYdb::TParamsBuilder paramsBuilder;

    ss << "--!syntax_v1" << std::endl;

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "DECLARE $log_id_" << row << " AS Utf8;" << std::endl;
        ss << "DECLARE $timestamp_" << row << " AS Timestamp;" << std::endl;
        ss << "DECLARE $level_" << row << " AS Int32;" << std::endl;
        ss << "DECLARE $service_name_" << row << " AS Utf8;" << std::endl;
        ss << "DECLARE $component_" << row << " AS Utf8?;" << std::endl;
        ss << "DECLARE $message_" << row << " AS Utf8;" << std::endl;
        ss << "DECLARE $request_id_" << row << " AS Utf8?;" << std::endl;
        ss << "DECLARE $metadata_" << row << " AS JsonDocument?;" << std::endl;
        ss << "DECLARE $ingested_at_" << row << " AS Timestamp?;" << std::endl;
        for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
            ss << "DECLARE $c" << i << "_" << row << " AS " << (i < Params.IntColumnsCnt ? "Uint64" : "Utf8");
            if (i >= Params.KeyColumnsCnt) {
                ss << "?";
            }
            ss << ";" << std::endl;
        }

        const auto& r = rows[row];

        paramsBuilder.AddParam("$log_id_" + ToString(row)).Utf8(r.LogId).Build();
        paramsBuilder.AddParam("$timestamp_" + ToString(row)).Timestamp(r.Ts).Build();
        paramsBuilder.AddParam("$level_" + ToString(row)).Int32(r.Level).Build();
        paramsBuilder.AddParam("$service_name_" + ToString(row)).Utf8(r.ServiceName).Build();
        paramsBuilder.AddParam("$component_" + ToString(row)).OptionalUtf8(!r.Component.empty() ? std::optional<std::string>(r.Component) : std::optional<std::string>()).Build();
        paramsBuilder.AddParam("$message_" + ToString(row)).Utf8(r.Message).Build();
        paramsBuilder.AddParam("$request_id_" + ToString(row)).OptionalUtf8(!r.RequestId.empty() ? std::optional<std::string>(r.RequestId) : std::optional<std::string>()).Build();
        paramsBuilder.AddParam("$metadata_" + ToString(row)).OptionalJsonDocument(!r.Metadata.empty() ? std::optional<std::string>(r.Metadata) : std::optional<std::string>()).Build();
        paramsBuilder.AddParam("$ingested_at_" + ToString(row)).OptionalTimestamp(r.IngestedAt != TInstant::Zero() ? std::optional<TInstant>(r.IngestedAt) : std::optional<TInstant>()).Build();
        for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
            auto& p = paramsBuilder.AddParam(TStringBuilder() << "$c" << i << "_" << row);
            if (i < Params.IntColumnsCnt) {
                const auto value = r.Ints[i];
                if (i < Params.KeyColumnsCnt) {
                    p.Uint64(value);
                } else {
                    p.OptionalUint64(value ? std::optional<ui64>(value) : std::optional<ui64>());
                }
            } else {
                const auto& value = r.Strings[i - Params.IntColumnsCnt];
                if (i < Params.KeyColumnsCnt) {
                    p.Utf8(value);
                } else {
                    p.OptionalUtf8(value ? std::optional<std::string>(value) : std::optional<std::string>());
                }
            }
            p.Build();
        }
    }

    ss << operation << " INTO `" << Params.TableName << "` (log_id, timestamp, level, service_name, component, message, request_id, metadata, ingested_at";
    for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
        ss << ", c" << i;
    }
    ss << ") VALUES" ;
    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        ss << "($log_id_" << row << ", $timestamp_" << row << ", $level_" << row << ", $service_name_" << row << ", $component_" << row << ", $message_" << row << ", $request_id_" << row << ", $metadata_" << row << ", $ingested_at_" << row;
        for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
            ss << ", $c" << i << "_" << row;
        }
        ss << ")";
        if (row + 1 < Params.RowsCnt) {
            ss << ", ";
        }
    }
    return TQueryInfoList(1, TQueryInfo(ss.str(), paramsBuilder.Build()));
}

TQueryInfoList TLogGenerator::Insert(TVector<TRow>&& rows) const {
    return WriteRows("INSERT", std::move(rows));
}

TQueryInfoList TLogGenerator::Upsert(TVector<TRow>&& rows) const {
    return WriteRows("UPSERT", std::move(rows));
}

TQueryInfoList TLogGenerator::BulkUpsert(TVector<TRow>&& rows) const {
    NYdb::TValueBuilder valueBuilder;
    valueBuilder.BeginList();
    for (const TRow& row : rows) {
        auto &listItem = valueBuilder.AddListItem();
        listItem.BeginStruct();
        listItem.AddMember("log_id").Utf8(row.LogId);
        listItem.AddMember("timestamp").Timestamp(row.Ts);
        listItem.AddMember("level").Int32(row.Level);
        listItem.AddMember("service_name").Utf8(row.ServiceName);
        listItem.AddMember("component").OptionalUtf8(!row.Component.empty() ? std::optional<std::string>(row.Component) : std::optional<std::string>());
        listItem.AddMember("message").Utf8(row.Message);
        listItem.AddMember("request_id").OptionalUtf8(!row.RequestId.empty() ? std::optional<std::string>(row.RequestId) : std::optional<std::string>());
        listItem.AddMember("metadata").OptionalJsonDocument(!row.Metadata.empty() ? std::optional<std::string>(row.Metadata) : std::optional<std::string>());
        listItem.AddMember("ingested_at").OptionalTimestamp(row.IngestedAt != TInstant::Zero() ? std::optional<TInstant>(row.IngestedAt) : std::optional<TInstant>());
        for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
            auto& m = listItem.AddMember(TStringBuilder() << "c" << i);
            if (i < Params.IntColumnsCnt) {
                const auto value = row.Ints[i];
                if (i < Params.KeyColumnsCnt) {
                    m.Uint64(value);
                } else {
                    m.OptionalUint64(value ? std::optional<ui64>(value) : std::optional<ui64>());
                }
            } else {
                const auto& value = row.Strings[i - Params.IntColumnsCnt];
                if (i < Params.KeyColumnsCnt) {
                    m.Utf8(value);
                } else {
                    m.OptionalUtf8(value ? std::optional<std::string>(value) : std::optional<std::string>());
                }
            }
        }
        listItem.EndStruct();
    }
    valueBuilder.EndList();
    TString tablePath = Params.DbPath + "/" + Params.TableName;
    NYdb::TValue rowsValue = valueBuilder.Build();
    auto bulkUpsertOperation = [tablePath, rowsValue](NYdb::NTable::TTableClient& tableClient) {
        auto r = rowsValue;
        auto status = tableClient.BulkUpsert(tablePath, std::move(r));
        return status.GetValueSync();
    };
    TQueryInfo queryInfo;
    queryInfo.TableOperation = bulkUpsertOperation;
    return TQueryInfoList(1, std::move(queryInfo));
}

TQueryInfoList TLogGenerator::Select() const {
    const auto queries = StringSplitter(NResource::Find("workload_logs_select_queries.sql")).Split(';').SkipEmpty().ToList<std::string>();
    TQueryInfoList result;
    for(const auto& query: queries) {
        result.emplace_back(SubstGlobalCopy(query, std::string("{table}"), Params.TableName), NYdb::TParamsBuilder().Build());
    }
    return result;
}

TQueryInfoList TLogGenerator::GetInitialData() {
    TQueryInfoList res;
    return res;
}

TVector<std::string> TLogGenerator::GetCleanPaths() const {
    return { Params.TableName };
}

std::string TLogGenerator::RandomWord(bool canBeEmpty) const {
    if (canBeEmpty && !RandomIsNotNull()) {
        return {};
    }
    size_t len = RandomNumber<size_t>(Params.StringLen);
    std::stringstream ss;
    for (size_t i = 0; i < len; ++i) {
        ss << 'a' + RandomNumber<char>(26);
    }
    return ss.str();
}

std::string TLogGenerator::RandomPhrase(ui32 maxLen, ui32 minLen) const {
    std::stringstream result;
    for (ui32 len = RandomNumber<ui32>(maxLen - minLen) + minLen; len > 0; --len) {
        static const std::string delimiters = " .,=;-+:";
        result << RandomWord(false) << delimiters[RandomNumber<size_t>(delimiters.length() - 1)];
    }
    return result.str();
}

TInstant TLogGenerator::RandomInstant() const {
    auto result = TInstant::Now();
    i64 millisecondsDiff = 60 * 1000 * NormalRandom<double>(0., Params.TimestampStandardDeviationMinutes);
    if (millisecondsDiff >= 0) { // TDuration::MilliSeconds can't be negative for some reason...
        result += TDuration::MilliSeconds(millisecondsDiff);
    } else {
        result -= TDuration::MilliSeconds(-millisecondsDiff);
    }
    return result;
}

bool TLogGenerator::RandomIsNotNull() const {
    return RandomNumber<ui32>(100) >= Params.NullPercent;
}

TVector<TRow> TLogGenerator::GenerateRandomRows() const {
    TVector<TRow> result;
    result.reserve(Params.RowsCnt);

    for (size_t row = 0; row < Params.RowsCnt; ++row) {
        result.emplace_back();
        result.back().LogId = CreateGuidAsString().c_str();
        result.back().Ts = RandomInstant();
        result.back().Level = RandomNumber<ui32>(10);
        result.back().ServiceName = RandomWord(false);
        result.back().Component = RandomWord(true);
        result.back().Message += RandomPhrase(100);

        if (RandomIsNotNull()) {
            NJson::TJsonValue json(NJson::JSON_MAP);
            if (RandomIsNotNull()) {
                json["adv_engine_id"] = ToString(RandomNumber<ui32>(10));
            }
            if (RandomIsNotNull()) {
                json["client_ip"] = RandomNumber<ui64>();
            }
            if (RandomIsNotNull()) {
                json["dont_count"] = RandomNumber<bool>();
            }
            if (RandomIsNotNull()) {
                json["is_download"] = RandomNumber<bool>();
            }
            if (RandomIsNotNull()) {
                json["is_link"] = RandomNumber<bool>();
            }
            if (RandomIsNotNull()) {
                json["is_refresh"] = RandomNumber<bool>();
            }
            if (RandomIsNotNull()) {
                json["referer"] = ToString(RandomNumber<ui32>(10));
                json["referer_hash"] = ToString(RandomNumber<ui64>());
            }
            if (RandomIsNotNull()) {
                json["response_time"] = RandomNumber<double>();
            }
            if (RandomIsNotNull()) {
                json["search_engine_id"] = ToString(RandomNumber<ui32>(10));
            }
            if (RandomIsNotNull()) {
                json["title"] = RandomPhrase(100);
            }
            if (RandomIsNotNull()) {
                json["traffic_source_id"] = ToString(RandomNumber<ui32>(10));
            }
            if (RandomIsNotNull()) {
                json["url"] = TStringBuilder() << (RandomNumber<bool>() ? "api:" : "http:") << RandomNumber<ui64>();
                json["url_hash"] = ToString(RandomNumber<ui64>());
            }
            if (RandomIsNotNull()) {
                json["window_client_height"] = RandomNumber<ui32>();
            }
            if (RandomIsNotNull()) {
                json["window_client_width"] = RandomNumber<ui32>();
            }
            result.back().Metadata = json.GetStringRobust().c_str();
        }
        result.back().IngestedAt = RandomIsNotNull() ? RandomInstant() : TInstant::Zero();
        for (ui32 i = 0; i < Params.IntColumnsCnt + Params.StrColumnsCnt; ++i) {
            if (i < Params.IntColumnsCnt) {
                result.back().Ints.emplace_back(i < Params.KeyColumnsCnt || RandomIsNotNull() ? RandomNumber<ui64>(Max<ui64>() - 1) + 1 : 0);
            } else {
                result.back().Strings.emplace_back(RandomWord(i >= Params.KeyColumnsCnt));
            }
        }
    }

    return result;
}

void TLogWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    opts.AddLongOption('p', "path", "Path where benchmark tables are located")
        .Optional()
        .DefaultValue(TableName)
        .Handler1T<TStringBuf>([this](TStringBuf arg) {
            while(arg.SkipPrefix("/"));
            while(arg.ChopSuffix("/"));
            TableName = arg;
        });
    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("min-partitions", "Minimum partitions for tables.")
            .DefaultValue(MinPartitions).StoreResult(&MinPartitions);
        opts.AddLongOption("max-partitions", "Maximum partitions for tables.")
            .DefaultValue(MaxPartitions).StoreResult(&MaxPartitions);
        opts.AddLongOption("partition-size", "Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB).")
            .DefaultValue(PartitionSizeMb).StoreResult(&PartitionSizeMb);
        opts.AddLongOption("auto-partition", "Enable auto partitioning by load.")
            .DefaultValue(PartitionsByLoad).StoreResult(&PartitionsByLoad);
        opts.AddLongOption("len", "String len")
            .DefaultValue(StringLen).StoreResult(&StringLen);
        opts.AddLongOption("int-cols", "Number of int columns")
            .DefaultValue(IntColumnsCnt).StoreResult(&IntColumnsCnt);
        opts.AddLongOption("str-cols", "Number of string columns")
            .DefaultValue(StrColumnsCnt).StoreResult(&StrColumnsCnt);
        opts.AddLongOption("key-cols", "Number of key columns")
            .DefaultValue(KeyColumnsCnt).StoreResult(&KeyColumnsCnt);
        opts.AddLongOption("ttl", "TTL for timestamp column in minutes")
            .DefaultValue(TimestampTtlMinutes).StoreResult(&TimestampTtlMinutes);
        opts.AddLongOption("store", "Storage type."
                " Options: row, column\n"
                "  row - use row-based storage engine;\n"
                "  column - use column-based storage engine.")
            .DefaultValue(StoreType)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                const auto l = to_lower(TString(arg));
                if (!TryFromString(arg, StoreType)) {
                    throw yexception() << "Ivalid store type: " << arg;
                }
            });
        opts.AddLongOption("null-percent", "Percent of nulls in generated data")
            .DefaultValue(NullPercent).StoreResult(&NullPercent);
        break;
    case TWorkloadParams::ECommandType::Run:
        switch (static_cast<TLogGenerator::EType>(workloadType)) {
        case TLogGenerator::EType::Insert:
        case TLogGenerator::EType::Upsert:
        case TLogGenerator::EType::BulkUpsert:
            opts.AddLongOption("len", "String len")
                .DefaultValue(StringLen).StoreResult(&StringLen);
            opts.AddLongOption("int-cols", "Number of int columns")
                .DefaultValue(IntColumnsCnt).StoreResult(&IntColumnsCnt);
            opts.AddLongOption("str-cols", "Number of string columns")
                .DefaultValue(StrColumnsCnt).StoreResult(&StrColumnsCnt);
            opts.AddLongOption("key-cols", "Number of key columns")
                .DefaultValue(KeyColumnsCnt).StoreResult(&KeyColumnsCnt);
            opts.AddLongOption("rows", "Number of rows to upsert")
                .DefaultValue(RowsCnt).StoreResult(&RowsCnt);
            opts.AddLongOption("timestamp_deviation", "Standard deviation. For each timestamp, a random variable with a specified standard deviation in minutes is added.")
                .DefaultValue(TimestampStandardDeviationMinutes).StoreResult(&TimestampStandardDeviationMinutes);
            opts.AddLongOption("null-percent", "Percent of nulls in generated data")
                .DefaultValue(NullPercent).StoreResult(&NullPercent);
            break;
        case TLogGenerator::EType::Select:
        break;
        }
        break;
    default:
        break;
    }
}

THolder<IWorkloadQueryGenerator> TLogWorkloadParams::CreateGenerator() const {
    return MakeHolder<TLogGenerator>(this);
}

TString TLogWorkloadParams::GetWorkloadName() const {
    return "Log";
}

} // namespace NLog

} // namespace NYdbWorkload
