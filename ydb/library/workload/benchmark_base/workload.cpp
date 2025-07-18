#include "workload.h"
#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/node/parse.h>
#include <util/string/cast.h>
#include <util/system/spinlock.h>


namespace NYdbWorkload {

const TString TWorkloadGeneratorBase::TsvDelimiter = "\t";
const TString TWorkloadGeneratorBase::TsvFormatString = [] () {
    Ydb::Formats::CsvSettings settings;
    settings.set_delimiter(TsvDelimiter);
    settings.set_header(true);
    settings.mutable_quoting()->set_disabled(true);
    return settings.SerializeAsString();
} ();

const TString TWorkloadGeneratorBase::PsvDelimiter = "|";
const TString TWorkloadGeneratorBase::PsvFormatString = [] () {
    Ydb::Formats::CsvSettings settings;
    settings.set_delimiter(PsvDelimiter);
    settings.set_header(true);
    settings.mutable_quoting()->set_disabled(true);
    return settings.SerializeAsString();
} ();

const TString TWorkloadGeneratorBase::CsvDelimiter = ",";
const TString TWorkloadGeneratorBase::CsvFormatString = [] () {
    Ydb::Formats::CsvSettings settings;
    settings.set_delimiter(CsvDelimiter);
    settings.set_header(true);
    return settings.SerializeAsString();
} ();

namespace {

    TString KeysList(const NJson::TJsonValue& table, const TString& key) {
        TVector<TStringBuf> keysV;
        for (const auto& k: table[key].GetArray()) {
            keysV.emplace_back(k.GetString());
        }
        return JoinSeq(", ", keysV);
    }

}

ui32 TWorkloadGeneratorBase::GetDefaultPartitionsCount(const TString& /*tableName*/) const {
    return 64;
}

void TWorkloadGeneratorBase::GenerateDDLForTable(IOutputStream& result, const NJson::TJsonValue& table, const NJson::TJsonValue& common, bool single) const {
    auto specialTypes = GetSpecialDataTypes();
    specialTypes["string_type"] = Params.GetStringType();
    specialTypes["date_type"] = Params.GetDateType();
    specialTypes["datetime_type"] = Params.GetDatetimeType();
    specialTypes["timestamp_type"] = Params.GetTimestampType();

    const auto& tableName = table["name"].GetString();
    const auto path = Params.GetFullTableName((single && Params.GetPath())? nullptr : tableName.c_str());
    result << Endl << "CREATE ";
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << "EXTERNAL ";
    }
    result << "TABLE `" << path << "` (" << Endl;
    TVector<TStringBuilder> columns;
    for (const auto& column: table["columns"].GetArray()) {
        const auto& columnName = column["name"].GetString();
        columns.emplace_back();
        auto& so = columns.back();
        so << "    " << columnName << " ";
        const auto& type = column["type"].GetString();
        if (const auto* st = MapFindPtr(specialTypes, type)) {
            so << *st;
        } else {
            so << type;
        }
        if (column["not_null"].GetBooleanSafe(false) && Params.GetStoreType() != TWorkloadBaseParams::EStoreType::Row) {
            so << " NOT NULL";
        }
    }
    result << JoinSeq(",\n", columns);
    const auto keys = KeysList(table, "primary_key");
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << Endl;
    } else {
        result << "," << Endl << "    PRIMARY KEY (" << keys << ")" << Endl;
    }
    result << ")" << Endl;

    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::Column) {
        result << "PARTITION BY HASH (" <<  (table.Has("partition_by") ? KeysList(table, "partition_by") : keys) << ")" << Endl;
    }

    const ui64 partitioning = table["partitioning"].GetUIntegerSafe(
        common["partitioning"].GetUIntegerSafe(GetDefaultPartitionsCount(tableName)));

    result << "WITH (" << Endl;
    switch (Params.GetStoreType()) {
    case TWorkloadBaseParams::EStoreType::ExternalS3:
        result << "    DATA_SOURCE = \""+ Params.GetFullTableName(nullptr) + "_s3_external_source\", FORMAT = \"parquet\", LOCATION = \"" << Params.GetS3Prefix()
            << "/" << (single ? TFsPath(Params.GetPath()).GetName() : (tableName + "/")) << "\"" << Endl;
        break;
    case TWorkloadBaseParams::EStoreType::Column:
        result << "    STORE = COLUMN," << Endl;
        result << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << partitioning << Endl;
        break;
    case TWorkloadBaseParams::EStoreType::Row:
        result << "    STORE = ROW," << Endl;
        result << "    AUTO_PARTITIONING_PARTITION_SIZE_MB = " << Params.GetPartitionSizeMb() << ", " << Endl;
        result << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << partitioning << Endl;
    }
    result << ");" << Endl;
    if (TString actions = table["actions"].GetStringSafe(common["actions"].GetStringSafe(""))) {
        SubstGlobal(actions, "{table}", path);
        result << actions << Endl;
    }
}

std::string TWorkloadGeneratorBase::GetDDLQueries() const {
    const auto json = GetTablesJson();

    TStringBuilder result;
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << "CREATE EXTERNAL DATA SOURCE `" << Params.GetFullTableName(nullptr) << "_s3_external_source` WITH (" << Endl
            << "    SOURCE_TYPE=\"ObjectStorage\"," << Endl
            << "    LOCATION=\"" << Params.GetS3Endpoint() << "\"," << Endl
            << "    AUTH_METHOD=\"NONE\"" << Endl
            << ");" << Endl;
    }

    for (const auto& table: json["tables"].GetArray()) {
        GenerateDDLForTable(result.Out, table, json["every_table"], false);
    }
    if (json.Has("table")) {
        GenerateDDLForTable(result.Out, json["table"], json["every_table"], true);
    }
    if (result) {
        return "--!syntax_v1\n" + result;
    }
    return result;
}

NJson::TJsonValue TWorkloadGeneratorBase::GetTablesJson() const {
    const auto tablesYaml = GetTablesYaml();
    if (!tablesYaml) {
        return NJson::JSON_NULL;
    }
    const auto yaml = YAML::Load(tablesYaml.c_str());
    return NKikimr::NYaml::Yaml2Json(yaml, true);
}

TVector<std::string> TWorkloadGeneratorBase::GetCleanPaths() const {
    const auto json = GetTablesJson();
    TVector<std::string> result;
    for (const auto& table: json["tables"].GetArray()) {
        result.emplace_back(Params.GetPath() + "/" + table["name"].GetString());
    }
    if (json.Has("table")) {
        result.emplace_back(Params.GetPath() ? Params.GetPath() : json["table"]["name"].GetString());
    }
    return result;
}

TWorkloadDataInitializerBase::TWorkloadDataInitializerBase(const TString& name, const TString& description, const TWorkloadBaseParams& params)
    : TWorkloadDataInitializer(name, description)
    , Params(params)
{}

void TWorkloadDataInitializerBase::ConfigureOpts(NLastGetopt::TOpts& opts) {
    opts.AddLongOption("clear-state", "Clear tables before init").NoArgument()
        .Optional().StoreResult(&Clear, true);
        opts.AddLongOption("state", "Path for save generation state.").StoreResult(&StatePath);
}

TBulkDataGeneratorList TWorkloadDataInitializerBase::GetBulkInitialData() {
    if (StatePath.IsDefined()) {
        StateProcessor = MakeHolder<TGeneratorStateProcessor>(StatePath, Clear);
    }
    return DoGetBulkInitialData();
}

void TWorkloadBaseParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int /*workloadType*/) {
    switch (commandType) {
    default:
        break;
    case TWorkloadParams::ECommandType::Init:
        opts.AddLongOption("store", "Storage type."
                " Options: row, column, external-s3\n"
                "row - use row-based storage engine;\n"
                "column - use column-based storage engine.\n"
                "external-s3 - use cloud bucket")
            .DefaultValue(StoreType)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                const auto l = to_lower(TString(arg));
                if (!TryFromString(arg, StoreType)) {
                    throw yexception() << "Ivalid store type: " << arg;
                }
            });
        opts.AddLongOption("external-s3-prefix", "Root path to dataset in s3 storage")
            .Optional()
            .StoreResult(&S3Prefix);
        opts.AddLongOption('e', "external-s3-endpoint", "Endpoint of S3 bucket with dataset")
            .Optional()
            .StoreResult(&S3Endpoint);
        opts.AddLongOption("string", "Use String type in tables instead Utf8 one.").NoArgument().StoreValue(&StringType, "String");
        opts.AddLongOption("datetime", "Use Date and Timestamp types in tables instead Date32 and Timestamp64 ones.").NoArgument()
            .StoreValue(&DateType, "Date").StoreValue(&TimestampType, "Timestamp").StoreValue(&DatetimeType, "Datetime");
        opts.AddLongOption("partition-size", "Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB) for row tables.")
            .DefaultValue(PartitionSizeMb).StoreResult(&PartitionSizeMb);
        break;
    case TWorkloadParams::ECommandType::Run:
        opts.AddLongOption('c', "check-canonical", "Use deterministic queries and check results with canonical ones.")
            .NoArgument().StoreTrue(&CheckCanonical);
        break;
    case TWorkloadParams::ECommandType::Root:
        opts.AddLongOption('p', "path", "Path where benchmark tables are located")
            .Optional()
            .DefaultValue(Path)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                while(arg.ChopSuffix("/"));
                Path = arg;
            });
        break;
    }
}

void TWorkloadBaseParams::Validate(const ECommandType /*commandType*/, int /*workloadType*/) {
    if (Path.StartsWith('/')) {
        if (!Path.StartsWith("/" + DbPath)) {
            throw yexception() << "Absolute path does not start with " << DbPath << ": " << Path;
        }
        Path = Path.substr(DbPath.size() + 1);
    }
}

TString TWorkloadBaseParams::GetFullTableName(const char* table) const {
    TStringBuilder result;
    if (Path.StartsWith('/')) {
        result << Path;
    } else {
        result << DbPath;
        if (Path) {
            result << "/" << Path;
        }
    }
    if (TStringBuf(table)){
        result << "/" << table;
    }
    return result;
}

TWorkloadGeneratorBase::TWorkloadGeneratorBase(const TWorkloadBaseParams& params)
    : Params(params)
{}

TString TWorkloadBaseParams::GetTablePathQuote(EQuerySyntax syntax) {
    switch(syntax) {
    case EQuerySyntax::YQL:
        return "`";
    case EQuerySyntax::PG:
        return "\"";
    }
}

}
