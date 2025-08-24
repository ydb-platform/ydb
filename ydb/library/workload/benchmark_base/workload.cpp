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

void TWorkloadGeneratorBase::GenerateDDLForTable(IOutputStream& result, const NJson::TJsonValue& table, bool single) const {
    auto specialTypes = GetSpecialDataTypes();
    specialTypes["string_type"] = Params.GetStringType();
    specialTypes["date_type"] = Params.GetDateType();
    specialTypes["timestamp_type"] = Params.GetTimestampType();

    const auto& tableName = table["name"].GetString();
    const auto path = Params.GetFullTableName(single ? nullptr : tableName.c_str());
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
    TVector<TStringBuf> keysV;
    for (const auto& k: table["primary_key"].GetArray()) {
        keysV.emplace_back(k.GetString());
    }
    const TString keys = JoinSeq(", ", keysV);
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << Endl;
    } else {
        result << "," << Endl << "    PRIMARY KEY (" << keys << ")" << Endl;
    }
    result << ")" << Endl;

    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::Column) {
        result << "PARTITION BY HASH (" << keys << ")" << Endl;
    }

    result << "WITH (" << Endl;
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << "    DATA_SOURCE = \""+ Params.GetFullTableName(nullptr) + "_s3_external_source\", FORMAT = \"parquet\", LOCATION = \"" << Params.GetS3Prefix()
            << "/" << (single ? TFsPath(Params.GetPath()).GetName() : (tableName + "/")) << "\"" << Endl;
    } else {
        if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::Column) {
            result << "    STORE = COLUMN," << Endl;
        }
        result << "    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << table["partitioning"].GetUIntegerSafe(64) << ", " << Endl;
        result << "    AUTO_PARTITIONING_PARTITION_SIZE_MB = 50" << Endl;
    }
    result << ");" << Endl;
}

std::string TWorkloadGeneratorBase::GetDDLQueries() const {
    const auto json = GetTablesJson();

    TStringBuilder result;
    result << "--!syntax_v1" << Endl;
    if (Params.GetStoreType() == TWorkloadBaseParams::EStoreType::ExternalS3) {
        result << "CREATE EXTERNAL DATA SOURCE `" << Params.GetFullTableName(nullptr) << "_s3_external_source` WITH (" << Endl
            << "    SOURCE_TYPE=\"ObjectStorage\"," << Endl
            << "    LOCATION=\"" << Params.GetS3Endpoint() << "\"," << Endl
            << "    AUTH_METHOD=\"NONE\"" << Endl
            << ");" << Endl;
    }

    for (const auto& table: json["tables"].GetArray()) {
        GenerateDDLForTable(result.Out, table, false);
    }
    if (json.Has("table")) {
        GenerateDDLForTable(result.Out, json["table"], true);
    }
    return result;
}

NJson::TJsonValue TWorkloadGeneratorBase::GetTablesJson() const {
    const auto tablesYaml = GetTablesYaml();
    const auto yaml = YAML::Load(tablesYaml.c_str());
    return NKikimr::NYaml::Yaml2Json(yaml, true);
}

TVector<std::string> TWorkloadGeneratorBase::GetCleanPaths() const {
    return { Params.GetPath().c_str() };
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
            .StoreValue(&DateType, "Date").StoreValue(&TimestampType, "Timestamp");
        break;
    case TWorkloadParams::ECommandType::Root:
        opts.AddLongOption('p', "path", "Path where benchmark tables are located")
            .Optional()
            .DefaultValue(Path)
            .Handler1T<TStringBuf>([this](TStringBuf arg) {
                while(arg.SkipPrefix("/"));
                while(arg.ChopSuffix("/"));
                Path = arg;
            });
        break;
    }
}

TString TWorkloadBaseParams::GetFullTableName(const char* table) const {
    return DbPath + "/" + Path + (TStringBuf(table) ? "/" + TString(table) : TString());
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
