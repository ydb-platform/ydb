#include "workload.h"
#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/public/api/protos/ydb_formats.pb.h>
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

const TString TWorkloadGeneratorBase::CsvDelimiter = ",";
const TString TWorkloadGeneratorBase::CsvFormatString = [] () {
    Ydb::Formats::CsvSettings settings;
    settings.set_delimiter(CsvDelimiter);
    settings.set_header(true);
    return settings.SerializeAsString();
} ();

std::string TWorkloadGeneratorBase::GetDDLQueries() const {
    TString storageType = "-- ";
    TString notNull = "";
    TString createExternalDataSource;
    TString external;
    TString partitioning = "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT";
    TString primaryKey = ", PRIMARY KEY";
    TString partitionBy = "-- ";
    switch (Params.GetStoreType()) {
    case TWorkloadBaseParams::EStoreType::Column:
        storageType = "STORE = COLUMN, --";
        notNull = "NOT NULL";
        partitionBy = "PARTITION BY HASH";
        break;
    case TWorkloadBaseParams::EStoreType::ExternalS3:
        storageType = fmt::format(R"(DATA_SOURCE = "{}_tpc_s3_external_source", FORMAT = "parquet", LOCATION = )", Params.GetPath());
        notNull = "NOT NULL";
        createExternalDataSource = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{}_tpc_s3_external_source` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{}",
                AUTH_METHOD="NONE"
            );
        )", Params.GetFullTableName(nullptr), Params.GetS3Endpoint());
        external = "EXTERNAL";
        partitioning = "--";
        primaryKey = "--";
    case TWorkloadBaseParams::EStoreType::Row:
        break;
    }
    auto createSql = DoGetDDLQueries();
    SubstGlobal(createSql, "{createExternal}", createExternalDataSource);
    SubstGlobal(createSql, "{external}", external);
    SubstGlobal(createSql, "{notnull}", notNull);
    SubstGlobal(createSql, "{partitioning}", partitioning);
    SubstGlobal(createSql, "{path}", Params.GetFullTableName(nullptr));
    SubstGlobal(createSql, "{primary_key}", primaryKey);
    SubstGlobal(createSql, "{s3_prefix}", Params.GetS3Prefix());
    SubstGlobal(createSql, "{store}", storageType);
    SubstGlobal(createSql, "{partition_by}", partitionBy);
    SubstGlobal(createSql, "{string_type}", Params.GetStringType());
    SubstGlobal(createSql, "{date_type}", Params.GetDateType());
    SubstGlobal(createSql, "{timestamp_type}", Params.GetTimestampType());
    return createSql.c_str();
}

TVector<std::string> TWorkloadGeneratorBase::GetCleanPaths() const {
    return { Params.GetPath().c_str() };
}

TBulkDataGeneratorList TWorkloadGeneratorBase::GetBulkInitialData() const {
    return TBulkDataGeneratorList();
}

void TWorkloadBaseParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int /*workloadType*/) {
    switch (commandType) {
    case TWorkloadParams::ECommandType::Run:
    case TWorkloadParams::ECommandType::Clean:
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
        opts.AddLongOption("date32", "Use Date32 type in tables instead Date one.").NoArgument().StoreValue(&DateType, "Date32");
        opts.AddLongOption("timestamp64", "Use Timestamp64 type in tables instead Timestamp one.").NoArgument().StoreValue(&TimestampType, "Timestamp64");
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
    return TFsPath(DbPath) / Path/ table;
}

TWorkloadGeneratorBase::TWorkloadGeneratorBase(const TWorkloadBaseParams& params)
    : Params(params)
{}

}
