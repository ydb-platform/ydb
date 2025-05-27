#pragma once

#include "ydb_command.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/lib/ydb_cli/common/aws.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parseable_struct.h>

namespace NYdb::NConsoleClient {

class TCommandImport : public TClientCommandTree {
public:
    TCommandImport();
};

class TCommandImportFromS3 : public TYdbOperationCommand,
                           public TCommandWithAwsCredentials,
                           public TCommandWithOutput {
public:
    TCommandImportFromS3();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    void ExtractParams(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    struct TItemFields {
        TString Source;
        TString Destination;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination);

    TString AwsEndpoint;
    ES3Scheme AwsScheme = ES3Scheme::HTTPS;
    TString AwsBucket;
    TVector<TItem> Items;
    TString Description;
    ui32 NumberOfRetries = 10;
    bool UseVirtualAddressing = true;
    bool NoACL = false;
    bool SkipChecksumValidation = false;
};

class TCommandImportFromFile : public TClientCommandTree {
public:
    TCommandImportFromFile();
};

class TCommandImportFileBase : public TYdbCommand,
    public TCommandWithPath, public TCommandWithInput {
public:
    TCommandImportFileBase(const TString& cmd, const TString& cmdDescription)
      : TYdbCommand(cmd, {}, cmdDescription)
    {
        Args[0] = "<input files...>";
    }
    void Config(TConfig& config) override;
    void ExtractParams(TConfig& config) override;
    void Parse(TConfig& config) override;

protected:
    TVector<TString> FilePaths;
    TString BytesPerRequest;
    ui64 MaxInFlightRequests = 1;
    ui64 Threads = 0;
    TDuration OperationTimeout;
};

class TCommandImportFromCsv : public TCommandImportFileBase {
public:
    TCommandImportFromCsv(const TString& cmd = "csv", const TString& cmdDescription = "Import data from CSV file")
        : TCommandImportFileBase(cmd, cmdDescription)
    {
        InputFormat = EDataFormat::Csv;
        Delimiter = ",";
    }
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

protected:
    TString HeaderRow;
    TString Delimiter;
    std::optional<TString> NullValue;
    ui32 SkipRows = 0;
    bool Header = false;
    bool NewlineDelimited = true;
};

class TCommandImportFromTsv : public TCommandImportFromCsv {
public:
    TCommandImportFromTsv()
        : TCommandImportFromCsv("tsv", "Import data from TSV file")
    {
        InputFormat = EDataFormat::Tsv;
        Delimiter = "\t";
    }
};

class TCommandImportFromJson : public TCommandImportFileBase {
public:
    TCommandImportFromJson()
       : TCommandImportFileBase("json", "Import data from JSON file")
    {
        InputFormat = EDataFormat::JsonUnicode;
    }
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;
};

class TCommandImportFromParquet : public TCommandImportFileBase {
public:
    TCommandImportFromParquet(const TString& cmd = "parquet", const TString& cmdDescription = "Import data from Parquet file")
        : TCommandImportFileBase(cmd, cmdDescription)
        {
            InputFormat = EDataFormat::Parquet;
        }
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

}
