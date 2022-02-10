#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/ydb_cli/common/aws.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parseable_struct.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandImport : public TClientCommandTree {
public:
    TCommandImport();
};

class TCommandImportFromS3 : public TYdbOperationCommand,
                           public TCommandWithAwsCredentials,
                           public TCommandWithFormat {
public:
    TCommandImportFromS3();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

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
};

class TCommandImportFromFile : public TClientCommandTree {
public:
    TCommandImportFromFile();
};

class TCommandImportFromCsv : public TYdbCommand,
                            public TCommandWithPath {
public:
    TCommandImportFromCsv(const TString& cmd = "csv", const TString& cmdDescription = "Import data from CSV file");
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

protected:
    EOutputFormat InputFormat = EOutputFormat::Csv;
    TString FilePath;
    TString Delimiter = ",";
    TString NullValue;
    ui32 SkipRows = 0;
    bool Header = false;
    TString BytesPerRequest;
    ui64 MaxInFlightRequests = 1; 
};

class TCommandImportFromTsv : public TCommandImportFromCsv {
public:
    TCommandImportFromTsv()
        : TCommandImportFromCsv("tsv", "Import data from TSV file")
    {
        InputFormat = EOutputFormat::Tsv;
        Delimiter = "\t";
    }
};

}
}
