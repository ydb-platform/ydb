#include "ydb_service_import.h"

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/import/import.h>
#include <ydb/library/backup/util.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/stream/format.h> // for SF_BYTES

namespace NYdb {
namespace NConsoleClient {

TCommandImport::TCommandImport()
    : TClientCommandTree("import", {}, "Import service operations")
{
    AddCommand(std::make_unique<TCommandImportFromS3>());
    AddCommand(std::make_unique<TCommandImportFromFile>());
}

/// S3
TCommandImportFromS3::TCommandImportFromS3()
    : TYdbOperationCommand("s3", {}, "Create import from S3")
{
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "S3 object key prefix", true}},
        {"Destination", {{"destination", "dst", "d"}, "Database path to a table to import to", true}},
    });
}

void TCommandImportFromS3::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.Opts->AddLongOption("s3-endpoint", "S3 endpoint to connect to")
        .Required().RequiredArgument("ENDPOINT").StoreResult(&AwsEndpoint);

    config.Opts->AddLongOption("scheme", "S3 endpoint scheme")
        .RequiredArgument("SCHEME").StoreResult(&AwsScheme).DefaultValue(AwsScheme);

    config.Opts->AddLongOption("bucket", "S3 bucket")
        .Required().RequiredArgument("BUCKET").StoreResult(&AwsBucket);

    TStringBuilder accessKeyHelp;
    accessKeyHelp << "Access key id" << Endl
        << "  Search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"AWS_ACCESS_KEY_ID\" environment variable" << Endl
        << "    3. \"aws_access_key_id\" key in \"" << AwsCredentialsFile << "\" file";
    config.Opts->AddLongOption("access-key", accessKeyHelp)
        .RequiredArgument("STRING");

    TStringBuilder secretKeyHelp;
    secretKeyHelp << "Secret key" << Endl
        << "  Search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"AWS_SECRET_ACCESS_KEY\" environment variable" << Endl
        << "    3. \"aws_secret_access_key\" key in \"" << AwsCredentialsFile << "\" file";
    config.Opts->AddLongOption("secret-key", secretKeyHelp)
        .RequiredArgument("STRING");

    TStringBuilder itemHelp;
    itemHelp << "[At least one] Item specification" << Endl
        << "  Possible property names:" << Endl
        << TItem::FormatHelp(2);
    config.Opts->AddLongOption("item", itemHelp)
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("description", "Textual description of import operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    AddDeprecatedJsonOption(config);
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandImportFromS3::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();

    ParseAwsAccessKey(config, "access-key");
    ParseAwsSecretKey(config, "secret-key");

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }

    for (auto& item : Items) {
        NConsoleClient::AdjustPath(item.Destination, config);
    }
}

int TCommandImportFromS3::Run(TConfig& config) {
    using namespace NImport;

    TImportFromS3Settings settings = FillSettings(TImportFromS3Settings());

    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);

    for (const auto& item : Items) {
        settings.AppendItem({item.Source, item.Destination});
    }

    if (Description) {
        settings.Description(Description);
    }

    settings.NumberOfRetries(NumberOfRetries);

    TImportClient client(CreateDriver(config));
    TImportFromS3Response response = client.ImportFromS3(std::move(settings)).GetValueSync();
    ThrowOnError(response);
    PrintOperation(response, OutputFormat);

    return EXIT_SUCCESS;
}

/// File

TCommandImportFromFile::TCommandImportFromFile()
    : TClientCommandTree("file", {}, "Import data from file")
{
    AddCommand(std::make_unique<TCommandImportFromCsv>());
    AddCommand(std::make_unique<TCommandImportFromTsv>());
}

/// CSV

TCommandImportFromCsv::TCommandImportFromCsv(const TString& cmd, const TString& cmdDescription)
    : TYdbCommand(cmd, {}, cmdDescription)
{}

void TCommandImportFromCsv::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('p', "path", "Database path to table")
        .Required().RequiredArgument("STRING").StoreResult(&Path);
    config.Opts->AddLongOption("input-file", "Path to file to import in a local filesystem")
        .Required().RequiredArgument("STRING").StoreResult(&FilePath);
    config.Opts->AddLongOption("skip-rows",
            "Number of header rows to skip (not including the row of column names, if any)")
        .RequiredArgument("NUM").StoreResult(&SkipRows).DefaultValue(SkipRows);
    config.Opts->AddLongOption("header",
            "Set if file contains column names at the first not skipped row")
        .StoreTrue(&Header);
    if (InputFormat == EOutputFormat::Csv) {
        config.Opts->AddLongOption("delimiter", "Field delimiter in rows")
            .RequiredArgument("STRING").StoreResult(&Delimiter).DefaultValue(Delimiter);
    }
    config.Opts->AddLongOption("null-value", "Value that would be interpreted as NULL")
            .RequiredArgument("STRING").StoreResult(&NullValue).DefaultValue(NullValue);
    // TODO: quoting/quote_char

    TImportFileSettings defaults;

    config.Opts->AddLongOption("batch-bytes",
            "Use portions of this size in bytes to parse and upload file data")
        .DefaultValue(HumanReadableSize(defaults.BytesPerRequest_, SF_BYTES)).StoreResult(&BytesPerRequest);

    config.Opts->AddLongOption("max-in-flight",
            "Maximum number of in-flight requests; increase to load big files faster (more memory needed)")
        .DefaultValue(defaults.MaxInFlightRequests_).StoreResult(&MaxInFlightRequests);
}

void TCommandImportFromCsv::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    AdjustPath(config);
}

int TCommandImportFromCsv::Run(TConfig& config) {
    TImportFileSettings settings;
    settings.SkipRows(SkipRows);
    settings.Header(Header);
    settings.NullValue(NullValue);

    if (auto bytesPerRequest = NYdb::SizeFromString(BytesPerRequest)) {
        if (bytesPerRequest > TImportFileSettings::MaxBytesPerRequest) {
            throw TMisuseException()
                << "--batch-bytes cannot be larger than "
                << HumanReadableSize(TImportFileSettings::MaxBytesPerRequest, SF_BYTES);
        }

        settings.BytesPerRequest(bytesPerRequest);
    }

    if (MaxInFlightRequests == 0) {
        MaxInFlightRequests = 1;
    }
    settings.MaxInFlightRequests(MaxInFlightRequests);

    if (Delimiter.size() != 1) {
        throw TMisuseException()
            << "--delimiter should be a one symbol string. Got: '" << Delimiter << "'";
    } else {
        settings.Delimiter(Delimiter);
    }

    TImportFileClient client(CreateDriver(config));
    ThrowOnError(client.Import(FilePath, Path, settings));

    return EXIT_SUCCESS;
}

}
}
