#include "ydb_service_import.h"

#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/import/import.h>
#include <ydb/library/backup/util.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/stream/format.h> // for SF_BYTES

#if defined(_win32_)
#include <io.h>
#elif defined(_unix_)
#include <unistd.h>
#endif

namespace NYdb::NDump {
    extern const char SCHEME_FILE_NAME[];
}

namespace NYdb::NConsoleClient {

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

    TStringBuilder profileHelp;
    profileHelp << "Named profile in \"" << AwsCredentialsFile << "\" file" << Endl
        << "  Search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"AWS_PROFILE\" environment variable";
    config.Opts->AddLongOption("aws-profile", profileHelp)
        .RequiredArgument("STRING").DefaultValue(AwsDefaultProfileName);

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

    config.Opts->AddLongOption("use-virtual-addressing", "S3 bucket virtual addressing")
        .RequiredArgument("BOOL").StoreResult<bool>(&UseVirtualAddressing).DefaultValue("true");

    AddDeprecatedJsonOption(config);
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandImportFromS3::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();

    ParseAwsProfile(config, "aws-profile");
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

    if (Description) {
        settings.Description(Description);
    }

    settings.NumberOfRetries(NumberOfRetries);
#if defined(_win32_)
    for (const auto& item : Items) {
        settings.AppendItem({item.Source, item.Destination});
    }
#else
    InitAwsAPI();
    try {
        auto s3Client = CreateS3ClientWrapper(settings);
        for (auto item : Items) {
            std::optional<TString> token;
            if (!item.Source.empty() && item.Source.back() != '/') {
                item.Source += "/";
            }
            if (!item.Destination.empty() && item.Destination.back() == '.') {
                item.Destination.pop_back();
            }
            if (item.Destination.empty() || item.Destination.back() != '/') {
                item.Destination += "/";
            }
            do {
                auto listResult = s3Client->ListObjectKeys(item.Source, token);
                token = listResult.NextToken;
                for (TStringBuf key : listResult.Keys) {
                    if (key.ChopSuffix(NDump::SCHEME_FILE_NAME)) {
                        TString destination = item.Destination + key.substr(item.Source.Size());
                        settings.AppendItem({TString(key), std::move(destination)});
                    }
                }
            } while (token);
        }
    } catch (...) {
        ShutdownAwsAPI();
        throw;
    }
    ShutdownAwsAPI();
#endif

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
    AddCommand(std::make_unique<TCommandImportFromJson>());
    AddCommand(std::make_unique<TCommandImportFromParquet>());
}

/// Import File Shared Config

void TCommandImportFileBase::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->SetTrailingArgTitle("<input files...>",
            "One or more file paths to import from");
    config.Opts->AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server")
        .RequiredArgument("VAL").StoreResult(&OperationTimeout).DefaultValue(TDuration::Seconds(5 * 60));

    config.Opts->AddLongOption('p', "path", "Database path to table")
        .Required().RequiredArgument("STRING").StoreResult(&Path);
    config.Opts->AddLongOption('i', "input-file").AppendTo(&FilePaths).Hidden();

    const TImportFileSettings defaults;
    config.Opts->AddLongOption("batch-bytes",
            "Use portions of this size in bytes to parse and upload file data")
        .DefaultValue(HumanReadableSize(defaults.BytesPerRequest_, SF_BYTES)).StoreResult(&BytesPerRequest);
    config.Opts->AddLongOption("max-in-flight",
        "Maximum number of in-flight requests; increase to load big files faster (more memory needed)")
        .DefaultValue(defaults.MaxInFlightRequests_).StoreResult(&MaxInFlightRequests);
    config.Opts->AddLongOption("threads",
        "Maximum number of threads; number of available processors if not specified")
        .DefaultValue(defaults.Threads_).StoreResult(&Threads);
}

void TCommandImportFileBase::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    AdjustPath(config);

    if (auto bytesPerRequest = NYdb::SizeFromString(BytesPerRequest)) {
        if (bytesPerRequest > TImportFileSettings::MaxBytesPerRequest) {
            throw TMisuseException()
                << "--batch-bytes cannot be larger than "
                << HumanReadableSize(TImportFileSettings::MaxBytesPerRequest, SF_BYTES);
        }
    }

    if (MaxInFlightRequests == 0) {
        throw TMisuseException()
            << "--max-in-flight must be greater than zero";
    }

    for (const auto& filePath : config.ParseResult->GetFreeArgs()) {
        FilePaths.push_back(filePath);
    }
    for (const auto& filePath : FilePaths) {
        if (filePath.empty()) {
            throw TMisuseException() << "File path is not allowed to be empty";
        }
    }
    // If no filenames or stdin isn't connected to tty, read from stdin.
    if (FilePaths.empty() || !IsStdinInteractive()) {
        FilePaths.push_back("");
    }
}

/// Import CSV

void TCommandImportFromCsv::Config(TConfig& config) {
    TCommandImportFileBase::Config(config);

    config.Opts->AddLongOption("skip-rows",
            "Number of header rows to skip (not including the row of column names, if any)")
        .RequiredArgument("NUM").StoreResult(&SkipRows).DefaultValue(SkipRows);
    config.Opts->AddLongOption("header",
            "Set if file contains column names at the first not skipped row")
        .StoreTrue(&Header);
    config.Opts->AddLongOption("columns",
            "String with column names that replaces header")
        .RequiredArgument("STR").StoreResult(&HeaderRow);
    config.Opts->AddLongOption("newline-delimited",
            "No newline characters inside records, enables some import optimizations (see docs)")
        .StoreTrue(&NewlineDelimited);
    if (InputFormat == EOutputFormat::Csv) {
        config.Opts->AddLongOption("delimiter", "Field delimiter in rows")
            .RequiredArgument("STRING").StoreResult(&Delimiter).DefaultValue(Delimiter);
    }
    config.Opts->AddLongOption("null-value", "Value that would be interpreted as NULL")
            .RequiredArgument("STRING").StoreResult(&NullValue).DefaultValue(NullValue);
    // TODO: quoting/quote_char
}

int TCommandImportFromCsv::Run(TConfig& config) {
    TImportFileSettings settings;
    settings.OperationTimeout(OperationTimeout);
    settings.ClientTimeout(OperationTimeout + TDuration::MilliSeconds(200));
    settings.Format(InputFormat);
    settings.MaxInFlightRequests(MaxInFlightRequests);
    settings.BytesPerRequest(NYdb::SizeFromString(BytesPerRequest));
    settings.Threads(Threads);
    settings.SkipRows(SkipRows);
    settings.Header(Header);
    settings.NewlineDelimited(NewlineDelimited);
    settings.HeaderRow(HeaderRow);
    if (config.ParseResult->Has("null-value")) {
        settings.NullValue(NullValue);
    }

    if (Delimiter.size() != 1) {
        throw TMisuseException()
            << "--delimiter should be a one symbol string. Got: '" << Delimiter << "'";
    } else {
        settings.Delimiter(Delimiter);
    }

    TImportFileClient client(CreateDriver(config), config);
    ThrowOnError(client.Import(FilePaths, Path, settings));

    return EXIT_SUCCESS;
}


/// Import JSON

void TCommandImportFromJson::Config(TConfig& config) {
    TCommandImportFileBase::Config(config);

    AddInputFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64
    });
}

void TCommandImportFromJson::Parse(TConfig& config) {
    TCommandImportFileBase::Parse(config);

    ParseFormats();
}

int TCommandImportFromJson::Run(TConfig& config) {
    TImportFileSettings settings;
    settings.OperationTimeout(OperationTimeout);
    settings.Format(InputFormat);
    settings.MaxInFlightRequests(MaxInFlightRequests);
    settings.BytesPerRequest(NYdb::SizeFromString(BytesPerRequest));
    settings.Threads(Threads);

    TImportFileClient client(CreateDriver(config), config);
    ThrowOnError(client.Import(FilePaths, Path, settings));

    return EXIT_SUCCESS;
}

/// Import Parquet
void TCommandImportFromParquet::Config(TConfig& config) {
    TCommandImportFileBase::Config(config);
}

int TCommandImportFromParquet::Run(TConfig& config) {
    TImportFileSettings settings;
    settings.OperationTimeout(OperationTimeout);
    settings.Format(InputFormat);
    settings.MaxInFlightRequests(MaxInFlightRequests);
    settings.BytesPerRequest(NYdb::SizeFromString(BytesPerRequest));
    settings.Threads(Threads);

    TImportFileClient client(CreateDriver(config), config);
    ThrowOnError(client.Import(FilePaths, Path, settings));

    return EXIT_SUCCESS;
}

}
