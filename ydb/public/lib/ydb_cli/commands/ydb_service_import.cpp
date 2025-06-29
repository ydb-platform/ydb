#include "ydb_service_import.h"

#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/library/backup/util.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/stream/format.h> // for SF_BYTES

#if defined(_win32_)
#include <io.h>
#elif defined(_unix_)
#include <unistd.h>
#endif

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

    auto colors = NColorizer::AutoColors(Cout);
    config.Opts->AddLongOption("scheme", TStringBuilder()
            << "S3 endpoint scheme - "
            << colors.BoldColor() << "http" << colors.OldColor()
            << " or "
            << colors.BoldColor() << "https" << colors.OldColor())
        .RequiredArgument("SCHEME").StoreResult(&AwsScheme).DefaultValue(AwsScheme);

    config.Opts->AddLongOption("bucket", "S3 bucket")
        .Required().RequiredArgument("BUCKET").StoreResult(&AwsBucket);

    config.Opts->AddLongOption("access-key", "Access key id")
        .Env("AWS_ACCESS_KEY_ID", false)
        .ManualDefaultValueDescription(TStringBuilder() << colors.BoldColor() << "aws_access_key_id" << colors.OldColor() << " key in \"" << AwsCredentialsFile << "\" file")
        .RequiredArgument("STRING");

    config.Opts->AddLongOption("secret-key", "Secret key")
        .Env("AWS_SECRET_ACCESS_KEY", false)
        .ManualDefaultValueDescription(TStringBuilder() << colors.BoldColor() << "aws_secret_access_key" << colors.OldColor() << " key in \"" << AwsCredentialsFile << "\" file")
        .RequiredArgument("STRING");

    config.Opts->AddLongOption("aws-profile", TStringBuilder() << "Named profile in \"" << AwsCredentialsFile << "\" file")
        .RequiredArgument("STRING")
        .Env("AWS_PROFILE", false)
        .DefaultValue(AwsDefaultProfileName);

    TStringBuilder itemHelp;
    itemHelp << "Item specification" << Endl
        << "  Possible property names:" << Endl
        << TItem::FormatHelp(2);
    config.Opts->AddLongOption("item", itemHelp)
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("description", "Textual description of import operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    config.Opts->AddLongOption("use-virtual-addressing", TStringBuilder()
            << "Sets bucket URL style. Value "
            << colors.BoldColor() << "true" << colors.OldColor()
            << " means use Virtual-Hosted-Style URL, "
            << colors.BoldColor() << "false" << colors.OldColor()
            << " - Path-Style URL.")
        .RequiredArgument("BOOL").StoreResult<bool>(&UseVirtualAddressing).DefaultValue("true");

    config.Opts->AddLongOption("no-acl", "Prevent importing of ACL and owner")
        .RequiredArgument("BOOL").StoreTrue(&NoACL).DefaultValue("false");

    config.Opts->AddLongOption("skip-checksum-validation", "Skip checksum validation during import")
        .RequiredArgument("BOOL").StoreTrue(&SkipChecksumValidation).DefaultValue("false");

    AddDeprecatedJsonOption(config);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandImportFromS3::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();

    ParseAwsProfile(config, "aws-profile");
    ParseAwsAccessKey(config, "access-key");
    ParseAwsSecretKey(config, "secret-key");

    Items = TItem::Parse(config, "item");
}

void TCommandImportFromS3::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    for (auto& item : Items) {
        NConsoleClient::AdjustPath(item.Destination, config);
    }
}

bool IsSupportedObject(TStringBuf& key) {
    return key.ChopSuffix(NDump::NFiles::TableScheme().FileName)
        || key.ChopSuffix(NDump::NFiles::CreateView().FileName);
}

int TCommandImportFromS3::Run(TConfig& config) {
    using namespace NImport;

    TImportFromS3Settings settings = FillSettings(TImportFromS3Settings());

    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);
    settings.UseVirtualAddressing(UseVirtualAddressing);

    if (Description) {
        settings.Description(Description);
    }

    settings.NumberOfRetries(NumberOfRetries);
    settings.NoACL(NoACL);
    settings.SkipChecksumValidation(SkipChecksumValidation);
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
                    if (IsSupportedObject(key)) {
                        key.ChopSuffix("/");
                        TString destination;
                        if (const auto suffix = key.substr(item.Source.size())) {
                            destination = item.Destination + suffix;
                        } else {
                            destination = NormalizePath(item.Destination);
                        }
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

    config.Opts->GetOpts().SetTrailingArgTitle("<input files...>",
            "One or more file paths to import from");
    config.Opts->AddLongOption("timeout", "Operation timeout. Operation should be executed on server within this timeout. "
            "There could also be a delay up to 200ms to receive timeout error from server")
        .RequiredArgument("VAL").StoreResult(&OperationTimeout).DefaultValue(TDuration::Seconds(5 * 60));

    config.Opts->AddLongOption('p', "path", "Database path to table")
        .Required().RequiredArgument("STRING").StoreResult(&Path);
    config.Opts->AddLongOption('i', "input-file").AppendTo(&FilePaths).Hidden();

    const TImportFileSettings defaults;
    config.Opts->AddLongOption("batch-bytes",
            "Use portions of this size in bytes to parse and upload file data."
            " Remember that in worse case memory consumption will be: \n"
            "    batch-bytes * (threads * 2 + max-in-flight)")
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

void TCommandImportFileBase::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    AdjustPath(config);
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
    TStringStream description;
    description << "Format that data will be serialized to before sending to YDB. Available options: ";
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    description << "\n  " << colors.BoldColor() << "tvalue" << colors.OldColor()
        << "\n    " << "A default YDB protobuf format";
    description << "\n  " << colors.BoldColor() << "arrow" << colors.OldColor()
        << "\n    " << "Apache Arrow format";
    description << "\nDefault: " << colors.CyanColor() << "\"" << "tvalue" << "\"" << colors.OldColor() << ".";
    config.Opts->AddLongOption("send-format", description.Str())
        .RequiredArgument("STRING").StoreResult(&SendFormat)
        .Hidden();
    if (InputFormat == EDataFormat::Csv) {
        config.Opts->AddLongOption("delimiter", "Field delimiter in rows")
            .RequiredArgument("STRING").StoreResult(&Delimiter).DefaultValue(Delimiter);
    }
    config.Opts->AddLongOption("null-value", "Value that would be interpreted as NULL, no NULL value by default")
            .RequiredArgument("STRING").StoreResult(&NullValue);
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
    settings.NullValue(NullValue);
    settings.Verbose(config.IsVerbose());
    settings.SendFormat(SendFormat);

    if (Delimiter.size() != 1) {
        throw TMisuseException()
            << "--delimiter should be a one symbol string. Got: '" << Delimiter << "'";
    } else {
        settings.Delimiter(Delimiter);
    }

    TImportFileClient client(CreateDriver(config), config, settings);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Import(FilePaths, Path));

    return EXIT_SUCCESS;
}


/// Import JSON

void TCommandImportFromJson::Config(TConfig& config) {
    TCommandImportFileBase::Config(config);

    AddLegacyJsonInputFormats(config);
}

void TCommandImportFromJson::Parse(TConfig& config) {
    TCommandImportFileBase::Parse(config);

    ParseInputFormats();
}

int TCommandImportFromJson::Run(TConfig& config) {
    TImportFileSettings settings;
    settings.OperationTimeout(OperationTimeout);
    settings.Format(EDataFormat::Json);
    settings.BinaryStringsEncoding(InputBinaryStringEncoding);
    settings.MaxInFlightRequests(MaxInFlightRequests);
    settings.BytesPerRequest(NYdb::SizeFromString(BytesPerRequest));
    settings.Threads(Threads);

    TImportFileClient client(CreateDriver(config), config, settings);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Import(FilePaths, Path));

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

    TImportFileClient client(CreateDriver(config), config, settings);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(client.Import(FilePaths, Path));

    return EXIT_SUCCESS;
}

}
