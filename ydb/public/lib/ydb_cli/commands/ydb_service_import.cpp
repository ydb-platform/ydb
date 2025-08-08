#include "ydb_service_import.h"

#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/library/backup/util.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/stream/format.h> // for SF_BYTES
#include <util/string/hex.h>

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
    : TYdbOperationCommand("s3", {}, "Create import from S3.\nFor more info go to: ydb.tech/docs/en/reference/ydb-cli/export-import/import-s3")
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

    config.Opts->AddLongOption("access-key", "AWS access key id")
        .Env("AWS_ACCESS_KEY_ID", false)
        .ManualDefaultValueDescription(TStringBuilder() << colors.Cyan() << "aws_access_key_id" << colors.OldColor() << " key in AWS credentials file \"" << AwsCredentialsFile << "\"")
        .RequiredArgument("STRING");

    config.Opts->AddLongOption("secret-key", "AWS secret key")
        .Env("AWS_SECRET_ACCESS_KEY", false)
        .ManualDefaultValueDescription(TStringBuilder() << colors.Cyan() << "aws_secret_access_key" << colors.OldColor() << " key in AWS credentials file \"" << AwsCredentialsFile << "\"")
        .RequiredArgument("STRING");

    config.Opts->AddLongOption("aws-profile", TStringBuilder() << "Named profile in AWS credentials file \"" << AwsCredentialsFile << "\"")
        .RequiredArgument("STRING")
        .Env("AWS_PROFILE", false)
        .DefaultValue(AwsDefaultProfileName);

    config.Opts->AddLongOption("source-prefix", "Source prefix for export in the bucket")
        .RequiredArgument("PREFIX").StoreResult(&CommonSourcePrefix);

    config.Opts->AddLongOption("destination-path", "Destination folder in database for the objects being imported")
        .RequiredArgument("PATH").StoreResult(&CommonDestinationPath);

    config.Opts->AddLongOption("include", "Schema objects to be included in the import. Directories are traversed recursively. The option can be used multiple times")
        .RequiredArgument("PATH").AppendTo(&IncludePaths);

    config.Opts->AddLongOption("item", TItem::FormatHelp("Item specification", config.HelpCommandVerbosiltyLevel, 2))
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

    config.Opts->AddLongOption("encryption-key-file", "File path that contains encryption key or env that contains hex encoded key value")
        .Env("YDB_ENCRYPTION_KEY_FILE", true, "encryption key file")
        .Env("YDB_ENCRYPTION_KEY", false)
        .FileName("encryption key file").RequiredArgument("PATH")
        .StoreFilePath(&EncryptionKeyFile)
        .StoreResult(&EncryptionKey);

    config.Opts->AddLongOption('l', "list", "List objects in an existing export")
        .RequiredArgument("BOOL").StoreTrue(&ListObjectsInExistingExport).DefaultValue("false");

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
    if (Items.empty() && !CommonSourcePrefix) {
        throw TMisuseException() << "No source prefix was provided";
    }

    if (!Items.empty() && !IncludePaths.empty()) {
        throw TMisuseException() << "Both --item and --include parameters are not supported together";
    }

    if (!Items.empty() && ListObjectsInExistingExport) {
        throw TMisuseException() << "Cannot use --item parameter with --list";
    }

    if (!Items.empty() && (!EncryptionKey.empty() || !EncryptionKeyFile.empty())) {
        throw TMisuseException() << "Cannot use --item parameter with encrypted exports";
    }
}

void TCommandImportFromS3::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    for (auto& item : Items) {
        if (item.Destination) {
            if (CommonDestinationPath && item.Destination[0] != '/') {
                item.Destination = CommonDestinationPath + "/" + item.Destination;
            }
            NConsoleClient::AdjustPath(item.Destination, config);
        }
    }
}

bool IsSupportedObject(TStringBuf& key) {
    return key.ChopSuffix(NDump::NFiles::TableScheme().FileName)
        || key.ChopSuffix(NDump::NFiles::CreateView().FileName);
}

void TCommandImportFromS3::FillItems(NYdb::NImport::TImportFromS3Settings& settings) const {
    if (!Items.empty()) {
        FillItemsFromItemParam(settings);
    } else {
        FillItemsFromIncludeParam(settings);
    }
}

void TCommandImportFromS3::FillItemsFromItemParam(NYdb::NImport::TImportFromS3Settings& settings) const {
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
}

void TCommandImportFromS3::FillItemsFromIncludeParam(NYdb::NImport::TImportFromS3Settings& settings) const {
    for (const TString& path : IncludePaths) {
        settings.AppendItem({.Src = {}, .Dst = {}, .SrcPath = path});
    }
}

template <class TSettings>
TSettings TCommandImportFromS3::MakeSettings() {
    TSettings settings = FillSettings<TSettings>(TSettings());

    const bool encryption = !EncryptionKey.empty();
    constexpr bool isListRequest = std::is_same_v<TSettings, NImport::TListObjectsInS3ExportSettings>;

    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);
    settings.UseVirtualAddressing(UseVirtualAddressing);

    if (encryption) {
        settings.SymmetricKey(EncryptionKey);
    }

    if constexpr (isListRequest) {
        if (CommonSourcePrefix) {
            settings.Prefix(CommonSourcePrefix);
        }

        for (const TString& path : IncludePaths) {
            settings.AppendItem({.Path = path});
        }
    } else {
        if (CommonSourcePrefix) {
            settings.SourcePrefix(CommonSourcePrefix);
        }

        if (CommonDestinationPath) {
            settings.DestinationPath(CommonDestinationPath);
        }

        if (Description) {
            settings.Description(Description);
        }

        settings.NumberOfRetries(NumberOfRetries);
        settings.NoACL(NoACL);
        settings.SkipChecksumValidation(SkipChecksumValidation);

        FillItems(settings);
    }

    return settings;
}

static int PrintListObjectResultPretty(const NImport::TListObjectsInS3ExportResult& result) {
    TVector<TString> tableColums {"Path", "Prefix"};
    TPrettyTable table(tableColums);
    for (const NImport::TListObjectsInS3ExportResult::TItem& item : result.GetItems()) {
        auto& row = table.AddRow();
        row.Column(0, item.Path);
        row.Column(1, item.Prefix);
    }
    table.Print(Cout);
    return EXIT_SUCCESS;
}

static int PrintListObjectResultProtoJsonBase64(const NImport::TListObjectsInS3ExportResult& result) {
    return PrintProtoJsonBase64(TProtoAccessor::GetProto(result));
}

static int PrintListObjectResult(const NImport::TListObjectsInS3ExportResult& result, EDataFormat format) {
    switch (format) {
        case EDataFormat::Default:
        case EDataFormat::Pretty:
            return PrintListObjectResultPretty(result);
        case EDataFormat::ProtoJsonBase64:
            return PrintListObjectResultProtoJsonBase64(result);
        default:
            throw std::runtime_error(TStringBuilder() << "Unsupported output format: " << format);
    }
}

int TCommandImportFromS3::Run(TConfig& config) {
    if (EncryptionKey && !EncryptionKeyFile) { // We read key from env YDB_ENCRYPTION_KEY, treat as hex encoded
        try {
            EncryptionKey = HexDecode(EncryptionKey);
        } catch (const std::exception&) {
            // Don't print error, it may contain secret.
            Cerr << "Failed to decode encryption key from hex" << Endl;
            return EXIT_FAILURE;
        }
    }

    const bool encryption = !EncryptionKey.empty();
    if (encryption && !CommonSourcePrefix) {
        Cerr << "--source-prefix parameter is required" << Endl;
        return EXIT_FAILURE;
    }

    using namespace NImport;
    TImportClient client(CreateDriver(config));

    int returnCode = EXIT_SUCCESS;
    if (ListObjectsInExistingExport) {
        TListObjectsInS3ExportSettings settings = MakeSettings<TListObjectsInS3ExportSettings>();
        TListObjectsInS3ExportResult result = client.ListObjectsInS3Export(std::move(settings)).GetValueSync();
        NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
        returnCode = PrintListObjectResult(result, OutputFormat);
    } else {
        TImportFromS3Settings settings = MakeSettings<TImportFromS3Settings>();
        TImportFromS3Response response = client.ImportFromS3(std::move(settings)).GetValueSync();
        ThrowOnError(response);
        PrintOperation(response, OutputFormat);
    }

    return returnCode;
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
