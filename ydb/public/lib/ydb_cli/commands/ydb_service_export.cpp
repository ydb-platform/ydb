#include "ydb_service_export.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/lib/ydb_cli/common/exclude_item.h>
#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <util/generic/is_in.h>
#include <util/generic/serialized_enum.h>
#include <library/cpp/getopt/small/completer.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

namespace NYdb {
namespace NConsoleClient {

static const TString AppendPrefix = "<append=true>";

namespace {

    const char slashC = '/';
    const TStringBuf slash(&slashC, 1);

    using TFilterOp = TRecursiveListSettings::TFilterOp;

    bool FilterTables(const NScheme::TSchemeEntry& entry) {
        return entry.Type == NScheme::ESchemeEntryType::Table;
    }

    bool FilterAllSupportedSchemeObjects(const NScheme::TSchemeEntry& entry) {
        return IsIn({
            NScheme::ESchemeEntryType::Table,
            NScheme::ESchemeEntryType::ColumnTable,
            NScheme::ESchemeEntryType::View,
            NScheme::ESchemeEntryType::Topic,
        }, entry.Type);
    }

    TStatus FilterAsyncReplicaTables(NTable::TSession& session, TVector<NScheme::TSchemeEntry>& entries) {
        auto isAsyncReplicaTable = [&session](const NScheme::TSchemeEntry& entry) {
            if (entry.Type != NScheme::ESchemeEntryType::Table) {
                return false;
            }
            auto describeResult = session.DescribeTable(entry.Name).ExtractValueSync();
            NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);

            const auto& attributes = describeResult.GetTableDescription().GetAttributes();
            auto it = attributes.find("__async_replica");
            return it != attributes.end() && it->second == "true";
        };

        try {
            std::erase_if(entries, isAsyncReplicaTable);
        } catch (NStatusHelpers::TYdbErrorException& e) {
            return e.ExtractStatus();
        }
        return TStatus(EStatus::SUCCESS, {});
    }

    TVector<std::pair<TString, TString>> ExpandItem(
        NScheme::TSchemeClient& schemeClient,
        NTable::TTableClient tableClient,
        TStringBuf srcPath,
        TStringBuf dstPath,
        const TFilterOp& filter)
    {
        // cut trailing slash
        srcPath.ChopSuffix(slash);
        dstPath.ChopSuffix(slash);

        auto ret = RecursiveList(schemeClient, TString{srcPath}, TRecursiveListSettings().Filter(filter));
        NStatusHelpers::ThrowOnErrorOrPrintIssues(ret.Status);

        tableClient.RetryOperationSync([&ret](NTable::TSession session) {
            return FilterAsyncReplicaTables(session, ret.Entries);
        });

        if (ret.Entries.size() == 1 && srcPath == ret.Entries[0].Name) {
            return {{TString{srcPath}, TString{dstPath}}};
        }

        TVector<std::pair<TString, TString>> result;
        for (const auto& table : ret.Entries) {
            TStringBuilder dstPathBuilder;
            if (dstPath) { // It is not recommended to use this path for encrypted exports, because it shows real database structure in S3
                dstPathBuilder << dstPath << TStringBuf(table.Name).RNextTok(srcPath);
            }
            result.emplace_back(table.Name, dstPathBuilder);
        }

        return result;
    }

    template <typename TSettings>
    void ExpandItems(
        NScheme::TSchemeClient& schemeClient,
        NTable::TTableClient tableClient,
        TSettings& settings,
        const TVector<TRegExMatch>& exclusionPatterns,
        const TFilterOp& filter = FilterTables)
    {
        auto items(std::move(settings.Item_));
        for (const auto& item : items) {
            for (const auto& [src, dst] : ExpandItem(schemeClient, tableClient, item.Src, item.Dst, filter)) {
                settings.AppendItem({src, dst});
            }
        }

        ExcludeItems(settings, exclusionPatterns);
    }

} // anonymous namespace

TCommandExport::TCommandExport(bool useExportToYt)
    : TClientCommandTree("export", {}, "Export service operations")
{
    if (useExportToYt) {
        AddCommand(std::make_unique<TCommandExportToYt>());
    }
    AddCommand(std::make_unique<TCommandExportToS3>());
    AddCommand(std::make_unique<TCommandExportToNfs>());
}

/// YT
TCommandExportToYt::TCommandExportToYt()
    : TYdbOperationCommand("yt", {}, "Create export to YT")
{
    NColorizer::TColors colors = NConsoleClient::AutoColors(Cout);
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "Database path to a directory or a table to be exported", true}},
        {"Destination", {{"destination", "dst", "d"}, "Path to a table or a directory in YT", true}},
        {"Append", {{"append", "a"}, TStringBuilder() << "Append rows to existent YT table (default: "
            << colors.CyanColor() << "true" << colors.OldColor() << ")", false}}
    });
}

void TCommandExportToYt::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    TStringBuilder proxyHelp;
    proxyHelp << "YT proxy to connect to" << Endl
        << "  Search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YT_PROXY\" environment variable";
    config.Opts->AddLongOption("proxy", proxyHelp)
        .RequiredArgument("PROXY");

    TStringBuilder tokenHelp;
    tokenHelp << "OAuth token" << Endl
        << "  Search order:" << Endl
        << "    1. This option" << Endl
        << "    2. \"YT_TOKEN\" environment variable" << Endl
        << "    3. \"" << YtTokenFile << "\" file";
    config.Opts->AddLongOption("token", tokenHelp)
        .RequiredArgument("TOKEN");

    config.Opts->AddLongOption("item", TItem::FormatHelp("[At least one] Item specification", config.HelpCommandVerbosityLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("exclude", "Pattern (PCRE) for paths excluded from export operation")
        .RequiredArgument("STRING").Handler([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });

    config.Opts->AddLongOption("description", "Textual description of export operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    config.Opts->AddLongOption("use-type-v3", "Use YT's type_v3")
        .NoArgument().StoreTrue(&UseTypeV3);

    AddDeprecatedJsonOption(config);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandExportToYt::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();

    ParseYtProxy(config, "proxy");
    ParseYtToken(config, "token");

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }
}

void TCommandExportToYt::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    for (auto& item : Items) {
        NConsoleClient::AdjustPath(item.Source, config);

        const bool hasAppendPrefix = item.Destination.StartsWith(AppendPrefix);
        if (item.Append && !hasAppendPrefix) {
            item.Destination.prepend(AppendPrefix);
        } else if (!item.Append && hasAppendPrefix) {
            Cerr << "warning: 'Append' option is false, but path has "
                 << "'" << AppendPrefix << "' prefix: " << item.Destination << Endl;
        }
    }
}

int TCommandExportToYt::Run(TConfig& config) {
    using namespace NExport;
    using namespace NScheme;
    using namespace NTable;

    TExportToYtSettings settings = FillSettings(TExportToYtSettings());

    settings.Host(YtHost);
    settings.Port(YtPort);
    settings.Token(YtToken);

    for (const auto& item : Items) {
        settings.AppendItem({item.Source, item.Destination});
    }

    if (Description) {
        settings.Description(Description);
    }

    settings.NumberOfRetries(NumberOfRetries);
    settings.UseTypeV3(UseTypeV3);

    const TDriver driver = CreateDriver(config);

    TSchemeClient schemeClient(driver);
    TTableClient tableClient(driver);
    ExpandItems(schemeClient, tableClient, settings, ExclusionPatterns);

    TExportClient client(driver);
    TExportToYtResponse response = client.ExportToYt(std::move(settings)).GetValueSync();
    ThrowOnError(response);
    PrintOperation(response, OutputFormat);

    return EXIT_SUCCESS;
}

TCommandExportBase::TCommandExportBase(const TString& name, const TString& description)
    : TYdbOperationCommand(name, {}, description)
{
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "Database path to a directory or a table to be exported", true}},
        {"Destination", {{"destination", "dst", "d"}, "Destination path", true}},
    });
}

void TCommandExportBase::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    auto colors = NConsoleClient::AutoColors(Cout);

    config.Opts->AddLongOption("root-path", "Root directory in database for the objects being exported, database root if not provided")
        .RequiredArgument("PATH").StoreResult(&CommonSourcePath);

    config.Opts->AddLongOption("include", "Schema objects to be included in the export. Directories are traversed recursively. The option can be used multiple times")
        .RequiredArgument("PATH").Handler([this](const TString& arg) {
            TItem item;
            item.Source = arg;
            Items.emplace_back(std::move(item));
        });

    config.Opts->AddLongOption("exclude", "Pattern (PCRE) for paths excluded from export operation")
        .RequiredArgument("STRING").Handler([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });

    config.Opts->AddLongOption("description", "Textual description of export operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    {
        TStringBuilder codecHelp;
        codecHelp << "Codec used to compress data. Available options: ";
        if (config.HelpCommandVerbosityLevel >= 2) {
            codecHelp << Endl
                << "    - " << colors.BoldColor() << "zstd" << colors.OldColor() << Endl
                << "    - " << colors.BoldColor() << "zstd-N" << colors.OldColor() << " (N is compression level in range [1, 22], e.g. zstd-3)" << Endl;
        } else {
            codecHelp << colors.BoldColor() << "zstd" << colors.OldColor() << ", "
                << colors.BoldColor() << "zstd-N" << colors.OldColor();
        }
        config.Opts->AddLongOption("compression", codecHelp)
            .RequiredArgument("STRING").StoreResult(&Compression)
            .Completer(NLastGetopt::NComp::Choice({{"zstd", "ZSTD default level"}}));
    }

    {
        TStringBuilder help;
        help << "Include index data or not";
        if (config.HelpCommandVerbosityLevel >= 2) {
            help << Endl << "    By default, only index metadata is uploaded and indexes are built during import — it"
                 << Endl << "    saves space and reduces export time, but it can potentially increase the import time."
                 << Endl << "    Index data can be uploaded and downloaded back during import.";
        }
        config.Opts->AddLongOption("include-index-data", help)
            .RequiredArgument("BOOL").StoreResult<bool>(&IncludeIndexData).DefaultValue("false");
    }

    {
        TStringBuilder encryptionAlgorithmHelp;
        encryptionAlgorithmHelp << "Encryption algorithm. Supported values: ";
        bool first = true;
        for (const auto& alg : {"AES-128-GCM", "AES-256-GCM", "ChaCha20-Poly1305"}) {
            if (first) {
                first = false;
            } else {
                encryptionAlgorithmHelp << ", ";
            }
            encryptionAlgorithmHelp << colors.BoldColor() << alg << colors.OldColor();
        }
        config.Opts->AddLongOption("encryption-algorithm", encryptionAlgorithmHelp)
            .RequiredArgument("NAME").StoreResult(&EncryptionAlgorithm)
            .ChoicesWithCompletion({{"AES-128-GCM"}, {"AES-256-GCM"}, {"ChaCha20-Poly1305"}});
    }

    config.Opts->AddLongOption("encryption-key-file", "File path that contains encryption key or env that contains hex encoded key value")
        .Env("YDB_ENCRYPTION_KEY_FILE", true, "encryption key file")
        .Env("YDB_ENCRYPTION_KEY", false)
        .FileName("encryption key file").RequiredArgument("PATH")
        .StoreFilePath(&EncryptionKeyFile)
        .StoreResult(&EncryptionKey);

    AddDeprecatedJsonOption(config);
    AddOutputFormats(config, { EDataFormat::Pretty, EDataFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandExportBase::ParseItems(TConfig& config, const TString& optionName) {
    auto items = TItem::Parse(config, "item");
    Items.insert(Items.end(), items.begin(), items.end());
    if (Items.empty() && !CommonDestinationPrefix) {
        throw TMisuseException() << "No " << optionName << " was provided";
    }
}

void TCommandExportBase::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseOutputFormats();
}

void TCommandExportBase::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);

    for (auto& item : Items) {
        if (CommonSourcePath && item.Source && item.Source[0] != '/') {
            item.Source = CommonSourcePath + "/" + item.Source;
        }
        NConsoleClient::AdjustPath(item.Source, config);
    }
}

template<typename TResponse>
struct TExportTraits;

template<>
struct TExportTraits<NExport::TExportToS3Response> {
    using TSettings = NExport::TExportToS3Settings;
    static auto Call(NExport::TExportClient& client, const TSettings& settings) {
        return client.ExportToS3(settings);
    }
};

template<>
struct TExportTraits<NExport::TExportToFsResponse> {
    using TSettings = NExport::TExportToFsSettings;
    static auto Call(NExport::TExportClient& client, const TSettings& settings) {
        return client.ExportToFs(settings);
    }
};

template<typename TResponse>
auto CallExport(NExport::TExportClient& client, const typename TExportTraits<TResponse>::TSettings& settings) {
    return TExportTraits<TResponse>::Call(client, settings);
}

template <typename TSettings, typename TResponse>
int TCommandExportBase::Run(TConfig& config, TSettings& settings) {
    if (EncryptionKey && !EncryptionKeyFile) { // We read key from env YDB_ENCRYPTION_KEY, treat as hex encoded
        try {
            EncryptionKey = HexDecode(EncryptionKey);
        } catch (const std::exception&) {
            // Don't print error, it may contain secret.
            Cerr << "Failed to decode encryption key from hex" << Endl;
            return EXIT_FAILURE;
        }
    }

    if (EncryptionAlgorithm && !EncryptionKey) {
        Cerr << "No encryption key provided" << Endl;
        return EXIT_FAILURE;
    }

    if (EncryptionKey && !EncryptionAlgorithm) {
        Cerr << "No encryption algorithm provided" << Endl;
        return EXIT_FAILURE;
    }

    for (const auto& item : Items) {
        settings.AppendItem({item.Source, item.Destination});
    }

    if (Description) {
        settings.Description(Description);
    }

    settings.NumberOfRetries(NumberOfRetries);

    if (Compression) {
        settings.Compression(Compression);
    }

    if (CommonSourcePath) {
        settings.SourcePath(CommonSourcePath);
    }

    const bool encryption = EncryptionAlgorithm && EncryptionKey;
    if (encryption) {
        settings.SymmetricEncryption(EncryptionAlgorithm, EncryptionKey);
    }

    settings.IncludeIndexData(IncludeIndexData);

    // YDB supported recursive directories handling along with --destination-prefix option.
    // So if we use it, then we can suppose that YDB already supports expanding of items.
    const bool expandItems = (!CommonDestinationPrefix || !ExclusionPatterns.empty());
    if (expandItems && settings.Item_.empty()) {
        constexpr bool isFs = std::is_same_v<TSettings, NExport::TExportToFsSettings>;
        settings.AppendItem(typename TSettings::TItem{.Src = CommonSourcePath ? CommonSourcePath : config.Database, .Dst = !encryption && !isFs ? CommonDestinationPrefix : TString{}});
    }

    const TDriver driver = CreateDriver(config);

    using namespace NExport;
    using namespace NScheme;
    using namespace NTable;

    TSchemeClient schemeClient(driver);
    TExportClient client(driver);
    TTableClient tableClient(driver);

    auto originalItems = settings.Item_;
    if (expandItems) {
        ExpandItems(schemeClient, tableClient, settings, ExclusionPatterns, FilterAllSupportedSchemeObjects);
    }

    TResponse response = CallExport<TResponse>(client, settings).ExtractValueSync();
    if (expandItems && response.Status().GetStatus() == EStatus::BAD_REQUEST) {
        // Retry the export operation limiting the scope to tables only.
        // This approach ensures compatibility with servers running an older version of YDB.
        settings.Item_ = std::move(originalItems);
        ExpandItems(schemeClient, tableClient, settings, ExclusionPatterns, FilterTables);
        response = CallExport<TResponse>(client, settings).ExtractValueSync();
    }
    ThrowOnError(response);
    PrintOperation(response, OutputFormat);

    return EXIT_SUCCESS;
}

/// S3
TCommandExportToS3::TCommandExportToS3()
    : TCommandExportBase("s3", "Create export to S3.\nFor more info go to: ydb.tech/docs/en/reference/ydb-cli/export-import/export-s3")
{
    TItemS3::DefineFields({
        {"Source", {{"source", "src", "s"}, "Database path to a directory or a table to be exported", true}},
        {"Destination", {{"destination", "dst", "d"}, "S3 object key prefix", true}},
    });
}

void TCommandExportToS3::Config(TConfig& config) {
    TCommandExportBase::Config(config);

    config.Opts->AddLongOption("s3-endpoint", "S3 endpoint to connect to")
        .Required().RequiredArgument("ENDPOINT").StoreResult(&AwsEndpoint);

    auto colors = NConsoleClient::AutoColors(Cout);
    config.Opts->AddLongOption("scheme", TStringBuilder()
            << "S3 endpoint scheme - "
            << colors.BoldColor() << "http" << colors.OldColor()
            << " or "
            << colors.BoldColor() << "https" << colors.OldColor())
        .RequiredArgument("SCHEME").StoreResult(&AwsScheme).DefaultValue(AwsScheme)
        .ChoicesWithCompletion({{"http", "HTTP"}, {"https", "HTTPS"}});

    {
        TStringBuilder storageClassHelp;
        storageClassHelp << "S3 storage class. Available options: ";
        TVector<NLastGetopt::NComp::TChoice> storageClassChoices;
        bool first = true;
        for (auto value : GetEnumAllValues<EStorageClass>()) {
            if (value == EStorageClass::UNKNOWN) {
                continue;
            }
            storageClassChoices.emplace_back(ToString(value));
            if (config.HelpCommandVerbosityLevel >= 2) {
                storageClassHelp << Endl << "    - " << value;
            } else {
                if (first) {
                    first = false;
                } else {
                    storageClassHelp << ", ";
                }
                storageClassHelp << colors.BoldColor() << value << colors.OldColor();
            }
        }
        storageClassHelp << Endl;
        config.Opts->AddLongOption("storage-class", storageClassHelp)
            .RequiredArgument("STORAGE_CLASS").StoreResult(&AwsStorageClass).DefaultValue(AwsStorageClass)
            .Completer(NLastGetopt::NComp::Choice(std::move(storageClassChoices)));
    }

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

    config.Opts->AddLongOption("destination-prefix", "Destination prefix for export in bucket")
        .RequiredArgument("PREFIX").StoreResult(&CommonDestinationPrefix);

    config.Opts->AddLongOption("item", TItemS3::FormatHelp("Item specification", config.HelpCommandVerbosityLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("use-virtual-addressing", TStringBuilder()
            << "Sets bucket URL style. Value "
            << colors.BoldColor() << "true" << colors.OldColor()
            << " means use Virtual-Hosted-Style URL, "
            << colors.BoldColor() << "false" << colors.OldColor()
            << " - Path-Style URL")
        .RequiredArgument("BOOL").StoreResult<bool>(&UseVirtualAddressing).DefaultValue("true");
}

void TCommandExportToS3::Parse(TConfig& config) {
    TCommandExportBase::Parse(config);

    ParseAwsProfile(config, "aws-profile");
    ParseAwsAccessKey(config, "access-key");
    ParseAwsSecretKey(config, "secret-key");

    ParseItems(config, "destination-prefix");
}

void TCommandExportToS3::ExtractParams(TConfig& config) {
    TCommandExportBase::ExtractParams(config);
}

int TCommandExportToS3::Run(TConfig& config) {
    using namespace NExport;
    using namespace NScheme;
    using namespace NTable;

    TExportToS3Settings settings = FillSettings(TExportToS3Settings());

    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.StorageClass(AwsStorageClass);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);
    settings.UseVirtualAddressing(UseVirtualAddressing);

    if (CommonDestinationPrefix) {
        settings.DestinationPrefix(CommonDestinationPrefix);
    }

    const bool encryption = EncryptionAlgorithm && EncryptionKey;
    if (encryption && !CommonDestinationPrefix) {
        Cerr << "--destination-prefix parameter is required for exports with encryption" << Endl;
        return EXIT_FAILURE;
    }

    return TCommandExportBase::Run<TExportToS3Settings, TExportToS3Response>(config, settings);
}

TCommandExportToNfs::TCommandExportToNfs()
    : TCommandExportBase("nfs", "Create a massively parallel export to a network file system shared across YDB hosts.\n"
        "As a server-side operation, export files are written in massively parallel to an identical NFS-mounted directory path accessed by all YDB hosts.\n"
        "Ensure this directory is mounted on every YDB host.")
{
    CompletionDescription = "Create export to a shared NFS directory";

    TItemNfs::DefineFields({
        {"Source", {{"source", "src", "s"}, "Database path to a directory or a table to be exported", true}},
        {"Destination", {{"destination", "dst", "d"}, "Path in file system (relative to fs-path)", true}},
    });
}

void TCommandExportToNfs::Config(TConfig& config) {
    TCommandExportBase::Config(config);

    config.Opts->AddLongOption("fs-path",
            "The absolute path in the file system on every YDB host where the export files will be located. "
            "Use the full path to the mounted directory. Example: /mnt/export/path.")
        .Required().RequiredArgument("PATH").StoreResult(&CommonDestinationPrefix);

    config.Opts->AddLongOption("item", TItemNfs::FormatHelp("Item specification", config.HelpCommandVerbosityLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");
}

void TCommandExportToNfs::Parse(TConfig& config) {
    TCommandExportBase::Parse(config);
    ParseItems(config, "fs-path");
}

void TCommandExportToNfs::ExtractParams(TConfig& config) {
    TCommandExportBase::ExtractParams(config);
}

int TCommandExportToNfs::Run(TConfig& config) {
    using namespace NExport;

    TExportToFsSettings settings = FillSettings(TExportToFsSettings());

    settings.BasePath(CommonDestinationPrefix);

    return TCommandExportBase::Run<TExportToFsSettings, TExportToFsResponse>(config, settings);
}

template int TCommandExportBase::Run<NExport::TExportToS3Settings, NExport::TExportToS3Response>(TConfig& config, NExport::TExportToS3Settings& settings);
template int TCommandExportBase::Run<NExport::TExportToFsSettings, NExport::TExportToFsResponse>(TConfig& config, NExport::TExportToFsSettings& settings);

}
}
