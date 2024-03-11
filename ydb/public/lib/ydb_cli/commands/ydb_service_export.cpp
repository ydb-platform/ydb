#include "ydb_service_export.h"

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>

#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>

namespace NYdb {
namespace NConsoleClient {

static const TString AppendPrefix = "<append=true>";

namespace {

    const char slashC = '/';
    const TStringBuf slash(&slashC, 1);

    bool FilterTables(const NScheme::TSchemeEntry& entry) {
        return entry.Type == NScheme::ESchemeEntryType::Table;
    }

    TVector<std::pair<TString, TString>> ExpandItem(NScheme::TSchemeClient& client, TStringBuf srcPath, TStringBuf dstPath) {
        // cut trailing slash
        srcPath.ChopSuffix(slash);
        dstPath.ChopSuffix(slash);

        const auto ret = RecursiveList(client, TString{srcPath}, TRecursiveListSettings().Filter(&FilterTables));
        ThrowOnError(ret.Status);

        if (ret.Entries.size() == 1 && srcPath == ret.Entries[0].Name) {
            return {{TString{srcPath}, TString{dstPath}}};
        }

        TVector<std::pair<TString, TString>> result;
        for (const auto& table : ret.Entries) {
            result.emplace_back(table.Name, TStringBuilder() << dstPath << TStringBuf(table.Name).RNextTok(srcPath));
        }

        return result;
    }

    template <typename TSettings>
    void ExpandItems(NScheme::TSchemeClient& client, TSettings& settings, const TVector<TRegExMatch>& exclusions) {
        auto isExclusion = [&exclusions](const char* str) -> bool {
            for (const auto& pattern : exclusions) {
                if (pattern.Match(str)) {
                    return true;
                }
            }

            return false;
        };

        auto items(std::move(settings.Item_));
        for (const auto& item : items) {
            for (const auto& [src, dst] : ExpandItem(client, item.Src, item.Dst)) {
                if (isExclusion(src.c_str())) {
                    continue;
                }

                settings.AppendItem({src, dst});
            }
        }
    }

} // anonymous namespace

TCommandExport::TCommandExport(bool useExportToYt)
    : TClientCommandTree("export", {}, "Export service operations")
{
    if (useExportToYt) {
        AddCommand(std::make_unique<TCommandExportToYt>());
    }
    AddCommand(std::make_unique<TCommandExportToS3>());
}

/// YT
TCommandExportToYt::TCommandExportToYt()
    : TYdbOperationCommand("yt", {}, "Create export to YT")
{
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
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

    TStringBuilder itemHelp;
    itemHelp << "[At least one] Item specification" << Endl
        << "  Possible property names:" << Endl
        << TItem::FormatHelp(2);
    config.Opts->AddLongOption("item", itemHelp)
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("exclude", "Pattern (PCRE) for paths excluded from export operation")
        .RequiredArgument("STRING").Handler1T<TString>([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });

    config.Opts->AddLongOption("description", "Textual description of export operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    config.Opts->AddLongOption("use-type-v3", "Use YT's type_v3")
        .NoArgument().StoreTrue(&UseTypeV3);

    AddDeprecatedJsonOption(config);
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandExportToYt::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();

    ParseYtProxy(config, "proxy");
    ParseYtToken(config, "token");

    Items = TItem::Parse(config, "item");
    if (Items.empty()) {
        throw TMisuseException() << "At least one item should be provided";
    }

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
    ExpandItems(schemeClient, settings, ExclusionPatterns);

    TExportClient client(driver);
    TExportToYtResponse response = client.ExportToYt(std::move(settings)).GetValueSync();
    ThrowOnError(response);
    PrintOperation(response, OutputFormat);

    return EXIT_SUCCESS;
}

/// S3
TCommandExportToS3::TCommandExportToS3()
    : TYdbOperationCommand("s3", {}, "Create export to S3")
{
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "Database path to a directory or a table to be exported", true}},
        {"Destination", {{"destination", "dst", "d"}, "S3 object key prefix", true}},
    });
}

void TCommandExportToS3::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);

    config.Opts->AddLongOption("s3-endpoint", "S3 endpoint to connect to")
        .Required().RequiredArgument("ENDPOINT").StoreResult(&AwsEndpoint);

    config.Opts->AddLongOption("scheme", "S3 endpoint scheme")
        .RequiredArgument("SCHEME").StoreResult(&AwsScheme).DefaultValue(AwsScheme);

    TStringBuilder storageClassHelp;
    storageClassHelp << "S3 storage class" << Endl
        << "  Available options:" << Endl;
    for (auto value : GetEnumAllValues<EStorageClass>()) {
        storageClassHelp << "    - " << value << Endl;
    }
    config.Opts->AddLongOption("storage-class", storageClassHelp)
        .RequiredArgument("STORAGE_CLASS").StoreResult(&AwsStorageClass).DefaultValue(AwsStorageClass);

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

    config.Opts->AddLongOption("exclude", "Pattern (PCRE) for paths excluded from export operation")
        .RequiredArgument("STRING").Handler1T<TString>([this](const TString& arg) {
            ExclusionPatterns.emplace_back(TRegExMatch(arg));
        });

    config.Opts->AddLongOption("description", "Textual description of export operation")
        .RequiredArgument("STRING").StoreResult(&Description);

    config.Opts->AddLongOption("retries", "Number of retries")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    config.Opts->AddLongOption("compression", TStringBuilder()
            << "Codec used to compress data" << Endl
            << "  Available options:" << Endl
            << "    - zstd" << Endl
            << "    - zstd-N (N is compression level, e.g. zstd-3)" << Endl)
        .RequiredArgument("STRING").StoreResult(&Compression);

    config.Opts->AddLongOption("use-virtual-addressing", "S3 bucket virtual addressing")
        .RequiredArgument("BOOL").StoreResult<bool>(&UseVirtualAddressing).DefaultValue("true");

    AddDeprecatedJsonOption(config);
    AddFormats(config, { EOutputFormat::Pretty, EOutputFormat::ProtoJsonBase64 });
    config.Opts->MutuallyExclusive("json", "format");
}

void TCommandExportToS3::Parse(TConfig& config) {
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
        NConsoleClient::AdjustPath(item.Source, config);
    }
}

int TCommandExportToS3::Run(TConfig& config) {
    using namespace NExport;
    using namespace NScheme;

    TExportToS3Settings settings = FillSettings(TExportToS3Settings());

    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.StorageClass(AwsStorageClass);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);
    settings.UseVirtualAddressing(UseVirtualAddressing);

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

    const TDriver driver = CreateDriver(config);

    TSchemeClient schemeClient(driver);
    ExpandItems(schemeClient, settings, ExclusionPatterns);

    TExportClient client(driver);
    TExportToS3Response response = client.ExportToS3(std::move(settings)).GetValueSync();
    ThrowOnError(response);
    PrintOperation(response, OutputFormat);

    return EXIT_SUCCESS;
}

}
}
