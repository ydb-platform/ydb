#include "ydb_tools_validate.h"

#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/validate/validate.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/hex.h>

#include <exception>

namespace NYdb::NConsoleClient {
namespace {

template <typename TFn>
auto RetryIo(ui32 retries, TFn&& fn) -> decltype(fn()) {
    const ui32 attempts = retries == 0 ? 1 : retries;
    for (ui32 attempt = 1;; ++attempt) {
        try {
            return fn();
        } catch (const std::exception&) {
            if (attempt >= attempts) {
                throw;
            }
        }
    }
}

class TFsBackupStorage : public IBackupStorage {
public:
    TFsBackupStorage(const TString& root, ui32 retries)
        : Root(root)
        , Retries(retries)
    {
        if (!Root.Exists() || !Root.IsDirectory()) {
            throw TMisuseException() << "fs-path does not exist or is not a directory: " << root;
        }
    }

    bool Exists(const TString& key) const override {
        const TFsPath path = ToPath(key);
        return path.Exists() && path.IsFile();
    }

    TVector<TString> List(const TString& prefix) const override {
        return RetryIo(Retries, [&] {
            TVector<TString> keys;
            const TFsPath dir = ToPath(prefix);
            if (dir.Exists() && dir.IsDirectory()) {
                Walk(dir, prefix, keys);
            }
            return keys;
        });
    }

    TString Read(const TString& key) const override {
        TString data;
        ReadChunks(
            key,
            [&] { data.clear(); },
            [&](TStringBuf chunk) { data.append(chunk.data(), chunk.size()); });
        return data;
    }

    void ReadChunks(
        const TString& key,
        const std::function<void()>& beginAttempt,
        const std::function<void(TStringBuf)>& onChunk) const override
    {
        RetryIo(Retries, [&] {
            beginAttempt();
            const TFsPath path = ToPath(key);
            if (!path.Exists() || !path.IsFile()) {
                ythrow yexception() << "file is missing: " << key;
            }
            TFileInput input(path);
            TString buf;
            buf.resize(1 << 20);
            for (;;) {
                const size_t read = input.Read(buf.begin(), buf.size());
                if (read == 0) {
                    break;
                }
                onChunk(TStringBuf(buf.data(), read));
            }
        });
    }

private:
    TFsPath ToPath(const TString& key) const {
        TFsPath path = Root;
        TStringBuf rest(key);
        while (rest) {
            const TStringBuf part = rest.NextTok('/');
            if (!part || part == ".") {
                continue;
            }
            if (part == "..") {
                ythrow yexception() << "backup path must not contain '..'";
            }
            path /= TString(part);
        }
        return path;
    }

    static void Walk(const TFsPath& dir, const TString& rel, TVector<TString>& keys) {
        TVector<TFsPath> children;
        dir.List(children);
        for (const TFsPath& child : children) {
            const TString key = rel ? rel + "/" + child.GetName() : child.GetName();
            if (child.IsDirectory()) {
                Walk(child, key, keys);
            } else if (child.IsFile()) {
                keys.push_back(key);
            }
        }
    }

    TFsPath Root;
    ui32 Retries = 1;
};

class TS3BackupStorage : public IBackupStorage {
public:
    TS3BackupStorage(std::unique_ptr<IS3ClientWrapper> client, ui32 retries)
        : Client(std::move(client))
        , Retries(retries)
    {
    }

    bool Exists(const TString& key) const override {
        return RetryIo(Retries, [&] {
            return Client->ObjectExists(key);
        });
    }

    TVector<TString> List(const TString& prefix) const override {
        return RetryIo(Retries, [&] {
            TVector<TString> keys;
            const TString listPrefix = prefix ? prefix + "/" : TString();
            std::optional<TString> token;
            do {
                const TListS3Result page = Client->ListObjectKeys(listPrefix ? listPrefix : prefix, token);
                token = page.NextToken;
                for (const TString& key : page.Keys) {
                    if (prefix.empty() || key == prefix || key.StartsWith(listPrefix)) {
                        keys.push_back(key);
                    }
                }
            } while (token);
            return keys;
        });
    }

    TString Read(const TString& key) const override {
        TString data;
        ReadChunks(
            key,
            [&] { data.clear(); },
            [&](TStringBuf chunk) { data.append(chunk.data(), chunk.size()); });
        return data;
    }

    void ReadChunks(
        const TString& key,
        const std::function<void()>& beginAttempt,
        const std::function<void(TStringBuf)>& onChunk) const override
    {
        RetryIo(Retries, [&] {
            beginAttempt();
            Client->GetObject(key, onChunk);
        });
    }

private:
    std::unique_ptr<IS3ClientWrapper> Client;
    ui32 Retries = 1;
};

int PrintReport(const IBackupStorage& storage, const TVector<TString>& paths, const TValidateSettings& settings) {
    size_t issues = 0;
    size_t warnings = 0;
    size_t checked = 0;
    for (const TString& path : paths) {
        const TValidationReport report = ValidateBackup(storage, path, settings);
        checked += report.Checked.size();
        for (const TValidationIssue& warning : report.Warnings) {
            ++warnings;
            Cerr << "warning: " << warning.Path << ": " << warning.Message << Endl;
        }
        for (const TValidationIssue& issue : report.Issues) {
            ++issues;
            Cerr << issue.Path << ": " << issue.Message << Endl;
        }
        if (settings.FailFast && issues != 0) {
            break;
        }
    }
    if (issues != 0) {
        Cerr << "Backup validation failed: " << issues << " issue(s), checked " << checked << " object(s)";
        if (warnings != 0) {
            Cerr << ", " << warnings << " warning(s)";
        }
        Cerr << Endl;
        return EXIT_FAILURE;
    }
    Cout << "Backup validation succeeded: checked " << checked << " object(s)";
    if (warnings != 0) {
        Cout << ", " << warnings << " warning(s)";
    }
    Cout << Endl;
    return EXIT_SUCCESS;
}

TMaybe<TVector<TString>> LoadExpectedObjects(const TString& path) {
    if (!path) {
        return {};
    }
    try {
        TFileInput input(path);
        return ParseExpectedObjects(input.ReadAll());
    } catch (const std::exception& ex) {
        throw TMisuseException() << "Cannot read --expected-objects file \"" << path << "\": " << ex.what();
    }
}

TValidateSettings MakeSettings(
    bool schemeOnly,
    bool failFast,
    const TString& encryptionKey,
    const TMaybe<TVector<TString>>& expectedObjects)
{
    TValidateSettings settings;
    settings.SchemeOnly = schemeOnly;
    settings.FailFast = failFast;
    settings.EncryptionKey = encryptionKey;
    settings.ExpectedObjects = expectedObjects;
    return settings;
}

} // namespace

TCommandValidate::TCommandValidate()
    : TClientCommandTree("validate", {}, "Validate integrity of a full backup or one exported schema object")
{
    AddCommand(std::make_unique<TCommandValidateFromS3>());
    AddCommand(std::make_unique<TCommandValidateFromNfs>());
}

void TCommandValidate::Config(TConfig& config) {
    TClientCommandTree::Config(config);
    config.NeedToConnect = false;
}

TCommandValidateBase::TCommandValidateBase(const TString& name, const TString& description)
    : TYdbCommand(name, {}, description)
{
    TItem::DefineFields({
        {"Source", {{"source", "src", "s"}, "Path of a full backup or one exported object", true}},
        {"Destination", {{"destination", "dst", "d"}, "Accepted for compatibility with ydb import and ignored", false}},
    });
}

void TCommandValidateBase::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.NeedToConnect = false;
    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption("retries", "Number of attempts to read a backup file after an I/O error")
        .RequiredArgument("NUM").StoreResult(&NumberOfRetries).DefaultValue(NumberOfRetries);

    config.Opts->AddLongOption("scheme-only",
            "Validate file composition and metadata structure only. "
            "Data file bytes are not read and their checksums are not compared. "
            "Without this option, file composition, metadata structure, and data file contents are validated.")
        .StoreTrue(&SchemeOnly);

    config.Opts->AddLongOption("fail-fast",
            "Stop validation at the first error. "
            "By default every error is reported. Warnings do not stop the check.")
        .StoreTrue(&FailFast);

    config.Opts->AddLongOption("encryption-key-file", "File path that contains encryption key or env that contains hex encoded key value")
        .Env("YDB_ENCRYPTION_KEY_FILE", true, "encryption key file")
        .Env("YDB_ENCRYPTION_KEY", false)
        .FileName("encryption key file").RequiredArgument("PATH")
        .StoreFilePath(&EncryptionKeyFile)
        .StoreResult(&EncryptionKey);

    config.Opts->AddLongOption("expected-objects",
            "Text file listing objects expected in a backup created with --item (one name per line, "
            "relative to the validated path). Empty lines are ignored. "
            "An object found in the backup and absent from the file is a warning. "
            "An object listed in the file and absent from the backup is an error. "
            "Index implementation tables stored under a listed object are part of that object. "
            "The option does not apply to backups that contain SchemaMapping.")
        .RequiredArgument("PATH").StoreResult(&ExpectedObjectsFile);
}

void TCommandValidateBase::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    Items = TItem::Parse(config, "item");
}

bool TCommandValidateBase::DecodeEncryptionKey() {
    if (EncryptionKey && !EncryptionKeyFile) {
        try {
            EncryptionKey = HexDecode(EncryptionKey);
        } catch (const std::exception&) {
            Cerr << "Failed to decode encryption key from hex" << Endl;
            return false;
        }
    }
    return true;
}

TCommandValidateFromS3::TCommandValidateFromS3()
    : TCommandValidateBase("s3", "Validate a backup stored in S3-compatible storage. "
        "The backup is read by this command; a YDB connection is not required.")
{
    TItemS3::DefineFields({
        {"Source", {{"source", "src", "s"}, "S3 object key prefix of a full backup or one exported object", true}},
        {"Destination", {{"destination", "dst", "d"}, "Accepted for compatibility with ydb import s3 and ignored", false}},
    });
}

void TCommandValidateFromS3::Config(TConfig& config) {
    TCommandValidateBase::Config(config);

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

    config.Opts->AddLongOption("source-prefix",
            "Key prefix of a full backup or one exported object. "
            "Used when --item is not set. With --item, each item source is a full key prefix, same as ydb import s3.")
        .RequiredArgument("PREFIX").StoreResult(&CommonSourcePrefix);

    config.Opts->AddLongOption("item", TItemS3::FormatHelp("Object to validate", config.HelpCommandVerbosityLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");

    config.Opts->AddLongOption("use-virtual-addressing", TStringBuilder()
            << "Sets bucket URL style. Value "
            << colors.BoldColor() << "true" << colors.OldColor()
            << " means use Virtual-Hosted-Style URL, "
            << colors.BoldColor() << "false" << colors.OldColor()
            << " - Path-Style URL.")
        .RequiredArgument("BOOL").StoreResult<bool>(&UseVirtualAddressing).DefaultValue("true");
}

void TCommandValidateFromS3::Parse(TConfig& config) {
    TCommandValidateBase::Parse(config);
    if (Items.empty() && !CommonSourcePrefix) {
        throw TMisuseException() << "No source prefix was provided";
    }
    ParseAwsProfile(config, "aws-profile");
    ParseAwsAccessKey(config, "access-key");
    ParseAwsSecretKey(config, "secret-key");
}

int TCommandValidateFromS3::Run(TConfig& config) {
    Y_UNUSED(config);
    if (!DecodeEncryptionKey()) {
        return EXIT_FAILURE;
    }

    NImport::TImportFromS3Settings settings;
    settings.Endpoint(AwsEndpoint);
    settings.Scheme(AwsScheme);
    settings.Bucket(AwsBucket);
    settings.AccessKey(AwsAccessKey);
    settings.SecretKey(AwsSecretKey);
    settings.UseVirtualAddressing(UseVirtualAddressing);

    TVector<TString> paths;
    if (Items.empty()) {
        paths.push_back(CommonSourcePrefix);
    } else {
        for (const TItem& item : Items) {
            paths.push_back(item.Source);
        }
    }

    const TMaybe<TVector<TString>> expectedObjects = LoadExpectedObjects(ExpectedObjectsFile);
    InitAwsAPI();
    try {
        TS3BackupStorage storage(CreateS3ClientWrapper(settings), NumberOfRetries);
        const int code = PrintReport(storage, paths, MakeSettings(SchemeOnly, FailFast, EncryptionKey, expectedObjects));
        ShutdownAwsAPI();
        return code;
    } catch (...) {
        ShutdownAwsAPI();
        throw;
    }
}

TCommandValidateFromNfs::TCommandValidateFromNfs()
    : TCommandValidateBase("nfs", "Validate a backup stored on a local or mounted filesystem. "
        "The backup is read by this command; a YDB connection is not required.")
{
    TItemNfs::DefineFields({
        {"Source", {{"source", "src", "s"}, "Path of a full backup or one exported object, relative to --fs-path", true}},
        {"Destination", {{"destination", "dst", "d"}, "Accepted for compatibility with ydb import nfs and ignored", false}},
    });
}

void TCommandValidateFromNfs::Config(TConfig& config) {
    TCommandValidateBase::Config(config);

    config.Opts->AddLongOption("fs-path",
            "Directory that contains the backup. "
            "Without --item, this directory itself is validated. "
            "With --item, each source path is relative to this directory.")
        .Required().RequiredArgument("PATH").StoreResult(&FsPath);

    config.Opts->AddLongOption("item", TItemNfs::FormatHelp("Object to validate", config.HelpCommandVerbosityLevel, 2))
        .RequiredArgument("PROPERTY=VALUE,...");
}

int TCommandValidateFromNfs::Run(TConfig& config) {
    Y_UNUSED(config);
    if (!DecodeEncryptionKey()) {
        return EXIT_FAILURE;
    }

    TVector<TString> paths;
    if (Items.empty()) {
        paths.push_back(TString());
    } else {
        for (const TItem& item : Items) {
            paths.push_back(item.Source);
        }
    }

    TFsBackupStorage storage(FsPath, NumberOfRetries);
    return PrintReport(storage, paths, MakeSettings(SchemeOnly, FailFast, EncryptionKey, LoadExpectedObjects(ExpectedObjectsFile)));
}

} // namespace NYdb::NConsoleClient
