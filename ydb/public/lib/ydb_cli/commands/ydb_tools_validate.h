#pragma once

#include "ydb_command.h"

#include <ydb/public/lib/ydb_cli/common/aws.h>
#include <ydb/public/lib/ydb_cli/common/parseable_struct.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/s3_settings.h>

namespace NYdb::NConsoleClient {

class TCommandValidate : public TClientCommandTree {
public:
    TCommandValidate();
    void Config(TConfig& config) override;
};

class TCommandValidateBase : public TYdbCommand {
public:
    TCommandValidateBase(const TString& name, const TString& description);
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;

protected:
    struct TItemFields {
        TString Source;
        TString Destination;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination);

    // Returns false when the hex encryption key from the environment cannot be decoded.
    bool DecodeEncryptionKey();

    TVector<TItem> Items;
    ui32 NumberOfRetries = 10;
    bool SchemeOnly = false;
    bool FailFast = false;
    TString EncryptionKey;
    TString EncryptionKeyFile;
    TString ExpectedObjectsFile;
};

class TCommandValidateFromS3 : public TCommandValidateBase,
                               public TCommandWithAwsCredentials {
public:
    TCommandValidateFromS3();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    DEFINE_PARSEABLE_STRUCT(TItemS3, TItemFields, Source, Destination);

    TString AwsEndpoint;
    ES3Scheme AwsScheme = ES3Scheme::HTTPS;
    TString AwsBucket;
    TString CommonSourcePrefix;
    bool UseVirtualAddressing = true;
};

class TCommandValidateFromNfs : public TCommandValidateBase {
public:
    TCommandValidateFromNfs();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    DEFINE_PARSEABLE_STRUCT(TItemNfs, TItemFields, Source, Destination);

    TString FsPath;
};

} // namespace NYdb::NConsoleClient
