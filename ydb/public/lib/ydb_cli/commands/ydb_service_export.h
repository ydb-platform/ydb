#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/lib/ydb_cli/common/aws.h>
#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/lib/ydb_cli/common/parseable_struct.h>
#include <ydb/public/lib/ydb_cli/common/yt.h>

#include <library/cpp/regex/pcre/regexp.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandExport : public TClientCommandTree {
public:
    TCommandExport(bool useExportToYt);
};

class TCommandExportToYt : public TYdbOperationCommand,
                           public TCommandWithYtProxy,
                           public TCommandWithYtToken,
                           public TCommandWithOutput {
public:
    TCommandExportToYt();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

private:
    struct TItemFields {
        TString Source;
        TString Destination;
        bool Append = true;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination, Append);

    TVector<TItem> Items;
    TVector<TRegExMatch> ExclusionPatterns;
    TString Description;
    ui32 NumberOfRetries = 1000;
    bool UseTypeV3 = false;
};

class TCommandExportBase : public TYdbOperationCommand,
                           public TCommandWithOutput {
public:
    TCommandExportBase(const TString& name, const TString& description);
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;

protected:
    struct TItemFields {
        TString Source;
        TString Destination;
    };
    DEFINE_PARSEABLE_STRUCT(TItem, TItemFields, Source, Destination);

    TVector<TItem> Items;
    TVector<TRegExMatch> ExclusionPatterns;
    TString Description;
    ui32 NumberOfRetries = 10;
    TString Compression;
    bool IncludeIndexData = false;
    TString CommonSourcePath;
    TString CommonDestinationPrefix;

    // Encryption params
    TString EncryptionAlgorithm;
    TString EncryptionKey;
    TString EncryptionKeyFile;
};

class TCommandExportToS3 : public TCommandExportBase,
                           public TCommandWithAwsCredentials {
    using EStorageClass = NExport::TExportToS3Settings::EStorageClass;

public:
    TCommandExportToS3();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;

    template <typename TSettings, typename TResponse>
    int Run(TConfig& config, TSettings& settings);

private:
    void ParseItems(TConfig& config, const TString& optionName);
    TString AwsEndpoint;
    ES3Scheme AwsScheme = ES3Scheme::HTTPS;
    EStorageClass AwsStorageClass = EStorageClass::NOT_SET;
    TString AwsBucket;
    bool UseVirtualAddressing = true;
};

class TCommandExportToFs : public TCommandExportBase {
public:
    TCommandExportToFs();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual void ExtractParams(TConfig& config) override;
    virtual int Run(TConfig& config) override;
};

}
}
