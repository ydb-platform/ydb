#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/sdk/cpp/client/ydb_export/export.h>
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
                           public TCommandWithFormat {
public:
    TCommandExportToYt();
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
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

class TCommandExportToS3 : public TYdbOperationCommand,
                           public TCommandWithAwsCredentials,
                           public TCommandWithFormat {
    using EStorageClass = NExport::TExportToS3Settings::EStorageClass;

public:
    TCommandExportToS3();
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
    EStorageClass AwsStorageClass = EStorageClass::NOT_SET;
    TString AwsBucket;
    TVector<TItem> Items;
    TVector<TRegExMatch> ExclusionPatterns;
    TString Description;
    ui32 NumberOfRetries = 10;
    TString Compression;
    bool UseVirtualAddressing = true;
};

}
}
