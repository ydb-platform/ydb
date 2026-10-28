#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_udf.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandUdf: public TClientCommandTree {
public:
    TCommandUdf();
};

class TCommandUdfUpload: public TYdbOperationCommand {
public:
    TCommandUdfUpload();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Format = "text";
    TString FilePath;
    TString ManifestPath;
    TString PackagePath;
    TString WriteMode;
    TString ExpectedUid;
    TString ExpectedMd5;
    bool CreateOnly = false;
    bool ReplaceOnly = false;
};

class TCommandUdfDelete: public TYdbOperationCommand {
public:
    TCommandUdfDelete();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Name;
    TString Type;
    TString Kind;
    TString ExpectedUid;
};

class TCommandUdfList: public TYdbOperationCommand {
public:
    TCommandUdfList();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Type;
    TString Kind;
    TString Format = "table";
};

class TCommandUdfDescribe: public TYdbOperationCommand {
public:
    TCommandUdfDescribe();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Name;
    TString Format = "json";
};

} // namespace NConsoleClient
} // namespace NYdb
