#pragma once

#include "ydb_command.h"
#include "ydb_common.h"

#include <ydb/public/lib/ydb_cli/common/format.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/udf/udf.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandUdf : public TClientCommandTree {
public:
    TCommandUdf();
};

class TCommandUdfUpload : public TYdbOperationCommand, public TCommandWithOutput {
public:
    TCommandUdfUpload();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Kind;
    TString FilePath;
    TString ManifestPath;
    TString LibraryName;
    TString WriteMode;
    TString ExpectedUid;
    TString ExpectedMd5;
    bool CreateOnly = false;
    bool ReplaceOnly = false;
};

class TCommandUdfDelete : public TYdbOperationCommand {
public:
    TCommandUdfDelete();
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Name;
    TString Kind;
    TString ExpectedUid;
};

class TCommandUdfList : public TYdbOperationCommand, public TCommandWithOutput {
public:
    TCommandUdfList();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Kind;
    TString Status;
};

class TCommandUdfDescribe : public TYdbOperationCommand, public TCommandWithOutput {
public:
    TCommandUdfDescribe();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Name;
};

} // namespace NConsoleClient
} // namespace NYdb
