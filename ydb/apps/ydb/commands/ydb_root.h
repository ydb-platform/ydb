#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>

namespace NYdb {
namespace NConsoleClient {

class TClientCommandRoot : public TClientCommandRootCommon {
public:
    TClientCommandRoot(const TString& name, const TClientSettings& settings);

private:
    void FillConfig(TConfig& config) override;
    void SetCredentialsGetter(TConfig& config) override;
};

class TYdbClientCommandRoot : public TClientCommandRoot {
public:
    TYdbClientCommandRoot(const TString& name, const TClientSettings& settings);
    void Config(TConfig& config) override;
    int Run(TConfig& config) override;
};

int NewYdbClient(int argc, char** argv);

}
}
