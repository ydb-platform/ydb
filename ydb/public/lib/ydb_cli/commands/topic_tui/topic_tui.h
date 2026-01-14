#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <optional>

namespace NYdb::NConsoleClient {

class TCommandTopicTui : public TYdbCommand {
public:
    TCommandTopicTui();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString InferViewerEndpoint(const TString& grpcAddress);
    TString ResolvePath(const TString& path, const TConfig& config) const;
    
    TString RawPath_;  // Raw path argument, resolved in Run()
    TDuration RefreshRate_ = TDuration::Seconds(2);
    TString ViewerEndpoint_;
};

} // namespace NYdb::NConsoleClient
