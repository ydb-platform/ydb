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
    
    TString Path_ = "/";
    TDuration RefreshRate_ = TDuration::Seconds(2);
    TString ViewerEndpoint_;
    
    // Optional direct navigation to topic/partition
    TString InitialTopicPath_;     // If set, navigate directly to this topic
    std::optional<ui32> InitialPartition_;  // If set, open partition view instead of topic details
};

} // namespace NYdb::NConsoleClient
