#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

namespace NYdb::NConsoleClient {

class TCommandTopicTui : public TYdbCommand {
public:
    TCommandTopicTui();
    void Config(TConfig& config) override;
    void Parse(TConfig& config) override;
    int Run(TConfig& config) override;

private:
    TString Path_ = "/";
    TDuration RefreshRate_ = TDuration::Seconds(2);
    TString ViewerEndpoint_;
};

} // namespace NYdb::NConsoleClient
