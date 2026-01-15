#pragma once

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>

#include <optional>
#include <util/system/types.h>

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
    int RefreshRateSec_ = 2;  // 0 = off (no auto-refresh)
    TString ViewerEndpoint_;
    ui64 MessageSizeLimit_ = 4096;  // Max single message size (bytes) for Viewer API
};

} // namespace NYdb::NConsoleClient
