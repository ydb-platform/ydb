#include "topic_tui.h"
#include "topic_tui_app.h"

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TCommandTopicTui::TCommandTopicTui()
    : TYdbCommand("tui", {}, "Interactive TUI for topic management and monitoring")
{}

void TCommandTopicTui::Config(TConfig& config) {
    TYdbCommand::Config(config);
    
    config.Opts->AddLongOption("refresh", "Auto-refresh interval in seconds")
        .Optional()
        .DefaultValue(2)
        .StoreResult(&RefreshRate_);
    
    config.Opts->AddLongOption("viewer-endpoint", "Viewer HTTP endpoint for message preview (auto-detected if not specified)")
        .Optional()
        .StoreResult(&ViewerEndpoint_);
    
    config.Opts->SetFreeArgsNum(0, 1);
    SetFreeArgTitle(0, "<path>", "Starting directory path (default: database root)");
}

void TCommandTopicTui::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    
    if (config.ParseResult->GetFreeArgCount() > 0) {
        Path_ = config.ParseResult->GetFreeArgs()[0];
    }
}

// Helper to infer viewer endpoint from gRPC address
TString TCommandTopicTui::InferViewerEndpoint(const TString& grpcAddress) {
    // grpcAddress format: grpc[s]://host:port or host:port
    TStringBuf addr = grpcAddress;
    
    // Remove scheme if present
    TStringBuf scheme, rest;
    if (addr.TrySplit("://", scheme, rest)) {
        addr = rest;
    }
    
    // Find host (before port)
    TStringBuf host, port;
    if (addr.TrySplit(':', host, port)) {
        // Replace gRPC port with viewer port 8765
        return TStringBuilder() << "http://" << host << ":8765";
    }
    
    // No port specified, just use host with viewer port
    return TStringBuilder() << "http://" << addr << ":8765";
}

int TCommandTopicTui::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    
    // Auto-detect viewer endpoint if not specified
    TString viewerEndpoint = ViewerEndpoint_;
    if (viewerEndpoint.empty() && !config.Address.empty()) {
        viewerEndpoint = InferViewerEndpoint(config.Address);
    }
    
    TTopicTuiApp app(driver, Path_, RefreshRate_, viewerEndpoint);
    return app.Run();
}

} // namespace NYdb::NConsoleClient
