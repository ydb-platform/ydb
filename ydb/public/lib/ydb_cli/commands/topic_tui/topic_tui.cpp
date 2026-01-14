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
    
    // Helper to resolve relative paths using database path as base
    auto resolvePath = [&config](const TString& path) -> TString {
        if (path.empty()) {
            return config.Database;
        }
        if (path.StartsWith("/")) {
            return path;  // Already absolute
        }
        // Relative path - prepend database
        TString base = config.Database;
        if (!base.EndsWith("/")) {
            base += "/";
        }
        return base + path;
    };
    
    if (config.ParseResult->GetFreeArgCount() > 0) {
        TString arg = config.ParseResult->GetFreeArgs()[0];
        
        // Check for partition suffix: /path/to/topic:N or topic:N
        TStringBuf path = arg;
        TStringBuf base, partitionStr;
        if (path.TryRSplit(':', base, partitionStr)) {
            // Verify it looks like a partition number (all digits)
            bool isPartition = !partitionStr.empty();
            for (char c : partitionStr) {
                if (!std::isdigit(c)) {
                    isPartition = false;
                    break;
                }
            }
            if (isPartition) {
                // Resolve the base path (before colon)
                InitialTopicPath_ = resolvePath(TString(base));
                InitialPartition_ = FromString<ui32>(partitionStr);
                // Set Path_ to parent directory for navigation context  
                TStringBuf parent, discard;
                if (TStringBuf(InitialTopicPath_).TryRSplit('/', parent, discard)) {
                    Path_ = parent ? TString(parent) : "/";
                } else {
                    Path_ = "/";
                }
                return;
            }
        }
        
        // No partition suffix - resolve path and use for both topic and directory
        TString resolved = resolvePath(arg);
        InitialTopicPath_ = resolved;
        Path_ = resolved;
    } else {
        // No argument provided - use database as root path
        Path_ = config.Database;
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
    
    // Determine start path: use explicit path, or database, or fallback to "/"
    TString startPath = Path_;
    if (startPath.empty() || startPath == "/") {
        // No explicit path - use database if available
        if (!config.Database.empty()) {
            startPath = config.Database;
        } else {
            startPath = "/";  // Ultimate fallback
        }
    }
    
    TTopicTuiApp app(driver, startPath, RefreshRate_, viewerEndpoint, 
                     InitialTopicPath_, InitialPartition_);
    return app.Run();
}

} // namespace NYdb::NConsoleClient
