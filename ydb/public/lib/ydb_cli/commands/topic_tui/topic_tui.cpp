#include "topic_tui.h"
#include "topic_tui_app.h"

#include <util/string/builder.h>

namespace NYdb::NConsoleClient {

TCommandTopicTui::TCommandTopicTui()
    : TYdbCommand("tui", {}, "Interactive TUI for topic management and monitoring")
{}

void TCommandTopicTui::Config(TConfig& config) {
    TYdbCommand::Config(config);
    
    config.Opts->AddLongOption("refresh", "Auto-refresh interval in seconds (0 = off)")
        .Optional()
        .DefaultValue(2)
        .StoreResult(&RefreshRateSec_);
    
    config.Opts->AddLongOption("viewer-endpoint", "Viewer HTTP endpoint for message preview (auto-detected if not specified)")
        .Optional()
        .StoreResult(&ViewerEndpoint_);

    config.Opts->AddLongOption("message-size-limit", "Max single message size to fetch from Viewer API (bytes)")
        .Optional()
        .DefaultValue(4096)
        .StoreResult(&MessageSizeLimit_);
    
    config.Opts->SetFreeArgsNum(0, 1);
    SetFreeArgTitle(0, "<path>", "Starting directory path (default: database root)");
}

void TCommandTopicTui::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    
    // Store raw argument - path resolution happens in Run() where config is fully populated
    if (config.ParseResult->GetFreeArgCount() > 0) {
        RawPath_ = config.ParseResult->GetFreeArgs()[0];
    }
}

// Helper to resolve relative paths using database path as base
TString TCommandTopicTui::ResolvePath(const TString& path, const TConfig& config) const {
    if (path.empty()) {
        return config.Database;
    }
    if (path.StartsWith("/")) {
        return path;  // Already absolute
    }
    // Relative path - prepend database (same pattern as AdjustPath)
    TString base = config.Database;
    if (!base.empty() && !base.EndsWith("/")) {
        base += "/";
    }
    return base + path;
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
    
    // Resolve paths NOW, when config.Database is available
    TString startPath;
    TString initialTopicPath;
    std::optional<ui32> initialPartition;
    TString initialConsumer;
    
    if (!RawPath_.empty()) {
        TStringBuf pathBuf = RawPath_;
        
        // First, check for @consumer suffix
        TStringBuf base, consumerPart;
        if (pathBuf.TryRSplit('@', base, consumerPart)) {
            // Extract consumer name (ignore any :partition after it for now)
            TStringBuf consumerName, partAfterConsumer;
            if (consumerPart.TryRSplit(':', consumerName, partAfterConsumer)) {
                // Format: topic@consumer:partition - not fully supported yet
                // Just treat the whole consumerPart as consumer name
                initialConsumer = TString(consumerPart);
            } else {
                initialConsumer = TString(consumerPart);
            }
            // Resolve base path as topic
            initialTopicPath = ResolvePath(TString(base), config);
            // Set startPath to parent directory
            TStringBuf parent, discard;
            if (TStringBuf(initialTopicPath).TryRSplit('/', parent, discard)) {
                startPath = parent ? TString(parent) : config.Database;
            } else {
                startPath = config.Database;
            }
        } else {
            // No @consumer - check for :partition suffix
            TStringBuf partBase, partitionStr;
            if (pathBuf.TryRSplit(':', partBase, partitionStr)) {
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
                    initialTopicPath = ResolvePath(TString(partBase), config);
                    initialPartition = FromString<ui32>(partitionStr);
                    // Set startPath to parent directory for navigation context  
                    TStringBuf parent, discard;
                    if (TStringBuf(initialTopicPath).TryRSplit('/', parent, discard)) {
                        startPath = parent ? TString(parent) : config.Database;
                    } else {
                        startPath = config.Database;
                    }
                } else {
                    // Not a partition suffix - resolve the whole path
                    TString resolved = ResolvePath(RawPath_, config);
                    initialTopicPath = resolved;
                    startPath = resolved;
                }
            } else {
                // No colon - resolve path and use for both topic and directory
                TString resolved = ResolvePath(RawPath_, config);
                initialTopicPath = resolved;
                startPath = resolved;
            }
        }
    } else {
        // No argument provided - use database as root path
        startPath = config.Database;
    }
    
    // Fallback if startPath is still empty
    if (startPath.empty()) {
        startPath = "/";
    }
    
    // Convert refresh rate: 0 means "off" (no auto-refresh)
    TDuration refreshRate = (RefreshRateSec_ == 0) 
        ? TDuration::Max() 
        : TDuration::Seconds(RefreshRateSec_);
    
    TTopicTuiApp app(driver, startPath, refreshRate, viewerEndpoint, MessageSizeLimit_,
                     initialTopicPath, initialPartition, initialConsumer);
    return app.Run();
}

} // namespace NYdb::NConsoleClient
