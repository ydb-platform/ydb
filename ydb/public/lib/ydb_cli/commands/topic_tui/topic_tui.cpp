#include "topic_tui.h"
#include "topic_tui_app.h"

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
    
    config.Opts->AddLongOption("viewer-endpoint", "Viewer HTTP endpoint for message preview (e.g., http://localhost:8765)")
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

int TCommandTopicTui::Run(TConfig& config) {
    TDriver driver = CreateDriver(config);
    
    TTopicTuiApp app(driver, Path_, RefreshRate_, ViewerEndpoint_);
    return app.Run();
}

} // namespace NYdb::NConsoleClient
