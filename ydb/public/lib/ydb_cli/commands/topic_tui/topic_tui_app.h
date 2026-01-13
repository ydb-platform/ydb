#pragma once

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <memory>
#include <thread>
#include <atomic>

namespace NYdb::NConsoleClient {

// Forward declarations
class TTopicListView;
class TTopicDetailsView;
class TConsumerView;
class TMessagePreviewView;
class TChartsView;

enum class EViewType {
    TopicList,
    TopicDetails,
    ConsumerDetails,
    MessagePreview,
    Charts
};

// Shared state between views
struct TAppState {
    // Current navigation
    TString CurrentPath;
    TString SelectedTopic;
    TString SelectedConsumer;
    ui32 SelectedPartition = 0;
    
    // Refresh control
    std::atomic<bool> ShouldRefresh{false};
    std::atomic<bool> ShouldExit{false};
    
    // Current view
    EViewType CurrentView = EViewType::TopicList;
    
    // Last error message
    TString LastError;
};

class TTopicTuiApp {
public:
    TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint);
    ~TTopicTuiApp();
    
    int Run();
    
    // Getters for clients (used by views)
    NScheme::TSchemeClient& GetSchemeClient() { return *SchemeClient_; }
    NTopic::TTopicClient& GetTopicClient() { return *TopicClient_; }
    TAppState& GetState() { return State_; }
    const TString& GetViewerEndpoint() const { return ViewerEndpoint_; }
    
    // Navigation
    void NavigateTo(EViewType view);
    void NavigateBack();
    void ShowError(const TString& message);
    void RequestRefresh();
    void RequestExit();

private:
    ftxui::Component BuildMainComponent();
    ftxui::Component BuildHelpBar();
    void StartRefreshThread();
    void StopRefreshThread();
    
private:
    // SDK clients
    std::unique_ptr<NScheme::TSchemeClient> SchemeClient_;
    std::unique_ptr<NTopic::TTopicClient> TopicClient_;
    
    // Screen and UI
    ftxui::ScreenInteractive Screen_;
    
    // Configuration
    TDuration RefreshRate_;
    TString ViewerEndpoint_;
    
    // State
    TAppState State_;
    
    // Views
    std::shared_ptr<TTopicListView> TopicListView_;
    std::shared_ptr<TTopicDetailsView> TopicDetailsView_;
    std::shared_ptr<TConsumerView> ConsumerView_;
    std::shared_ptr<TMessagePreviewView> MessagePreviewView_;
    std::shared_ptr<TChartsView> ChartsView_;
    
    // Refresh thread
    std::thread RefreshThread_;
    std::atomic<bool> RefreshThreadRunning_{false};
};

} // namespace NYdb::NConsoleClient
