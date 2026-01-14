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
#include <optional>

namespace NYdb::NConsoleClient {

// Forward declarations
class TTopicListView;
class TTopicDetailsView;
class TConsumerView;
class TMessagePreviewView;
class TChartsView;
class TTopicForm;
class TDeleteConfirmForm;
class TConsumerForm;
class TWriteMessageForm;

enum class EViewType {
    TopicList,
    TopicDetails,
    ConsumerDetails,
    MessagePreview,
    Charts,
    TopicForm,
    DeleteConfirm,
    ConsumerForm,
    WriteMessage,
    OffsetForm
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
    TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint,
                 const TString& initialTopicPath = TString(), std::optional<ui32> initialPartition = std::nullopt);
    ~TTopicTuiApp();
    
    int Run();
    
    // Getters for clients (used by views)
    NScheme::TSchemeClient& GetSchemeClient() { return *SchemeClient_; }
    NTopic::TTopicClient& GetTopicClient() { return *TopicClient_; }
    TAppState& GetState() { return State_; }
    const TString& GetViewerEndpoint() const { return ViewerEndpoint_; }
    const TString& GetDatabaseRoot() const { return DatabaseRoot_; }
    
    // Navigation
    void NavigateTo(EViewType view);
    void NavigateBack();
    void ShowError(const TString& message);
    void RequestRefresh();
    void RequestExit();
    
    // Thread-safe UI refresh trigger
    void PostRefresh() { Screen_.PostEvent(ftxui::Event::Custom); }

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
    TString DatabaseRoot_;  // Root path for navigation boundary
    
    // State
    TAppState State_;
    
    // Views
    std::shared_ptr<TTopicListView> TopicListView_;
    std::shared_ptr<TTopicDetailsView> TopicDetailsView_;
    std::shared_ptr<TConsumerView> ConsumerView_;
    std::shared_ptr<TMessagePreviewView> MessagePreviewView_;
    std::shared_ptr<TChartsView> ChartsView_;
    std::shared_ptr<TTopicForm> TopicForm_;
    std::shared_ptr<TDeleteConfirmForm> DeleteConfirmForm_;
    std::shared_ptr<TConsumerForm> ConsumerForm_;
    std::shared_ptr<TWriteMessageForm> WriteMessageForm_;
    
    // Refresh thread
    std::thread RefreshThread_;
    std::atomic<bool> RefreshThreadRunning_{false};
    
    // Initial navigation (for direct topic/partition opening)
    TString InitialTopicPath_;
    std::optional<ui32> InitialPartition_;
    
    // Auto-refresh tracking
    TInstant LastTopicRefreshTime_;
    TInstant LastConsumerRefreshTime_;
    
    // Built components (for focus management)
    ftxui::Component TopicListComponent_;
    ftxui::Component TopicDetailsComponent_;
    ftxui::Component TopicFormComponent_;
    ftxui::Component DeleteConfirmComponent_;
};

} // namespace NYdb::NConsoleClient
