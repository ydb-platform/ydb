#pragma once

#include "app_interface.h"
#include "view_registry.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

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
class TDropConsumerForm;
class TEditConsumerForm;
class TOffsetForm;
class TTopicInfoView;

// View types and State are defined in app_context.h via app_interface.h

class TTopicTuiApp : public ITuiApp {
public:
    TTopicTuiApp(TDriver& driver, const TString& startPath, TDuration refreshRate, const TString& viewerEndpoint,
                 ui64 messageSizeLimit,
                 const TString& initialTopicPath = TString(), std::optional<ui32> initialPartition = std::nullopt,
                 const TString& initialConsumer = TString());
    ~TTopicTuiApp();
    
    int Run();
    
    // Getters for clients (used by views)
    NScheme::TSchemeClient& GetSchemeClient() override { return *SchemeClient_; }
    NTopic::TTopicClient& GetTopicClient() override { return *TopicClient_; }
    TAppState& GetState() override { return State_; }
    const TString& GetViewerEndpoint() const override { return ViewerEndpoint_; }
    const TString& GetDatabaseRoot() const override { return DatabaseRoot_; }
    TDuration GetRefreshRate() const override { return RefreshRate_; }
    ui64 GetMessageSizeLimit() const override { return MessageSizeLimit_; }
    TString GetRefreshRateLabel() const override;  // Returns human readable rate
    void CycleRefreshRate() override;  // Cycle through predefined rates
    
    // Navigation
    void NavigateTo(EViewType view) override;
    void NavigateBack() override;
    void ShowError(const TString& message) override;
    void RequestRefresh() override;
    void RequestExit() override;
    bool IsExiting() const override { return Exiting_.load(); }
    
    // Thread-safe UI refresh trigger
    void PostRefresh() override { Screen_.PostEvent(ftxui::Event::Custom); }
    
    // View Target Configuration (ITuiApp interface)
    void SetTopicDetailsTarget(const TString& topicPath) override;
    void SetConsumerViewTarget(const TString& topicPath, const TString& consumerName) override;
    void SetMessagePreviewTarget(const TString& topicPath, ui32 partition, i64 offset) override;
    void SetTopicFormCreateMode(const TString& parentPath) override;
    void SetTopicFormEditMode(const TString& topicPath) override;
    void SetDeleteConfirmTarget(const TString& path) override;
    void SetDropConsumerTarget(const TString& topicPath, const TString& consumerName) override;
    void SetEditConsumerTarget(const TString& topicPath, const TString& consumerName) override;
    void SetWriteMessageTarget(const TString& topicPath, std::optional<ui32> partition) override;
    void SetConsumerFormTarget(const TString& topicPath) override;
    void SetOffsetFormTarget(const TString& topicPath, const TString& consumerName,
                              ui64 partition, ui64 currentOffset, ui64 endOffset) override;
    void SetTopicInfoTarget(const TString& topicPath) override;

private:
    ftxui::Component BuildMainComponent();
    ftxui::Component BuildHelpBar();
    ftxui::Element RenderHelpOverlay();
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
    int RefreshRateIndex_ = 1;  // Index into predefined rates (0=1s, 1=2s, 2=5s, 3=10s, 4=off)
    TString ViewerEndpoint_;
    ui64 MessageSizeLimit_ = 4096;
    TString DatabaseRoot_;  // Root path for navigation boundary
    
    // State
    TAppState State_;
    
    // View Registry - manages all views and forms
    TViewRegistry ViewRegistry_;
    
    // Typed view accessors (for views that need specific method calls)
    // These are aliases to views in the registry
    std::shared_ptr<TTopicListView> TopicListView_;
    std::shared_ptr<TTopicDetailsView> TopicDetailsView_;
    std::shared_ptr<TConsumerView> ConsumerView_;
    std::shared_ptr<TMessagePreviewView> MessagePreviewView_;
    std::shared_ptr<TChartsView> ChartsView_;
    std::shared_ptr<TTopicForm> TopicForm_;
    std::shared_ptr<TDeleteConfirmForm> DeleteConfirmForm_;
    std::shared_ptr<TConsumerForm> ConsumerForm_;
    std::shared_ptr<TWriteMessageForm> WriteMessageForm_;
    std::shared_ptr<TDropConsumerForm> DropConsumerForm_;
    std::shared_ptr<TEditConsumerForm> EditConsumerForm_;
    std::shared_ptr<TOffsetForm> OffsetForm_;
    std::shared_ptr<TTopicInfoView> TopicInfoView_;
    
    // Refresh thread
    std::thread RefreshThread_;
    std::atomic<bool> RefreshThreadRunning_{false};
    
    // Initial navigation (for direct topic/partition opening)
    TString InitialTopicPath_;
    std::optional<ui32> InitialPartition_;
    TString InitialConsumer_;  // @consumer syntax
    
    // Auto-refresh tracking
    TInstant LastTopicRefreshTime_;
    TInstant LastConsumerRefreshTime_;
    
    // Built components (for focus management)
    ftxui::Component TopicListComponent_;
    ftxui::Component TopicDetailsComponent_;
    ftxui::Component ConsumerComponent_;
    ftxui::Component TopicFormComponent_;
    ftxui::Component DeleteConfirmComponent_;
    ftxui::Component OffsetFormComponent_;
    
    // Shutdown tracking
    std::atomic<bool> Exiting_{false};
};

} // namespace NYdb::NConsoleClient
