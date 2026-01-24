#pragma once

#include "../app_interface.h"
#include "../app_context.h"

#include <util/generic/string.h>
#include <util/datetime/base.h>

#include <vector>
#include <optional>

namespace NYdb::NConsoleClient::NTest {

// Recorded navigation event for verification
struct TNavigationEvent {
    enum class EType {
        NavigateTo,
        NavigateBack,
        SetTopicDetailsTarget,
        SetConsumerViewTarget,
        SetMessagePreviewTarget,
        SetTopicFormCreateMode,
        SetTopicFormEditMode,
        SetDeleteConfirmTarget,
        SetDropConsumerTarget,
        SetEditConsumerTarget,
        SetWriteMessageTarget,
        SetConsumerFormTarget,
        SetOffsetFormTarget,
        SetTopicInfoTarget,
        ShowError,
        RequestRefresh,
        RequestExit,
        PostRefresh
    };
    
    EType Type;
    EViewType ViewType = EViewType::TopicList;  // For NavigateTo
    TString Path;
    TString Path2;  // For consumer name, etc.
    ui32 Partition = 0;
    i64 Offset = 0;
    ui64 CurrentOffset = 0;
    ui64 EndOffset = 0;
    std::optional<ui32> OptionalPartition;
};

// Mock implementation of ITuiApp for testing views
// Records all calls for verification without actual SDK operations
class TMockTuiApp : public ITuiApp {
public:
    TMockTuiApp() {
        State_.CurrentPath = "/Root";
        State_.CurrentView = EViewType::TopicList;
        State_.PreviousView = EViewType::TopicList;
    }
    
    // === ITuiApp Interface Implementation ===
    
    // SDK Access - Not implemented in mock (would require real clients)
    NScheme::TSchemeClient& GetSchemeClient() override {
        throw std::runtime_error("MockTuiApp: GetSchemeClient not available in mock");
    }
    
    NTopic::TTopicClient& GetTopicClient() override {
        throw std::runtime_error("MockTuiApp: GetTopicClient not available in mock");
    }
    
    // State Access
    TAppState& GetState() override { return State_; }
    const TString& GetViewerEndpoint() const override { return ViewerEndpoint_; }
    const TString& GetDatabaseRoot() const override { return DatabaseRoot_; }
    TDuration GetRefreshRate() const override { return RefreshRate_; }
    ui64 GetMessageSizeLimit() const override { return MessageSizeLimit_; }
    
    // Navigation & UI
    void NavigateTo(EViewType view) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::NavigateTo;
        event.ViewType = view;
        RecordEvent(std::move(event));
        
        // Track previous view for proper back navigation
        State_.PreviousView = State_.CurrentView;
        State_.CurrentView = view;
    }
    
    void NavigateBack() override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::NavigateBack;
        RecordEvent(std::move(event));
        
        // Implement simplified back navigation logic
        switch (State_.CurrentView) {
            case EViewType::TopicDetails:
                State_.CurrentView = EViewType::TopicList;
                break;
            case EViewType::ConsumerDetails:
            case EViewType::MessagePreview:
            case EViewType::Charts:
                State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                    ? EViewType::TopicList : EViewType::TopicDetails;
                break;
            case EViewType::TopicForm:
            case EViewType::DeleteConfirm:
            case EViewType::ConsumerForm:
            case EViewType::WriteMessage:
            case EViewType::DropConsumerConfirm:
            case EViewType::EditConsumer:
                State_.CurrentView = State_.PreviousView;
                break;
            default:
                break;
        }
    }
    
    void ShowError(const TString& message) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::ShowError;
        event.Path = message;
        RecordEvent(event);
        LastError_ = message;
    }
    
    void RequestRefresh() override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::RequestRefresh;
        RecordEvent(std::move(event));
        RefreshRequested_ = true;
    }
    
    void RequestExit() override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::RequestExit;
        RecordEvent(std::move(event));
        ExitRequested_ = true;
    }
    
    bool IsExiting() const override {
        return ExitRequested_;
    }
    
    void PostRefresh() override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::PostRefresh;
        RecordEvent(std::move(event));
        PostRefreshCalled_ = true;
    }
    
    // View Target Configuration
    void SetTopicDetailsTarget(const TString& topicPath) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetTopicDetailsTarget;
        event.Path = topicPath;
        RecordEvent(event);
        State_.SelectedTopic = topicPath;
    }
    
    void SetConsumerViewTarget(const TString& topicPath, const TString& consumerName) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetConsumerViewTarget;
        event.Path = topicPath;
        event.Path2 = consumerName;
        RecordEvent(event);
        State_.SelectedTopic = topicPath;
        State_.SelectedConsumer = consumerName;
    }
    
    void SetMessagePreviewTarget(const TString& topicPath, ui32 partition, i64 offset) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetMessagePreviewTarget;
        event.Path = topicPath;
        event.Partition = partition;
        event.Offset = offset;
        RecordEvent(event);
        State_.SelectedTopic = topicPath;
        State_.SelectedPartition = partition;
    }
    
    void SetTopicFormCreateMode(const TString& parentPath) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetTopicFormCreateMode;
        event.Path = parentPath;
        RecordEvent(event);
    }
    
    void SetTopicFormEditMode(const TString& topicPath) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetTopicFormEditMode;
        event.Path = topicPath;
        RecordEvent(event);
    }
    
    void SetDeleteConfirmTarget(const TString& path) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetDeleteConfirmTarget;
        event.Path = path;
        RecordEvent(event);
    }
    
    void SetDropConsumerTarget(const TString& topicPath, const TString& consumerName) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetDropConsumerTarget;
        event.Path = topicPath;
        event.Path2 = consumerName;
        RecordEvent(event);
    }
    
    void SetEditConsumerTarget(const TString& topicPath, const TString& consumerName) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetEditConsumerTarget;
        event.Path = topicPath;
        event.Path2 = consumerName;
        RecordEvent(event);
    }
    
    void SetWriteMessageTarget(const TString& topicPath, std::optional<ui32> partition) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetWriteMessageTarget;
        event.Path = topicPath;
        event.OptionalPartition = partition;
        RecordEvent(event);
    }
    
    void SetConsumerFormTarget(const TString& topicPath) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetConsumerFormTarget;
        event.Path = topicPath;
        RecordEvent(event);
    }
    
    void SetOffsetFormTarget(const TString& topicPath, const TString& consumerName,
                              ui64 partition, ui64 currentOffset, ui64 endOffset) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetOffsetFormTarget;
        event.Path = topicPath;
        event.Path2 = consumerName;
        event.Partition = partition;
        event.CurrentOffset = currentOffset;
        event.EndOffset = endOffset;
        RecordEvent(event);
    }
    
    void SetTopicInfoTarget(const TString& topicPath) override {
        TNavigationEvent event;
        event.Type = TNavigationEvent::EType::SetTopicInfoTarget;
        event.Path = topicPath;
        RecordEvent(event);
    }
    
    // Formatting
    TString GetRefreshRateLabel() const override {
        if (RefreshRate_ == TDuration::Seconds(1)) return "1s";
        if (RefreshRate_ == TDuration::Seconds(2)) return "2s";
        if (RefreshRate_ == TDuration::Seconds(5)) return "5s";
        if (RefreshRate_ == TDuration::Seconds(10)) return "10s";
        return "Off";
    }
    
    void CycleRefreshRate() override {
        static const TDuration rates[] = {
            TDuration::Seconds(1),
            TDuration::Seconds(2),
            TDuration::Seconds(5),
            TDuration::Seconds(10),
            TDuration::Max()
        };
        
        for (int i = 0; i < 5; ++i) {
            if (RefreshRate_ == rates[i]) {
                RefreshRate_ = rates[(i + 1) % 5];
                return;
            }
        }
        RefreshRate_ = TDuration::Seconds(2);  // Default
    }
    
    // === Mock-specific Methods for Testing ===
    
    void ResetEvents() {
        Events_.clear();
        RefreshRequested_ = false;
        ExitRequested_ = false;
        PostRefreshCalled_ = false;
        LastError_.clear();
    }
    
    const std::vector<TNavigationEvent>& GetEvents() const {
        return Events_;
    }
    
    size_t GetEventCount() const {
        return Events_.size();
    }
    
    size_t CountEventsOfType(TNavigationEvent::EType type) const {
        size_t count = 0;
        for (const auto& e : Events_) {
            if (e.Type == type) ++count;
        }
        return count;
    }
    
    const TNavigationEvent* GetLastEvent() const {
        return Events_.empty() ? nullptr : &Events_.back();
    }
    
    const TNavigationEvent* GetLastEventOfType(TNavigationEvent::EType type) const {
        for (auto it = Events_.rbegin(); it != Events_.rend(); ++it) {
            if (it->Type == type) return &(*it);
        }
        return nullptr;
    }
    
    bool WasRefreshRequested() const { return RefreshRequested_; }
    bool WasExitRequested() const { return ExitRequested_; }
    bool WasPostRefreshCalled() const { return PostRefreshCalled_; }
    const TString& GetLastError() const { return LastError_; }
    
    // Configuration setters for test setup
    void SetViewerEndpoint(const TString& endpoint) { ViewerEndpoint_ = endpoint; }
    void SetDatabaseRoot(const TString& root) { DatabaseRoot_ = root; }
    void SetRefreshRate(TDuration rate) { RefreshRate_ = rate; }
    void SetMessageSizeLimit(ui64 limit) { MessageSizeLimit_ = limit; }
    
private:
    void RecordEvent(TNavigationEvent event) {
        Events_.push_back(std::move(event));
    }
    
    TAppState State_;
    TString ViewerEndpoint_;
    TString DatabaseRoot_ = "/Root";
    TDuration RefreshRate_ = TDuration::Seconds(2);
    ui64 MessageSizeLimit_ = 4096;
    
    std::vector<TNavigationEvent> Events_;
    bool RefreshRequested_ = false;
    bool ExitRequested_ = false;
    bool PostRefreshCalled_ = false;
    TString LastError_;
};

} // namespace NYdb::NConsoleClient::NTest
