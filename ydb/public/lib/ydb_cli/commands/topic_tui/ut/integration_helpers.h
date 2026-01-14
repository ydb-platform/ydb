#pragma once

#include "../topic_tui_app.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NConsoleClient::NTest {

// Test helper to access TAppState without full app initialization
// This allows testing navigation logic and state transitions

// Navigation path parsing result
struct TPathParseResult {
    TString TopicPath;
    std::optional<ui32> PartitionId;
    TString ConsumerName;
};

// Parse path with optional :partition or @consumer suffix
// Format: /path/topic:partition or /path/topic@consumer
inline TPathParseResult ParseNavigationPath(const TString& inputPath) {
    TPathParseResult result;
    
    TStringBuf pathBuf = inputPath;
    
    // First, check for @consumer suffix
    TStringBuf base, consumerPart;
    if (pathBuf.TryRSplit('@', base, consumerPart)) {
        result.ConsumerName = TString(consumerPart);
        result.TopicPath = TString(base);
        return result;
    }
    
    // Check for :partition suffix
    TStringBuf partBase, partitionStr;
    if (pathBuf.TryRSplit(':', partBase, partitionStr)) {
        bool isPartition = !partitionStr.empty();
        for (char c : partitionStr) {
            if (!std::isdigit(c)) {
                isPartition = false;
                break;
            }
        }
        if (isPartition) {
            result.TopicPath = TString(partBase);
            result.PartitionId = FromString<ui32>(partitionStr);
            return result;
        }
    }
    
    result.TopicPath = inputPath;
    return result;
}

// State transition helper - simulates what would happen on view changes
class TNavigationSimulator {
public:
    TNavigationSimulator() {
        State_.CurrentPath = "/Root";
        State_.CurrentView = EViewType::TopicList;
        State_.PreviousView = EViewType::TopicList;
    }
    
    // Navigate to a view
    void NavigateTo(EViewType view) {
        // Track previous view for forms/dialogs and main views
        if (view == EViewType::TopicForm || view == EViewType::DeleteConfirm ||
            view == EViewType::ConsumerForm || view == EViewType::WriteMessage ||
            view == EViewType::DropConsumerConfirm || view == EViewType::EditConsumer ||
            view == EViewType::ConsumerDetails || view == EViewType::TopicDetails ||
            view == EViewType::MessagePreview || view == EViewType::Charts) {
            State_.PreviousView = State_.CurrentView;
        }
        State_.CurrentView = view;
    }
    
    // Navigate back
    void NavigateBack() {
        switch (State_.CurrentView) {
            case EViewType::TopicDetails:
                State_.CurrentView = EViewType::TopicList;
                break;
            case EViewType::ConsumerDetails:
                State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                    ? EViewType::TopicList : EViewType::TopicDetails;
                break;
            case EViewType::MessagePreview:
                State_.CurrentView = (State_.PreviousView == EViewType::TopicList) 
                    ? EViewType::TopicList : EViewType::TopicDetails;
                break;
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
    
    // Set topic for navigation
    void SetSelectedTopic(const TString& topic) {
        State_.SelectedTopic = topic;
    }
    
    void SetSelectedConsumer(const TString& consumer) {
        State_.SelectedConsumer = consumer;
    }
    
    void SetCurrentPath(const TString& path) {
        State_.CurrentPath = path;
    }
    
    // Getters
    EViewType GetCurrentView() const { return State_.CurrentView; }
    EViewType GetPreviousView() const { return State_.PreviousView; }
    TString GetSelectedTopic() const { return State_.SelectedTopic; }
    TString GetSelectedConsumer() const { return State_.SelectedConsumer; }
    TString GetCurrentPath() const { return State_.CurrentPath; }
    
private:
    TAppState State_;
};

// Refresh rate cycling helper
class TRefreshRateSimulator {
public:
    TRefreshRateSimulator() : RateIndex_(1) {}  // Start at 2s (index 1)
    
    static constexpr TDuration Rates[] = {
        TDuration::Seconds(1),
        TDuration::Seconds(2),
        TDuration::Seconds(5),
        TDuration::Seconds(10),
        TDuration::Max()  // Off
    };
    
    static constexpr const char* Labels[] = {"1s", "2s", "5s", "10s", "Off"};
    static constexpr int NumRates = 5;
    
    void CycleRate() {
        RateIndex_ = (RateIndex_ + 1) % NumRates;
    }
    
    TDuration GetCurrentRate() const {
        return Rates[RateIndex_];
    }
    
    const char* GetCurrentLabel() const {
        return Labels[RateIndex_];
    }
    
    int GetRateIndex() const {
        return RateIndex_;
    }
    
private:
    int RateIndex_;
};

} // namespace NYdb::NConsoleClient::NTest
