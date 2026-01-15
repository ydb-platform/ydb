#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <atomic>

namespace NYdb::NConsoleClient {

enum class EViewType {
    TopicList,
    TopicDetails,
    TopicInfo,      // Topic info full screen
    TopicTablets,   // Tablets list full screen
    ConsumerDetails,
    MessagePreview,
    Charts,
    TopicForm,
    DeleteConfirm,
    ConsumerForm,
    WriteMessage,
    OffsetForm,
    DropConsumerConfirm,
    EditConsumer
};

enum class ETopicListMode {
    Normal,
    Search,
    GoToPath
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
    
    // Current view and navigation
    EViewType CurrentView = EViewType::TopicList;
    EViewType PreviousView = EViewType::TopicList;  // For proper back navigation
    
    // Input capture mode - when true, global shortcuts are suppressed
    // Views should set this when entering text-input modes (search, dialogs, etc.)
    bool InputCaptureActive = false;
    
    // Help overlay toggle
    bool ShowHelpOverlay = false;
    
    // Topic details focus state (0 = partitions, 1 = consumers)
    int TopicDetailsFocusPanel = 0;

    // Topic list input mode (affects footer shortcuts)
    ETopicListMode TopicListMode = ETopicListMode::Normal;
    
    // Last error message
    TString LastError;
};

} // namespace NYdb::NConsoleClient
