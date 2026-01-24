#include "mock_tui_app.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace NYdb::NConsoleClient::NTest;

// These tests verify complete user interaction scenarios using MockITuiApp.
// They test navigation sequences that would occur when users interact with views.

Y_UNIT_TEST_SUITE(ViewInteractionScenarioTests) {
    
    // === Topic List Navigation Scenarios ===
    
    Y_UNIT_TEST(Scenario_SelectTopic_ViewDetails) {
        // User selects a topic in TopicListView and views details
        TMockTuiApp app;
        
        // Simulate TopicListView selecting a topic
        app.SetTopicDetailsTarget("/Root/my-topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        // Verify correct events were recorded
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 2);
        
        auto* targetEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetTopicDetailsTarget);
        UNIT_ASSERT(targetEvent != nullptr);
        UNIT_ASSERT_EQUAL(targetEvent->Path, "/Root/my-topic");
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/my-topic");
    }
    
    Y_UNIT_TEST(Scenario_CreateNewTopic) {
        // User presses 'n' to create new topic from TopicListView
        TMockTuiApp app;
        app.GetState().CurrentPath = "/Root/mydb";
        
        // TopicListView opens create form
        app.SetTopicFormCreateMode("/Root/mydb");
        app.NavigateTo(EViewType::TopicForm);
        
        // Verify
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicForm);
        
        auto* createEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetTopicFormCreateMode);
        UNIT_ASSERT(createEvent != nullptr);
        UNIT_ASSERT_EQUAL(createEvent->Path, "/Root/mydb");
    }
    
    Y_UNIT_TEST(Scenario_DeleteTopic_Confirm) {
        // User presses 'd' to delete topic, confirms in dialog
        TMockTuiApp app;
        
        // TopicListView shows delete confirmation
        app.SetDeleteConfirmTarget("/Root/topic-to-delete");
        app.NavigateTo(EViewType::DeleteConfirm);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::DeleteConfirm);
        
        // User confirms - form handles deletion and navigates back
        app.NavigateBack();
        app.RequestRefresh();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
        UNIT_ASSERT(app.WasRefreshRequested());
    }
    
    Y_UNIT_TEST(Scenario_DeleteTopic_Cancel) {
        // User cancels delete confirmation
        TMockTuiApp app;
        
        app.SetDeleteConfirmTarget("/Root/topic");
        app.NavigateTo(EViewType::DeleteConfirm);
        
        // User presses Escape
        app.NavigateBack();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
        UNIT_ASSERT(!app.WasRefreshRequested());  // No refresh on cancel
    }
    
    // === Topic Details Navigation Scenarios ===
    
    Y_UNIT_TEST(Scenario_ViewConsumerDetails) {
        // User navigates to topic details, then selects a consumer
        TMockTuiApp app;
        
        // Navigate to topic details
        app.SetTopicDetailsTarget("/Root/orders");
        app.NavigateTo(EViewType::TopicDetails);
        
        // Select consumer
        app.SetConsumerViewTarget("/Root/orders", "analytics-consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::ConsumerDetails);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/orders");
        UNIT_ASSERT_EQUAL(app.GetState().SelectedConsumer, "analytics-consumer");
        
        // Back returns to topic details
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Scenario_ViewMessages) {
        // User views messages for a specific partition
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/events");
        app.NavigateTo(EViewType::TopicDetails);
        
        // User presses 'm' for partition 5
        app.SetMessagePreviewTarget("/Root/events", 5, 0);
        app.NavigateTo(EViewType::MessagePreview);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::MessagePreview);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedPartition, 5);
        
        auto* previewEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetMessagePreviewTarget);
        UNIT_ASSERT_EQUAL(previewEvent->Partition, 5);
        UNIT_ASSERT_EQUAL(previewEvent->Offset, 0);
    }
    
    Y_UNIT_TEST(Scenario_WriteMessage) {
        // User writes a message to a topic
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/input-stream");
        app.NavigateTo(EViewType::TopicDetails);
        
        // User presses 'w' to write message (no specific partition)
        app.SetWriteMessageTarget("/Root/input-stream", std::nullopt);
        app.NavigateTo(EViewType::WriteMessage);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::WriteMessage);
        
        auto* writeEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetWriteMessageTarget);
        UNIT_ASSERT_EQUAL(writeEvent->Path, "/Root/input-stream");
        UNIT_ASSERT(!writeEvent->OptionalPartition.has_value());
        
        // After sending, form closes
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Scenario_AddConsumer) {
        // User adds a new consumer
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetConsumerFormTarget("/Root/topic");
        app.NavigateTo(EViewType::ConsumerForm);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::ConsumerForm);
        
        // Form submits successfully
        app.NavigateBack();
        app.RequestRefresh();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT(app.WasRefreshRequested());
    }
    
    Y_UNIT_TEST(Scenario_DropConsumer) {
        // User drops (removes) a consumer
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetDropConsumerTarget("/Root/topic", "old-consumer");
        app.NavigateTo(EViewType::DropConsumerConfirm);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::DropConsumerConfirm);
        
        auto* dropEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetDropConsumerTarget);
        UNIT_ASSERT_EQUAL(dropEvent->Path, "/Root/topic");
        UNIT_ASSERT_EQUAL(dropEvent->Path2, "old-consumer");
    }
    
    Y_UNIT_TEST(Scenario_EditConsumer) {
        // User edits consumer settings from consumer view
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetConsumerViewTarget("/Root/topic", "my-consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        app.SetEditConsumerTarget("/Root/topic", "my-consumer");
        app.NavigateTo(EViewType::EditConsumer);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::EditConsumer);
        
        // Back from edit returns to consumer view
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::ConsumerDetails);
    }
    
    // === Edit Topic Scenarios ===
    
    Y_UNIT_TEST(Scenario_EditTopic_FromList) {
        // User edits topic from TopicListView
        TMockTuiApp app;
        
        app.SetTopicFormEditMode("/Root/my-topic");
        app.NavigateTo(EViewType::TopicForm);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicForm);
        
        auto* editEvent = app.GetLastEventOfType(TNavigationEvent::EType::SetTopicFormEditMode);
        UNIT_ASSERT_EQUAL(editEvent->Path, "/Root/my-topic");
        
        // Cancel returns to list
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Scenario_EditTopic_FromDetails) {
        // User edits topic from TopicDetailsView
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/my-topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetTopicFormEditMode("/Root/my-topic");
        app.NavigateTo(EViewType::TopicForm);
        
        // Submit changes
        app.NavigateBack();
        app.RequestRefresh();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT(app.WasRefreshRequested());
    }
    
    // === Error Handling Scenarios ===
    
    Y_UNIT_TEST(Scenario_FormSubmitError) {
        // Form submission fails with error
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicForm);
        
        // Simulate SDK error
        app.ShowError("Topic with this name already exists");
        
        // Still on form (user can retry or cancel)
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicForm);
        UNIT_ASSERT_EQUAL(app.GetLastError(), "Topic with this name already exists");
        
        // User cancels
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Scenario_MultipleErrors) {
        // Multiple form submissions with different errors
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicForm);
        
        app.ShowError("Invalid partition count");
        UNIT_ASSERT_EQUAL(app.GetLastError(), "Invalid partition count");
        
        // User fixes and submits again
        app.ResetEvents();  // Don't clear previous state, just events
        
        app.ShowError("Retention period too long");
        UNIT_ASSERT_EQUAL(app.GetLastError(), "Retention period too long");
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::ShowError), 1);
    }
    
    // === Deep Navigation Scenarios ===
    
    Y_UNIT_TEST(Scenario_CompleteUserJourney) {
        // Full user journey: browse -> details -> add consumer -> view consumer -> edit -> back to list
        TMockTuiApp app;
        
        // 1. View topic details
        app.SetTopicDetailsTarget("/Root/events");
        app.NavigateTo(EViewType::TopicDetails);
        
        // 2. Add a consumer
        app.SetConsumerFormTarget("/Root/events");
        app.NavigateTo(EViewType::ConsumerForm);
        app.NavigateBack();
        app.RequestRefresh();
        
        // 3. View the new consumer
        app.SetConsumerViewTarget("/Root/events", "new-consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        // 4. Edit consumer settings
        app.SetEditConsumerTarget("/Root/events", "new-consumer");
        app.NavigateTo(EViewType::EditConsumer);
        app.NavigateBack();
        app.RequestRefresh();
        
        // 5. Back to topic details
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        
        // 6. Back to topic list
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
        
        // Verify event history
        // NavigateTo: TopicDetails, ConsumerForm, ConsumerDetails, EditConsumer = 4
        // NavigateBack: ConsumerForm->TopicDetails, EditConsumer->ConsumerDetails, 
        //               ConsumerDetails->TopicDetails, TopicDetails->TopicList = 4
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::NavigateTo), 4);
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::NavigateBack), 4);
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::RequestRefresh), 2);
    }
    
    Y_UNIT_TEST(Scenario_RapidBackNavigation) {
        // User rapidly presses back multiple times
        TMockTuiApp app;
        
        // Deep navigation
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateTo(EViewType::ConsumerDetails);
        app.NavigateTo(EViewType::EditConsumer);
        
        // Rapid back navigation (3 times)
        app.NavigateBack();  // -> ConsumerDetails
        app.NavigateBack();  // -> TopicDetails
        app.NavigateBack();  // -> TopicList
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
        
        // Extra back should stay at TopicList (it's the root)
        app.NavigateBack();
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
    }
}

Y_UNIT_TEST_SUITE(ViewStateValidationTests) {
    
    Y_UNIT_TEST(State_SelectedDataPersists) {
        // Selected topic/consumer/partition should persist across navigation
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/persistent-topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetConsumerViewTarget("/Root/persistent-topic", "persistent-consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        // Navigate away and back
        app.NavigateTo(EViewType::EditConsumer);
        app.NavigateBack();
        
        // Data should still be selected
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/persistent-topic");
        UNIT_ASSERT_EQUAL(app.GetState().SelectedConsumer, "persistent-consumer");
    }
    
    Y_UNIT_TEST(State_MessagePreviewPartition) {
        // Partition selection should be stored correctly
        TMockTuiApp app;
        
        app.SetMessagePreviewTarget("/Root/topic", 42, 12345);
        app.NavigateTo(EViewType::MessagePreview);
        
        UNIT_ASSERT_EQUAL(app.GetState().SelectedPartition, 42);
        
        // Different partition
        app.NavigateBack();
        app.SetMessagePreviewTarget("/Root/topic", 99, 0);
        app.NavigateTo(EViewType::MessagePreview);
        
        UNIT_ASSERT_EQUAL(app.GetState().SelectedPartition, 99);
    }
    
    Y_UNIT_TEST(State_PreviousViewTracking) {
        // Previous view should be tracked correctly for back navigation
        TMockTuiApp app;
        
        // TopicList -> TopicDetails
        app.NavigateTo(EViewType::TopicDetails);
        UNIT_ASSERT_EQUAL(app.GetState().PreviousView, EViewType::TopicList);
        
        // TopicDetails -> TopicForm
        app.NavigateTo(EViewType::TopicForm);
        UNIT_ASSERT_EQUAL(app.GetState().PreviousView, EViewType::TopicDetails);
        
        // Back to TopicDetails
        app.NavigateBack();
        // After back, previous view might be stale, but current should be correct
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(State_RefreshRequestReset) {
        // Refresh flag should be clearable
        TMockTuiApp app;
        
        app.RequestRefresh();
        UNIT_ASSERT(app.WasRefreshRequested());
        
        app.ResetEvents();
        UNIT_ASSERT(!app.WasRefreshRequested());
    }
    
    Y_UNIT_TEST(State_ConfiguredEndpoint) {
        // Viewer endpoint configuration
        TMockTuiApp app;
        
        UNIT_ASSERT(app.GetViewerEndpoint().empty());
        
        app.SetViewerEndpoint("http://viewer.example.com:8765");
        UNIT_ASSERT_EQUAL(app.GetViewerEndpoint(), "http://viewer.example.com:8765");
    }
}

Y_UNIT_TEST_SUITE(WriteMessagePartitionTests) {
    
    Y_UNIT_TEST(WriteMessage_ToSpecificPartition) {
        TMockTuiApp app;
        
        app.SetWriteMessageTarget("/Root/topic", 7);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT(event->OptionalPartition.has_value());
        UNIT_ASSERT_EQUAL(*event->OptionalPartition, 7);
    }
    
    Y_UNIT_TEST(WriteMessage_ToAnyPartition) {
        TMockTuiApp app;
        
        app.SetWriteMessageTarget("/Root/topic", std::nullopt);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT(!event->OptionalPartition.has_value());
    }
    
    Y_UNIT_TEST(WriteMessage_PartitionZero) {
        TMockTuiApp app;
        
        // Partition 0 is valid
        app.SetWriteMessageTarget("/Root/topic", 0);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT(event->OptionalPartition.has_value());
        UNIT_ASSERT_EQUAL(*event->OptionalPartition, 0);
    }
}
