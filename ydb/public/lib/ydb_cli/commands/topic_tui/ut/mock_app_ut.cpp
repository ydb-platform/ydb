#include "mock_tui_app.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace NYdb::NConsoleClient::NTest;

Y_UNIT_TEST_SUITE(MockTuiAppTests) {
    
    // === Basic State Tests ===
    
    Y_UNIT_TEST(InitialState) {
        TMockTuiApp app;
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
        UNIT_ASSERT_EQUAL(app.GetState().CurrentPath, "/Root");
        UNIT_ASSERT_EQUAL(app.GetDatabaseRoot(), "/Root");
        UNIT_ASSERT_EQUAL(app.GetRefreshRate(), TDuration::Seconds(2));
        UNIT_ASSERT(app.GetViewerEndpoint().empty());
    }
    
    Y_UNIT_TEST(ConfigurationSetters) {
        TMockTuiApp app;
        
        app.SetViewerEndpoint("http://localhost:8765");
        app.SetDatabaseRoot("/MyDB");
        app.SetRefreshRate(TDuration::Seconds(5));
        
        UNIT_ASSERT_EQUAL(app.GetViewerEndpoint(), "http://localhost:8765");
        UNIT_ASSERT_EQUAL(app.GetDatabaseRoot(), "/MyDB");
        UNIT_ASSERT_EQUAL(app.GetRefreshRate(), TDuration::Seconds(5));
    }
    
    // === Navigation Tests ===
    
    Y_UNIT_TEST(NavigateTo_RecordsEvent) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 1);
        auto* event = app.GetLastEvent();
        UNIT_ASSERT(event != nullptr);
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::NavigateTo);
        UNIT_ASSERT_EQUAL(event->ViewType, EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(NavigateTo_UpdatesState) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT_EQUAL(app.GetState().PreviousView, EViewType::TopicList);
    }
    
    Y_UNIT_TEST(NavigateBack_RecordsEvent) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateBack();
        
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 2);
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::NavigateBack), 1);
    }
    
    Y_UNIT_TEST(NavigateBack_UpdatesState) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateBack();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
    }
    
    Y_UNIT_TEST(NavigateBack_FormReturnsToPreview) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateTo(EViewType::TopicForm);
        app.NavigateBack();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
    }
    
    // === Target Setters Tests ===
    
    Y_UNIT_TEST(SetTopicDetailsTarget_RecordsAndSetsState) {
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/my-topic");
        
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 1);
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetTopicDetailsTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/my-topic");
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/my-topic");
    }
    
    Y_UNIT_TEST(SetConsumerViewTarget_RecordsBothPaths) {
        TMockTuiApp app;
        
        app.SetConsumerViewTarget("/Root/topic", "my-consumer");
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetConsumerViewTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic");
        UNIT_ASSERT_EQUAL(event->Path2, "my-consumer");
        UNIT_ASSERT_EQUAL(app.GetState().SelectedConsumer, "my-consumer");
    }
    
    Y_UNIT_TEST(SetMessagePreviewTarget_RecordsPartitionAndOffset) {
        TMockTuiApp app;
        
        app.SetMessagePreviewTarget("/Root/topic", 5, 1000);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetMessagePreviewTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic");
        UNIT_ASSERT_EQUAL(event->Partition, 5);
        UNIT_ASSERT_EQUAL(event->Offset, 1000);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedPartition, 5);
    }
    
    Y_UNIT_TEST(SetTopicFormModes) {
        TMockTuiApp app;
        
        app.SetTopicFormCreateMode("/Root");
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Type, TNavigationEvent::EType::SetTopicFormCreateMode);
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Path, "/Root");
        
        app.SetTopicFormEditMode("/Root/my-topic");
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Type, TNavigationEvent::EType::SetTopicFormEditMode);
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Path, "/Root/my-topic");
    }
    
    Y_UNIT_TEST(SetDeleteConfirmTarget) {
        TMockTuiApp app;
        
        app.SetDeleteConfirmTarget("/Root/topic-to-delete");
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetDeleteConfirmTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic-to-delete");
    }
    
    Y_UNIT_TEST(SetDropConsumerTarget) {
        TMockTuiApp app;
        
        app.SetDropConsumerTarget("/Root/topic", "consumer-to-drop");
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetDropConsumerTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic");
        UNIT_ASSERT_EQUAL(event->Path2, "consumer-to-drop");
    }
    
    Y_UNIT_TEST(SetWriteMessageTarget_WithPartition) {
        TMockTuiApp app;
        
        app.SetWriteMessageTarget("/Root/topic", 3);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetWriteMessageTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic");
        UNIT_ASSERT(event->OptionalPartition.has_value());
        UNIT_ASSERT_EQUAL(*event->OptionalPartition, 3);
    }
    
    Y_UNIT_TEST(SetWriteMessageTarget_NoPartition) {
        TMockTuiApp app;
        
        app.SetWriteMessageTarget("/Root/topic", std::nullopt);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT(!event->OptionalPartition.has_value());
    }
    
    Y_UNIT_TEST(SetOffsetFormTarget_RecordsAllFields) {
        TMockTuiApp app;
        
        app.SetOffsetFormTarget("/Root/topic", "my-consumer", 7, 1000, 5000);
        
        auto* event = app.GetLastEvent();
        UNIT_ASSERT_EQUAL(event->Type, TNavigationEvent::EType::SetOffsetFormTarget);
        UNIT_ASSERT_EQUAL(event->Path, "/Root/topic");
        UNIT_ASSERT_EQUAL(event->Path2, "my-consumer");
        UNIT_ASSERT_EQUAL(event->Partition, 7);
        UNIT_ASSERT_EQUAL(event->CurrentOffset, 1000);
        UNIT_ASSERT_EQUAL(event->EndOffset, 5000);
    }
    
    // === UI Action Tests ===
    
    Y_UNIT_TEST(ShowError_RecordsMessage) {
        TMockTuiApp app;
        
        app.ShowError("Something went wrong");
        
        UNIT_ASSERT_EQUAL(app.GetLastError(), "Something went wrong");
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Type, TNavigationEvent::EType::ShowError);
        UNIT_ASSERT_EQUAL(app.GetLastEvent()->Path, "Something went wrong");
    }
    
    Y_UNIT_TEST(RequestRefresh_SetsFlag) {
        TMockTuiApp app;
        
        UNIT_ASSERT(!app.WasRefreshRequested());
        app.RequestRefresh();
        UNIT_ASSERT(app.WasRefreshRequested());
    }
    
    Y_UNIT_TEST(RequestExit_SetsFlag) {
        TMockTuiApp app;
        
        UNIT_ASSERT(!app.WasExitRequested());
        app.RequestExit();
        UNIT_ASSERT(app.WasExitRequested());
    }
    
    Y_UNIT_TEST(PostRefresh_SetsFlag) {
        TMockTuiApp app;
        
        UNIT_ASSERT(!app.WasPostRefreshCalled());
        app.PostRefresh();
        UNIT_ASSERT(app.WasPostRefreshCalled());
    }
    
    // === Refresh Rate Tests ===
    
    Y_UNIT_TEST(CycleRefreshRate) {
        TMockTuiApp app;
        
        // Default is 2s
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "2s");
        
        app.CycleRefreshRate();  // 5s
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "5s");
        
        app.CycleRefreshRate();  // 10s
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "10s");
        
        app.CycleRefreshRate();  // Off
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "Off");
        
        app.CycleRefreshRate();  // 1s (wrap)
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "1s");
        
        app.CycleRefreshRate();  // 2s
        UNIT_ASSERT_STRINGS_EQUAL(app.GetRefreshRateLabel(), "2s");
    }
    
    // === Event Tracking Tests ===
    
    Y_UNIT_TEST(ResetEvents) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.RequestRefresh();
        app.ShowError("error");
        
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 3);
        UNIT_ASSERT(app.WasRefreshRequested());
        UNIT_ASSERT(!app.GetLastError().empty());
        
        app.ResetEvents();
        
        UNIT_ASSERT_EQUAL(app.GetEventCount(), 0);
        UNIT_ASSERT(!app.WasRefreshRequested());
        UNIT_ASSERT(app.GetLastError().empty());
    }
    
    Y_UNIT_TEST(CountEventsOfType) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateTo(EViewType::ConsumerDetails);
        app.NavigateBack();
        app.NavigateTo(EViewType::TopicForm);
        
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::NavigateTo), 3);
        UNIT_ASSERT_EQUAL(app.CountEventsOfType(TNavigationEvent::EType::NavigateBack), 1);
    }
    
    Y_UNIT_TEST(GetLastEventOfType) {
        TMockTuiApp app;
        
        app.SetTopicDetailsTarget("/Root/topic1");
        app.NavigateTo(EViewType::TopicDetails);
        app.SetTopicDetailsTarget("/Root/topic2");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        auto* lastNav = app.GetLastEventOfType(TNavigationEvent::EType::NavigateTo);
        UNIT_ASSERT(lastNav != nullptr);
        UNIT_ASSERT_EQUAL(lastNav->ViewType, EViewType::ConsumerDetails);
        
        auto* lastTarget = app.GetLastEventOfType(TNavigationEvent::EType::SetTopicDetailsTarget);
        UNIT_ASSERT(lastTarget != nullptr);
        UNIT_ASSERT_EQUAL(lastTarget->Path, "/Root/topic2");
    }
}

Y_UNIT_TEST_SUITE(ITuiAppNavigationPatternTests) {
    
    // Test common navigation patterns using MockITuiApp
    
    Y_UNIT_TEST(PrepareAndNavigate_TopicDetails) {
        TMockTuiApp app;
        
        // Pattern: Set target, then navigate
        app.SetTopicDetailsTarget("/Root/my-topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/my-topic");
    }
    
    Y_UNIT_TEST(PrepareAndNavigate_Consumer) {
        TMockTuiApp app;
        
        app.SetConsumerViewTarget("/Root/topic", "my-consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::ConsumerDetails);
        UNIT_ASSERT_EQUAL(app.GetState().SelectedTopic, "/Root/topic");
        UNIT_ASSERT_EQUAL(app.GetState().SelectedConsumer, "my-consumer");
    }
    
    Y_UNIT_TEST(FormSubmitWithRefresh) {
        TMockTuiApp app;
        
        // Simulate form submission pattern
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateTo(EViewType::TopicForm);
        
        // Form submits successfully
        app.NavigateBack();
        app.RequestRefresh();
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        UNIT_ASSERT(app.WasRefreshRequested());
    }
    
    Y_UNIT_TEST(FormSubmitWithError) {
        TMockTuiApp app;
        
        app.NavigateTo(EViewType::TopicDetails);
        app.NavigateTo(EViewType::TopicForm);
        
        // Form submission fails
        app.ShowError("Failed to create topic");
        
        // Still on form
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicForm);
        UNIT_ASSERT_EQUAL(app.GetLastError(), "Failed to create topic");
    }
    
    Y_UNIT_TEST(DeepNavigationChain) {
        TMockTuiApp app;
        
        // TopicList -> TopicDetails -> Consumer -> DropConsumerConfirm -> back x3
        app.SetTopicDetailsTarget("/Root/topic");
        app.NavigateTo(EViewType::TopicDetails);
        
        app.SetConsumerViewTarget("/Root/topic", "consumer");
        app.NavigateTo(EViewType::ConsumerDetails);
        
        app.SetDropConsumerTarget("/Root/topic", "consumer");
        app.NavigateTo(EViewType::DropConsumerConfirm);
        
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::DropConsumerConfirm);
        
        app.NavigateBack();  // -> ConsumerDetails
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::ConsumerDetails);
        
        app.NavigateBack();  // -> TopicDetails
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicDetails);
        
        app.NavigateBack();  // -> TopicList
        UNIT_ASSERT_EQUAL(app.GetState().CurrentView, EViewType::TopicList);
    }
}
