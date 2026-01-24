#include "integration_helpers.h"
#include "../views/topic_list_view.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace NYdb::NConsoleClient::NTest;

Y_UNIT_TEST_SUITE(PathParsingTests) {
    
    // === Basic Path Parsing ===
    
    Y_UNIT_TEST(ParsePath_Simple) {
        auto result = ParseNavigationPath("/Root/my-topic");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/my-topic");
        UNIT_ASSERT(!result.PartitionId.has_value());
        UNIT_ASSERT(result.ConsumerName.empty());
    }
    
    Y_UNIT_TEST(ParsePath_WithPartition) {
        auto result = ParseNavigationPath("/Root/my-topic:5");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/my-topic");
        UNIT_ASSERT(result.PartitionId.has_value());
        UNIT_ASSERT_EQUAL(*result.PartitionId, 5);
        UNIT_ASSERT(result.ConsumerName.empty());
    }
    
    Y_UNIT_TEST(ParsePath_WithConsumer) {
        auto result = ParseNavigationPath("/Root/my-topic@my-consumer");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/my-topic");
        UNIT_ASSERT(!result.PartitionId.has_value());
        UNIT_ASSERT_EQUAL(result.ConsumerName, "my-consumer");
    }
    
    Y_UNIT_TEST(ParsePath_PartitionZero) {
        auto result = ParseNavigationPath("/Root/topic:0");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/topic");
        UNIT_ASSERT(result.PartitionId.has_value());
        UNIT_ASSERT_EQUAL(*result.PartitionId, 0);
    }
    
    Y_UNIT_TEST(ParsePath_LargePartition) {
        auto result = ParseNavigationPath("/Root/topic:999");
        
        UNIT_ASSERT(result.PartitionId.has_value());
        UNIT_ASSERT_EQUAL(*result.PartitionId, 999);
    }
    
    Y_UNIT_TEST(ParsePath_ColonNotPartition) {
        // If the part after colon is not all digits, it's not a partition
        auto result = ParseNavigationPath("/Root/topic:abc");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/topic:abc");
        UNIT_ASSERT(!result.PartitionId.has_value());
    }
    
    Y_UNIT_TEST(ParsePath_DeepPath) {
        auto result = ParseNavigationPath("/Root/dir1/dir2/dir3/topic:10");
        
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/dir1/dir2/dir3/topic");
        UNIT_ASSERT_EQUAL(*result.PartitionId, 10);
    }
    
    Y_UNIT_TEST(ParsePath_ConsumerWithDash) {
        auto result = ParseNavigationPath("/Root/topic@my-consumer-name");
        
        UNIT_ASSERT_EQUAL(result.ConsumerName, "my-consumer-name");
    }
    
    Y_UNIT_TEST(ParsePath_ConsumerPrecedence) {
        // @ takes precedence - if there's @, we don't look for :
        auto result = ParseNavigationPath("/Root/topic:5@consumer");
        
        // The @ split happens first, so topic:5 becomes the path
        UNIT_ASSERT_EQUAL(result.TopicPath, "/Root/topic:5");
        UNIT_ASSERT_EQUAL(result.ConsumerName, "consumer");
        UNIT_ASSERT(!result.PartitionId.has_value());
    }
}

Y_UNIT_TEST_SUITE(NavigationFlowTests) {
    
    // === Basic Navigation ===
    
    Y_UNIT_TEST(Navigation_InitialState) {
        TNavigationSimulator nav;
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
        UNIT_ASSERT_EQUAL(nav.GetCurrentPath(), "/Root");
    }
    
    Y_UNIT_TEST(Navigation_TopicList_To_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.SetSelectedTopic("/Root/my-topic");
        nav.NavigateTo(EViewType::TopicDetails);
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
        UNIT_ASSERT_EQUAL(nav.GetPreviousView(), EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Navigation_TopicDetails_Back_To_TopicList) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
    }
    
    // === Consumer Navigation ===
    
    Y_UNIT_TEST(Navigation_TopicDetails_To_Consumer) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.SetSelectedConsumer("my-consumer");
        nav.NavigateTo(EViewType::ConsumerDetails);
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::ConsumerDetails);
        UNIT_ASSERT_EQUAL(nav.GetPreviousView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_Consumer_Back_To_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::ConsumerDetails);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_Consumer_Direct_From_TopicList) {
        TNavigationSimulator nav;
        
        // Direct navigation from TopicList to Consumer (via go-to @consumer syntax)
        nav.NavigateTo(EViewType::ConsumerDetails);
        nav.NavigateBack();
        
        // Should go back to TopicList (the previous view)
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
    }
    
    // === Form Navigation ===
    
    Y_UNIT_TEST(Navigation_TopicList_To_TopicForm) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicForm);
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicForm);
        UNIT_ASSERT_EQUAL(nav.GetPreviousView(), EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Navigation_TopicForm_Cancel_Returns) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicForm);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Navigation_DeleteConfirm_From_TopicList) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::DeleteConfirm);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
    }
    
    Y_UNIT_TEST(Navigation_ConsumerForm_From_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::ConsumerForm);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_WriteMessage_From_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::WriteMessage);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_DropConsumer_From_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::DropConsumerConfirm);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_EditConsumer_From_ConsumerDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::ConsumerDetails);
        nav.NavigateTo(EViewType::EditConsumer);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::ConsumerDetails);
    }
    
    // === Charts and MessagePreview ===
    
    Y_UNIT_TEST(Navigation_Charts_From_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::Charts);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    Y_UNIT_TEST(Navigation_MessagePreview_From_TopicDetails) {
        TNavigationSimulator nav;
        
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::MessagePreview);
        nav.NavigateBack();
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
    }
    
    // === Complex Navigation Chains ===
    
    Y_UNIT_TEST(Navigation_DeepChain) {
        TNavigationSimulator nav;
        
        // TopicList -> TopicDetails -> Consumer -> EditConsumer -> Back -> Back -> Back
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::ConsumerDetails);
        nav.NavigateTo(EViewType::EditConsumer);
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::EditConsumer);
        
        nav.NavigateBack();  // EditConsumer -> ConsumerDetails
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::ConsumerDetails);
        
        nav.NavigateBack();  // ConsumerDetails -> TopicDetails
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicDetails);
        
        nav.NavigateBack();  // TopicDetails -> TopicList
        UNIT_ASSERT_EQUAL(nav.GetCurrentView(), EViewType::TopicList);
    }
}

Y_UNIT_TEST_SUITE(RefreshRateTests) {
    
    Y_UNIT_TEST(RefreshRate_Default) {
        TRefreshRateSimulator sim;
        
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 1);
        UNIT_ASSERT_EQUAL(sim.GetCurrentRate(), TDuration::Seconds(2));
        UNIT_ASSERT_STRINGS_EQUAL(sim.GetCurrentLabel(), "2s");
    }
    
    Y_UNIT_TEST(RefreshRate_Cycle) {
        TRefreshRateSimulator sim;
        
        // Start at 2s (index 1)
        sim.CycleRate();  // 5s
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(sim.GetCurrentLabel(), "5s");
        
        sim.CycleRate();  // 10s
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 3);
        UNIT_ASSERT_STRINGS_EQUAL(sim.GetCurrentLabel(), "10s");
        
        sim.CycleRate();  // Off
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 4);
        UNIT_ASSERT_STRINGS_EQUAL(sim.GetCurrentLabel(), "Off");
        
        sim.CycleRate();  // Wrap to 1s
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 0);
        UNIT_ASSERT_STRINGS_EQUAL(sim.GetCurrentLabel(), "1s");
        
        sim.CycleRate();  // 2s
        UNIT_ASSERT_EQUAL(sim.GetRateIndex(), 1);
    }
    
    Y_UNIT_TEST(RefreshRate_OffIsDurationMax) {
        TRefreshRateSimulator sim;
        
        // Cycle to "Off"
        sim.CycleRate();  // 5s
        sim.CycleRate();  // 10s
        sim.CycleRate();  // Off
        
        UNIT_ASSERT_EQUAL(sim.GetCurrentRate(), TDuration::Max());
    }
}

Y_UNIT_TEST_SUITE(StateManagementTests) {
    
    Y_UNIT_TEST(State_SetTopic) {
        TNavigationSimulator nav;
        
        nav.SetSelectedTopic("/Root/my-topic");
        
        UNIT_ASSERT_EQUAL(nav.GetSelectedTopic(), "/Root/my-topic");
    }
    
    Y_UNIT_TEST(State_SetConsumer) {
        TNavigationSimulator nav;
        
        nav.SetSelectedConsumer("my-consumer");
        
        UNIT_ASSERT_EQUAL(nav.GetSelectedConsumer(), "my-consumer");
    }
    
    Y_UNIT_TEST(State_SetPath) {
        TNavigationSimulator nav;
        
        nav.SetCurrentPath("/Root/subdir");
        
        UNIT_ASSERT_EQUAL(nav.GetCurrentPath(), "/Root/subdir");
    }
    
    Y_UNIT_TEST(State_NavigationPreservesData) {
        TNavigationSimulator nav;
        
        nav.SetSelectedTopic("/Root/my-topic");
        nav.SetSelectedConsumer("consumer");
        nav.NavigateTo(EViewType::TopicDetails);
        nav.NavigateTo(EViewType::ConsumerDetails);
        
        // Data should be preserved across navigation
        UNIT_ASSERT_EQUAL(nav.GetSelectedTopic(), "/Root/my-topic");
        UNIT_ASSERT_EQUAL(nav.GetSelectedConsumer(), "consumer");
    }
}

Y_UNIT_TEST_SUITE(GlobMatchingTests) {
    
    // Glob matching is implemented as static helper in TTopicListView
    // We just test the logic here
    
    bool Match(const char* pattern, const char* text) {
        return TTopicListView::GlobMatch(pattern, text);
    }
    
    Y_UNIT_TEST(Glob_ExactMatch) {
        UNIT_ASSERT(Match("test", "test"));
        UNIT_ASSERT(!Match("test", "testing"));
        UNIT_ASSERT(!Match("testing", "test"));
    }
    
    Y_UNIT_TEST(Glob_WildcardStar) {
        // Star at end
        UNIT_ASSERT(Match("test*", "test"));
        UNIT_ASSERT(Match("test*", "testing"));
        UNIT_ASSERT(Match("test*", "test-123"));
        UNIT_ASSERT(!Match("test*", "other"));
        
        // Star at beginning
        UNIT_ASSERT(Match("*test", "test"));
        UNIT_ASSERT(Match("*test", "my-test"));
        UNIT_ASSERT(!Match("*test", "test-other"));
        
        // Star in middle
        UNIT_ASSERT(Match("te*st", "test"));
        UNIT_ASSERT(Match("te*st", "te-abc-st"));
        UNIT_ASSERT(Match("t*t", "test"));
        UNIT_ASSERT(Match("t*t", "tt"));
    }
    
    Y_UNIT_TEST(Glob_WildcardQuestion) {
        UNIT_ASSERT(Match("t?st", "test"));
        UNIT_ASSERT(Match("t?st", "tast"));
        UNIT_ASSERT(!Match("t?st", "teest"));
        UNIT_ASSERT(!Match("t?st", "tst"));
    }
    
    Y_UNIT_TEST(Glob_MultipleWildcards) {
        UNIT_ASSERT(Match("*t?st*", "my-test-case"));
        UNIT_ASSERT(Match("a*b*c", "abc"));
        UNIT_ASSERT(Match("a*b*c", "a--b--c"));
        UNIT_ASSERT(!Match("a*b*c", "ac"));
    }
    
    Y_UNIT_TEST(Glob_EdgeCases) {
        UNIT_ASSERT(Match("*", ""));
        UNIT_ASSERT(Match("*", "anything"));
        UNIT_ASSERT(Match("", ""));
        UNIT_ASSERT(!Match("", "anything"));
        UNIT_ASSERT(!Match("anything", ""));
    }
}
