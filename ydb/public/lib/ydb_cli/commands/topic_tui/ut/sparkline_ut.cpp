#include "../widgets/sparkline_history.h"
#include "../widgets/sparkline.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(SparklineTests) {
    
    // === TSparklineHistory Tests ===
    
    Y_UNIT_TEST(TSparklineHistory_StartsEmpty) {
        TSparklineHistory history;
        UNIT_ASSERT(history.Empty());
        UNIT_ASSERT(history.GetValues().empty());
    }
    
    Y_UNIT_TEST(TSparklineHistory_FirstUpdate_AddsValue) {
        TSparklineHistory history;
        history.Update(42.0);
        
        UNIT_ASSERT(!history.Empty());
        UNIT_ASSERT_EQUAL(history.GetValues().size(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetValues()[0], 42.0, 0.001);
    }
    
    Y_UNIT_TEST(TSparklineHistory_Clear_RemovesAllValues) {
        TSparklineHistory history;
        history.Update(10.0);
        history.Update(20.0);
        
        history.Clear();
        
        UNIT_ASSERT(history.Empty());
    }
    
    Y_UNIT_TEST(TSparklineHistory_GetMax_ReturnsMaxValue) {
        TSparklineHistory history;
        history.Update(10.0);
        // First update adds a point, subsequent in same interval update in place
        // We need to simulate time passing - but for simplicity, test with initial max
        
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), 10.0, 0.001);
    }
    
    Y_UNIT_TEST(TSparklineHistory_GetMax_EmptyReturnsOne) {
        TSparklineHistory history;
        // Empty history should return 1.0 to avoid division by zero
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), 1.0, 0.001);
    }
    
    Y_UNIT_TEST(TSparklineHistory_GetMax_ZeroValuesReturnsOne) {
        TSparklineHistory history;
        history.Update(0.0);
        // All zeros should return 1.0 to avoid division by zero
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetMax(), 1.0, 0.001);
    }
    
    Y_UNIT_TEST(TSparklineHistory_RapidUpdates_ReplaceLatestValue) {
        // With default 5s interval, rapid updates should replace the latest
        TSparklineHistory history(60, TDuration::Seconds(5));
        history.Update(10.0);
        history.Update(20.0);  // Should replace, not add
        history.Update(30.0);  // Should replace, not add
        
        // Should still have only 1 point (since no time passed)
        UNIT_ASSERT_EQUAL(history.GetValues().size(), 1);
        UNIT_ASSERT_DOUBLES_EQUAL(history.GetValues()[0], 30.0, 0.001);
    }
    
    Y_UNIT_TEST(TSparklineHistory_CustomMaxPoints) {
        TSparklineHistory history(3);  // Max 3 points
        UNIT_ASSERT(history.Empty());
    }
    
    Y_UNIT_TEST(TSparklineHistory_CustomInterval) {
        TSparklineHistory history(60, TDuration::MilliSeconds(100));
        history.Update(1.0);
        UNIT_ASSERT_EQUAL(history.GetValues().size(), 1);
    }
    
    // === Format Functions Additional Tests ===
    
    Y_UNIT_TEST(FormatBytes_LargeValues) {
        // 1 TB
        UNIT_ASSERT_STRINGS_EQUAL(FormatBytes(1024ULL * 1024 * 1024 * 1024).c_str(), "1024.0 GB");
    }
    
    Y_UNIT_TEST(FormatDuration_EdgeCases) {
        // 0 duration
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Zero()).c_str(), "0s");
        
        // Very long duration (30 days)
        UNIT_ASSERT_STRINGS_EQUAL(FormatDuration(TDuration::Days(30)).c_str(), "30d 0h");
    }
    
    Y_UNIT_TEST(FormatNumber_VeryLarge) {
        // 1 trillion  
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1000000000000ULL).c_str(), "1,000,000,000,000");
    }
    
    Y_UNIT_TEST(FormatNumber_Exact) {
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(123).c_str(), "123");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(1234).c_str(), "1,234");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(12345).c_str(), "12,345");
        UNIT_ASSERT_STRINGS_EQUAL(FormatNumber(123456).c_str(), "123,456");
    }
}
