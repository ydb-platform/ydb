#include "../widgets/theme.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient::NTheme;
using namespace ftxui;

Y_UNIT_TEST_SUITE(ThemeTests) {
    
    // === Column Width Constants Tests ===
    
    Y_UNIT_TEST(ColumnWidths_ArePositive) {
        UNIT_ASSERT(ColPartitionId > 0);
        UNIT_ASSERT(ColNodeId > 0);
        UNIT_ASSERT(ColOffset > 0);
        UNIT_ASSERT(ColCount > 0);
        UNIT_ASSERT(ColLag > 0);
        UNIT_ASSERT(ColBytes > 0);
        UNIT_ASSERT(ColBytesPerMin > 0);
        UNIT_ASSERT(ColDuration > 0);
        UNIT_ASSERT(ColSessionId > 0);
        UNIT_ASSERT(ColReaderName > 0);
        UNIT_ASSERT(ColConsumerName > 0);
        UNIT_ASSERT(ColSparkline > 0);
    }
    
    Y_UNIT_TEST(FormDimensions_AreReasonable) {
        UNIT_ASSERT(FormLabelWidth > 10);
        UNIT_ASSERT(FormInputWidth > 20);
        UNIT_ASSERT(FormWidth > FormLabelWidth);
    }
    
    // === GetLagColor Tests ===
    
    Y_UNIT_TEST(GetLagColor_ZeroIsNormal) {
        Color result = GetLagColor(0);
        UNIT_ASSERT_EQUAL(result, LagNormal);
    }
    
    Y_UNIT_TEST(GetLagColor_SmallIsNormal) {
        Color result = GetLagColor(50);
        UNIT_ASSERT_EQUAL(result, LagNormal);
    }
    
    Y_UNIT_TEST(GetLagColor_MediumIsWarning) {
        Color result = GetLagColor(500);
        UNIT_ASSERT_EQUAL(result, LagWarning);
    }
    
    Y_UNIT_TEST(GetLagColor_HighIsCritical) {
        Color result = GetLagColor(5000);
        UNIT_ASSERT_EQUAL(result, LagCritical);
    }
    
    Y_UNIT_TEST(GetLagColor_Boundary100) {
        // 100 exactly is normal
        Color result = GetLagColor(100);
        UNIT_ASSERT_EQUAL(result, LagNormal);
    }
    
    Y_UNIT_TEST(GetLagColor_Boundary101) {
        // 101 is warning
        Color result = GetLagColor(101);
        UNIT_ASSERT_EQUAL(result, LagWarning);
    }
    
    Y_UNIT_TEST(GetLagColor_Boundary1000) {
        // 1000 is warning
        Color result = GetLagColor(1000);
        UNIT_ASSERT_EQUAL(result, LagWarning);
    }
    
    Y_UNIT_TEST(GetLagColor_Boundary1001) {
        // 1001 is critical
        Color result = GetLagColor(1001);
        UNIT_ASSERT_EQUAL(result, LagCritical);
    }
    
    // === Color Constants Tests ===
    
    Y_UNIT_TEST(ColorConstants_AreDefined) {
        // Just verify the colors are accessible (won't crash)
        Color bg = HighlightBg;
        Color header = HeaderBg;
        Color accent = AccentText;
        Color success = SuccessText;
        Color warning = WarningText;
        Color error = ErrorText;
        
        // Use them to avoid "unused variable" warnings
        UNIT_ASSERT(bg != Color::Default || bg == Color::Default);  // Always true
        UNIT_ASSERT(header != Color::Default || header == Color::Default);
        UNIT_ASSERT(accent != Color::Default || accent == Color::Default);
        UNIT_ASSERT(success != Color::Default || success == Color::Default);
        UNIT_ASSERT(warning != Color::Default || warning == Color::Default);
        UNIT_ASSERT(error != Color::Default || error == Color::Default);
    }
    
    // === Spinner Tests ===
    
    Y_UNIT_TEST(SpinnerFrames_HasMultipleFrames) {
        UNIT_ASSERT(SpinnerFrames.size() >= 4);
    }
    
    Y_UNIT_TEST(SpinnerFrames_AllFramesNonEmpty) {
        for (const auto& frame : SpinnerFrames) {
            UNIT_ASSERT(!frame.empty());
        }
    }
}
