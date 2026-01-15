#include "../widgets/form_base.h"
#include "../widgets/sparkline.h"
#include "../widgets/table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace ftxui;

Y_UNIT_TEST_SUITE(FormBaseTests) {
    
    // === Form Navigation Pattern Tests ===
    
    Y_UNIT_TEST(FormLabel_Sizing) {
        // Verify form label width constant is reasonable
        UNIT_ASSERT(NTheme::FormLabelWidth > 10);
        UNIT_ASSERT(NTheme::FormLabelWidth <= 30);
    }
    
    Y_UNIT_TEST(FormInput_Sizing) {
        // Verify form input width constant is reasonable
        UNIT_ASSERT(NTheme::FormInputWidth > 20);
        UNIT_ASSERT(NTheme::FormInputWidth <= 60);
    }
    
    Y_UNIT_TEST(FormWidth_Total) {
        // Form width should be larger than label + input
        UNIT_ASSERT(NTheme::FormWidth >= NTheme::FormLabelWidth + 10);
    }
}

Y_UNIT_TEST_SUITE(MoreSparklineTests) {
    
    // === RenderSparkline Edge Cases ===
    
    Y_UNIT_TEST(SparklineChars_AllLevelsAvailable) {
        // Should have 9 characters: space + 8 box chars
        for (int i = 0; i <= 8; i++) {
            UNIT_ASSERT(SparklineChars[i] != nullptr);
            UNIT_ASSERT(strlen(SparklineChars[i]) > 0);
        }
    }
    
    Y_UNIT_TEST(FormatBytes_Precision) {
        // Test that precision is 1 decimal place for KB/MB/GB
        TString kb = FormatBytes(1536);
        UNIT_ASSERT(kb.Contains("1.5"));
        
        TString mb = FormatBytes(1536 * 1024);
        UNIT_ASSERT(mb.Contains("1.5"));
    }
    
    Y_UNIT_TEST(FormatDuration_WeeksNotSupported) {
        // 14 days should show as days, not weeks
        TString result = FormatDuration(TDuration::Days(14));
        UNIT_ASSERT(result.Contains("14d"));
    }
}

Y_UNIT_TEST_SUITE(AdditionalTableTests) {
    
    static TVector<TTableColumn> CreateTestColumns() {
        return {
            {"Name", -1},
            {"Value", 10},
            {"Status", 8}
        };
    }
    
    // === More Sort Tests ===
    
    Y_UNIT_TEST(Sort_DefaultsToFirstColumnAscending) {
        TTable table(CreateTestColumns());
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 0);
        UNIT_ASSERT(table.IsSortAscending());
    }
    
    Y_UNIT_TEST(Sort_SetSortOutOfBounds_Handled) {
        TTable table(CreateTestColumns());
        // Setting sort to column that doesn't exist should be handled gracefully
        table.SetSort(100, true);  // Column 100 doesn't exist
        // Should not crash, behavior is implementation-defined
        UNIT_ASSERT(true);
    }
    
    Y_UNIT_TEST(ToggleSort_DirectAPI_ChangesColumn) {
        TTable table(CreateTestColumns());
        table.SetSort(0, true);  // Start at column 0, ascending
        
        // Toggle to different column
        table.ToggleSort(1);
        
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);
        UNIT_ASSERT(table.IsSortAscending());  // New column starts ascending
    }
    
    Y_UNIT_TEST(ToggleSort_DirectAPI_TogglesDirection) {
        TTable table(CreateTestColumns());
        table.SetSort(1, true);  // Start at column 1, ascending
        
        // Toggle same column
        table.ToggleSort(1);
        
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);  // Same column
        UNIT_ASSERT(!table.IsSortAscending());       // Now descending
    }
    
    // === Multiple Updates ===
    
    Y_UNIT_TEST(UpdateCell_MultipleConsecutive) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        table.UpdateCell(0, 0, "first");
        table.UpdateCell(0, 0, "second");
        table.UpdateCell(0, 0, "third");
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "third");
    }
    
    Y_UNIT_TEST(UpdateCell_TracksChangedTime) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        table.SetCell(0, 0, "initial");
        
        // Wait a tiny bit then update
        table.UpdateCell(0, 0, "changed");
        
        // ChangedAt should be set
        UNIT_ASSERT(table.GetRow(0).Cells[0].ChangedAt != TInstant());
    }
    
    // === Large Table Tests ===
    
    Y_UNIT_TEST(Table_LargeRowCount) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1000);
        
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 1000);
        
        // Set first and last rows
        table.SetCell(0, 0, "first");
        table.SetCell(999, 0, "last");
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "first");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(999).Cells[0].Text.c_str(), "last");
    }
    
    Y_UNIT_TEST(Table_NavigateToEnd_LargeTable) {
        TTable table(CreateTestColumns());
        table.SetRowCount(100);
        table.SetFocused(true);
        table.SetSelectedRow(0);
        
        table.HandleEvent(Event::End);
        
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 99);
    }
    
    Y_UNIT_TEST(Table_NavigateToHome_LargeTable) {
        TTable table(CreateTestColumns());
        table.SetRowCount(100);
        table.SetFocused(true);
        table.SetSelectedRow(99);
        
        table.HandleEvent(Event::Home);
        
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 0);
    }
}

Y_UNIT_TEST_SUITE(TTableCellAdvancedTests) {
    
    Y_UNIT_TEST(TTableCell_ColorConstructor) {
        TTableCell cell("colored", Color::Red);
        UNIT_ASSERT_STRINGS_EQUAL(cell.Text.c_str(), "colored");
        UNIT_ASSERT_EQUAL(cell.TextColor, Color::Red);
    }
    
    Y_UNIT_TEST(TTableCell_WithColor) {
        TTableCell cell("test");
        cell.WithColor(Color::Blue);
        UNIT_ASSERT_EQUAL(cell.TextColor, Color::Blue);
    }
    
    Y_UNIT_TEST(TTableCell_WithBgColor) {
        TTableCell cell("test");
        cell.WithBgColor(Color::GrayDark);
        UNIT_ASSERT_EQUAL(cell.BgColor, Color::GrayDark);
    }
    
    Y_UNIT_TEST(TTableCell_FullChaining) {
        TTableCell cell("styled");
        cell.WithColor(Color::Green)
            .WithBgColor(Color::Black)
            .WithBold()
            .WithDim();
        
        UNIT_ASSERT_EQUAL(cell.TextColor, Color::Green);
        UNIT_ASSERT_EQUAL(cell.BgColor, Color::Black);
        UNIT_ASSERT(cell.Bold);
        UNIT_ASSERT(cell.Dim);
    }
}

Y_UNIT_TEST_SUITE(TTableRowAdvancedTests) {
    
    Y_UNIT_TEST(TTableRow_VectorConstructor) {
        TVector<TTableCell> cells = {
            TTableCell("a"),
            TTableCell("b"),
            TTableCell("c")
        };
        TTableRow row(cells);
        
        UNIT_ASSERT_EQUAL(row.Cells.size(), 3);
        UNIT_ASSERT_STRINGS_EQUAL(row.Cells[0].Text.c_str(), "a");
    }
    
    Y_UNIT_TEST(TTableRow_WithColor) {
        TTableRow row;
        row.WithColor(Color::Yellow);
        UNIT_ASSERT_EQUAL(row.RowColor, Color::Yellow);
    }
    
    Y_UNIT_TEST(TTableRow_WithBgColor) {
        TTableRow row;
        row.WithBgColor(Color::Blue);
        UNIT_ASSERT_EQUAL(row.RowBgColor, Color::Blue);
    }
    
    Y_UNIT_TEST(TTableRow_FullChaining) {
        TTableRow row;
        row.WithType("custom")
           .WithColor(Color::Magenta)
           .WithBgColor(Color::Black)
           .WithBold()
           .WithDim();
        
        UNIT_ASSERT_STRINGS_EQUAL(row.RowType.c_str(), "custom");
        UNIT_ASSERT_EQUAL(row.RowColor, Color::Magenta);
        UNIT_ASSERT_EQUAL(row.RowBgColor, Color::Black);
        UNIT_ASSERT(row.RowBold);
        UNIT_ASSERT(row.RowDim);
    }
}

// ======================================================================
// OffsetForm Validation Tests
// Test the offset validation logic used in TOffsetForm::HandleSubmit()
// ======================================================================

#include <util/string/cast.h>

namespace {

// Simulates TOffsetForm validation logic
struct TOffsetValidation {
    ui64 EndOffset = 0;
    std::string OffsetInput;
    TString ErrorMessage;
    
    // Returns true if validation passes, false otherwise
    bool Validate() {
        ui64 newOffset;
        try {
            newOffset = FromString<ui64>(TString(OffsetInput.c_str()));
        } catch (...) {
            ErrorMessage = "Invalid offset value";
            return false;
        }
        
        if (newOffset > EndOffset) {
            ErrorMessage = "Offset cannot exceed end offset (" + std::to_string(EndOffset) + ")";
            return false;
        }
        
        ErrorMessage.clear();
        return true;
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(OffsetValidationTests) {
    
    // === Valid Offset Parsing ===
    
    Y_UNIT_TEST(ValidOffset_Zero) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "0";
        UNIT_ASSERT(v.Validate());
        UNIT_ASSERT(v.ErrorMessage.empty());
    }
    
    Y_UNIT_TEST(ValidOffset_Positive) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "50";
        UNIT_ASSERT(v.Validate());
        UNIT_ASSERT(v.ErrorMessage.empty());
    }
    
    Y_UNIT_TEST(ValidOffset_ExactlyAtEnd) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "100";
        UNIT_ASSERT(v.Validate());  // Offset == EndOffset is valid
        UNIT_ASSERT(v.ErrorMessage.empty());
    }
    
    Y_UNIT_TEST(ValidOffset_LargeNumber) {
        TOffsetValidation v;
        v.EndOffset = 999999999999ULL;
        v.OffsetInput = "999999999999";
        UNIT_ASSERT(v.Validate());
        UNIT_ASSERT(v.ErrorMessage.empty());
    }
    
    // === Invalid Offset Parsing ===
    
    Y_UNIT_TEST(InvalidOffset_NonNumeric) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "abc";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
    
    Y_UNIT_TEST(InvalidOffset_Empty) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
    
    Y_UNIT_TEST(InvalidOffset_Negative) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "-1";
        UNIT_ASSERT(!v.Validate());  // FromString<ui64> fails on negative
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
    
    Y_UNIT_TEST(InvalidOffset_Decimal) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "50.5";
        UNIT_ASSERT(!v.Validate());  // ui64 doesn't accept decimals
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
    
    Y_UNIT_TEST(InvalidOffset_WithSpaces) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = " 50 ";
        // FromString may or may not handle leading/trailing spaces
        // This test documents the behavior
        bool result = v.Validate();
        // If it fails, it should be "Invalid offset value"
        if (!result) {
            UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
        }
    }
    
    Y_UNIT_TEST(InvalidOffset_MixedContent) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "50abc";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
    
    // === Range Validation ===
    
    Y_UNIT_TEST(RangeError_ExceedsEnd) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "101";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT(v.ErrorMessage.Contains("cannot exceed"));
        UNIT_ASSERT(v.ErrorMessage.Contains("100"));
    }
    
    Y_UNIT_TEST(RangeError_WayOverEnd) {
        TOffsetValidation v;
        v.EndOffset = 100;
        v.OffsetInput = "99999";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT(v.ErrorMessage.Contains("cannot exceed"));
    }
    
    Y_UNIT_TEST(RangeError_EndOffsetZero) {
        TOffsetValidation v;
        v.EndOffset = 0;
        v.OffsetInput = "1";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT(v.ErrorMessage.Contains("cannot exceed"));
        UNIT_ASSERT(v.ErrorMessage.Contains("0"));
    }
    
    // === Boundary Cases ===
    
    Y_UNIT_TEST(Boundary_ZeroOffsetZeroEnd) {
        TOffsetValidation v;
        v.EndOffset = 0;
        v.OffsetInput = "0";
        UNIT_ASSERT(v.Validate());  // 0 <= 0 is valid
    }
    
    Y_UNIT_TEST(Boundary_MaxUi64) {
        TOffsetValidation v;
        v.EndOffset = std::numeric_limits<ui64>::max();
        v.OffsetInput = std::to_string(std::numeric_limits<ui64>::max());
        UNIT_ASSERT(v.Validate());
    }
    
    Y_UNIT_TEST(Boundary_OverflowAttempt) {
        TOffsetValidation v;
        v.EndOffset = 100;
        // A number larger than ui64::max should fail to parse
        v.OffsetInput = "999999999999999999999999999999";
        UNIT_ASSERT(!v.Validate());
        UNIT_ASSERT_STRINGS_EQUAL(v.ErrorMessage.c_str(), "Invalid offset value");
    }
}

