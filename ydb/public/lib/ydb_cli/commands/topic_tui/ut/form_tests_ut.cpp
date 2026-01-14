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
