#include "../widgets/table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;
using namespace ftxui;

Y_UNIT_TEST_SUITE(TTableTests) {
    
    static TVector<TTableColumn> CreateTestColumns() {
        return {
            {"Name", -1},  // flex
            {"Value", 10},
            {"Status", 8}
        };
    }
    
    // === Row Count Tests ===
    
    Y_UNIT_TEST(SetRowCount_AddsRows) {
        TTable table(CreateTestColumns());
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 0);
        
        table.SetRowCount(5);
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 5);
    }
    
    Y_UNIT_TEST(SetRowCount_ReducesRows) {
        TTable table(CreateTestColumns());
        table.SetRowCount(10);
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 10);
        
        table.SetRowCount(3);
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 3);
    }
    
    Y_UNIT_TEST(SetRowCount_ClampsSelection) {
        TTable table(CreateTestColumns());
        table.SetRowCount(10);
        table.SetSelectedRow(9);  // Last row
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 9);
        
        table.SetRowCount(5);
        // Selection should be clamped to valid range
        UNIT_ASSERT(table.GetSelectedRow() < 5);
    }
    
    Y_UNIT_TEST(Clear_RemovesAllRows) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.Clear();
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 0);
    }
    
    // === Cell Operations Tests ===
    
    Y_UNIT_TEST(SetCell_StoresValue) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        table.SetCell(0, 0, "test value");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "test value");
    }
    
    Y_UNIT_TEST(UpdateCell_ReturnsTrue_WhenValueChanges) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        table.SetCell(0, 0, "initial");
        
        bool changed = table.UpdateCell(0, 0, "new value");
        UNIT_ASSERT(changed);
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "new value");
    }
    
    Y_UNIT_TEST(UpdateCell_ReturnsFalse_WhenValueSame) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        table.SetCell(0, 0, "same value");
        
        bool changed = table.UpdateCell(0, 0, "same value");
        UNIT_ASSERT(!changed);
    }
    
    Y_UNIT_TEST(UpdateCell_OutOfBounds_ReturnsFalse) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        bool changed = table.UpdateCell(5, 0, "value");  // Row out of bounds
        UNIT_ASSERT(!changed);
        
        changed = table.UpdateCell(0, 10, "value");  // Column out of bounds
        UNIT_ASSERT(!changed);
    }
    
    // === Selection Tests ===
    
    Y_UNIT_TEST(SetSelectedRow_ValidRange) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        
        table.SetSelectedRow(3);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 3);
    }
    
    Y_UNIT_TEST(SetSelectedRow_NegativeIgnored) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetSelectedRow(2);
        
        table.SetSelectedRow(-1);  // Should be ignored
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 2);
    }
    
    Y_UNIT_TEST(SetSelectedRow_OutOfBoundsIgnored) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetSelectedRow(2);
        
        table.SetSelectedRow(10);  // Should be ignored
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 2);
    }
    
    // === Sort State Tests ===
    
    Y_UNIT_TEST(SetSort_ChangesState) {
        TTable table(CreateTestColumns());
        
        table.SetSort(1, false);
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);
        UNIT_ASSERT_EQUAL(table.IsSortAscending(), false);
    }
    
    Y_UNIT_TEST(SetSort_FiresCallback) {
        TTable table(CreateTestColumns());
        
        int callbackCol = -1;
        bool callbackAsc = true;
        table.OnSortChanged = [&](int col, bool asc) {
            callbackCol = col;
            callbackAsc = asc;
        };
        
        table.SetSort(2, false);
        
        UNIT_ASSERT_EQUAL(callbackCol, 2);
        UNIT_ASSERT_EQUAL(callbackAsc, false);
    }
    
    Y_UNIT_TEST(ToggleSort_NewColumn_StartsAscending) {
        TTable table(CreateTestColumns());
        table.SetSort(0, false);  // Start with column 0 descending
        
        table.ToggleSort(1);  // Switch to new column
        
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);
        UNIT_ASSERT(table.IsSortAscending());  // Should start ascending
    }
    
    Y_UNIT_TEST(ToggleSort_SameColumn_TogglesDirection) {
        TTable table(CreateTestColumns());
        table.SetSort(1, true);
        
        table.ToggleSort(1);  // Same column
        
        UNIT_ASSERT_EQUAL(table.GetSortColumn(), 1);
        UNIT_ASSERT(!table.IsSortAscending());  // Should toggle to descending
    }
    
    // === Focus Tests ===
    
    Y_UNIT_TEST(SetFocused_ChangesState) {
        TTable table(CreateTestColumns());
        
        // Default is focused (IsFocused_ = true)
        UNIT_ASSERT(table.IsFocused());
        
        table.SetFocused(false);
        UNIT_ASSERT(!table.IsFocused());
        
        table.SetFocused(true);
        UNIT_ASSERT(table.IsFocused());
    }
    
    // === TTableCell Builder Pattern Tests ===
    
    Y_UNIT_TEST(TTableCell_DefaultConstructor) {
        TTableCell cell;
        UNIT_ASSERT(cell.Text.empty());
        UNIT_ASSERT(!cell.Bold);
        UNIT_ASSERT(!cell.Dim);
    }
    
    Y_UNIT_TEST(TTableCell_TextConstructor) {
        TTableCell cell("hello");
        UNIT_ASSERT_STRINGS_EQUAL(cell.Text.c_str(), "hello");
    }
    
    Y_UNIT_TEST(TTableCell_WithBold) {
        TTableCell cell("test");
        cell.WithBold();
        UNIT_ASSERT(cell.Bold);
    }
    
    Y_UNIT_TEST(TTableCell_WithDim) {
        TTableCell cell("test");
        cell.WithDim();
        UNIT_ASSERT(cell.Dim);
    }
    
    Y_UNIT_TEST(TTableCell_Chaining) {
        TTableCell cell("styled");
        cell.WithBold().WithDim();
        UNIT_ASSERT(cell.Bold);
        UNIT_ASSERT(cell.Dim);
    }
    
    // === TTableRow Builder Pattern Tests ===
    
    Y_UNIT_TEST(TTableRow_DefaultConstructor) {
        TTableRow row;
        UNIT_ASSERT(row.Cells.empty());
        UNIT_ASSERT(!row.RowBold);
        UNIT_ASSERT(!row.RowDim);
        UNIT_ASSERT(row.RowType.empty());
    }
    
    Y_UNIT_TEST(TTableRow_WithType) {
        TTableRow row;
        row.WithType("topic");
        UNIT_ASSERT_STRINGS_EQUAL(row.RowType.c_str(), "topic");
    }
    
    Y_UNIT_TEST(TTableRow_WithBold) {
        TTableRow row;
        row.WithBold();
        UNIT_ASSERT(row.RowBold);
    }
    
    Y_UNIT_TEST(TTableRow_WithDim) {
        TTableRow row;
        row.WithDim();
        UNIT_ASSERT(row.RowDim);
    }
    
    Y_UNIT_TEST(TTableRow_Chaining) {
        TTableRow row;
        row.WithType("dir").WithBold().WithDim();
        UNIT_ASSERT_STRINGS_EQUAL(row.RowType.c_str(), "dir");
        UNIT_ASSERT(row.RowBold);
        UNIT_ASSERT(row.RowDim);
    }
    
    Y_UNIT_TEST(TTableRow_WithUserData) {
        int testData = 42;
        TTableRow row;
        row.WithUserData(&testData);
        UNIT_ASSERT_EQUAL(row.UserData, &testData);
    }
    
    // === SetRow Tests ===
    
    Y_UNIT_TEST(SetRow_WithTTableRow) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        TTableRow row;
        row.WithType("test").WithBold();
        table.SetRow(0, row);
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).RowType.c_str(), "test");
        UNIT_ASSERT(table.GetRow(0).RowBold);
    }
    
    Y_UNIT_TEST(SetRow_WithStringVector) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        TVector<TString> values = {"name", "100", "ok"};
        table.SetRow(0, values);
        
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[0].Text.c_str(), "name");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[1].Text.c_str(), "100");
        UNIT_ASSERT_STRINGS_EQUAL(table.GetRow(0).Cells[2].Text.c_str(), "ok");
    }
    
    Y_UNIT_TEST(SetRow_WithCellVector) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        TVector<TTableCell> cells = {
            TTableCell("a").WithBold(),
            TTableCell("b"),
            TTableCell("c").WithDim()
        };
        table.SetRow(0, cells);
        
        UNIT_ASSERT(table.GetRow(0).Cells[0].Bold);
        UNIT_ASSERT(!table.GetRow(0).Cells[1].Bold);
        UNIT_ASSERT(table.GetRow(0).Cells[2].Dim);
    }
    
    // === Change Highlighting Config Tests ===
    
    Y_UNIT_TEST(HighlightDuration_DefaultIsOneSecond) {
        TTable table(CreateTestColumns());
        UNIT_ASSERT_EQUAL(table.GetHighlightDuration(), TDuration::Seconds(1));
    }
    
    Y_UNIT_TEST(SetHighlightDuration_ChangesValue) {
        TTable table(CreateTestColumns());
        table.SetHighlightDuration(TDuration::MilliSeconds(500));
        UNIT_ASSERT_EQUAL(table.GetHighlightDuration(), TDuration::MilliSeconds(500));
    }
    
    // === Edge Cases ===
    
    Y_UNIT_TEST(EmptyTable_GetRowReturnsEmptyRow) {
        TTable table(CreateTestColumns());
        // GetRow on empty table should return a static empty row
        const TTableRow& row = table.GetRow(0);
        UNIT_ASSERT(row.Cells.empty());
    }
    
    Y_UNIT_TEST(SetCell_OutOfBounds_Ignored) {
        TTable table(CreateTestColumns());
        table.SetRowCount(1);
        
        // These should not crash
        table.SetCell(10, 0, "test");  // Row out of bounds
        table.SetCell(0, 10, "test");  // Column out of bounds
        
        UNIT_ASSERT(true);  // Just verify no crash
    }
    
    Y_UNIT_TEST(SetRowCount_ToZero) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetRowCount(0);
        UNIT_ASSERT_EQUAL(table.GetRowCount(), 0);
    }
    
    Y_UNIT_TEST(Selection_DefaultsToZero) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 0);
    }
    
    // === Callback Tests ===
    
    Y_UNIT_TEST(OnNavigate_CalledWhenSelectionChanges) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        
        int navigatedRow = -1;
        table.OnNavigate = [&](int row) { navigatedRow = row; };
        
        // Simulate arrow down
        table.HandleEvent(Event::ArrowDown);
        
        UNIT_ASSERT_EQUAL(navigatedRow, 1);
    }
    
    Y_UNIT_TEST(OnSelect_CalledOnEnter) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(2);
        
        int selectedRow = -1;
        table.OnSelect = [&](int row) { selectedRow = row; };
        
        table.HandleEvent(Event::Return);
        
        UNIT_ASSERT_EQUAL(selectedRow, 2);
    }
    
    // === HandleEvent Tests ===
    
    Y_UNIT_TEST(HandleEvent_EmptyTable_ReturnsFalse) {
        TTable table(CreateTestColumns());
        // Don't add rows - table is empty
        table.SetFocused(true);
        
        bool handled = table.HandleEvent(Event::ArrowDown);
        UNIT_ASSERT(!handled);  // Empty table doesn't handle events
    }
    
    Y_UNIT_TEST(HandleEvent_ArrowDown_MovesSelection) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(0);
        
        table.HandleEvent(Event::ArrowDown);
        
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 1);
    }
    
    Y_UNIT_TEST(HandleEvent_ArrowUp_MovesSelection) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(3);
        
        table.HandleEvent(Event::ArrowUp);
        
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 2);
    }
    
    Y_UNIT_TEST(HandleEvent_AtBoundary_ClampedButConsumed) {
        TTable table(CreateTestColumns());
        table.SetRowCount(5);
        table.SetFocused(true);
        table.SetSelectedRow(0);
        
        bool handled = table.HandleEvent(Event::ArrowUp);
        
        UNIT_ASSERT(handled);  // Event consumed even at boundary
        UNIT_ASSERT_EQUAL(table.GetSelectedRow(), 0);  // Stays at 0
    }
}

