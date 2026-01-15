#pragma once

#include "theme.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYdb::NConsoleClient {

// =============================================================================
// Column Definition
// =============================================================================

struct TTableColumn {
    TString Header;
    int Width;  // Fixed width in characters, -1 for flex
    
    enum EAlign {
        Left,
        Right,
        Center
    };
    EAlign Align = Left;
    
    TTableColumn(TString header, int width, EAlign align = Left)
        : Header(std::move(header))
        , Width(width)
        , Align(align)
    {}
};

// =============================================================================
// Cell Data with Change Tracking
// =============================================================================

struct TTableCell {
    TString Text;
    ftxui::Color TextColor = ftxui::Color::Default;
    ftxui::Color BgColor = ftxui::Color::Default;
    bool Bold = false;
    bool Dim = false;
    
    // Change tracking
    TInstant ChangedAt;  // When this cell's value last changed
    
    TTableCell() = default;
    TTableCell(TString text) : Text(std::move(text)) {}
    TTableCell(TString text, ftxui::Color color) : Text(std::move(text)), TextColor(color) {}
    
    // Builder pattern for chaining
    TTableCell& WithColor(ftxui::Color c) { TextColor = c; return *this; }
    TTableCell& WithBgColor(ftxui::Color c) { BgColor = c; return *this; }
    TTableCell& WithBold() { Bold = true; return *this; }
    TTableCell& WithDim() { Dim = true; return *this; }
};

// =============================================================================
// Row with optional custom styling
// =============================================================================

struct TTableRow {
    TVector<TTableCell> Cells;
    
    // Row-level styling (applied to all cells unless overridden)
    ftxui::Color RowColor = ftxui::Color::Default;
    ftxui::Color RowBgColor = ftxui::Color::Default;
    bool RowBold = false;
    bool RowDim = false;
    
    // Row type for heterogeneous tables (e.g., "topic", "directory", "parent")
    TString RowType;
    
    // Custom data pointer (for callbacks to identify the source data)
    void* UserData = nullptr;
    
    // For hit detection
    ftxui::Box Box;
    
    TTableRow() = default;
    TTableRow(TVector<TTableCell> cells) : Cells(std::move(cells)) {}
    
    // Builder pattern
    TTableRow& WithType(TString type) { RowType = std::move(type); return *this; }
    TTableRow& WithColor(ftxui::Color c) { RowColor = c; return *this; }
    TTableRow& WithBgColor(ftxui::Color c) { RowBgColor = c; return *this; }
    TTableRow& WithBold() { RowBold = true; return *this; }
    TTableRow& WithDim() { RowDim = true; return *this; }
    TTableRow& WithUserData(void* data) { UserData = data; return *this; }
};

// =============================================================================
// TTable - Reusable Table Component with Per-Cell Updates
// =============================================================================

class TTable {
public:
    explicit TTable(TVector<TTableColumn> columns);
    
    // Build the FTXUI component
    ftxui::Component Build();
    
    // ----- Data management -----
    
    void Clear();
    void SetRowCount(size_t count);
    size_t GetRowCount() const { return Rows_.size(); }
    
    // Get row for modification
    TTableRow& GetRow(size_t row);
    const TTableRow& GetRow(size_t row) const;
    
    // Set entire row at once
    void SetRow(size_t row, TTableRow rowData);
    void SetRow(size_t row, const TVector<TString>& values);
    void SetRow(size_t row, const TVector<TTableCell>& cells);
    
    // ----- Per-cell updates with change detection -----
    
    // Update single cell - tracks changes automatically
    // Returns true if the value actually changed
    bool UpdateCell(size_t row, size_t col, const TString& text);
    bool UpdateCell(size_t row, size_t col, const TTableCell& cell);
    
    // Legacy setters (don't track changes)
    void SetCell(size_t row, size_t col, const TString& text);
    void SetCell(size_t row, size_t col, const TTableCell& cell);
    
    // ----- Change highlighting -----
    
    // How long to highlight changed cells with bold (default: 1 second)
    void SetHighlightDuration(TDuration duration) { HighlightDuration_ = duration; }
    TDuration GetHighlightDuration() const { return HighlightDuration_; }
    
    // ----- Selection -----
    
    int GetSelectedRow() const { return SelectedRow_; }
    void SetSelectedRow(int row);
    
    // Focus state (for multi-panel views)
    bool IsFocused() const { return IsFocused_; }
    void SetFocused(bool focused) { IsFocused_ = focused; }
    
    // ----- Sorting -----
    
    int GetSortColumn() const { return SortColumn_; }
    bool IsSortAscending() const { return SortAscending_; }
    void SetSort(int column, bool ascending);
    void ToggleSort(int column);  // Cycle: none -> asc -> desc -> none
    
    // ----- Horizontal scrolling -----
    void SetHorizontalScrollEnabled(bool enabled);

    // ----- Callbacks -----
    
    std::function<void(int row)> OnSelect;   // Enter key or click
    std::function<void(int row)> OnNavigate; // Arrow keys changed selection
    std::function<void(int col, bool ascending)> OnSortChanged;  // Sort changed
    
    // ----- Rendering -----
    
    // Render as element (for embedding in custom layouts)
    ftxui::Element Render();
    
    // Handle keyboard events (returns true if handled)
    bool HandleEvent(ftxui::Event event);
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderRow(size_t rowIndex);
    ftxui::Element RenderCell(const TTableCell& cell, const TTableColumn& col, 
                              const TTableRow& row, bool isSelected);
    
private:
    TVector<TTableColumn> Columns_;
    TVector<TTableRow> Rows_;
    int SelectedRow_ = 0;
    int HoveredRow_ = -1;  // -1 = no hover
    bool IsFocused_ = true;
    
    // Change highlighting config
    TDuration HighlightDuration_ = TDuration::Seconds(1);
    
    // Sort state
    int SortColumn_ = 0;        // Default: sort by first column
    bool SortAscending_ = true;  // Default: ascending
    
    // For mouse coordinate tracking
    ftxui::Box Box_;

    // Horizontal scroll state
    bool HorizontalScrollEnabled_ = false;
    int HorizontalOffset_ = 0;
};

} // namespace NYdb::NConsoleClient
