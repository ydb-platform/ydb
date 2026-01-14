#pragma once

#include "theme.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

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
// Cell Data
// =============================================================================

struct TTableCell {
    TString Text;
    ftxui::Color TextColor = ftxui::Color::Default;
    bool Bold = false;
    
    TTableCell() = default;
    TTableCell(TString text) : Text(std::move(text)) {}
    TTableCell(TString text, ftxui::Color color) : Text(std::move(text)), TextColor(color) {}
};

// =============================================================================
// TTable - Reusable Table Component
// =============================================================================

class TTable {
public:
    explicit TTable(TVector<TTableColumn> columns);
    
    // Build the FTXUI component
    ftxui::Component Build();
    
    // Data management
    void Clear();
    void SetRowCount(size_t count);
    void SetCell(size_t row, size_t col, const TString& text);
    void SetCell(size_t row, size_t col, const TTableCell& cell);
    
    // Convenience: set entire row at once
    void SetRow(size_t row, const TVector<TString>& values);
    void SetRow(size_t row, const TVector<TTableCell>& cells);
    
    // Selection
    int GetSelectedRow() const { return SelectedRow_; }
    void SetSelectedRow(int row);
    
    // Focus state (for multi-panel views)
    bool IsFocused() const { return IsFocused_; }
    void SetFocused(bool focused) { IsFocused_ = focused; }
    
    // Callbacks
    std::function<void(int row)> OnSelect;   // Enter key
    std::function<void(int row)> OnNavigate; // Arrow keys changed selection
    
    // Render as element (for embedding in custom layouts)
    ftxui::Element Render();
    
    // Handle keyboard events (returns true if handled)
    bool HandleEvent(ftxui::Event event);
    
private:
    ftxui::Element RenderHeader();
    ftxui::Element RenderRow(size_t rowIndex);
    ftxui::Element RenderCell(const TTableCell& cell, const TTableColumn& col);
    
private:
    TVector<TTableColumn> Columns_;
    TVector<TVector<TTableCell>> Rows_;
    int SelectedRow_ = 0;
    bool IsFocused_ = true;
};

} // namespace NYdb::NConsoleClient
