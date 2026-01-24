#include "table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/node.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/requirement.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/terminal.hpp>
#include <contrib/libs/ftxui/include/ftxui/util/autoreset.hpp>

#include <algorithm>

using namespace ftxui;

namespace NYdb::NConsoleClient {

namespace {

class THorizontalScroll : public Node {
public:
    THorizontalScroll(Elements children, int* offset)
        : Node(std::move(children))
        , Offset_(offset)
    {}

    void ComputeRequirement() override {
        Node::ComputeRequirement();
        requirement_ = children_[0]->requirement();
    }

    void SetBox(Box box) override {
        box_ = box;
        const int external_dimx = box.x_max - box.x_min;
        const int internal_dimx = std::max(requirement_.min_x, external_dimx);
        const int max_offset = std::max(0, internal_dimx - external_dimx);
        int offset = std::clamp(*Offset_, 0, max_offset);
        *Offset_ = offset;

        Box child_box = box;
        child_box.x_min = box.x_min - offset;
        child_box.x_max = box.x_min + internal_dimx - offset;
        children_[0]->SetBox(child_box);
    }

    void Render(Screen& screen) override {
        const AutoReset<Box> stencil(&screen.stencil, Box::Intersection(box_, screen.stencil));
        children_[0]->Render(screen);
    }

private:
    int* Offset_;
};

Element HorizontalScroll(Element child, int* offset) {
    return std::make_shared<THorizontalScroll>(Elements{std::move(child)}, offset);
}

} // namespace

TTable::TTable(TVector<TTableColumn> columns)
    : Columns_(std::move(columns))
{}

Component TTable::Build() {
    return Renderer([this] {
        return Render();
    }) | CatchEvent([this](Event event) {
        // Don't handle events when not focused - prevents inactive views
        // in Container::Stacked from consuming events meant for active view
        if (!IsFocused_) {
            return false;
        }
        return HandleEvent(event);
    });
}

void TTable::Clear() {
    Rows_.clear();
    // Don't reset SelectedRow_ - cursor position should persist across refresh
    // SetRowCount() will clamp to valid range after repopulation
}

void TTable::SetRowCount(size_t count) {
    Rows_.resize(count);
    for (auto& row : Rows_) {
        row.Cells.resize(Columns_.size());
    }
    // Clamp selection
    if (SelectedRow_ >= static_cast<int>(count)) {
        SelectedRow_ = count > 0 ? static_cast<int>(count) - 1 : 0;
    }
}

TTableRow& TTable::GetRow(size_t row) {
    static TTableRow empty;
    if (row < Rows_.size()) {
        return Rows_[row];
    }
    return empty;
}

const TTableRow& TTable::GetRow(size_t row) const {
    static TTableRow empty;
    if (row < Rows_.size()) {
        return Rows_[row];
    }
    return empty;
}

void TTable::SetRow(size_t row, TTableRow rowData) {
    if (row < Rows_.size()) {
        // Ensure cells vector is properly sized
        rowData.Cells.resize(Columns_.size());
        Rows_[row] = std::move(rowData);
    }
}

void TTable::SetRow(size_t row, const TVector<TString>& values) {
    if (row >= Rows_.size()) return;
    Rows_[row].Cells.resize(Columns_.size());
    for (size_t i = 0; i < values.size() && i < Columns_.size(); ++i) {
        Rows_[row].Cells[i] = TTableCell(values[i]);
    }
}

void TTable::SetRow(size_t row, const TVector<TTableCell>& cells) {
    if (row >= Rows_.size()) return;
    Rows_[row].Cells.resize(Columns_.size());
    for (size_t i = 0; i < cells.size() && i < Columns_.size(); ++i) {
        Rows_[row].Cells[i] = cells[i];
    }
}

void TTable::SetCell(size_t row, size_t col, const TString& text) {
    if (row < Rows_.size() && col < Columns_.size()) {
        if (Rows_[row].Cells.size() <= col) {
            Rows_[row].Cells.resize(Columns_.size());
        }
        Rows_[row].Cells[col] = TTableCell(text);
    }
}

void TTable::SetCell(size_t row, size_t col, const TTableCell& cell) {
    if (row < Rows_.size() && col < Columns_.size()) {
        if (Rows_[row].Cells.size() <= col) {
            Rows_[row].Cells.resize(Columns_.size());
        }
        Rows_[row].Cells[col] = cell;
    }
}

// Check if text is a placeholder (not real data)
static bool IsPlaceholder(const TString& text) {
    return text.empty() || text == "-" || text == "..." || text == " -" || text == " ...";
}

bool TTable::UpdateCell(size_t row, size_t col, const TString& text) {
    if (row >= Rows_.size() || col >= Columns_.size()) {
        return false;
    }
    
    if (Rows_[row].Cells.size() <= col) {
        Rows_[row].Cells.resize(Columns_.size());
    }
    
    auto& cell = Rows_[row].Cells[col];
    if (cell.Text != text) {
        // Only highlight if cell had real data (not placeholder) that changed
        bool hadRealData = !IsPlaceholder(cell.Text);
        bool hasRealData = !IsPlaceholder(text);
        cell.Text = text;
        if (hadRealData && hasRealData) {
            cell.ChangedAt = TInstant::Now();
        }
        return true;
    }
    return false;
}

bool TTable::UpdateCell(size_t row, size_t col, const TTableCell& newCell) {
    if (row >= Rows_.size() || col >= Columns_.size()) {
        return false;
    }
    
    if (Rows_[row].Cells.size() <= col) {
        Rows_[row].Cells.resize(Columns_.size());
    }
    
    auto& cell = Rows_[row].Cells[col];
    if (cell.Text != newCell.Text) {
        // Only highlight if cell had real data (not placeholder) that changed
        bool hadRealData = !IsPlaceholder(cell.Text);
        bool hasRealData = !IsPlaceholder(newCell.Text);
        cell = newCell;
        if (hadRealData && hasRealData) {
            cell.ChangedAt = TInstant::Now();
        }
        return true;
    }
    // Value same but maybe other properties changed - update without change timestamp
    cell.TextColor = newCell.TextColor;
    cell.BgColor = newCell.BgColor;
    cell.Bold = newCell.Bold;
    cell.Dim = newCell.Dim;
    return false;
}

void TTable::SetSelectedRow(int row) {
    if (row >= 0 && row < static_cast<int>(Rows_.size())) {
        SelectedRow_ = row;
    }
}

void TTable::SetSort(int column, bool ascending) {
    if (column >= -1 && column < static_cast<int>(Columns_.size())) {
        SortColumn_ = column;
        SortAscending_ = ascending;
        if (OnSortChanged) {
            OnSortChanged(SortColumn_, SortAscending_);
        }
    }
}

void TTable::ToggleSort(int column) {
    if (column < 0 || column >= static_cast<int>(Columns_.size())) {
        return;
    }
    
    if (SortColumn_ != column) {
        // New column - start with ascending
        SetSort(column, true);
    } else if (SortAscending_) {
        // Same column, was ascending -> descending
        SetSort(column, false);
    } else {
        // Was descending -> back to ascending (for simplicity)
        SetSort(column, true);
    }
}

void TTable::SetHorizontalScrollEnabled(bool enabled) {
    HorizontalScrollEnabled_ = enabled;
    if (!enabled) {
        HorizontalOffset_ = 0;
    }
}

Element TTable::Render() {
    if (Rows_.empty()) {
        return text("No data") | dim | center;
    }
    
    Elements rows;
    rows.push_back(RenderHeader());
    
    for (size_t i = 0; i < Rows_.size(); ++i) {
        rows.push_back(RenderRow(i));
    }
    
    Element table = vbox(rows);
    table = table | yframe;
    if (HorizontalScrollEnabled_) {
        table = HorizontalScroll(table, &HorizontalOffset_);
    }
    
    // Use reflect() to track table position for mouse coordinates
    return table | flex | reflect(Box_);
}

bool TTable::HandleEvent(Event event) {
    // Note: Focus management is done at the view level, not here
    if (Rows_.empty()) {
        return false;
    }
    
    int oldSelection = SelectedRow_;

    if (HorizontalScrollEnabled_) {
        constexpr int kScrollStep = 5;
        if (event == Event::ArrowLeft || event == Event::Character('h')) {
            HorizontalOffset_ = std::max(0, HorizontalOffset_ - kScrollStep);
            return true;
        }
        if (event == Event::ArrowRight || event == Event::Character('l')) {
            HorizontalOffset_ += kScrollStep;
            return true;
        }
    }
    
    // Up navigation (ArrowUp or k)
    if (event == Event::ArrowUp || event == Event::Character('k')) {
        if (SelectedRow_ > 0) {
            SelectedRow_--;
        }
        // Always consume these keys even at boundary
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    }
    // Down navigation (ArrowDown or j)
    if (event == Event::ArrowDown || event == Event::Character('j')) {
        if (SelectedRow_ < static_cast<int>(Rows_.size()) - 1) {
            SelectedRow_++;
        }
        // Always consume these keys even at boundary
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    }
    if (event == Event::PageUp) {
        // Get actual screen height for page scrolling
        auto termSize = Terminal::Size();
        int pageSize = std::max(5, termSize.dimy - 4);  // Subtract header/footer lines
        SelectedRow_ = std::max(0, SelectedRow_ - pageSize);
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    } else if (event == Event::PageDown) {
        auto termSize = Terminal::Size();
        int pageSize = std::max(5, termSize.dimy - 4);
        SelectedRow_ = std::min(static_cast<int>(Rows_.size()) - 1, SelectedRow_ + pageSize);
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    } else if (event == Event::Home) {
        SelectedRow_ = 0;
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    } else if (event == Event::End) {
        SelectedRow_ = static_cast<int>(Rows_.size()) - 1;
        if (SelectedRow_ != oldSelection && OnNavigate) {
            OnNavigate(SelectedRow_);
        }
        return true;
    } else if (event == Event::Return) {
        if (OnSelect && SelectedRow_ >= 0) {
            OnSelect(SelectedRow_);
        }
        return true;
    } else if (event == Event::Character('<')) {
        // Previous column for sorting
        int newCol = SortColumn_ > 0 ? SortColumn_ - 1 : static_cast<int>(Columns_.size()) - 1;
        ToggleSort(newCol);
        return true;
    } else if (event == Event::Character('>')) {
        // Next column for sorting
        int newCol = (SortColumn_ + 1) % static_cast<int>(Columns_.size());
        ToggleSort(newCol);
        return true;
    }
    
    // Mouse handling
    if (event.is_mouse()) {
        auto& mouse = event.mouse();
        
        // Hit detection using per-row boxes (handles scrolling correctly)
        int rowUnderMouse = -1;
        for (size_t i = 0; i < Rows_.size(); ++i) {
            if (Rows_[i].Box.Contain(mouse.x, mouse.y)) {
                rowUnderMouse = static_cast<int>(i);
                break;
            }
        }

        if (rowUnderMouse != -1) {
            // Update hover state
            HoveredRow_ = rowUnderMouse;
            
            // Handle click
            if (mouse.button == Mouse::Left && mouse.motion == Mouse::Pressed) {
                SelectedRow_ = rowUnderMouse;
                if (OnSelect) {
                    OnSelect(SelectedRow_);
                }
                return true;
            }
        } else {
            HoveredRow_ = -1;
        }
        
        // Handle scroll wheel
        if (mouse.button == Mouse::WheelUp) {
            if (SelectedRow_ > 0) {
                SelectedRow_--;
                if (OnNavigate) OnNavigate(SelectedRow_);
            }
            return true;
        } else if (mouse.button == Mouse::WheelDown) {
            if (SelectedRow_ < static_cast<int>(Rows_.size()) - 1) {
                SelectedRow_++;
                if (OnNavigate) OnNavigate(SelectedRow_);
            }
            return true;
        }
        
        return false;  // Don't consume move events to allow hover updates
    }
    
    return false;
}

Element TTable::RenderHeader() {
    Elements cells;
    
    // Add space to align with selection marker column in data rows
    cells.push_back(text(" "));
    
    for (size_t i = 0; i < Columns_.size(); ++i) {
        const auto& col = Columns_[i];
        
        // Add sort indicator if this is the sorted column
        std::string headerText = " " + std::string(col.Header.c_str());
        if (static_cast<int>(i) == SortColumn_) {
            headerText += SortAscending_ ? " ▲" : " ▼";
        }
        
        Element cell = text(headerText) | bold;
        
        if (col.Width > 0) {
            cell = cell | size(WIDTH, EQUAL, col.Width);
        } else {
            cell = cell | flex;
        }
        
        cells.push_back(cell);
        
        // Add separator except after last column
        if (i < Columns_.size() - 1) {
            cells.push_back(separator());
        }
    }
    
    return hbox(cells) | bgcolor(NTheme::HeaderBg);
}

Element TTable::RenderRow(size_t rowIndex) {
    bool selected = IsFocused_ && static_cast<int>(rowIndex) == SelectedRow_;
    auto& rowData = Rows_[rowIndex];
    
    Elements cells;
    
    // Add selection marker (visible in text-only terminals)
    if (selected) {
        cells.push_back(text(">") | bold);
    } else {
        cells.push_back(text(" "));
    }
    
    for (size_t i = 0; i < Columns_.size(); ++i) {
        const auto& col = Columns_[i];
        const auto& cellData = (i < rowData.Cells.size()) 
            ? rowData.Cells[i] 
            : TTableCell();
        
        cells.push_back(RenderCell(cellData, col, rowData, selected));
        
        // Add separator except after last column
        if (i < Columns_.size() - 1) {
            cells.push_back(separator());
        }
    }
    
    Element row = hbox(cells);
    
    // Apply row-level background color
    if (rowData.RowBgColor != Color::Default && !selected) {
        row = row | bgcolor(rowData.RowBgColor);
    }
    
    // Apply hover highlight (if not selected)
    bool hovered = static_cast<int>(rowIndex) == HoveredRow_;
    if (hovered && !selected) {
        row = row | bgcolor(NTheme::HoverBg);
    }
    
    if (selected) {
        row = row | bgcolor(NTheme::HighlightBg) | focus;
    }
    
    return row | reflect(rowData.Box);
}

Element TTable::RenderCell(const TTableCell& cell, const TTableColumn& col,
                           const TTableRow& row, bool isSelected) {
    std::string content = " " + std::string(cell.Text.c_str());
    
    Element elem = text(content);
    
    // Determine text color (cell > row > default)
    Color textColor = Color::Default;
    if (cell.TextColor != Color::Default) {
        textColor = cell.TextColor;
    } else if (row.RowColor != Color::Default) {
        textColor = row.RowColor;
    }
    
    if (textColor != Color::Default) {
        elem = elem | color(textColor);
    }
    
    // Apply bold (cell or row level)
    if (cell.Bold || row.RowBold) {
        elem = elem | bold;
    }
    
    // Apply dim (cell or row level)
    if (cell.Dim || row.RowDim) {
        elem = elem | dim;
    }
    
    // Apply width
    if (col.Width > 0) {
        elem = elem | size(WIDTH, EQUAL, col.Width);
    } else {
        elem = elem | flex;
    }
    
    // Check for change highlighting (only if not selected)
    if (!isSelected && cell.ChangedAt != TInstant()) {
        TInstant now = TInstant::Now();
        if (now - cell.ChangedAt < HighlightDuration_) {
            elem = elem | bold;
        }
    }
    
    // Apply cell-level background (if set and not highlighted)
    if (!isSelected && cell.BgColor != Color::Default) {
        if (cell.ChangedAt == TInstant() || 
            TInstant::Now() - cell.ChangedAt >= HighlightDuration_) {
            elem = elem | bgcolor(cell.BgColor);
        }
    }
    
    return elem;
}

} // namespace NYdb::NConsoleClient
