#include "table.h"

#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>

using namespace ftxui;

namespace NYdb::NConsoleClient {

TTable::TTable(TVector<TTableColumn> columns)
    : Columns_(std::move(columns))
{}

Component TTable::Build() {
    return Renderer([this] {
        return Render();
    }) | CatchEvent([this](Event event) {
        return HandleEvent(event);
    });
}

void TTable::Clear() {
    Rows_.clear();
    SelectedRow_ = 0;
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

bool TTable::UpdateCell(size_t row, size_t col, const TString& text) {
    if (row >= Rows_.size() || col >= Columns_.size()) {
        return false;
    }
    
    if (Rows_[row].Cells.size() <= col) {
        Rows_[row].Cells.resize(Columns_.size());
    }
    
    auto& cell = Rows_[row].Cells[col];
    if (cell.Text != text) {
        cell.Text = text;
        cell.ChangedAt = TInstant::Now();
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
        // Value changed - copy new data and set change timestamp
        cell = newCell;
        cell.ChangedAt = TInstant::Now();
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

Element TTable::Render() {
    if (Rows_.empty()) {
        return text("No data") | dim | center;
    }
    
    Elements rows;
    rows.push_back(RenderHeader());
    
    for (size_t i = 0; i < Rows_.size(); ++i) {
        rows.push_back(RenderRow(i));
    }
    
    return vbox(rows) | yframe | flex;
}

bool TTable::HandleEvent(Event event) {
    if (!IsFocused_ || Rows_.empty()) {
        return false;
    }
    
    int oldSelection = SelectedRow_;
    
    if (event == Event::ArrowUp) {
        if (SelectedRow_ > 0) {
            SelectedRow_--;
        }
    } else if (event == Event::ArrowDown) {
        if (SelectedRow_ < static_cast<int>(Rows_.size()) - 1) {
            SelectedRow_++;
        }
    } else if (event == Event::PageUp) {
        SelectedRow_ = std::max(0, SelectedRow_ - 10);
    } else if (event == Event::PageDown) {
        SelectedRow_ = std::min(static_cast<int>(Rows_.size()) - 1, SelectedRow_ + 10);
    } else if (event == Event::Home) {
        SelectedRow_ = 0;
    } else if (event == Event::End) {
        SelectedRow_ = static_cast<int>(Rows_.size()) - 1;
    } else if (event == Event::Return) {
        if (OnSelect && SelectedRow_ >= 0) {
            OnSelect(SelectedRow_);
        }
        return true;
    } else {
        return false;
    }
    
    if (SelectedRow_ != oldSelection && OnNavigate) {
        OnNavigate(SelectedRow_);
    }
    
    return true;
}

Element TTable::RenderHeader() {
    Elements cells;
    
    for (size_t i = 0; i < Columns_.size(); ++i) {
        const auto& col = Columns_[i];
        
        Element cell = text(" " + std::string(col.Header.c_str())) | bold;
        
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
    const auto& rowData = Rows_[rowIndex];
    
    Elements cells;
    
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
    
    if (selected) {
        row = row | bgcolor(NTheme::HighlightBg) | focus;
    }
    
    return row;
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
            elem = elem | bgcolor(HighlightColor_);
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
