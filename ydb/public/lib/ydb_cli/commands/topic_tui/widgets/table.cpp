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
        row.resize(Columns_.size());
    }
    // Clamp selection
    if (SelectedRow_ >= static_cast<int>(count)) {
        SelectedRow_ = count > 0 ? static_cast<int>(count) - 1 : 0;
    }
}

void TTable::SetCell(size_t row, size_t col, const TString& text) {
    if (row < Rows_.size() && col < Columns_.size()) {
        Rows_[row][col] = TTableCell(text);
    }
}

void TTable::SetCell(size_t row, size_t col, const TTableCell& cell) {
    if (row < Rows_.size() && col < Columns_.size()) {
        Rows_[row][col] = cell;
    }
}

void TTable::SetRow(size_t row, const TVector<TString>& values) {
    if (row >= Rows_.size()) return;
    for (size_t i = 0; i < values.size() && i < Columns_.size(); ++i) {
        Rows_[row][i] = TTableCell(values[i]);
    }
}

void TTable::SetRow(size_t row, const TVector<TTableCell>& cells) {
    if (row >= Rows_.size()) return;
    for (size_t i = 0; i < cells.size() && i < Columns_.size(); ++i) {
        Rows_[row][i] = cells[i];
    }
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
    
    Elements cells;
    
    for (size_t i = 0; i < Columns_.size(); ++i) {
        const auto& col = Columns_[i];
        const auto& cellData = (i < Rows_[rowIndex].size()) 
            ? Rows_[rowIndex][i] 
            : TTableCell();
        
        cells.push_back(RenderCell(cellData, col));
        
        // Add separator except after last column
        if (i < Columns_.size() - 1) {
            cells.push_back(separator());
        }
    }
    
    Element row = hbox(cells);
    
    if (selected) {
        row = row | bgcolor(NTheme::HighlightBg) | focus;
    }
    
    return row;
}

Element TTable::RenderCell(const TTableCell& cell, const TTableColumn& col) {
    std::string content = " " + std::string(cell.Text.c_str());
    
    Element elem = text(content);
    
    // Apply color if specified
    if (cell.TextColor != Color::Default) {
        elem = elem | color(cell.TextColor);
    }
    
    // Apply bold if specified
    if (cell.Bold) {
        elem = elem | bold;
    }
    
    // Apply width
    if (col.Width > 0) {
        elem = elem | size(WIDTH, EQUAL, col.Width);
    } else {
        elem = elem | flex;
    }
    
    // Apply alignment (for right alignment, we'd need custom logic)
    // For now, left alignment is default
    
    return elem;
}

} // namespace NYdb::NConsoleClient
