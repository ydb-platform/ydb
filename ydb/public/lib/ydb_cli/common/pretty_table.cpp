#include "pretty_table.h"
#include "common.h"

#include <util/generic/algorithm.h>
#include <util/generic/xrange.h>
#include <util/stream/format.h>

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb {
namespace NConsoleClient {

TPrettyTable::TRow::TRow(size_t nColumns)
    : Columns(nColumns)
{
}

size_t TPrettyTable::TRow::ColumnWidth(size_t columnIndex) const {
    Y_VERIFY(columnIndex < Columns.size());

    size_t width = 0;
    for (const auto& line : Columns.at(columnIndex)) {
        width = Max(width, line.size());
    }

    return width;
}

bool TPrettyTable::TRow::PrintColumns(IOutputStream& o, const TVector<size_t>& widths, size_t lineNumber) const {
    bool next = false;

    for (size_t columnIndex : xrange(Columns.size())) {
        if (columnIndex == 0) {
            o << "| ";
        } else {
            o << " | ";
        }

        if (const size_t width = widths.at(columnIndex)) {
            const auto& column = Columns.at(columnIndex);

            TStringBuf data;
            size_t l = 0;
            for (const auto& line : column) {
                data = line;
                while (data && l < lineNumber) {
                    data.Skip(width);
                    ++l;
                }
            }

            if (data) {
                o << RightPad(data.SubStr(0, width), width);
            } else {
                o << RightPad(' ', width);
            }

            if (data.size() > width) {
                next = true;
            }
        }
    }
    o << " |" << Endl;

    return next;
}

bool TPrettyTable::TRow::HasFreeText() const {
    return !!Text;
}

void TPrettyTable::TRow::PrintFreeText(IOutputStream& o, size_t width) const {
    Y_VERIFY(HasFreeText());

    for (auto& line : StringSplitter(Text).Split('\n')) {
        TStringBuf token = line.Token();

        while (token) {
            o << "| " << RightPad(token.SubStr(0, width), width) << " |" << Endl;
            token.Skip(width);
        }
    }
}

TPrettyTable::TRow& TPrettyTable::AddRow() {
    return Rows.emplace_back(Columns);
}

static void PrintDelim(IOutputStream& o, const TVector<size_t>& widths,
        const TString& b, const TString& s, const TString& e, bool dotted = false) {
    o << b;

    for (auto i : xrange(widths.size())) {
        const size_t width = widths.at(i);

        for (auto x : xrange(width + 2)) {
            Y_UNUSED(x);
            o << (dotted ? "╴" : "─");
        }

        if (i != widths.size() - 1) {
            o << s;
        }
    }

    o << e << Endl;
}

void TPrettyTable::Print(IOutputStream& o) const {
    if (!Rows) {
        return;
    }

    TVector<size_t> widths = CalcWidths();

    PrintDelim(o, widths, "┌", "┬", "┐");
    for (auto i : xrange(Rows.size())) {
        const auto& row = Rows.at(i);

        size_t line = 0;
        while (row.PrintColumns(o, widths, line)) {
            ++line;
        }

        if (row.HasFreeText()) {
            PrintDelim(o, widths, "├", "┴", "┤", true);
            row.PrintFreeText(o, Accumulate(widths, (Columns - 1) * 3));
        }

        if ((Config.DelimRows && i != Rows.size() - 1) || (Config.Header && i == 0)) {
            PrintDelim(o, widths, "├", row.HasFreeText() ? "┬" : "┼", "┤");
        }
    }
    PrintDelim(o, widths, "└", Rows.back().HasFreeText() ? "─" : "┴", "┘");
}

TVector<size_t> TPrettyTable::CalcWidths() const {
    TVector<size_t> widths(Columns);

    // max
    for (const auto& row : Rows) {
        for (size_t columnIndex : xrange(Columns)) {
            widths[columnIndex] = Max(widths[columnIndex], row.ColumnWidth(columnIndex));
        }
    }

    // adjust
    auto terminalWidth = GetTerminalWidth();
    size_t lineLength = terminalWidth ? *terminalWidth : Max<size_t>();
    const size_t maxWidth = Max(Config.Width, lineLength) - ((Columns * 3) + 1);
    size_t totalWidth = Accumulate(widths, (size_t)0);
    while (totalWidth > maxWidth) {
        auto it = MaxElement(widths.begin(), widths.end());
        if (*it == 1) {
            break;
        }

        if ((totalWidth - maxWidth) < (*it / 2)) {
            *it = *it - (totalWidth - maxWidth);
        } else {
            *it = Max(*it / 2, (size_t)1);
        }

        totalWidth = Accumulate(widths, 0);
    }

    return widths;
}

}
}
