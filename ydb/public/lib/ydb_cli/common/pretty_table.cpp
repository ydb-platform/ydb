#include "pretty_table.h"

#include <library/cpp/colorizer/colors.h>
#include <util/generic/algorithm.h>
#include <util/generic/xrange.h>
#include <util/stream/format.h>
#include <util/charset/utf8.h>

#include <contrib/restricted/patched/replxx/src/utf8string.hxx>
#include <ydb/public/lib/ydb_cli/common/interactive.h>


namespace NYdb {
namespace NConsoleClient {

TPrettyTable::TRow::TRow(size_t nColumns)
    : Columns(nColumns)
{
}

enum {
    COLOR_BEGIN = '\033',
    COLOR_END = 'm',
};

size_t TPrettyTable::TRow::ColumnWidth(size_t columnIndex) const {
    Y_ABORT_UNLESS(columnIndex < Columns.size());

    size_t width = 0;

    for (const auto& column: Columns.at(columnIndex)) {
        size_t printableSymbols = 0;

        for (size_t i = 0; i < column.size();) {
            if (column[i] == COLOR_BEGIN) {
                while (i < column.size() && column[i] != COLOR_END) {
                    ++i;
                }

                if (i < column.size() && column[i] == COLOR_END) {
                    ++i;
                }

                continue;
            }


            ++i;
            while (i < column.size() && IsUTF8ContinuationByte(column[i])) {
                ++i;
            }
            ++printableSymbols;
        }

        width = Max(width, printableSymbols);
    }

    return width;
}

class TColumnLinesPrinter {
public:
    TColumnLinesPrinter(
        IOutputStream& o,
        const TVector<TVector<TString>>& columns,
        const TVector<size_t>& widths
    )
        : Output_(o)
        , Columns_(columns)
        , Widths_(widths)
        , PrintedIndexByColumnIndex_(columns.size())
    {}

    bool HasNext() {
        bool allColumnsPrinted = true; 

        for (size_t i = 0; i < PrintedIndexByColumnIndex_.size(); ++i) {
            if (!Columns_[i].empty() && Columns_[i][0].size() > PrintedIndexByColumnIndex_[i]) {
                allColumnsPrinted = false;
            }
        }

        return !allColumnsPrinted;
    }

    void Print() {        
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        
        Output_ << colors.Default();
        Output_ << "│ ";

        for (size_t columnIndex : xrange(Columns_.size())) {
            Output_ << colors.Default();
            if (columnIndex != 0) {
                Output_ << " │ ";
            }

            size_t printedSymbols = PrintColumnLine(columnIndex);
            Output_ << TString(Widths_[columnIndex] - printedSymbols, ' ');
        }
        
        Output_ << colors.Default();
        Output_ << " │" << Endl;
    }

private:
    /* return's printed symbols cnt */
    size_t PrintColumnLine(size_t columnIndex) {
        if (Columns_[columnIndex].empty()) {
            return 0;
        }

        size_t printedSymbols = 0;
        const auto& column = Columns_[columnIndex][0];

        size_t i = PrintedIndexByColumnIndex_[columnIndex];

        for (; i < column.size() && printedSymbols < Widths_[columnIndex];) {
            if (column[i] == COLOR_BEGIN) {
                while (i < column.size() && column[i] != COLOR_END) {
                    Output_ << column[i++];
                }

                if (i < column.size() && column[i] == COLOR_END) {
                    Output_ << column[i++];
                }
            
                continue;
            }


            Output_ << column[i++];
            while (i < column.size() && IsUTF8ContinuationByte(column[i])) {
                Output_ << column[i++];
            }
            ++printedSymbols;
        }

        PrintedIndexByColumnIndex_[columnIndex] = i;
        return printedSymbols;
    }

private:
    IOutputStream& Output_;
    const TVector<TVector<TString>>& Columns_;
    const TVector<size_t>& Widths_;
    TVector<size_t> PrintedIndexByColumnIndex_;
};

void TPrettyTable::TRow::PrintColumns(IOutputStream& o, const TVector<size_t>& widths) const {
    TColumnLinesPrinter printer(o, Columns, widths);

    while (printer.HasNext()) {
        printer.Print();
    }
}

bool TPrettyTable::TRow::HasFreeText() const {
    return !!Text;
}

void TPrettyTable::TRow::PrintFreeText(IOutputStream& o, size_t width) const {
    Y_ABORT_UNLESS(HasFreeText());

    for (auto& line : StringSplitter(Text).Split('\n')) {
        TStringBuf token = line.Token();

        while (token) {
            o << "│ " << RightPad(token.SubStr(0, width), width) << " │" << Endl;
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
        row.PrintColumns(o, widths);

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
    const size_t maxWidth = Min(Config.Width ? Config.Width : lineLength, lineLength) - ((Columns * 3) + 1);
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
