#include "pretty_table.h"
#include "common.h"

#include <library/cpp/colorizer/colors.h>
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
    Y_ABORT_UNLESS(columnIndex < Columns.size());
    enum {
        TEXT,
        COLOR,
        UTF8,
    } state = TEXT;

    size_t width = 0;
    TStringBuf data;
    for (const auto& line : Columns.at(columnIndex)) {
        data = line;

        // flag of first symbol in color
        bool first = false;
        // flag of enfing of color
        bool endcolor = false;
        int utf8Len = 0;
        // count of visible chars
        size_t curLen = 0;
        for (char ch : data) {
            switch (state) {
                case TEXT:
                    // begin of color
                    if (ch == '\033') {
                        state = COLOR;
                        first = true;
                        endcolor = false;
                    // begin utf8
                    } else  if ((ch & 0x80) != 0) {
                        curLen++;
                        utf8Len = 0;
                        // if the first bit of the character is not 0, we met a multibyte
                        // counting the number of single bits at the beginning of a byte
                        while ((ch & 0x80) != 0) {
                            utf8Len++;
                            ch <<= 1;
                        }
                        state = UTF8;
                    // common text
                    } else {
                        curLen++;
                    }
                    break;
                case UTF8:
                    // skip n chars
                    utf8Len -= 1;
                    if (utf8Len == 0) {
                        curLen++;
                        while ((ch & 0x80) != 0) {
                            utf8Len++;
                            ch <<= 1;
                        }
                        if (utf8Len != 0) {
                            state = UTF8;
                        } else {
                            state = TEXT;
                        }
                    }
                    break;
                case COLOR:
                    // first symbol must be [
                    if (first) {
                        if (ch != '[') {
                            state = TEXT;
                        }
                        first = false;
                    // at the end of color can be digits, m and ;
                    } else if (endcolor) {
                        if (ch != ';' && !isdigit(ch) && ch != 'm' ) {
                            curLen++;
                            state = TEXT;
                        }
                    // ending after ;
                    } else {
                        if (ch == ';') {
                            endcolor = true;
                        }
                    }
                    break;
            }
        }

        width = Max(width, curLen);
    }

    return width;
}

bool TPrettyTable::TRow::PrintColumnsNextLine(IOutputStream& o, const TVector<size_t>& widths, size_t it, TString& oldColor) const {
    bool next = false;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    size_t fullPrintedColumnsCnt = 0;

    for (size_t columnIndex : xrange(Columns.size())) {
        enum {
            TEXT,
            COLOR,
            UTF8,
        } state = TEXT;
        o << colors.Default();
        if (columnIndex == 0) {
            o << "│ ";
            o << oldColor;
        } else {
            o << " │ ";
        }
        
        if (size_t width = widths.at(columnIndex)) {
            const auto& column = Columns.at(columnIndex);

            TStringBuf data;
            // count of visible chars
            size_t curLen = 0;
            // count of chars
            size_t absCurLen = 0;
            // flag of first symbol in color
            bool first = false;
            // flag of enfing of color
            bool endcolor = false;

            absCurLen = 0;
            
            fullPrintedColumnsCnt += column.empty();

            for (const auto& line : column) {
                data = line;
                TString s = "";
                
                data.Skip(it * width);
                fullPrintedColumnsCnt += data.empty();
                // len of utf8 symbol
                int utf8Len = 0;
                
                for (char ch : data) {
                    if (next && columnIndex == 0) {
                        break;
                    }
                    switch (state) {
                        case TEXT:
                            // begin of color
                            if (ch == '\033') {
                                
                                state = COLOR;
                                first = true;
                                endcolor = false;
                                o << s;
                                s = ch;
                            // begin utf8
                            } else  if ((ch & 0x80) != 0) {
                                o << s;
                                s = "";
                                utf8Len = 0;
                                curLen++;
                                // if the first bit of the character is not 0, we met a multibyte
                                // counting the number of single bits at the beginning of a byte
                                while ((ch & 0x80) != 0) {
                                    utf8Len++;
                                    ch <<= 1;
                                }
                                if (curLen + utf8Len > width) {
                                    next = true;
                                    curLen -= 1;
                                    break;
                                }
                                o << data.SubStr(absCurLen, utf8Len);
                                state = UTF8;
                            // common text
                            } else {
                                curLen++;
                                if (curLen > width) {
                                    next = true;
                                    curLen -= 1;
                                    break;
                                }
                                s += ch; 
                            }
                            break;
                        case UTF8:
                            // skip n chars
                            utf8Len -= 1;
                            if (utf8Len == 0) {
                                while ((ch & 0x80) != 0) {
                                    utf8Len++;
                                    ch <<= 1;
                                }
                                if (curLen + utf8Len > width) {
                                    next = true;
                                    curLen -= 1;
                                    break;
                                }
                                curLen++;
                                if (utf8Len != 0) {
                                    o << data.SubStr(absCurLen, utf8Len);
                                    state = UTF8;
                                } else {
                                    s = ch;
                                    state = TEXT;
                                }
                            }
                            break;
                        case COLOR:
                            // first symbol must be [
                            if (first) {
                                if (ch != '[') {
                                    o << s;
                                    s = ch;
                                    state = TEXT;
                                } else {
                                    s += ch;
                                }
                                first = false;
                            // at the end of color can be digits, m and ;
                            } else if (endcolor) {
                                if (ch != ';' && !isdigit(ch) && ch != 'm' ) {
                                    o << s;
                                    oldColor = s;
                                    curLen++;
                                    if (curLen > width) {
                                        next = true;
                                        curLen -= 1;
                                        break;
                                    }
                                    s = ch;
                                    state = TEXT;
                                } else {
                                    s += ch;
                                }
                            // ending after ;
                            } else {
                                if (ch == ';') {
                                    endcolor = true;
                                }
                                s += ch;
                            }
                            
                            break;
                    }
                    absCurLen++;
                }
                
                if (s != "") {
                    o << s;
                }
            }

            o << TString(width - curLen, ' ');
        }
    }
    
    o << colors.Default();
    o << " │" << Endl;

    if (fullPrintedColumnsCnt == Columns.size()) {
        return 0;
    }

    return 1;
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

        TString oldColor = "";
        size_t it = 0;
        while (row.PrintColumnsNextLine(o, widths, it, oldColor)) {
            ++it;
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
