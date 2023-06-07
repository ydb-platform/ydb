#include "tabbed_table.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include "common.h"
#include "print_utils.h"

namespace NYdb {
namespace NConsoleClient {


TAdaptiveTabbedTable::TAdaptiveTabbedTable(const TVector<NScheme::TSchemeEntry>& entries)
    : Entries(entries)
{
    CalculateColumns();
}

void TAdaptiveTabbedTable::Print(IOutputStream& o) const {

    /* Calculate the number of rows that will be in each column except possibly
        for a short column on the right.  */
    size_t rows = Entries.size() / ColumnCount + (Entries.size() % ColumnCount != 0);

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    for (size_t row = 0; row < rows; ++row) {
        for (size_t column = 0; column < ColumnCount; ++column) {
            size_t idx = column * rows + row;
            if (idx < Entries.size()) {
                PrintSchemeEntry(o, Entries[idx], colors);
                if (column != ColumnCount - 1) {
                    size_t columnWidth = ColumnInfo[ColumnCount - 1].LengthValues[column];
                    o << TString(columnWidth - Entries[idx].Name.length(), ' ');
                }
            }
        }
        o << Endl;
    }
}

void TAdaptiveTabbedTable::InitializeColumnInfo(size_t maxCols, size_t minColumnWidth) {
    ColumnInfo = TVector<TColumnInfo>(maxCols);

    for (size_t i = 0; i < maxCols; ++i) {
        ColumnInfo[i].LineLength = (i + 1) * minColumnWidth;
        for (size_t j = 0; j <= i; ++j) {
            ColumnInfo[i].LengthValues.push_back(minColumnWidth);
        }
    }
}

void TAdaptiveTabbedTable::CalculateColumns() {
    auto terminalWidth = GetTerminalWidth();
    size_t lineLength = terminalWidth ? *terminalWidth : Max<size_t>();
    size_t max_length = 0;
    for (auto entry : Entries) {
        if (entry.Name.length() > max_length) {
            max_length = entry.Name.length();
        }
    }
    if (lineLength < max_length + 1) {
        lineLength = Max<size_t>();
    }
    size_t minColumnWidth = 3;

    /* Determine the max possible number of display columns.  */
    size_t maxIdx = lineLength / minColumnWidth;

    /* Account for first display column not having a separator,
        or lineLength shorter than minColumnWidth.  */
    maxIdx += lineLength % minColumnWidth != 0;

    /* Normally the maximum number of columns is determined by the
        screen width.  But if few files are available this might limit it
        as well.  */
    size_t maxCols = Min(maxIdx, Entries.size());

    InitializeColumnInfo(maxCols, minColumnWidth);

    for (size_t filesno = 0; filesno < Entries.size(); ++filesno) {
        size_t name_length = Entries[filesno].Name.length();

        for (size_t i = 0; i < maxCols; ++i) {
            if (ColumnInfo[i].ValidLen)
            {
                size_t idx = filesno / ((Entries.size() + i) / (i + 1));
                size_t real_length = name_length + (idx == i ? 0 : 2);

                if (ColumnInfo[i].LengthValues[idx] < real_length) {
                    ColumnInfo[i].LineLength += (real_length - ColumnInfo[i].LengthValues[idx]);
                    ColumnInfo[i].LengthValues[idx] = real_length;
                    ColumnInfo[i].ValidLen = (ColumnInfo[i].LineLength < lineLength);
                    if (name_length > ColumnInfo[i].ColumnWidth) {
                        ColumnInfo[i].ColumnWidth = name_length;
                    }
                }
            }
        }
    }

    /* Find maximum allowed columns.  */
    for (size_t cols = maxCols; 0 < cols; --cols) {
        if (ColumnInfo[cols - 1].ValidLen) {
            ColumnCount = cols;
            return;
        }
    }

    Cerr << "Can't find valid column count to display" << Endl;
    return;
}

}
}
