#pragma once

#include "util/string/builder.h"
#include "util/system/types.h"

namespace NKikimr {
namespace NMiniKQL {

struct TEngineHostCounters {
    ui64 NSelectRow = 0;
    ui64 NSelectRange = 0;
    ui64 NUpdateRow  = 0;
    ui64 NEraseRow = 0;

    ui64 SelectRowRows = 0;
    ui64 SelectRowBytes = 0;
    ui64 SelectRangeRows = 0;
    ui64 SelectRangeBytes = 0;
    ui64 SelectRangeDeletedRowSkips = 0;
    ui64 UpdateRowBytes = 0;
    ui64 EraseRowBytes = 0;

    ui64 InvisibleRowSkips = 0;

    TEngineHostCounters& operator+=(const TEngineHostCounters& other) {
        NSelectRow += other.NSelectRow;
        NSelectRange += other.NSelectRange;
        NUpdateRow += other.NUpdateRow;
        NEraseRow += other.NEraseRow;
        SelectRowRows += other.SelectRowRows;
        SelectRowBytes += other.SelectRowBytes;
        SelectRangeRows += other.SelectRangeRows;
        SelectRangeBytes += other.SelectRangeBytes;
        SelectRangeDeletedRowSkips += other.SelectRangeDeletedRowSkips;
        UpdateRowBytes += other.UpdateRowBytes;
        EraseRowBytes += other.EraseRowBytes;
        InvisibleRowSkips += other.InvisibleRowSkips;
        return *this;
    }

    TString ToString() const {
        return TStringBuilder()
            << "{NSelectRow: " << NSelectRow
            << ", NSelectRange: " << NSelectRange
            << ", NUpdateRow: " << NUpdateRow
            << ", NEraseRow: " << NEraseRow
            << ", SelectRowRows: " << SelectRowRows
            << ", SelectRowBytes: " << SelectRowBytes
            << ", SelectRangeRows: " << SelectRangeRows
            << ", SelectRangeBytes: " << SelectRangeBytes
            << ", UpdateRowBytes: " << UpdateRowBytes
            << ", EraseRowBytes: " << EraseRowBytes
            << ", SelectRangeDeletedRowSkips: " << SelectRangeDeletedRowSkips
            << ", InvisibleRowSkips: " << InvisibleRowSkips
            << "}";
    }
};
}}
