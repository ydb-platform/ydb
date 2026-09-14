#pragma once

#include "issues.h"

namespace NKikimr::NTable {
    class TDatabase;
    struct IPages;
}

namespace NKikimr::NPDiskTool {

struct TDumpOptions {
    TString Output;             // directory the files are written to
    bool Csv = false;           // comma separated instead of tab separated
    bool TablesOnly = false;    // write the description and stop
    bool IncludeErased = false; // keep erased rows, with a leading op column
};

struct TTableDump {
    ui32 Table = 0;
    TString Name;
    TString File;
    ui64 Rows = 0;
    ui64 Erased = 0;
    ui64 Bytes = 0;
    // False when the scan could not reach the end of the table because a page of some part is not in
    // the input; the rows written before that point are still there.
    bool Complete = true;
};

struct TDumpStats {
    TVector<TTableDump> Tables;
    ui64 Rows = 0;
    ui64 Incomplete = 0;
    TString Description; // path of the file describing the tables
};

// Writes one file per table plus a description of the schema. A table that cannot be read to the end is
// truncated and reported rather than failing the run.
bool DumpTablet(NTable::TDatabase& db, NTable::IPages& env, const TDumpOptions& options,
    TIssueLog& issues, TDumpStats& stats);

// Renders one field the way the dump does: the delimiter, a quote, a newline or a control byte make the
// field quoted, with inner quotes doubled.
TString QuoteField(const TString& value, char delimiter);

} // namespace NKikimr::NPDiskTool
