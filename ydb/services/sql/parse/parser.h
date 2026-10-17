#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSql {

struct TIdentifier {
    TString Name;
    bool Quoted = false;
};

struct TSelectStmt {
    bool Star = false;
    TVector<TIdentifier> Columns;
    TIdentifier From;
};

struct TParseResult {
    std::optional<TSelectStmt> Select;
    TString SqlState;
    TString Message;
};

// SELECT of a column list or * FROM one table. Anything else is SQLSTATE 0A000.
TParseResult Parse(TStringBuf sql);

} // namespace NKikimr::NSql
