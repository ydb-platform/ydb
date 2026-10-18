#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSql {

struct TIdentifier {
    TString Name;
    bool Quoted = false;
};

enum class ECompareOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    Le,
    Ge,
};

struct TLiteral {
    enum class EKind {
        Integer,
        String,
    };

    EKind Kind = EKind::Integer;
    TString Text;
};

struct TComparison {
    TIdentifier Column;
    ECompareOp Op = ECompareOp::Eq;
    TLiteral Value;
};

struct TSelectStmt {
    bool Star = false;
    TVector<TIdentifier> Columns;
    std::optional<TIdentifier> Schema;
    TIdentifier From;
    TVector<TComparison> Where;
    std::optional<ui64> Limit;
};

struct TParseResult {
    std::optional<TSelectStmt> Select;
    TString SqlState;
    TString Message;
};

// SELECT of a column list or * FROM one table. Anything else is SQLSTATE 0A000.
TParseResult Parse(TStringBuf sql);

} // namespace NKikimr::NSql
