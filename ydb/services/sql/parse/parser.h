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

// PostgreSQL type name. Quoted names are explicit YDB types.
struct TPgType {
    TString Name;
    bool Quoted = false;
    std::optional<ui32> Precision;
    std::optional<ui32> Scale;
};

struct TColumnDef {
    TIdentifier Name;
    TPgType Type;
    bool NotNull = false;
    std::optional<TLiteral> Default;
};

struct TCreateTableStmt {
    bool IfNotExists = false;
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TVector<TColumnDef> Columns;
    TVector<TIdentifier> PrimaryKey;
    TVector<TVector<TIdentifier>> Unique;
};

struct TParseResult {
    std::optional<TSelectStmt> Select;
    std::optional<TCreateTableStmt> CreateTable;
    TString SqlState;
    TString Message;
};

// SELECT, or CREATE TABLE with columns, NULL and a primary key.
// Anything else is SQLSTATE 0A000 and is not a statement.
TParseResult Parse(TStringBuf sql);

} // namespace NKikimr::NSql
