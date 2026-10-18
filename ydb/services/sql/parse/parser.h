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

enum class EAlterTableKind {
    AddColumn,
    DropColumn,
    SetNotNull,
    DropNotNull,
    SetDefault,
    DropDefault,
};

enum class EDropKind {
    Table,
    Sequence,
    Schema,
};

struct TCreateIndexStmt {
    bool Unique = false;
    std::optional<TIdentifier> Schema;
    TIdentifier Table;
    TIdentifier Name;
    TVector<TIdentifier> Keys;
    TVector<TIdentifier> Include;
};

struct TSchemaStmt {
    bool IfNotExists = false;
    TIdentifier Name;
};

struct TDropStmt {
    EDropKind Kind = EDropKind::Table;
    bool IfExists = false;
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
};

struct TAlterSequenceStmt {
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TString DataType;
    bool Restart = false;
    std::optional<i64> RestartWith;
};

struct TCreateSequenceStmt {
    bool IfNotExists = false;
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TString DataType;
    std::optional<i64> Increment;
    std::optional<i64> MinValue;
    std::optional<i64> MaxValue;
    std::optional<i64> StartValue;
    std::optional<ui64> Cache;
    bool Cycle = false;
};

struct TAlterTableStmt {
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    EAlterTableKind Kind = EAlterTableKind::AddColumn;
    bool IfExists = false;
    TColumnDef Column;
};

struct TParseResult {
    std::optional<TSelectStmt> Select;
    std::optional<TCreateTableStmt> CreateTable;
    std::optional<TAlterTableStmt> AlterTable;
    std::optional<TCreateSequenceStmt> CreateSequence;
    std::optional<TAlterSequenceStmt> AlterSequence;
    std::optional<TDropStmt> Drop;
    std::optional<TSchemaStmt> CreateSchema;
    std::optional<TCreateIndexStmt> CreateIndex;
    TString SqlState;
    TString Message;
};

// PostgreSQL 18 gram.y in C++. Implemented: SELECT, CREATE TABLE, ALTER TABLE ADD COLUMN.
// Any other rule is SQLSTATE 0A000 and is not a statement.
TParseResult Parse(TStringBuf sql);

} // namespace NKikimr::NSql
