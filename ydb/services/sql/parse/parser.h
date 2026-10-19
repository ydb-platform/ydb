#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSql {

struct TIdentifier {
    TString Name;
    bool Quoted = false;
    TString Qualifier;
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
        Null,
        Parameter,
    };

    EKind Kind = EKind::Integer;
    TString Text;
};

struct TComparison {
    TIdentifier Column;
    ECompareOp Op = ECompareOp::Eq;
    TLiteral Value;
};

enum class EJoinKind {
    Inner,
    Left,
    Right,
    Cross,
    Full,
};

struct TJoin {
    EJoinKind Kind = EJoinKind::Inner;
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TString Alias;
    TString LeftAlias;
    TIdentifier LeftColumn;
    TIdentifier RightColumn;
};

struct TOrderKey {
    TIdentifier Column;
    bool Desc = false;
    bool NullsFirst = false;
};

enum class EAggFunc {
    Count,
    Sum,
    Min,
    Max,
};

struct TAggCall {
    EAggFunc Func = EAggFunc::Count;
    bool Star = false;
    TIdentifier Argument;
    size_t Target = 0;
};

enum class EWindowFunc {
    RowNumber,
    Rank,
};

struct TWindowCall {
    EWindowFunc Func = EWindowFunc::RowNumber;
    TVector<TIdentifier> Partition;
    TVector<TOrderKey> Order;
    size_t Target = 0;
};

struct TSelectStmt {
    bool Star = false;
    bool Distinct = false;
    TVector<TIdentifier> DistinctOn;
    TVector<TIdentifier> Columns;
    std::optional<TIdentifier> Schema;
    TIdentifier From;
    TString Alias;
    std::optional<TJoin> Join;
    TVector<TComparison> Where;
    TVector<TAggCall> Aggs;
    TVector<TWindowCall> Windows;
    TVector<TIdentifier> Group;
    TVector<TComparison> Having;
    TVector<TOrderKey> Order;
    TVector<int> ScalarOf;
    bool FromQuery = false;
    std::optional<ui64> Limit;
    std::optional<ui64> Offset;
    // Added to the single selected column of a recursive step. Zero means the column is copied.
    i64 Addend = 0;
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
    Index,
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

enum class EConflictAction {
    None,
    Nothing,
    Update,
};

struct TUpdateAssign {
    TIdentifier Column;
    bool Excluded = false;
    TIdentifier ExcludedColumn;
    TLiteral Value;
};

// One comparison. Excluded means the left side is EXCLUDED.column.
struct TConflictWhere {
    TIdentifier Column;
    bool Excluded = false;
    ECompareOp Op = ECompareOp::Eq;
    TLiteral Value;
};

struct TReturningItem {
    bool Star = false;
    TIdentifier Column;
    std::optional<TIdentifier> Alias;
};

// One VALUES tuple of literals. A second tuple, SELECT and RETURNING stay 0A000.
// ON CONFLICT is only the primary key, DO NOTHING or DO UPDATE of literals and EXCLUDED.
struct TInsertStmt {
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TVector<TIdentifier> Columns;
    TVector<TLiteral> Values;
    EConflictAction Conflict = EConflictAction::None;
    TVector<TIdentifier> ConflictTarget;
    TVector<TUpdateAssign> Update;
    std::optional<TConflictWhere> ConflictWhere;
    TVector<TReturningItem> Returning;
    std::optional<TSelectStmt> Source;
};

enum class ETxKind {
    Begin,
    Commit,
    Rollback,
};

struct TTxStmt {
    ETxKind Kind = ETxKind::Begin;
};

// SET is a literal or a column of the same row. FROM another table stays 0A000.
struct TSetAssign {
    TIdentifier Column;
    bool FromColumn = false;
    TIdentifier Source;
    TLiteral Value;
};

struct TDeleteStmt {
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TVector<TComparison> Where;
    TVector<TReturningItem> Returning;
};

struct TUpdateStmt {
    std::optional<TIdentifier> Schema;
    TIdentifier Name;
    TVector<TSetAssign> Set;
    TVector<TComparison> Where;
    TVector<TReturningItem> Returning;
};

struct TSetStmt {
    TString Name;
    TString Value;
};

struct TShowStmt {
    TString Name;
};

enum class ECombine {
    None,
    Union,
    UnionAll,
    Intersect,
    Except,
};

struct TParseResult {
    std::optional<TSelectStmt> Select;
    ECombine Combine = ECombine::None;
    std::optional<TSelectStmt> SelectRight;
    TVector<TSelectStmt> Scalars;
    std::optional<TSelectStmt> FromSubquery;
    TString CteName;
    std::optional<TSelectStmt> Cte;
    bool Recursive = false;
    std::optional<TSelectStmt> CteStep;
    ECombine CteCombine = ECombine::None;
    std::optional<TCreateTableStmt> CreateTable;
    std::optional<TAlterTableStmt> AlterTable;
    std::optional<TCreateSequenceStmt> CreateSequence;
    std::optional<TAlterSequenceStmt> AlterSequence;
    std::optional<TDropStmt> Drop;
    std::optional<TSchemaStmt> CreateSchema;
    std::optional<TCreateIndexStmt> CreateIndex;
    std::optional<TInsertStmt> Insert;
    std::optional<TTxStmt> Tx;
    std::optional<TUpdateStmt> Update;
    std::optional<TDeleteStmt> Delete;
    std::optional<TSetStmt> Set;
    std::optional<TShowStmt> Show;
    TString SqlState;
    TString Message;
};

// PostgreSQL 18 gram.y in C++. Implemented: SELECT, CREATE TABLE, ALTER TABLE ADD COLUMN, INSERT one row.
// Any other rule is SQLSTATE 0A000 and is not a statement.
TParseResult Parse(TStringBuf sql);

} // namespace NKikimr::NSql
