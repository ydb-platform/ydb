#pragma once

#include <ydb/core/scheme/scheme_type_info.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

// Column of one internal table. KeyOrder is the primary-key position, or -1.
struct TLocalSqlColumn {
    TString Name;
    NKikimr::NScheme::TTypeInfo Type;
    bool NotNull = false;
    i32 KeyOrder = -1;
};

struct TLocalSqlTable {
    TString Name;
    TVector<TLocalSqlColumn> Columns;
};

// Names are matched as written. Unquoted SQL identifiers are not lowercased:
// internal tables keep the names from the tablet scheme.
struct TLocalSqlSchema {
    THashMap<TString, TLocalSqlTable> Tables;
};

struct TLocalSqlTranslation {
    TString Program;
    TString Error;

    bool IsOk() const {
        return Error.empty();
    }
};

// True when the text is the s-expression language ConvertToMiniKQL already accepts.
// Whitespace and comments are skipped. Empty text is treated as that language.
bool IsLocalMiniKqlSExpr(TStringBuf text);

// Translate one or more statements of a narrow SQL dialect into that s-expression.
//
// Point reads, updates and deletes require an equality for every key column.
// A read without a full key becomes SelectRange. Bounds must be literals or
// parameters. A comparison that the range format cannot represent, a filter on
// a non-key column, and a key computed from another row are rejected.
//
//   SELECT cols FROM table [WHERE ...] [ORDER BY first_key [ASC|DESC]] [LIMIT n]
//   UPDATE table SET col = value, ... WHERE <full key>
//   DELETE FROM table WHERE <full key>
//   UPSERT INTO table (cols) VALUES (values)
TLocalSqlTranslation TranslateLocalSql(TStringBuf sql, const TLocalSqlSchema& schema);

} // namespace NYql
