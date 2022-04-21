#pragma once

#include "lexer.h"

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/stream/output.h>

namespace NSQLTranslation {

struct TSQLHint {
    // SQL Hints are placed inside comments and have following format:
    // /*+ Name('Val''ue1' Value2 Value3) ... Name2(Value1 Value2 Value3) */
    // or the same with single line comments:
    // --+ Name(Value1 Value2 Value3) ... Name2(Value1 Value2 Value3)
    TString Name;
    TVector<TString> Values;
    NYql::TPosition Pos;

    TString ToString() const;
    void Out(IOutputStream& o) const;
};

using TSQLHints = TMap<NYql::TPosition, TVector<TSQLHint>>;
// SQL hints are collected by lexer and bound to the position of nearest non-comment token to the left
// For example: SELECT /*+ Name(Value) */ -- Name2(Value2)
// in this case TSQLHints will consist of single entry with position of SELECT token

bool CollectSqlHints(ILexer& lexer, const TString& query, const TString& queryName, const TString& queryFile, TSQLHints& hints, NYql::TIssues& issues, size_t maxErrors);

}

Y_DECLARE_OUT_SPEC(inline, NSQLTranslation::TSQLHint, stream, value) {
    value.Out(stream);
}


