#pragma once

#include "kqp_ast.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/generic/yexception.h>

#include <memory>

namespace NKikimr::NKqp {

class TCompileTrace;

// Only unsupported functionality requests fallback. Parser errors are returned
// as diagnostics; unexpected exceptions must remain failures.
class TNewYqlParserFallback : public yexception {};

// A non-null AST is a complete syntax result, not a semantic success flag.
// Invalid SQL returns Issues without an AST; unsupported valid syntax uses
// the separate fallback exception so the host can select the legacy frontend.
struct TNewYqlParseResult {
    std::unique_ptr<NNewYql::TScript> Ast;
    NYql::TIssues Issues;

    bool IsOk() const {
        return bool(Ast);
    }
};

// Settings are copied because leading translation directives can change the
// lexer/dialect for this parse. The returned AST owns all spelling and source
// location data and may outlive queryText and the temporary parser objects.
TNewYqlParseResult ParseQueryWithNewParser(const TString& queryText,
    NSQLTranslation::TTranslationSettings settings, bool isAmbiguityError, TCompileTrace* trace = nullptr);

} // namespace NKikimr::NKqp
