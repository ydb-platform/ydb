#pragma once

#include "kqp_ast.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <util/generic/yexception.h>

#include <memory>

namespace NKikimr::NKqp {

// Only unsupported functionality requests fallback. Parser errors are returned
// as diagnostics; unexpected exceptions must remain failures.
class TNewYqlParserFallback : public yexception {};

struct TNewYqlParseResult {
    std::unique_ptr<NNewYql::TScript> Ast;
    NYql::TIssues Issues;

    bool IsOk() const {
        return bool(Ast);
    }
};

TNewYqlParseResult ParseQueryWithNewParser(const TString& queryText,
    NSQLTranslation::TTranslationSettings settings, bool isAmbiguityError);

} // namespace NKikimr::NKqp
