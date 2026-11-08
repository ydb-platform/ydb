#pragma once

#include "kqp_ast.h"

namespace antlr4 {
class ParserRuleContext;
}

namespace NKikimr::NKqp {

// Copies all AST data; the concrete syntax tree need only live for this call.
// Unsupported syntax throws TNewYqlParserFallback, never returning a partial AST.
std::unique_ptr<NNewYql::TScript> BuildNewYqlAst(antlr4::ParserRuleContext& root,
    bool ansiLexer, const TString& file);

} // namespace NKikimr::NKqp
