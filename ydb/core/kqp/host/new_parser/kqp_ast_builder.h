#pragma once

#include "kqp_ast.h"

namespace antlr4 {
class ParserRuleContext;
}

namespace NKikimr::NKqp {

// Copies all AST data; the concrete syntax tree need only live for this call.
// Unsupported syntax throws TNewYqlParserFallback, never returning a partial AST.
// The root and ansiLexer must come from the matching generated parser dialect.
// This boundary only constructs syntax: catalog access, identifier binding and
// YQL expression construction happen after the parse tree has been released.
std::unique_ptr<NNewYql::TScript> BuildNewYqlAst(antlr4::ParserRuleContext& root,
    bool ansiLexer, const TString& file);

} // namespace NKikimr::NKqp
