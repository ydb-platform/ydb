#include "parser_cache.h"

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLTranslationV1 {

void ClearDefaultParserCache() {
    NALPDefaultAntlr4::SQLv1Antlr4Lexer(nullptr)
        .template getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();

    NALPDefaultAntlr4::SQLv1Antlr4Parser(nullptr)
        .template getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();
}

} // namespace NSQLTranslationV1
