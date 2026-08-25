#include "parser_cache.h"

#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NSQLTranslationV1 {

void ClearAnsiParserCache() {
    NALPAnsiAntlr4::SQLv1Antlr4Lexer(nullptr)
        .template getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();

    NALPAnsiAntlr4::SQLv1Antlr4Parser(nullptr)
        .template getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();
}

} // namespace NSQLTranslationV1
