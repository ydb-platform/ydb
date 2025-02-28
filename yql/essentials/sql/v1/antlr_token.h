#pragma once

#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#define ANTLR3_TOKEN(NAME) (SQLv1LexerTokens::TOKEN_##NAME << 16)
#define ANTLR4_TOKEN(NAME) ((SQLv1Antlr4Lexer::TOKEN_##NAME << 16) + 1)
#define IS_TOKEN(USE_ANTLR4, ID, NAME) (UnifiedToken(USE_ANTLR4, ID) == ANTLR3_TOKEN(NAME) || UnifiedToken(USE_ANTLR4, ID) == ANTLR4_TOKEN(NAME))

namespace NSQLTranslationV1 {

inline ui32 UnifiedToken(bool useAntlr4, ui32 id) {
    return useAntlr4 + (id << 16);
}

}  // namespace NSQLTranslationV1
