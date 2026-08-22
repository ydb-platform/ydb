#pragma once

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#include <util/system/types.h>

#define ANTLR4_TOKEN(NAME) ((NALPDefaultAntlr4::SQLv1Antlr4Lexer::TOKEN_##NAME << 16) + 1)
#define IS_TOKEN(ID, NAME) (UnifiedToken(ID) == ANTLR4_TOKEN(NAME))

namespace NSQLTranslationV1 {

inline constexpr ui32 UnifiedToken(ui32 id) {
    return 1 + (id << 16);
}

} // namespace NSQLTranslationV1
