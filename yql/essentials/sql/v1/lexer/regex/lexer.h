#pragma once

#include "generic.h"

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

    TTokenMatcher ANSICommentMatcher(TTokenMatcher defaultComment);

    NSQLTranslation::TLexerFactoryPtr MakeRegexLexerFactory(bool ansi);

} // namespace NSQLTranslationV1
