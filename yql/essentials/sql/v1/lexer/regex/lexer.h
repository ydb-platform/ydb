#pragma once

#include "generic.h"

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

namespace NSQLTranslationV1 {

    TTokenMatcher ANSICommentMatcher(TString name, TTokenMatcher defaultComment);

    TRegexPattern KeywordPattern(const NSQLReflect::TLexerGrammar& grammar);

    TRegexPattern PuntuationPattern(const NSQLReflect::TLexerGrammar& grammar);

    NSQLTranslation::TLexerFactoryPtr MakeRegexLexerFactory(bool ansi);

} // namespace NSQLTranslationV1
