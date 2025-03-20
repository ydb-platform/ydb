#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

    NSQLTranslation::TLexerFactoryPtr MakeAntlr4PureAnsiLexerFactory();

} // namespace NSQLTranslationV1
