#pragma once

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi, bool antlr4);

}
