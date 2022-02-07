#pragma once

#include <ydb/library/yql/parser/lexer_common/lexer.h>

namespace NSQLTranslationV1 {

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi);

}
