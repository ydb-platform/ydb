#pragma once

#include <ydb/library/yql/parser/lexer_common/lexer.h>

namespace NSQLTranslationV0 {

NSQLTranslation::ILexer::TPtr MakeLexer();

}
