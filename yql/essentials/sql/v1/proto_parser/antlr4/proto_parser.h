#pragma once
#include <yql/essentials/parser/proto_ast/common.h>

namespace NSQLTranslationV1 {

NSQLTranslation::TParserFactoryPtr MakeAntlr4ParserFactory(
    bool isAmbiguityError = false,
    bool isAmbiguityDebugging = false);

} // namespace NSQLTranslationV1
