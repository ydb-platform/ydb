#pragma once
#include <yql/essentials/parser/proto_ast/common.h>

#include <util/generic/maybe.h>

namespace NSQLTranslationV1 {

NSQLTranslation::TParserFactoryPtr MakeAntlr4AnsiParserFactory(
    bool isAmbiguityError = false,
    bool isAmbiguityDebugging = false,
    TMaybe<size_t> maxParseTreeDepth = Nothing());

} // namespace NSQLTranslationV1
