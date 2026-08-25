#pragma once

#include "parse_tree.h"

#include <util/generic/ptr.h>

namespace NSQLPureAST {

class IParser {
public:
    using TPtr = THolder<IParser>;

    virtual ~IParser() = default;
    virtual IParseTree::TPtr Parse(TStringBuf text Y_LIFETIME_BOUND) const = 0;
};

IParser::TPtr MakeParser(bool isAnsiLexer);

void ClearParserCache();

} // namespace NSQLPureAST
