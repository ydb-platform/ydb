#pragma once

#include "parse_tree.h"

#include <util/generic/ptr.h>

namespace NSQLPureAST {

class IParser {
public:
    using TPtr = THolder<IParser>;

    virtual ~IParser() = default;
    virtual TParseTree Parse(TStringBuf text Y_LIFETIME_BOUND) = 0;
};

IParser::TPtr MakeParser(bool isAnsiLexer);

} // namespace NSQLPureAST
