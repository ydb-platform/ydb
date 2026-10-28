#pragma once

#include "parse_tree.h"

#include <util/generic/ptr.h>

namespace NSQLPureAST {

class IParser: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IParser>;

    ~IParser() override = default;
    virtual IParseTree::TPtr Parse(TStringBuf text Y_LIFETIME_BOUND) const = 0;
};

IParser::TPtr MakeParser(bool isAnsiLexer);

IParser::TPtr MakeParser();

void ClearParserCache();

} // namespace NSQLPureAST
