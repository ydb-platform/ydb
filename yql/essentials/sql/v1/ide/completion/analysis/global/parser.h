#pragma once

#include "input.h"

#include <util/generic/ptr.h>

namespace NSQLComplete {

class IParser {
public:
    using TPtr = THolder<IParser>;

    virtual ~IParser() = default;
    virtual TParsedInput Parse(TCompletionInput input Y_LIFETIME_BOUND) Y_LIFETIME_BOUND = 0;
};

IParser::TPtr MakeParser(bool isAnsiLexer);

} // namespace NSQLComplete
