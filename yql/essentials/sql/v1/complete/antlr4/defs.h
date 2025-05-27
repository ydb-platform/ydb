#pragma once

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>

#include <cstddef>
#include <vector>

namespace NSQLComplete {

    using TTokenId = size_t;
    using TRuleId = size_t;

    constexpr TTokenId TOKEN_EOF = antlr4::Token::EOF;

    // std::vector is used to prevent copying a C3 output
    using TParserCallStack = std::vector<TRuleId>;

} // namespace NSQLComplete
