#pragma once

#include "defs.h"

#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>

#include <string>

namespace NSQLComplete {

    std::string Display(const antlr4::dfa::Vocabulary& vocabulary, TTokenId tokenType);

} // namespace NSQLComplete
