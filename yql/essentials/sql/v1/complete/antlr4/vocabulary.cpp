#include "vocabulary.h"

namespace NSQLComplete {

    std::string Display(const antlr4::dfa::Vocabulary& vocabulary, TTokenId tokenType) {
        auto name = vocabulary.getDisplayName(tokenType);
        if (2 <= name.length() && name.starts_with('\'') && name.ends_with('\'')) {
            name.erase(static_cast<std::string::size_type>(0), 1);
            name.pop_back();
        }
        return name;
    }

} // namespace NSQLComplete
