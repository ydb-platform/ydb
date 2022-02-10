#include "lexer.h"


namespace NSQLTranslation {

IOutputStream& OutputTokens(IOutputStream& out, TParsedTokenList::const_iterator begin, TParsedTokenList::const_iterator end) {
    for (auto it = begin; it != end; ++it) {
        out << it->Content;
    }
    return out;
}

}
