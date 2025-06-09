#pragma once

namespace NSQLComplete {

    template <class Lexer, class Parser>
    struct TAntlrGrammar {
        using TLexer = Lexer;
        using TParser = Parser;

        TAntlrGrammar() = delete;
    };

} // namespace NSQLComplete
