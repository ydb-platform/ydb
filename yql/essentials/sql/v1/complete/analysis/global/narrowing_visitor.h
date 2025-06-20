#pragma once

#include "parse_tree.h"

namespace NSQLComplete {

    class TSQLv1NarrowingVisitor: public SQLv1Antlr4BaseVisitor {
    public:
        TSQLv1NarrowingVisitor(antlr4::TokenStream* tokens, size_t cursorPosition);

    protected:
        bool shouldVisitNextChild(antlr4::tree::ParseTree* node, const std::any& /*currentResult*/) override;
        std::any aggregateResult(std::any aggregate, std::any nextResult) override;

        bool IsEnclosing(antlr4::tree::ParseTree* tree) const;
        ssize_t CursorPosition() const;
        antlr4::misc::Interval TextInterval(antlr4::tree::ParseTree* tree) const;
        antlr4::misc::Interval CursorInterval() const;

    private:
        antlr4::TokenStream* Tokens_;
        size_t CursorPosition_;
    };

} // namespace NSQLComplete
