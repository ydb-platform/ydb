#include "narrowing_visitor.h"

namespace NSQLComplete {

    TSQLv1NarrowingVisitor::TSQLv1NarrowingVisitor(const TParsedInput& input)
        : Tokens_(input.Tokens)
        , CursorPosition_(input.Original.CursorPosition)
    {
    }

    bool TSQLv1NarrowingVisitor::shouldVisitNextChild(antlr4::tree::ParseTree* node, const std::any& /*currentResult*/) {
        return TextInterval(node).a < static_cast<ssize_t>(CursorPosition_);
    }

    bool TSQLv1NarrowingVisitor::IsEnclosing(antlr4::tree::ParseTree* tree) const {
        return TextInterval(tree).properlyContains(CursorInterval());
    }

    ssize_t TSQLv1NarrowingVisitor::CursorPosition() const {
        return CursorPosition_;
    }

    antlr4::misc::Interval TSQLv1NarrowingVisitor::TextInterval(antlr4::tree::ParseTree* tree) const {
        auto tokens = tree->getSourceInterval();
        if (tokens.b == -1) {
            tokens.b = tokens.a;
        }
        return antlr4::misc::Interval(
            Tokens_->get(tokens.a)->getStartIndex(),
            Tokens_->get(tokens.b)->getStopIndex());
    }

    antlr4::misc::Interval TSQLv1NarrowingVisitor::CursorInterval() const {
        auto cursor = CursorPosition();
        return antlr4::misc::Interval(cursor, cursor);
    }

} // namespace NSQLComplete
