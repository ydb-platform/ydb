#include "function.h"

#include "narrowing_visitor.h"

namespace NSQLComplete {

    namespace {

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(antlr4::TokenStream* tokens, size_t cursorPosition)
                : TSQLv1NarrowingVisitor(tokens, cursorPosition)
            {
            }

            std::any visit(antlr4::tree::ParseTree* tree) override {
                if (IsEnclosing(tree)) {
                    return TSQLv1NarrowingVisitor::visit(tree);
                }
                return {};
            }

            std::any visitTable_ref(SQLv1::Table_refContext* ctx) override {
                auto* function = ctx->an_id_expr();
                auto* lparen = ctx->TOKEN_LPAREN();
                if (function == nullptr || lparen == nullptr) {
                    return {};
                }

                if (CursorPosition() <= TextInterval(lparen).b) {
                    return {};
                }

                return function->getText();
            }
        };

    } // namespace

    TMaybe<TString> EnclosingFunction(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition) {
        std::any result = TVisitor(tokens, cursorPosition).visit(ctx);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<std::string>(result);
    }

} // namespace NSQLComplete
