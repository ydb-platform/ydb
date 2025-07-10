#include "function.h"

#include "narrowing_visitor.h"

#include <library/cpp/iterator/enumerate.h>

namespace NSQLComplete {

    namespace {

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(const TParsedInput& input)
                : TSQLv1NarrowingVisitor(input)
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

                return TFunctionContext{
                    .Name = function->getText(),
                    .ArgumentNumber = ArgumentNumber(ctx).GetOrElse(0),
                };
            }

        private:
            TMaybe<size_t> ArgumentNumber(SQLv1::Table_refContext* ctx) {
                for (auto [i, arg] : Enumerate(ctx->table_arg())) {
                    if (IsEnclosing(arg)) {
                        return i;
                    }
                }
                return Nothing();
            }
        };

    } // namespace

    TMaybe<TFunctionContext> EnclosingFunction(TParsedInput input) {
        std::any result = TVisitor(input).visit(input.SqlQuery);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TFunctionContext>(result);
    }

} // namespace NSQLComplete
