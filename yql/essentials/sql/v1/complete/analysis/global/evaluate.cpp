#include "evaluate.h"

namespace NSQLComplete {

    namespace {

        class TVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            explicit TVisitor(const TEnvironment* env)
                : Env_(env)
            {
            }

            std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
                std::string id = GetId(ctx);
                if (const NYT::TNode* node = Env_->Parameters.FindPtr(id)) {
                    return *node;
                }
                return defaultResult();
            }

            std::any defaultResult() override {
                return NYT::TNode();
            }

        private:
            std::string GetId(SQLv1::Bind_parameterContext* ctx) const {
                if (auto* x = ctx->an_id_or_type()) {
                    return x->getText();
                } else if (auto* x = ctx->TOKEN_TRUE()) {
                    return x->getText();
                } else if (auto* x = ctx->TOKEN_FALSE()) {
                    return x->getText();
                } else {
                    Y_ABORT("You should change implementation according grammar changes");
                }
            }

            const TEnvironment* Env_;
        };

        NYT::TNode EvaluateG(antlr4::ParserRuleContext* ctx, const TEnvironment& env) {
            return std::any_cast<NYT::TNode>(TVisitor(&env).visit(ctx));
        }

    } // namespace

    NYT::TNode Evaluate(SQLv1::Bind_parameterContext* ctx, const TEnvironment& env) {
        return EvaluateG(ctx, env);
    }

} // namespace NSQLComplete
