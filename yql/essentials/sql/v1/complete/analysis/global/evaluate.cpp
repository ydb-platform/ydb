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
                TMaybe<std::string> id = GetId(ctx);
                if (id.Empty()) {
                    return defaultResult();
                }

                id->insert(0, "$");
                if (const NYT::TNode* node = Env_->Parameters.FindPtr(*id)) {
                    return *node;
                }

                return defaultResult();
            }

            std::any defaultResult() override {
                return NYT::TNode();
            }

        private:
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
