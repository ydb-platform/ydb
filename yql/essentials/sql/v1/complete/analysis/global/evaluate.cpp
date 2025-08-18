#include "evaluate.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

namespace NSQLComplete {

    namespace {

        class TVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            explicit TVisitor(const TNamedNodes* nodes)
                : Nodes_(nodes)
            {
            }

            std::any visitCluster_expr(SQLv1::Cluster_exprContext* ctx) override {
                if (auto* x = ctx->pure_column_or_named()) {
                    return visit(x);
                }
                return defaultResult();
            }

            std::any visitTable_key(SQLv1::Table_keyContext* ctx) override {
                if (auto* x = ctx->id_table_or_type()) {
                    return visit(x);
                }
                return defaultResult();
            }

            std::any visitUnary_casual_subexpr(SQLv1::Unary_casual_subexprContext* ctx) override {
                TPartialValue prev;
                if (auto* x = ctx->id_expr()) {
                    prev = std::any_cast<TPartialValue>(visit(x));
                } else if (auto* x = ctx->atom_expr()) {
                    prev = std::any_cast<TPartialValue>(visit(x));
                }

                auto next = std::any_cast<TPartialValue>(visit(ctx->unary_subexpr_suffix()));
                if (!IsDefined(next)) {
                    return prev;
                }

                return defaultResult();
            }

            std::any visitMul_subexpr(SQLv1::Mul_subexprContext* ctx) override {
                auto args = ctx->con_subexpr();
                Y_ENSURE(!args.empty());

                if (args.size() == 1) {
                    return visit(args[0]);
                }

                NYT::TNode result;
                for (auto* arg : args) {
                    if (!arg) {
                        return defaultResult();
                    }

                    auto value = std::any_cast<TPartialValue>(visit(arg));
                    if (!std::holds_alternative<NYT::TNode>(value)) {
                        return defaultResult();
                    }

                    auto node = std::get<NYT::TNode>(value);
                    auto maybe = Concat(std::move(result), std::move(node));
                    if (!maybe) {
                        return defaultResult();
                    }

                    result = std::move(*maybe);
                }
                return TPartialValue(std::move(result));
            }

            std::any visitKeyword_compat(SQLv1::Keyword_compatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_expr_uncompat(SQLv1::Keyword_expr_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_table_uncompat(SQLv1::Keyword_table_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_select_uncompat(SQLv1::Keyword_select_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_alter_uncompat(SQLv1::Keyword_alter_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_in_uncompat(SQLv1::Keyword_in_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_window_uncompat(SQLv1::Keyword_window_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitKeyword_hint_uncompat(SQLv1::Keyword_hint_uncompatContext* ctx) override {
                return TPartialValue(GetText(ctx));
            }

            std::any visitTerminal(antlr4::tree::TerminalNode* node) override {
                switch (node->getSymbol()->getType()) {
                    case SQLv1::TOKEN_ID_QUOTED:
                        return TPartialValue(TString(Unquoted(GetText(node))));
                    case SQLv1::TOKEN_ID_PLAIN:
                        return TPartialValue(GetText(node));
                    case SQLv1::TOKEN_STRING_VALUE:
                        if (auto content = GetContent(node)) {
                            return TPartialValue(NYT::TNode(std::move(*content)));
                        }
                }
                return defaultResult();
            }

            std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
                TMaybe<std::string> id = GetName(ctx);
                if (id.Empty()) {
                    return defaultResult();
                }

                return EvaluateNode(std::move(*id));
            }

        protected:
            std::any defaultResult() override {
                return TPartialValue(std::monostate());
            }

        private:
            TPartialValue EvaluateNode(std::string name) {
                const TNamedNode* node = Nodes_->FindPtr(name);
                if (!node) {
                    return std::monostate();
                }

                if (std::holds_alternative<NYT::TNode>(*node)) {
                    return std::get<NYT::TNode>(*node);
                }

                if (std::holds_alternative<SQLv1::ExprContext*>(*node)) {
                    if (Resolving_.contains(name)) {
                        return std::monostate();
                    }

                    Resolving_.emplace(name);
                    Y_DEFER {
                        Resolving_.erase(name);
                    };

                    std::any any = visit(std::get<SQLv1::ExprContext*>(*node));
                    return std::any_cast<TPartialValue>(std::move(any));
                }

                return std::monostate();
            }

            TMaybe<NYT::TNode> Concat(NYT::TNode lhs, NYT::TNode rhs) {
                if (!lhs.HasValue()) {
                    return rhs;
                }

                NYT::TNode::EType type = rhs.GetType();
                if (type != lhs.GetType()) {
                    return Nothing();
                }

                switch (type) {
                    case NYT::TNode::String:
                        return lhs.AsString() + rhs.AsString();
                    case NYT::TNode::Int64:
                    case NYT::TNode::Uint64:
                    case NYT::TNode::Double:
                    case NYT::TNode::Bool:
                    case NYT::TNode::List:
                    case NYT::TNode::Map:
                    case NYT::TNode::Undefined:
                    case NYT::TNode::Null:
                        return Nothing();
                }
            }

            TIdentifier GetText(antlr4::tree::ParseTree* tree) const {
                return TIdentifier(tree->getText());
            }

            TMaybe<TString> GetContent(antlr4::tree::TerminalNode* node) const {
                static RE2 regex(R"re(["']([^"'\\]*)["'])re");

                TString text = GetText(node);
                TString content;
                if (!RE2::FullMatch(text, regex, &content)) {
                    return Nothing();
                }

                return content;
            }

            THashSet<std::string> Resolving_;
            const TNamedNodes* Nodes_;
        };

        TPartialValue EvaluateG(antlr4::ParserRuleContext* ctx, const TNamedNodes& nodes) {
            return std::any_cast<TPartialValue>(TVisitor(&nodes).visit(ctx));
        }

    } // namespace

    bool IsDefined(const TPartialValue& value) {
        return !std::holds_alternative<std::monostate>(value);
    }

    TMaybe<TString> ToObjectRef(const TPartialValue& value) {
        return std::visit([](const auto& value) -> TMaybe<TString> {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, NYT::TNode>) {
                if (!value.IsString()) {
                    return Nothing();
                }

                return value.AsString();
            } else if constexpr (std::is_same_v<T, TIdentifier>) {
                return value;
            } else if constexpr (std::is_same_v<T, std::monostate>) {
                return Nothing();
            } else {
                static_assert(false);
            }
        }, value);
    }

    NYT::TNode Evaluate(SQLv1::Bind_parameterContext* ctx, const TNamedNodes& nodes) {
        TPartialValue value = EvaluateG(ctx, nodes);
        if (std::holds_alternative<NYT::TNode>(value)) {
            return std::get<NYT::TNode>(value);
        }
        return NYT::TNode();
    }

    TPartialValue PartiallyEvaluate(antlr4::ParserRuleContext* ctx, const TNamedNodes& nodes) {
        return EvaluateG(ctx, nodes);
    }

} // namespace NSQLComplete
