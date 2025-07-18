#include "named_node.h"

#include "narrowing_visitor.h"

#include <library/cpp/iterator/iterate_keys.h>
#include <library/cpp/iterator/iterate_values.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NSQLComplete {

    namespace {

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(const TParsedInput& input, THashSet<TString>* names)
                : TSQLv1NarrowingVisitor(input)
                , Names_(names)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (ctx->declare_stmt() ||
                    ctx->import_stmt() ||
                    ctx->define_action_or_subquery_stmt() ||
                    ctx->named_nodes_stmt() ||
                    IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitDeclare_stmt(SQLv1::Declare_stmtContext* ctx) override {
                VisitNullable(ctx->bind_parameter());
                return {};
            }

            std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
                VisitNullable(ctx->named_bind_parameter_list());
                return {};
            }

            std::any visitDefine_action_or_subquery_stmt(
                SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
                VisitNullable(ctx->bind_parameter());
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
                VisitNullable(ctx->bind_parameter_list());
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitFor_stmt(SQLv1::For_stmtContext* ctx) override {
                VisitNullable(ctx->bind_parameter());
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitLambda(SQLv1::LambdaContext* ctx) override {
                VisitNullable(ctx->smart_parenthesis());
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitNamed_bind_parameter(
                SQLv1::Named_bind_parameterContext* ctx) override {
                VisitNullable(ctx->bind_parameter(0));
                return {};
            }

            std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
                if (IsEnclosing(ctx)) {
                    return {};
                }

                TMaybe<std::string> id = GetId(ctx);
                if (id.Empty() || id == "_") {
                    return {};
                }

                Names_->emplace(std::move(*id));
                return {};
            }

        private:
            void VisitNullable(antlr4::tree::ParseTree* tree) {
                if (tree == nullptr) {
                    return;
                }
                visit(tree);
            }

            THashSet<TString>* Names_;
        };

    } // namespace

    TVector<TString> CollectNamedNodes(TParsedInput input) {
        THashSet<TString> names;
        TVisitor(input, &names).visit(input.SqlQuery);
        return TVector<TString>(begin(names), end(names));
    }

} // namespace NSQLComplete
