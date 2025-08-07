#include "use.h"

#include "evaluate.h"
#include "narrowing_visitor.h"

namespace NSQLComplete {

    namespace {

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(const TParsedInput& input, const TNamedNodes* nodes)
                : TSQLv1NarrowingVisitor(input)
                , Nodes_(nodes)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (ctx->use_stmt() || IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitUse_stmt(SQLv1::Use_stmtContext* ctx) override {
                SQLv1::Cluster_exprContext* expr = ctx->cluster_expr();
                if (!expr) {
                    return {};
                }

                std::string provider;
                std::string cluster;

                if (SQLv1::An_idContext* ctx = expr->an_id()) {
                    provider = ctx->getText();
                }

                if (SQLv1::Pure_column_or_namedContext* ctx = expr->pure_column_or_named()) {
                    if (auto id = GetId(ctx)) {
                        cluster = std::move(*id);
                    }
                }

                if (cluster.empty()) {
                    return {};
                }

                return TUseContext{
                    .Provider = std::move(provider),
                    .Cluster = std::move(cluster),
                };
            }

        private:
            TMaybe<TString> GetId(SQLv1::Pure_column_or_namedContext* ctx) const {
                if (auto* x = ctx->bind_parameter()) {
                    return GetId(x);
                } else if (auto* x = ctx->an_id()) {
                    return x->getText();
                } else {
                    Y_ABORT("You should change implementation according grammar changes");
                }
            }

            TMaybe<TString> GetId(SQLv1::Bind_parameterContext* ctx) const {
                NYT::TNode node = Evaluate(ctx, *Nodes_);
                if (!node.HasValue() || !node.IsString()) {
                    return Nothing();
                }
                return node.AsString();
            }

            const TNamedNodes* Nodes_;
        };

    } // namespace

    // TODO(YQL-19747): Use any to maybe conversion function
    TMaybe<TUseContext> FindUseStatement(TParsedInput input, const TNamedNodes& nodes) {
        std::any result = TVisitor(input, &nodes).visit(input.SqlQuery);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TUseContext>(result);
    }

} // namespace NSQLComplete
