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

                TMaybe<TClusterContext> cluster = ParseClusterContext(expr, *Nodes_);
                if (!cluster) {
                    return {};
                }

                return *cluster;
            }

        private:
            const TNamedNodes* Nodes_;
        };

        class TClusterVisitor: public TSQLv1BaseVisitor {
        public:
            explicit TClusterVisitor(const TNamedNodes* nodes)
                : Nodes_(nodes)
            {
            }

            std::any visitCluster_expr(SQLv1::Cluster_exprContext* ctx) {
                std::string provider;
                std::string cluster;

                if (SQLv1::An_idContext* id = ctx->an_id()) {
                    provider = id->getText();
                }

                if (SQLv1::Pure_column_or_namedContext* named = ctx->pure_column_or_named()) {
                    if (auto id = GetId(named)) {
                        cluster = std::move(*id);
                    }
                }

                if (cluster.empty()) {
                    return {};
                }

                return TClusterContext{
                    .Provider = std::move(provider),
                    .Name = std::move(cluster),
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

    TMaybe<TClusterContext> ParseClusterContext(SQLv1::Cluster_exprContext* ctx, const TNamedNodes& nodes) {
        std::any result = TClusterVisitor(&nodes).visit(ctx);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TClusterContext>(result);
    }

    // TODO(YQL-19747): Use any to maybe conversion function
    TMaybe<TClusterContext> FindUseStatement(TParsedInput input, const TNamedNodes& nodes) {
        std::any result = TVisitor(input, &nodes).visit(input.SqlQuery);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TClusterContext>(result);
    }

} // namespace NSQLComplete
