#include "kqp_statement_rewrite.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr {
namespace NKqp {

namespace {
    struct TCreateTableAsResult {
        NYql::TExprNode::TPtr CreateTable = nullptr;
        NYql::TExprNode::TPtr ReplaceInto = nullptr;
    };

    std::optional<TCreateTableAsResult> RewriteCreateTableAs(NYql::TExprNode::TPtr root, NYql::TExprContext& ctx) {
        NYql::NNodes::TExprBase expr(root);
        auto maybeWrite = expr.Maybe<NYql::NNodes::TCoWrite>();
        if (!maybeWrite) {
            return std::nullopt;
        }
        auto write = maybeWrite.Cast();

        if (write.DataSink().FreeArgs().Count() < 2
            || write.DataSink().Category() != "kikimr"
            || write.DataSink().FreeArgs().Get(1).Cast<NYql::NNodes::TCoAtom>() != "db") {
            return std::nullopt;
        }

        auto writeArgs = write.FreeArgs();
        if (writeArgs.Count() < 5) {
            return std::nullopt;
        }

        auto maybeKey = writeArgs.Get(2).Maybe<NYql::NNodes::TCoKey>();
        if (!maybeKey) {
            return std::nullopt;
        }
        auto key = maybeKey.Cast();
        if (key.ArgCount() == 0) {
            return std::nullopt;
        }

        if (key.Ptr()->Child(0)->Child(0)->Content() != "tablescheme") {
            return std::nullopt;
        }

        auto maybeList = writeArgs.Get(4).Maybe<NYql::NNodes::TExprList>();
        if (!maybeList) {
            return std::nullopt;
        }
        auto settings = NYql::NCommon::ParseWriteTableSettings(maybeList.Cast(), ctx);
        if (!settings.Mode) {
            return std::nullopt;
        }

        auto mode = settings.Mode.Cast();
        if (mode != "create" && mode != "create_if_not_exists" && mode != "create_or_replace") {
            return std::nullopt;
        }

        const auto& insertData = writeArgs.Get(3);
        if (insertData.Ptr()->Content() == "Void") {
            return std::nullopt;
        }

        const auto pos = insertData.Ref().Pos();

        TCreateTableAsResult result;
        result.CreateTable = ctx.ReplaceNode(std::move(root), insertData.Ref(), ctx.NewCallable(pos, "Void", {}));

        const auto insert = ctx.NewCallable(pos, "Write!", {
            ctx.NewWorld(pos),
            ctx.NewCallable(pos, "DataSink", {
                ctx.NewAtom(pos, "kikimr"),
                ctx.NewAtom(pos, "db"),
            }),
            ctx.NewCallable(pos, "Key", {
                ctx.NewList(pos, {
                    ctx.NewAtom(pos, "table"),
                    ctx.NewCallable(pos, "String", {
                        ctx.NewAtom(pos, key.Ptr()->Child(0)->Child(1)->Child(0)->Content()),
                    }),
                }),
            }),
            insertData.Ptr(),
            ctx.NewList(pos, {
                ctx.NewList(pos, {
                    ctx.NewAtom(pos, "mode"),
                    ctx.NewAtom(pos, "replace"),
                }),
            }),
        });

        result.ReplaceInto = ctx.NewCallable(pos, "Commit!", {
            insert,
            ctx.NewCallable(pos, "DataSink", {
                ctx.NewAtom(pos, "kikimr"),
                ctx.NewAtom(pos, "db"),
            }),
            ctx.NewList(pos, {
                ctx.NewList(pos, {
                    ctx.NewAtom(pos, "mode"),
                    ctx.NewAtom(pos, "flush"),
                }),
            }),
        });

        return result;
    }
}

TVector<NYql::TExprNode::TPtr> RewriteExpression(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx) {
    // CREATE TABLE AS statement can be used only with perstatement execution.
    // Thus we assume that there is only one such statement.
    TVector<NYql::TExprNode::TPtr> result;
    VisitExpr(root, [&result, &ctx](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            const auto rewriteResult = RewriteCreateTableAs(node, ctx);
            if (rewriteResult) {
                YQL_ENSURE(result.empty());
                result.push_back(rewriteResult->CreateTable);
                result.push_back(rewriteResult->ReplaceInto);
            }
        }
        return true;
    });

    if (result.empty()) {
        result.push_back(root);
    }
    return result;
}

}
}
