#include "kqp_statement_rewrite.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>

namespace NKikimr {
namespace NKqp {

namespace {
    struct TCreateTableAsResult {
        NYql::TExprNode::TPtr CreateTable = nullptr;
        NYql::TExprNode::TPtr ReplaceInto = nullptr;
    };

    std::optional<TCreateTableAsResult> RewriteCreateTableAs(NYql::TExprNode::TPtr root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx, const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx, const TString& cluster) {
        Cerr << "REWRITE:>> " << NYql::NCommon::ExprToPrettyString(ctx, *root) << Endl;

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

        auto typeTransformer = NYql::TTransformationPipeline(&typeCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddIOAnnotation()
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(cluster, sessionCtx->TablesPtr(), typeCtx, sessionCtx->ConfigPtr()))
            .Build(false);

        Cerr << "AFTER:>> " << Endl;

        auto ptr = insertData.Ptr();
        auto transformResult = NYql::SyncTransform(*typeTransformer, ptr, ctx);
        if (transformResult != NYql::IGraphTransformer::TStatus::Ok) {
            Cerr << "ERRORS: " << ctx.IssueManager.GetIssues().ToString() << Endl;
        }
        YQL_ENSURE(transformResult == NYql::IGraphTransformer::TStatus::Ok);

        Cerr << "TRANSFORMED:>> " << Endl;

        Cerr << "TEST:>> " << NYql::NCommon::ExprToPrettyString(ctx, *ptr) << Endl;
        auto type = ptr->GetTypeAnn();
        YQL_ENSURE(type);
        Cerr << "TYPES:>> ";
        type->Out(Cerr);
        Cerr << Endl;

        type = type->Cast<NYql::TListExprType>()->GetItemType();
        YQL_ENSURE(type);
        auto rowType = type->Cast<NYql::TStructExprType>();
        YQL_ENSURE(rowType);

        auto create = ctx.ReplaceNode(std::move(root), insertData.Ref(), ctx.NewCallable(pos, "Void", {}));

        auto columns = create->Child(4)->Child(1)->Child(1);
        auto primaryKey = create->Child(4)->Child(2)->Child(1);
        THashSet<TStringBuf> primariKeyColumns;
        primaryKey->ForEachChild([&](const auto& child) {
            primariKeyColumns.insert(child.Content());
        });

        if (columns->ChildrenSize() != 0) {
            for (size_t index = 0; index < columns->ChildrenSize(); ++index) {
                const auto name = columns->Child(index)->ChildPtr(0)->Content();
                const bool notNull = primariKeyColumns.contains(name); //TODO: and storetype == columns

                auto currentType = rowType->FindItemType(name);

                if (notNull && currentType->GetKind() == NYql::ETypeAnnotationKind::Optional) {
                    currentType = currentType->Cast<NYql::TOptionalExprType>()->GetItemType();
                }

                auto typeNode = NYql::ExpandType(pos, *currentType, ctx);

                if (!notNull && currentType->GetKind() != NYql::ETypeAnnotationKind::Optional) {
                    typeNode = ctx.NewCallable(pos, "AsOptionalType", { typeNode });
                }

                // TODO: replacenodes
                create = ctx.ReplaceNode(std::move(create), *columns->Child(index), ctx.NewList(pos, {
                    columns->Child(index)->ChildPtr(0),
                    typeNode,
                    ctx.NewList(pos, {
                        ctx.NewAtom(pos, "columnConstrains"),
                        notNull
                            ? ctx.NewList(pos, {
                                ctx.NewList(pos, {
                                    ctx.NewAtom(pos, "not_null"),
                                }),
                            })
                            : ctx.NewList(pos, {}),
                    }),
                    ctx.NewList(pos, {}),
                }));
            }
        } else {
            std::vector<NYql::TExprNodePtr> columnNodes;
            for (const auto* item : rowType->GetItems()) {
                const auto name = item->GetName();
                auto currentType = item->GetItemType();

                const bool notNull = primariKeyColumns.contains(name); //TODO: and storetype == columns

                if (notNull && currentType->GetKind() == NYql::ETypeAnnotationKind::Optional) {
                    currentType = currentType->Cast<NYql::TOptionalExprType>()->GetItemType();
                }

                auto typeNode = NYql::ExpandType(pos, *currentType, ctx);

                if (!notNull && currentType->GetKind() != NYql::ETypeAnnotationKind::Optional) {
                    typeNode = ctx.NewCallable(pos, "AsOptionalType", { typeNode });
                }

                columnNodes.push_back(ctx.NewList(pos, {
                    ctx.NewAtom(pos, name),
                    typeNode,
                    ctx.NewList(pos, {
                        ctx.NewAtom(pos, "columnConstrains"),
                        notNull
                            ? ctx.NewList(pos, {
                                ctx.NewList(pos, {
                                    ctx.NewAtom(pos, "not_null"),
                                }),
                            })
                            : ctx.NewList(pos, {}),
                    }),
                    ctx.NewList(pos, {}),
                }));
            }

            create = ctx.ReplaceNode(std::move(create), *columns, ctx.NewList(pos, std::move(columnNodes)));
        }


        Cerr << "COLUMNS:PROCESSED>> " << NYql::NCommon::ExprToPrettyString(ctx, *create->Child(4)->Child(1)->Child(1)) << Endl;

        //TODO: Use io utils
        const NYql::TExprNode::TPtr* lastReadInTopologicalOrder = nullptr;
        NYql::VisitExpr(
            insertData.Ptr(),
            nullptr,
            [&lastReadInTopologicalOrder](const NYql::TExprNode::TPtr& node) {
                if (node->IsCallable(NYql::ReadName)) {
                    lastReadInTopologicalOrder = &node;
                }
                return true;
            }
        );

        const auto insert = ctx.NewCallable(pos, "Write!", {
            lastReadInTopologicalOrder == nullptr ? ctx.NewWorld(pos) : ctx.NewCallable(pos, "Left!", {*lastReadInTopologicalOrder}),
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

        TCreateTableAsResult result;
        result.CreateTable = create;
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

TVector<NYql::TExprNode::TPtr> RewriteExpression(const NYql::TExprNode::TPtr& root, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx, const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx, const TString& cluster) {
    // CREATE TABLE AS statement can be used only with perstatement execution.
    // Thus we assume that there is only one such statement.
    TVector<NYql::TExprNode::TPtr> result;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            const auto rewriteResult = RewriteCreateTableAs(node, ctx, typeCtx, sessionCtx, cluster);
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
