#include "kqp_statement_rewrite.h"

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/provider/rewrite_io_utils.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr {
namespace NKqp {

namespace {
    struct TCreateTableAsResult {
        NYql::TExprNode::TPtr CreateTable = nullptr;
        NYql::TExprNode::TPtr ReplaceInto = nullptr;
        NYql::TExprNode::TPtr MoveTable = nullptr;
    };

    bool IsOlap(const NYql::NNodes::TMaybeNode<NYql::NNodes::TCoNameValueTupleList>& tableSettings) {
        if (!tableSettings) {
            return false;
        }
        for (const auto& field : tableSettings.Cast()) {
            if (field.Name().Value() == "storeType") {
                YQL_ENSURE(field.Value().Maybe<NYql::NNodes::TCoAtom>());
                if (field.Value().Cast<NYql::NNodes::TCoAtom>().StringValue() == "COLUMN") {
                    return true;
                }
            }
        }
        return false;
    }

    std::optional<TCreateTableAsResult> RewriteCreateTableAs(
            NYql::TExprNode::TPtr root,
            NYql::TExprContext& exprCtx,
            NYql::TTypeAnnotationContext& typeCtx,
            const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
            const TString& cluster) {
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

        auto tableNameNode = key.Ptr()->Child(0)->Child(1)->Child(0);
        const TString tableName(tableNameNode->Content());

        auto maybeList = writeArgs.Get(4).Maybe<NYql::NNodes::TExprList>();
        if (!maybeList) {
            return std::nullopt;
        }
        auto settings = NYql::NCommon::ParseWriteTableSettings(maybeList.Cast(), exprCtx);
        if (!settings.Mode) {
            return std::nullopt;
        }

        auto mode = settings.Mode.Cast();
        if (mode != "create" && mode != "create_if_not_exists" && mode != "create_or_replace") {
            return std::nullopt;
        }

        const bool isOlap = IsOlap(settings.TableSettings);

        const auto& insertData = writeArgs.Get(3);
        if (insertData.Ptr()->Content() == "Void") {
            return std::nullopt;
        }

        const auto pos = insertData.Ref().Pos();

        auto prevEval = exprCtx.Step.IsDone(NYql::TExprStep::ExprEval);
        exprCtx.Step.Done(NYql::TExprStep::ExprEval);
        auto typeTransformer = NYql::TTransformationPipeline(&typeCtx)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddIOAnnotation()
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(cluster, sessionCtx->TablesPtr(), typeCtx, sessionCtx->ConfigPtr()))
            .Build(false);

        auto insertDataPtr = insertData.Ptr();
        const auto transformResult = NYql::SyncTransform(*typeTransformer, insertDataPtr, exprCtx);
        if (!prevEval) {
            exprCtx.Step.Repeat(NYql::TExprStep::ExprEval);
        }

        if (transformResult != NYql::IGraphTransformer::TStatus::Ok) {
            return std::nullopt;
        }
        auto type = insertDataPtr->GetTypeAnn();
        YQL_ENSURE(type);
        type = type->Cast<NYql::TListExprType>()->GetItemType();
        YQL_ENSURE(type);
        const auto rowType = type->Cast<NYql::TStructExprType>();
        YQL_ENSURE(rowType);

        auto create = exprCtx.ReplaceNode(std::move(root), insertData.Ref(), exprCtx.NewCallable(pos, "Void", {}));

        auto columns = create->Child(4)->Child(1)->Child(1);
        if (columns->ChildrenSize() != 0) {
            exprCtx.AddError(NYql::TIssue(exprCtx.GetPosition(pos), "CREATE TABLE AS with columns is not supported"));
            return std::nullopt;
        }

        auto primaryKey = create->Child(4)->Child(2)->Child(1);
        THashSet<TStringBuf> primariKeyColumns;
        primaryKey->ForEachChild([&](const auto& child) {
            primariKeyColumns.insert(child.Content());
        });

        std::vector<NYql::TExprNodePtr> columnNodes;
        for (const auto* item : rowType->GetItems()) {
            const auto name = item->GetName();
            auto currentType = item->GetItemType();

            const bool notNull = primariKeyColumns.contains(name) && isOlap;

            if (notNull && currentType->GetKind() == NYql::ETypeAnnotationKind::Optional) {
                currentType = currentType->Cast<NYql::TOptionalExprType>()->GetItemType();
            }

            auto typeNode = NYql::ExpandType(pos, *currentType, exprCtx);

            if (!notNull && currentType->GetKind() != NYql::ETypeAnnotationKind::Optional) {
                typeNode = exprCtx.NewCallable(pos, "AsOptionalType", { typeNode });
            }

            columnNodes.push_back(exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, name),
                typeNode,
                exprCtx.NewList(pos, {
                    exprCtx.NewAtom(pos, "columnConstrains"),
                    notNull
                        ? exprCtx.NewList(pos, {
                            exprCtx.NewList(pos, {
                                exprCtx.NewAtom(pos, "not_null"),
                            }),
                        })
                        : exprCtx.NewList(pos, {}),
                }),
                exprCtx.NewList(pos, {}),
            }));
        }

        const bool isTemporary = settings.Temporary.IsValid() && settings.Temporary.Cast().Value() == "true";
        if (isTemporary) {
            exprCtx.AddError(NYql::TIssue(exprCtx.GetPosition(pos), "CREATE TEMPORARY TABLE AS is not supported at current time"));
            return std::nullopt;
        }

        const bool isAtomicOperation = !isOlap;

        const TString tmpTableName = TStringBuilder()
            << tableName
            << "_cas_"
            << TAppData::RandomProvider->GenRand();

        const TString createTableName = !isAtomicOperation
            ? tableName
            : (TStringBuilder()
                << CanonizePath(AppData()->TenantName)
                << "/.tmp/sessions/"
                << sessionCtx->GetSessionId()
                << CanonizePath(tmpTableName));

        create = exprCtx.ReplaceNode(std::move(create), *columns, exprCtx.NewList(pos, std::move(columnNodes)));

        if (isAtomicOperation) {
            std::vector<NYql::TExprNodePtr> settingsNodes;
            for (size_t index = 0; index < create->Child(4)->ChildrenSize(); ++index) {
                settingsNodes.push_back(create->Child(4)->ChildPtr(index));
            }
            settingsNodes.push_back(
                exprCtx.NewList(pos, {exprCtx.NewAtom(pos, "temporary")}));
            create = exprCtx.ReplaceNode(std::move(create), *create->Child(4), exprCtx.NewList(pos, std::move(settingsNodes)));
            create = exprCtx.ReplaceNode(std::move(create), *tableNameNode, exprCtx.NewAtom(pos, tmpTableName));
        }

        const auto topLevelRead = NYql::FindTopLevelRead(insertData.Ptr());

        NYql::TExprNode::TListType insertSettings;
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "mode"),
                exprCtx.NewAtom(pos, "replace"),
            }));
        if (!isOlap) {
            insertSettings.push_back(
                exprCtx.NewList(pos, {
                    exprCtx.NewAtom(pos, "AllowInconsistentWrites"),
                }));
        }

        const auto insert = exprCtx.NewCallable(pos, "Write!", {
            topLevelRead == nullptr ? exprCtx.NewWorld(pos) : exprCtx.NewCallable(pos, "Left!", {topLevelRead.Get()}),
            exprCtx.NewCallable(pos, "DataSink", {
                exprCtx.NewAtom(pos, "kikimr"),
                exprCtx.NewAtom(pos, "db"),
            }),
            exprCtx.NewCallable(pos, "Key", {
                exprCtx.NewList(pos, {
                    exprCtx.NewAtom(pos, "table"),
                    exprCtx.NewCallable(pos, "String", {
                        exprCtx.NewAtom(pos, createTableName),
                    }),
                }),
            }),
            insertData.Ptr(),
            exprCtx.NewList(pos, std::move(insertSettings)),
        });

        TCreateTableAsResult result;
        result.CreateTable = create;
        result.ReplaceInto = exprCtx.NewCallable(pos, "Commit!", {
            insert,
            exprCtx.NewCallable(pos, "DataSink", {
                exprCtx.NewAtom(pos, "kikimr"),
                exprCtx.NewAtom(pos, "db"),
            }),
            exprCtx.NewList(pos, {
                exprCtx.NewList(pos, {
                    exprCtx.NewAtom(pos, "mode"),
                    exprCtx.NewAtom(pos, "flush"),
                }),
            }),
        });

        if (isAtomicOperation) {
            result.MoveTable = exprCtx.NewCallable(pos, "Write!", {
                exprCtx.NewWorld(pos),
                exprCtx.NewCallable(pos, "DataSink", {
                    exprCtx.NewAtom(pos, "kikimr"),
                    exprCtx.NewAtom(pos, "db"),
                }),
                exprCtx.NewCallable(pos, "Key", {
                    exprCtx.NewList(pos, {
                        exprCtx.NewAtom(pos, "tablescheme"),
                        exprCtx.NewCallable(pos, "String", {
                            exprCtx.NewAtom(pos, createTableName),
                        }),
                    }),
                }),
                exprCtx.NewCallable(pos, "Void", {}),
                exprCtx.NewList(pos, {
                    exprCtx.NewList(pos, {
                        exprCtx.NewAtom(pos, "mode"),
                        exprCtx.NewAtom(pos, "alter"),
                    }),
                    exprCtx.NewList(pos, {
                        exprCtx.NewAtom(pos, "actions"),
                        exprCtx.NewList(pos, {
                            exprCtx.NewList(pos, {
                                exprCtx.NewAtom(pos, "renameTo"),
                                exprCtx.NewAtom(pos, tableName),
                            }),
                        }),
                    }),
                }),
            });
        }

        return result;
    }
}

TVector<NYql::TExprNode::TPtr> RewriteExpression(
        const NYql::TExprNode::TPtr& root,
        NYql::TExprContext& exprCtx,
        NYql::TTypeAnnotationContext& typeCtx,
        const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
        const TString& cluster) {
    // CREATE TABLE AS statement can be used only with perstatement execution.
    // Thus we assume that there is only one such statement.
    TVector<NYql::TExprNode::TPtr> result;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            const auto rewriteResult = RewriteCreateTableAs(node, exprCtx, typeCtx, sessionCtx, cluster);
            if (rewriteResult) {
                YQL_ENSURE(result.empty());
                result.push_back(rewriteResult->CreateTable);
                result.push_back(rewriteResult->ReplaceInto);
                if (rewriteResult->MoveTable) {
                    result.push_back(rewriteResult->MoveTable);
                }
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
