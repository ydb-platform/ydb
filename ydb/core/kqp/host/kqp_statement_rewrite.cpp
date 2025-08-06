#include "kqp_statement_rewrite.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/provider/rewrite_io_utils.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

namespace NKikimr {
namespace NKqp {

namespace {
    struct TCreateTableAsResult {
        NYql::TExprNode::TPtr CreateTable = nullptr;
        NYql::TExprNode::TPtr ReplaceInto = nullptr;
        NYql::TExprNode::TPtr MoveTable = nullptr;
    };

    bool IsCreateTableAs(
            NYql::TExprNode::TPtr root,
            NYql::TExprContext& exprCtx) {
        NYql::NNodes::TExprBase expr(root);
        auto maybeWrite = expr.Maybe<NYql::NNodes::TCoWrite>();
        if (!maybeWrite) {
            return false;
        }
        auto write = maybeWrite.Cast();

        if (write.DataSink().FreeArgs().Count() < 2
            || write.DataSink().Category() != "kikimr"
            || write.DataSink().FreeArgs().Get(1).Cast<NYql::NNodes::TCoAtom>() != "db") {
            return false;
        }

        auto writeArgs = write.FreeArgs();
        if (writeArgs.Count() < 5) {
            return false;
        }

        auto maybeKey = writeArgs.Get(2).Maybe<NYql::NNodes::TCoKey>();
        if (!maybeKey) {
            return false;
        }
        auto key = maybeKey.Cast();
        if (key.ArgCount() == 0) {
            return false;
        }

        if (key.Ptr()->Child(0)->Child(0)->Content() != "tablescheme") {
            return false;
        }

        auto tableNameNode = key.Ptr()->Child(0)->Child(1)->Child(0);
        const TString tableName(tableNameNode->Content());

        auto maybeList = writeArgs.Get(4).Maybe<NYql::NNodes::TExprList>();
        if (!maybeList) {
            return false;
        }
        auto settings = NYql::NCommon::ParseWriteTableSettings(maybeList.Cast(), exprCtx);
        if (!settings.Mode) {
            return false;
        }

        auto mode = settings.Mode.Cast();
        if (mode != "create" && mode != "create_if_not_exists" && mode != "create_or_replace") {
            return false;
        }

        const auto& insertData = writeArgs.Get(3);
        if (insertData.Ptr()->Content() == "Void") {
            return false;
        }
        return true;
    }

    TPrepareRewriteInfo PrepareCreateTableAs(
            NYql::TExprNode::TPtr root,
            NYql::TExprContext& exprCtx,
            NYql::TTypeAnnotationContext& typeCtx,
            const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
            const NMiniKQL::IFunctionRegistry& funcRegistry,
            const TString& cluster) {
        NYql::NNodes::TExprBase expr(root);
        auto maybeWrite = expr.Maybe<NYql::NNodes::TCoWrite>();
        YQL_ENSURE(maybeWrite);
        auto write = maybeWrite.Cast();

        YQL_ENSURE(write.DataSink().FreeArgs().Count() > 1
            && write.DataSink().Category() == "kikimr"
            && write.DataSink().FreeArgs().Get(1).Cast<NYql::NNodes::TCoAtom>() == "db");

        auto writeArgs = write.FreeArgs();
        YQL_ENSURE(writeArgs.Count() > 4);

        auto maybeKey = writeArgs.Get(2).Maybe<NYql::NNodes::TCoKey>();
        YQL_ENSURE(maybeKey);

        auto key = maybeKey.Cast();
        YQL_ENSURE(key.ArgCount() != 0);

        YQL_ENSURE(key.Ptr()->Child(0)->Child(0)->Content() == "tablescheme");

        auto tableNameNode = key.Ptr()->Child(0)->Child(1)->Child(0);
        const TString tableName(tableNameNode->Content());

        auto maybeList = writeArgs.Get(4).Maybe<NYql::NNodes::TExprList>();
        YQL_ENSURE(maybeList);
        auto settings = NYql::NCommon::ParseWriteTableSettings(maybeList.Cast(), exprCtx);
        YQL_ENSURE(settings.Mode);

        auto mode = settings.Mode.Cast();
        YQL_ENSURE(mode == "create" || mode == "create_if_not_exists" || mode == "create_or_replace");

        const auto& insertData = writeArgs.Get(3);
        YQL_ENSURE(insertData.Ptr()->Content() != "Void");

        auto typeTransformer = NYql::TTransformationPipeline(&typeCtx)
            .AddServiceTransformers()
            .AddExpressionEvaluation(funcRegistry)
            .AddPreTypeAnnotation()
            .AddIOAnnotation()
            .AddTypeAnnotationTransformer(CreateKqpTypeAnnotationTransformer(cluster, sessionCtx->TablesPtr(), typeCtx, sessionCtx->ConfigPtr()))
            .Build(false);

        return TPrepareRewriteInfo{
            .InputExpr = insertData.Ptr(),
            .Transformer = typeTransformer,
        };
    }

    std::optional<TCreateTableAsResult> RewriteCreateTableAs(
            NYql::TExprNode::TPtr root,
            NYql::TExprContext& exprCtx,
            const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
            NYql::TExprNode::TPtr insertDataPtr) {
        NYql::NNodes::TExprBase expr(root);
        auto maybeWrite = expr.Maybe<NYql::NNodes::TCoWrite>();
        YQL_ENSURE(maybeWrite);
        auto write = maybeWrite.Cast();

        YQL_ENSURE(write.DataSink().FreeArgs().Count() > 1
            && write.DataSink().Category() == "kikimr"
            && write.DataSink().FreeArgs().Get(1).Cast<NYql::NNodes::TCoAtom>() == "db");

        auto writeArgs = write.FreeArgs();
        YQL_ENSURE(writeArgs.Count() > 4);

        auto maybeKey = writeArgs.Get(2).Maybe<NYql::NNodes::TCoKey>();
        YQL_ENSURE(maybeKey);

        auto key = maybeKey.Cast();
        YQL_ENSURE(key.ArgCount() != 0);

        YQL_ENSURE(key.Ptr()->Child(0)->Child(0)->Content() == "tablescheme");

        auto tableNameNode = key.Ptr()->Child(0)->Child(1)->Child(0);
        const TString tableName(tableNameNode->Content());

        auto maybeList = writeArgs.Get(4).Maybe<NYql::NNodes::TExprList>();
        YQL_ENSURE(maybeList);
        auto settings = NYql::NCommon::ParseWriteTableSettings(maybeList.Cast(), exprCtx);
        YQL_ENSURE(settings.Mode);

        auto mode = settings.Mode.Cast();
        YQL_ENSURE(mode == "create" || mode == "create_if_not_exists" || mode == "create_or_replace");

        const auto& insertData = writeArgs.Get(3);
        YQL_ENSURE(insertData.Ptr()->Content() != "Void");

        const auto pos = insertData.Ref().Pos();

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

            const bool notNull = (currentType->GetKind() != NYql::ETypeAnnotationKind::Optional);
            auto typeNode = NYql::ExpandType(pos, *currentType, exprCtx);

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

        const TString tmpTableName = TStringBuilder()
            << tableName
            << "_cas_"
            << TAppData::RandomProvider->GenRand();

        const TString createTableName = (TStringBuilder()
                << CanonizePath(AppData()->TenantName)
                << "/.tmp/sessions/"
                << sessionCtx->GetSessionId()
                << CanonizePath(tmpTableName));

        create = exprCtx.ReplaceNode(std::move(create), *columns, exprCtx.NewList(pos, std::move(columnNodes)));

        std::vector<NYql::TExprNodePtr> settingsNodes;
        for (size_t index = 0; index < create->Child(4)->ChildrenSize(); ++index) {
            settingsNodes.push_back(create->Child(4)->ChildPtr(index));
        }
        settingsNodes.push_back(
            exprCtx.NewList(pos, {exprCtx.NewAtom(pos, "temporary")}));
        create = exprCtx.ReplaceNode(std::move(create), *create->Child(4), exprCtx.NewList(pos, std::move(settingsNodes)));
        create = exprCtx.ReplaceNode(std::move(create), *tableNameNode, exprCtx.NewAtom(pos, tmpTableName));

        NYql::TNodeOnNodeOwnedMap deepClones;
        auto insertDataCopy = exprCtx.DeepCopy(insertData.Ref(), exprCtx, deepClones, false, false);
        const auto topLevelRead = NYql::FindTopLevelRead(insertDataCopy);

        NYql::TExprNode::TListType insertSettings;
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "mode"),
                exprCtx.NewAtom(pos, "fill_table"),
            }));
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "OriginalPath"),
                exprCtx.NewAtom(pos, tableName),
            }));
        insertSettings.push_back(
            exprCtx.NewList(pos, {
                exprCtx.NewAtom(pos, "AllowInconsistentWrites"),
            }));

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
            insertDataCopy,
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

        return result;
    }
}

bool NeedToSplit(
        const NYql::TExprNode::TPtr& root,
        NYql::TExprContext& exprCtx) {
    bool needToSplit = false;

    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            needToSplit |= IsCreateTableAs(node, exprCtx);
        }
        return !needToSplit;
    });

    return needToSplit;
}

bool CheckRewrite(
        const NYql::TExprNode::TPtr& root,
        NYql::TExprContext& exprCtx) {
    ui64 actionsCount = 0;
    ui64 createTableAsCount = 0;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            if (IsCreateTableAs(node, exprCtx)) {
                ++createTableAsCount;
            }
            ++actionsCount;
        }
        return actionsCount <= 1 && createTableAsCount <= 1;
    });

    if (createTableAsCount == 0) {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(NYql::NNodes::TExprBase(root).Pos()),
            "CTAS statement not found."));
        return false;
    } else if (createTableAsCount > 1) {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(NYql::NNodes::TExprBase(root).Pos()),
            "Several CTAS statement can't be used without per-statement mode."));
        return false;
    } else if (actionsCount > 1) {
        exprCtx.AddError(NYql::TIssue(
            exprCtx.GetPosition(NYql::NNodes::TExprBase(root).Pos()),
            "CTAS statement can't be used with other statements without per-statement mode."));
        return false;
    }
    return true;
}

TPrepareRewriteInfo PrepareRewrite(
        const NYql::TExprNode::TPtr& root,
        NYql::TExprContext& exprCtx,
        NYql::TTypeAnnotationContext& typeCtx,
        const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
        const NMiniKQL::IFunctionRegistry& funcRegistry,
        const TString& cluster) {
    // CREATE TABLE AS statement can be used only with perstatement execution.
    // Thus we assume that there is only one such statement. (it was checked in CheckRewrite)
    NYql::TExprNode::TPtr createTableAsNode = nullptr;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get()) && IsCreateTableAs(node, exprCtx)) {
            createTableAsNode = node;
        }
        return !createTableAsNode;
    });
    YQL_ENSURE(createTableAsNode);

    return PrepareCreateTableAs(createTableAsNode, exprCtx, typeCtx, sessionCtx, funcRegistry, cluster);
}

TVector<NYql::TExprNode::TPtr> RewriteExpression(
        const NYql::TExprNode::TPtr& root,
        NYql::TExprContext& exprCtx,
        const TIntrusivePtr<NYql::TKikimrSessionContext>& sessionCtx,
        NYql::TExprNode::TPtr insertDataPtr) {
    YQL_ENSURE(insertDataPtr);

    TVector<NYql::TExprNode::TPtr> result;
    VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
        if (NYql::NNodes::TCoWrite::Match(node.Get())) {
            if (IsCreateTableAs(node, exprCtx)) {
                const auto rewriteResult = RewriteCreateTableAs(node, exprCtx, sessionCtx, insertDataPtr);
                if (rewriteResult) {
                    result.push_back(rewriteResult->CreateTable);
                    result.push_back(rewriteResult->ReplaceInto);
                    if (rewriteResult->MoveTable) {
                        result.push_back(rewriteResult->MoveTable);
                    }
                }
            }
        }
        return true;
    });

    if (!exprCtx.IssueManager.GetIssues().Empty()) {
        return {};
    }

    if (result.empty()) {
        result.push_back(root);
    }
    return result;
}

}
}
