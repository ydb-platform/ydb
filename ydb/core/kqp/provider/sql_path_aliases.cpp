#include "sql_path_aliases.h"

#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/core/sql_types/yql_callable_names.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NYql {
namespace {

bool IsPathKey(TStringBuf tag) {
    return tag == "table" || tag == "tablescheme" || tag == "tablelist" || tag == "topic"
        || tag == "replication" || tag == "transfer" || tag == "sequence" || tag == "backupCollection"
        || tag == "backup" || tag == "restore" || tag == "databasePath" || tag == "secret"
        || tag == "objectId" || tag == "pgObject";
}

TExprNode::TPtr RewriteKey(const TExprNode::TPtr& key, TExprContext& ctx,
    const std::function<TString(TStringBuf)>& normalizePath) {
    if (!key->IsCallable("Key") || !key->ChildrenSize() || key->Child(0)->ChildrenSize() < 2) {
        return key;
    }

    const auto tag = key->Child(0)->Child(0)->Content();
    const auto* path = key->Child(0)->Child(1);
    if (!IsPathKey(tag) || path->ChildrenSize() != 1 || !path->Child(0)->IsAtom()) {
        return key;
    }

    const auto* atom = path->Child(0);
    TString normalized = normalizePath(atom->Content());
    auto newEntry = key->ChildPtr(0);
    if (normalized != atom->Content()) {
        auto newPath = ctx.ChangeChild(*path, 0, ctx.NewAtom(atom->Pos(), std::move(normalized)));
        newEntry = ctx.ChangeChild(*newEntry, 1, std::move(newPath));
    }

    if ((tag == "backupCollection" || tag == "backup" || tag == "restore") && key->Child(0)->ChildrenSize() > 2) {
        const auto* prefix = key->Child(0)->Child(2);
        if (prefix->ChildrenSize() == 1 && prefix->Child(0)->IsAtom()) {
            const auto* prefixAtom = prefix->Child(0);
            TString normalizedPrefix = normalizePath(prefixAtom->Content());
            if (normalizedPrefix != prefixAtom->Content()) {
                auto newPrefix = ctx.ChangeChild(*prefix, 0, ctx.NewAtom(prefixAtom->Pos(), std::move(normalizedPrefix)));
                newEntry = ctx.ChangeChild(*newEntry, 2, std::move(newPrefix));
            }
        }
    }

    return newEntry == key->ChildPtr(0) ? key : ctx.ChangeChild(*key, 0, std::move(newEntry));
}

TExprNode::TPtr RewritePathValue(const TExprNode::TPtr& value, TExprContext& ctx,
    const std::function<TString(TStringBuf)>& normalizePath) {
    if (value->IsAtom()) {
        TString normalized = normalizePath(value->Content());
        return normalized == value->Content() ? value : ctx.NewAtom(value->Pos(), std::move(normalized));
    }

    if (value->IsCallable("String") && value->ChildrenSize() == 1 && value->Child(0)->IsAtom()) {
        auto atom = RewritePathValue(value->ChildPtr(0), ctx, normalizePath);
        return atom == value->ChildPtr(0) ? value : ctx.ChangeChild(*value, 0, std::move(atom));
    }

    if (value->IsList()) {
        auto result = value;
        for (ui32 i = 0; i < value->ChildrenSize(); ++i) {
            auto child = RewritePathValue(value->ChildPtr(i), ctx, normalizePath);
            if (child != value->ChildPtr(i)) {
                result = ctx.ChangeChild(*result, i, std::move(child));
            }
        }
        return result;
    }

    return value;
}

bool IsPathOption(TStringBuf tag, TStringBuf option) {
    return (tag == "replication" && option == "local")
        || (tag == "permission" && option == "paths")
        || ((tag == "table" || tag == "tablescheme") && (option == "renameTo" || option == "data_source_path"))
        || (tag == "backupCollection" && option == "path");
}

TExprNode::TPtr RewriteOptionPaths(const TExprNode::TPtr& node, TStringBuf tag, TExprContext& ctx,
    const std::function<TString(TStringBuf)>& normalizePath) {
    if (!node->IsList()) {
        return node;
    }

    if (node->ChildrenSize() == 2 && node->Child(0)->IsAtom()
        && IsPathOption(tag, node->Child(0)->Content())) {
        auto value = RewritePathValue(node->ChildPtr(1), ctx, normalizePath);
        return value == node->ChildPtr(1) ? node : ctx.ChangeChild(*node, 1, std::move(value));
    }

    auto result = node;
    for (ui32 i = 0; i < node->ChildrenSize(); ++i) {
        auto child = RewriteOptionPaths(node->ChildPtr(i), tag, ctx, normalizePath);
        if (child != node->ChildPtr(i)) {
            result = ctx.ChangeChild(*result, i, std::move(child));
        }
    }
    return result;
}

}

bool RewriteSqlPathAliases(TExprNode::TPtr& query, TExprContext& ctx, TStringBuf localCluster,
    const std::function<TString(TStringBuf)>& normalizePath) {
    if (!normalizePath) {
        return true;
    }

    TExprNode::TPtr output;
    TOptimizeExprSettings settings(nullptr);
    settings.VisitChanges = false;
    const auto status = OptimizeExpr(query, output,
        [localCluster, &normalizePath](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
            const bool isRead = node->IsCallable(ReadName);
            if (!isRead && !node->IsCallable(WriteName)) {
                return node;
            }

            if (node->ChildrenSize() < (isRead ? 3U : 5U)) {
                return node;
            }
            const auto* provider = node->Child(1);
            if (!provider->IsCallable(isRead ? "DataSource" : "DataSink") || provider->ChildrenSize() < 2
                || provider->Child(0)->Content() != KikimrProviderName
                || provider->Child(1)->Content() != localCluster) {
                return node;
            }

            auto key = RewriteKey(node->ChildPtr(2), ctx, normalizePath);
            auto result = key == node->ChildPtr(2) ? node : ctx.ChangeChild(*node, 2, std::move(key));
            if (!isRead && result->Child(2)->IsCallable("Key") && result->Child(2)->ChildrenSize()
                && result->Child(2)->Child(0)->ChildrenSize()) {
                const auto tag = result->Child(2)->Child(0)->Child(0)->Content();
                auto options = RewriteOptionPaths(result->ChildPtr(4), tag, ctx, normalizePath);
                if (options != result->ChildPtr(4)) {
                    result = ctx.ChangeChild(*result, 4, std::move(options));
                }
            }
            return result;
        }, ctx, settings);

    if (status == IGraphTransformer::TStatus::Error) {
        return false;
    }
    query = std::move(output);
    return true;
}

}
