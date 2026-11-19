#include "yql_kikimr_provider_impl.h"

#include "yql_kikimr_provider.h"
#include "yql_kikimr_settings.h"

#include <yql/essentials/core/sql_types/yql_callable_names.h>

#include <util/generic/is_in.h>

#include <utility>

namespace NYql {
namespace {

// Keep path-bearing tags below aligned with TKikimrKey::Extract and provider option handling.
bool IsPathKey(TStringBuf tag) {
    static constexpr TStringBuf pathTags[] = {
        "table",
        "tablescheme",
        "tablelist",
        "topic",
        "replication",
        "transfer",
        "sequence",
        "backupCollection",
        "backup",
        "restore",
        "databasePath",
        "secret",
        "objectId",
        "pgObject",
    };
    return IsIn(pathTags, tag);
}

TExprNode::TPtr RewritePathAtom(const TExprNode::TPtr& atom, TExprContext& ctx,
    const std::function<TString(TStringBuf)>& normalizePath) {
    TString normalized = normalizePath(atom->Content());
    if (normalized == atom->Content()) {
        return atom;
    }
    return ctx.NewAtom(atom->Pos(), std::move(normalized));
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

    const ui32 pathChildren = (tag == "backupCollection" || tag == "backup" || tag == "restore")
        && key->Child(0)->ChildrenSize() > 2 ? 3 : 2;
    auto newEntry = key->ChildPtr(0);
    for (ui32 i = 1; i < pathChildren; ++i) {
        const auto* path = key->Child(0)->Child(i);
        if (path->ChildrenSize() != 1 || !path->Child(0)->IsAtom()) {
            continue;
        }
        auto atom = RewritePathAtom(path->ChildPtr(0), ctx, normalizePath);
        if (atom != path->ChildPtr(0)) {
            auto newPath = ctx.ChangeChild(*path, 0, std::move(atom));
            newEntry = ctx.ChangeChild(*newEntry, i, std::move(newPath));
        }
    }

    return newEntry == key->ChildPtr(0) ? key : ctx.ChangeChild(*key, 0, std::move(newEntry));
}

TExprNode::TPtr RewritePathValue(const TExprNode::TPtr& value, TExprContext& ctx,
    const std::function<TString(TStringBuf)>& normalizePath) {
    if (value->IsAtom()) {
        return RewritePathAtom(value, ctx, normalizePath);
    }

    // String(Atom) represents a literal path; computed expressions are left unchanged.
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
    static constexpr std::pair<TStringBuf, TStringBuf> pathOptions[] = {
        {"replication", "local"},
        {"transfer", "target"},
        {"permission", "paths"},
        {"table", "renameTo"},
        {"table", "data_source_path"},
        {"tablescheme", "renameTo"},
        {"tablescheme", "data_source_path"},
        {"backupCollection", "path"},
    };
    return IsIn(pathOptions, std::pair{tag, option});
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

TExprNode::TPtr RewriteSqlPathAliases(const TExprNode::TPtr& node, TExprContext& ctx, TStringBuf localCluster,
    const std::function<TString(TStringBuf)>& normalizePath) {
    const bool isRead = node->IsCallable(ReadName);
    if (!isRead && !node->IsCallable(WriteName)) {
        return node;
    }

    if (node->ChildrenSize() < (isRead ? 3U : 5U)) {
        return node;
    }
    // Database aliases apply only to Kikimr IO in the current query's cluster.
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
}

class TSqlPathAliasesTransformer : public TSyncTransformerBase {
public:
    TSqlPathAliasesTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx, TAutoPtr<IGraphTransformer> intents)
        : SessionCtx(std::move(sessionCtx))
        , Intents(std::move(intents))
    {}

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
        input = RewriteSqlPathAliases(input, ctx, SessionCtx->GetCluster(),
            SessionCtx->Config().NormalizePath);
        return Intents->Transform(input, output, ctx);
    }

    void Rewind() override {
        Intents->Rewind();
    }

private:
    TIntrusivePtr<TKikimrSessionContext> SessionCtx;
    TAutoPtr<IGraphTransformer> Intents;
};

}

TAutoPtr<IGraphTransformer> CreateSqlPathAliasesTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx,
    TAutoPtr<IGraphTransformer> intents) {
    if (!sessionCtx->Config().NormalizePath) {
        return intents;
    }
    return new TSqlPathAliasesTransformer(std::move(sessionCtx), std::move(intents));
}

}
