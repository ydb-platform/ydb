#include "yql_yt_key.h"
#include <library/cpp/yson/node/node_io.h>

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/generic/strbuf.h>

namespace NYql {

namespace {

THashSet<TStringBuf> EXT_KEY_CALLABLES = {
    TStringBuf("Key"),
    TStringBuf("TempTable"),
    MrFolderName,
    MrWalkFoldersName,
    MrWalkFoldersImplName
};

THashSet<TStringBuf> KEY_CALLABLES = {
    TStringBuf("Key"),
    TStringBuf("TempTable"),
};

}

bool TYtKey::Parse(const TExprNode& key, TExprContext& ctx, bool isOutput) {
    using namespace NNodes;
    if (!key.IsCallable(EXT_KEY_CALLABLES)) {
        ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuf("Expected key")));
        return false;
    }

    KeyNode = &key;
    if (key.IsCallable(TStringBuf("TempTable"))) {
        if (key.ChildrenSize() != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuf("TempTable must have a label")));
            return false;
        }

        if (!EnsureAtom(*key.Child(0), ctx)) {
            return false;
        }
        Type = EType::Table;
        Anonymous = true;
        Path = TString("@").append(key.Child(0)->Content());
        return true;
    }
    else if (key.IsCallable(MrFolderName)) {
        if (key.ChildrenSize() != 2 || !key.Child(0)->IsAtom() || !key.Child(1)->IsAtom()) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuilder() << "Incorrect format of " << MrFolderName));
            return false;
        }
        Folder.ConstructInPlace();
        Folder->Prefix = key.Child(0)->Content();
        Split(TString(key.Child(1)->Content()), ";", Folder->Attributes);
        Type = EType::Folder;
        return true;
    }

    if (key.ChildrenSize() < 1) {
        ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), TStringBuf("Key must have at least one component - table or tablescheme, "
            " and may have second tag - view")));
        return false;
    }
    else if (const auto maybeWalkFolders = TMaybeNode<TYtWalkFolders>(&key)) {
        Type = EType::WalkFolders;
        const auto walkFolders = maybeWalkFolders.Cast();

        TFolderList initialListFolder;
        initialListFolder.Prefix = walkFolders.Prefix();
        Split(TString(walkFolders.Attributes().StringValue()), ";", initialListFolder.Attributes);

        WalkFolderArgs = MakeMaybe(TWalkFoldersArgs{
            .InitialFolder = std::move(initialListFolder),
            .PickledUserState = walkFolders.PickledUserState().Ptr(),
            .UserStateType = walkFolders.UserStateType().Ptr(),
            .PreHandler = walkFolders.PreHandler().Ptr(),
            .ResolveHandler = walkFolders.ResolveHandler().Ptr(),
            .DiveHandler = walkFolders.DiveHandler().Ptr(),
            .PostHandler = walkFolders.PostHandler().Ptr(),
        });
        
        return true;
    }
    else if (const auto maybeWalkFolders = TMaybeNode<TYtWalkFoldersImpl>(&key)) {
        Type = EType::WalkFoldersImpl;
        const auto walkFolders = maybeWalkFolders.Cast();
        
        ui64 stateKey;
        if (!TryFromString(walkFolders.ProcessStateKey().StringValue(), stateKey)) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()),
                TStringBuilder() << MrWalkFoldersImplName << ": incorrect format of state map key"));
            return false;
        }
        
        WalkFolderImplArgs = MakeMaybe(TWalkFoldersImplArgs{
            .UserStateExpr = walkFolders.PickledUserState().Ptr(),
            .UserStateType = walkFolders.UserStateType().Ptr(),
            .StateKey = stateKey,
        });
        
        Type = EType::WalkFoldersImpl;
        return true;
    }

    auto tagName = key.Child(0)->Child(0)->Content();
    if (tagName == TStringBuf("table")) {
        Type = EType::Table;
        if (key.ChildrenSize() > 3) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Too many tags"));
            return false;
        }
    } else if (tagName == TStringBuf("tablescheme")) {
        Type = EType::TableScheme;
        if (key.ChildrenSize() > 3) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Too many tags"));
            return false;
        }
        if (isOutput) {
            Type = EType::Table;
        }
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(key.Child(0)->Pos()), TString("Unexpected tag: ") + tagName));
        return false;
    }

    const TExprNode* nameNode = key.Child(0)->Child(1);
    if (nameNode->IsCallable("String")) {
        if (!EnsureArgsCount(*nameNode, 1, ctx)) {
            return false;
        }

        if (!EnsureAtom(*nameNode->Child(0), ctx)) {
            return false;
        }

        const TExprNode* tableName = nameNode->Child(0);

        if (tableName->Content().empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(tableName->Pos()), "Table name must not be empty"));
            return false;
        }
        Path = tableName->Content();
    }
    else if (nameNode->IsCallable(MrTableRangeName) || nameNode->IsCallable(MrTableRangeStrictName)) {
        if (EType::TableScheme == Type) {
            ctx.AddError(TIssue(ctx.GetPosition(nameNode->Pos()), "MrTableRange[Strict] must not be used with tablescheme tag"));
            return false;
        }

        if (!EnsureMinArgsCount(*nameNode, 1, ctx)) {
            return false;
        }

        if (!EnsureMaxArgsCount(*nameNode, 3, ctx)) {
            return false;
        }

        if (!EnsureAtom(*nameNode->Child(0), ctx)) {
            return false;
        }

        if (nameNode->ChildrenSize() > 2){
            if (!EnsureAtom(*nameNode->Child(2), ctx)) {
                return false;
            }
        }

        Range.ConstructInPlace();
        Range->Prefix = nameNode->Child(0)->Content();
        if (nameNode->ChildrenSize() > 1) {
            Range->Filter = nameNode->Child(1);
        }
        if (nameNode->ChildrenSize() > 2) {
            Range->Suffix = nameNode->Child(2)->Content();
        }
        Range->IsStrict = nameNode->Content() == MrTableRangeStrictName;

    } else {
        ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Expected String or MrTableRange[Strict]"));
    }

    if (key.ChildrenSize() > 1) {
        for (ui32 i = 1; i < key.ChildrenSize(); ++i) {
            auto tag = key.Child(i)->Child(0);
            if (tag->Content() == TStringBuf("view")) {
                const TExprNode* viewNode = key.Child(i)->Child(1);
                if (!viewNode->IsCallable("String")) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Pos()), "Expected String"));
                    return false;
                }

                if (viewNode->ChildrenSize() != 1 || !EnsureAtom(*viewNode->Child(0), ctx)) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Child(0)->Pos()), "Dynamic views names are not supported"));
                    return false;
                }

                View = viewNode->Child(0)->Content();
                if (View.empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Child(0)->Pos()), "View name must not be empty"));
                    return false;
                }
            } else {
                ctx.AddError(TIssue(ctx.GetPosition(tag->Pos()), TStringBuilder() << "Unexpected tag: " << tag->Content()));
                return false;
            }
        }
    }
    return true;
}

bool TYtInputKeys::Parse(const TExprNode& keysNode, TExprContext& ctx) {
    bool hasNonKeys = false;
    TVector<const TExprNode*> keys;
    if (keysNode.Type() == TExprNode::List) {
        keysNode.ForEachChild([&](const TExprNode& k) {
            if (k.IsCallable(KEY_CALLABLES)) {
                keys.push_back(&k);
            } else {
                hasNonKeys = true;
            }
        });
    } else if (keysNode.IsCallable(MrTableConcatName)) {
        keysNode.ForEachChild([&](const TExprNode& k){
            keys.push_back(&k);
        });
        StrictConcat = false;
    } else {
        if (keysNode.IsCallable(EXT_KEY_CALLABLES)) {
            keys.push_back(&keysNode);
        } else {
            hasNonKeys = true;
        }
    }
    if (hasNonKeys) {
        if (!keys.empty()) {
            ctx.AddError(TIssue(ctx.GetPosition(keysNode.Pos()), "Mixed keys"));
            return false;
        }
        HasNonKeys = true;
        return true;
    }
    for (auto k: keys) {
        Keys.emplace_back();
        if (!Keys.back().Parse(*k, ctx)) {
            return false;
        }
        if (TYtKey::EType::Undefined == Type) {
            Type = Keys.back().GetType();
        } else if (Type != Keys.back().GetType()) {
            ctx.AddError(TIssue(ctx.GetPosition(k->Child(0)->Pos()), "Multiple keys must have the same tag"));
            return false;
        }
    }
    if (TYtKey::EType::Table != Type && Keys.size() > 1) {
        ctx.AddError(TIssue(ctx.GetPosition(keysNode.Pos()), "Expected single key"));
        return false;
    }
    return true;
}

bool TYtOutputKey::Parse(const TExprNode& keyNode, TExprContext& ctx) {
    if (!keyNode.IsCallable(KEY_CALLABLES)) {
        return true;
    }
    if (!TYtKey::Parse(keyNode, ctx, true)) {
        return false;
    }

    if (GetType() != TYtKey::EType::Table) {
        ctx.AddError(TIssue(ctx.GetPosition(keyNode.Child(0)->Child(0)->Pos()),
            TStringBuilder() << "Unexpected tag: " << keyNode.Child(0)->Child(0)->Content()));
        return false;
    }

    if (!GetView().empty()) {
        ctx.AddError(TIssue(ctx.GetPosition(keyNode.Pos()), "Unexpected view for output key"));
        return false;
    }

    if (GetRange().Defined()) {
        ctx.AddError(TIssue(ctx.GetPosition(keyNode.Pos()), "Unexpected MrTableRange[Strict] for output key"));
        return false;
    }
    return true;
}

}
