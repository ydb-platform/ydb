#pragma once

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/digest/numeric.h>
#include <util/str_stl.h>

namespace NYql {

class TYtKey {
public:
    enum class EType {
        Undefined,
        Table,
        TableScheme,
        Folder,
        WalkFolders,
        WalkFoldersImpl,
    };

    struct TRange {
        TString Prefix;
        TExprNode::TPtr Filter;
        TString Suffix;
        bool IsStrict = true;

        friend bool operator ==(const TRange& left, const TRange& right) {
            return left.Prefix == right.Prefix
                && left.Filter == right.Filter
                && left.Suffix == right.Suffix
                && left.IsStrict == right.IsStrict;
        }
    };

    struct TFolderList {
        TString Prefix;
        TVector<TString> Attributes;

        friend bool operator ==(const TFolderList& left, const TFolderList& right) {
            return left.Prefix == right.Prefix
                && left.Attributes == right.Attributes;
        }
    };
    
    struct TWalkFoldersArgs {
        TFolderList InitialFolder;

        TExprNode::TPtr PickledUserState;
        TExprNode::TPtr UserStateType;

        TExprNode::TPtr PreHandler;
        TExprNode::TPtr ResolveHandler;
        TExprNode::TPtr DiveHandler;
        TExprNode::TPtr PostHandler;
    };

    struct TWalkFoldersImplArgs {
        TExprNode::TPtr UserStateExpr;
        TExprNode::TPtr UserStateType;

        ui64 StateKey;
    };

public:
    TYtKey() {
    }

    EType GetType() const {
        return Type;
    }

    const TExprNode* GetNode() const {
        return KeyNode;
    }

    const TString& GetPath() const {
        YQL_ENSURE(Type != EType::Undefined);
        return Path;
    }

    const TString& GetView() const {
        YQL_ENSURE(Type != EType::Undefined);
        return View;
    }

    bool IsAnonymous() const {
        return Anonymous;
    }

    const TMaybe<TRange>& GetRange() const {
        YQL_ENSURE(Type != EType::Undefined);
        return Range;
    }

    const TMaybe<TFolderList>& GetFolder() const {
        YQL_ENSURE(Type != EType::Undefined);
        return Folder;
    }

    TMaybe<TWalkFoldersArgs>& GetWalkFolderArgs() {
        YQL_ENSURE(Type != EType::Undefined);
        return WalkFolderArgs;
    }

    TMaybe<TWalkFoldersImplArgs>& GetWalkFolderImplArgs() {
        YQL_ENSURE(Type != EType::Undefined);
        return WalkFolderImplArgs;
    }

    bool Parse(const TExprNode& key, TExprContext& ctx, bool isOutput = false);

private:
    EType Type = EType::Undefined;
    const TExprNode* KeyNode = nullptr;
    TString Path;
    TString View;
    bool Anonymous = false;
    TMaybe<TRange> Range;
    TMaybe<TFolderList> Folder;
    TMaybe<TWalkFoldersArgs> WalkFolderArgs;
    TMaybe<TWalkFoldersImplArgs> WalkFolderImplArgs;
};

class TYtInputKeys {
public:
    TYtInputKeys() {
    }

    TYtKey::EType GetType() const {
        return Type;
    }

    const TVector<TYtKey>& GetKeys() const {
        return Keys;
    }

    TVector<TYtKey>&& ExtractKeys() {
        return std::move(Keys);
    }

    bool IsProcessed() const {
        return HasNonKeys;
    }

    bool GetStrictConcat() const {
        return StrictConcat;
    }

    bool Parse(const TExprNode& keysNode, TExprContext& ctx);

private:
    TYtKey::EType Type = TYtKey::EType::Undefined;
    TVector<TYtKey> Keys;
    bool StrictConcat = true;
    bool HasNonKeys = false;
};

class TYtOutputKey: public TYtKey {
public:
    TYtOutputKey() {
    }

    bool Parse(const TExprNode& keyNode, TExprContext& ctx);
};

}

template <>
struct hash<NYql::TYtKey::TRange> {
    inline size_t operator()(const NYql::TYtKey::TRange& r) const {
        size_t res = ComputeHash(r.Prefix);
        res = CombineHashes(res, NumericHash(r.Filter.Get()));
        res = CombineHashes(res, ComputeHash(r.Suffix));
        res = CombineHashes(res, size_t(r.IsStrict));
        return res;
    }
};

template <>
struct hash<NYql::TYtKey::TFolderList> {
    inline size_t operator()(const NYql::TYtKey::TFolderList& r) const {
        size_t res = ComputeHash(r.Prefix);
        res = CombineHashes(res, r.Attributes.size());
        for (auto& a: r.Attributes) {
            res = CombineHashes(res, ComputeHash(a));
        }
        return res;
    }
};
