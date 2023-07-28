#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <utility>
#include <functional>

namespace NYql {

struct TTypeAnnotationContext;
class THashBuilder;

class TNodeHashCalculator {
public:
    using TArgIndex = THashMap<const TExprNode*, std::pair<ui32, ui32>>;
    using THasher = std::function<TString(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel)>;
    using THasherMap = THashMap<TStringBuf, THasher>;

    TNodeHashCalculator(const TTypeAnnotationContext& types, std::unordered_map<ui64, TString>& nodeHash, const TString& salt)
        : Types(types)
        , NodeHash(nodeHash)
        , Salt(salt)
    {
    }

    TString GetHash(const TExprNode& node) const {
        TArgIndex argIndex;
        return GetHashImpl(node, argIndex, 0);
    }

protected:
    void UpdateFileHash(THashBuilder& builder, TStringBuf alias) const;
    bool UpdateChildrenHash(THashBuilder& builder, const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel, size_t fromIndex = 0) const;
    TString GetHashImpl(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const;

protected:
    const TTypeAnnotationContext& Types;
    std::unordered_map<ui64, TString>& NodeHash;
    THasherMap Hashers;
    TString Salt;
};

}
