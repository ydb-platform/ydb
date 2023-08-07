#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/map.h>
#include <util/generic/strbuf.h>

#include <tuple>
#include <utility>
#include <functional>

namespace NYql {

using TKeyFilterPredicates = THashMultiMap<TString, std::pair<TString, TExprNode::TPtr>>;

class TSortMembersCollection {
public:
    struct TMemberDescr;
    using TMemberDescrPtr = TIntrusivePtr<TMemberDescr>;
    using TMemberDescrMap = THashMap<TString, TMemberDescrPtr>;

    struct TMemberDescr: public TThrRefBase {
        const TDataExprType* ColumnType = nullptr;
        bool IsColumnOptional = false;
        TMultiMap<size_t, std::tuple<TString, TExprNode::TPtr, const TDataExprType*>> Ranges;
        TSet<size_t> Tables;
        TMemberDescrMap NextMembers;
    };

    TSortMembersCollection() = default;
    ~TSortMembersCollection() = default;

    bool Empty() const {
        return Members.empty();
    }

    void AddTableInfo(size_t tableIndex, const TString& tableName,
        const TVector<TString>& sortMembers,
        const TTypeAnnotationNode::TListType& sortedByTypes,
        const TVector<bool>& sortDirections);

    bool ApplyRanges(const TKeyFilterPredicates& ranges, TExprContext& ctx);
    bool ApplyRanges(const TVector<TKeyFilterPredicates>& ranges, TExprContext& ctx);

protected:
    bool ApplyRangesImpl(size_t groupIndex, const TKeyFilterPredicates& ranges, TExprContext& ctx);
    static void DropMembersWithoutRanges(TMemberDescrMap& members);
    static void ApplyRecurs(TMemberDescrMap& members,
        const std::function<bool(const TString&, TMemberDescr&)>& func);

protected:
    TMemberDescrMap Members;
};

} // NYql
