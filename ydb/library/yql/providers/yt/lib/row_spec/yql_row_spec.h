#pragma once

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_constraint.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/generic/map.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

#include <functional>

namespace NYql {

class TStructExprType;
struct TYTSortInfo;

struct TYqlRowSpecInfo: public TThrRefBase {
    using TPtr = TIntrusivePtr<TYqlRowSpecInfo>;

    enum class ECopySort {
        Pure,
        WithDesc,
        WithCalc,
        Exact,
    };

    TYqlRowSpecInfo() {
    }

    TYqlRowSpecInfo(NNodes::TExprBase node, bool withTypes = true) {
        Parse(node, withTypes);
    }

    static bool Validate(const TExprNode& node, TExprContext& ctx, const TStructExprType*& type, TMaybe<TColumnOrder>& columnOrder);
    bool Parse(const TString& rowSpecYson, TExprContext& ctx, const TPositionHandle& pos = {});
    bool Parse(const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos = {});
    bool Parse(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos = {});
    void Parse(NNodes::TExprBase node, bool withTypes = true);
    bool Validate(TExprContext& ctx, TPositionHandle pos);

    TString ToYsonString() const;
    void FillCodecNode(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper = {}) const;
    void FillAttrNode(NYT::TNode& attrs, ui64 nativeTypeCompatibility, bool useCompactForm) const;
    NNodes::TExprBase ToExprNode(TExprContext& ctx, const TPositionHandle& pos) const;

    bool IsSorted() const {
        return !SortedBy.empty();
    }
    bool HasAuxColumns() const;
    TVector<std::pair<TString, const TTypeAnnotationNode*>> GetAuxColumns() const;
    // Includes aux columns
    const TStructExprType* GetExtendedType(TExprContext& ctx) const;
    // Returns true if sortness is changed
    bool CopySortness(TExprContext& ctx, const TYqlRowSpecInfo& from, ECopySort mode = ECopySort::Pure);
    // Returns true if sortness is changed
    bool MakeCommonSortness(TExprContext& ctx, const TYqlRowSpecInfo& from);
    bool CompareSortness(const TYqlRowSpecInfo& with, bool checkUniqueFlag = true) const;
    // Returns true if sortness is changed
    bool ClearSortness(TExprContext& ctx, size_t fromMember = 0);
    // Returns true if sortness is changed
    bool KeepPureSortOnly(TExprContext& ctx);
    bool ClearNativeDescendingSort(TExprContext& ctx);
    const TSortedConstraintNode* MakeSortConstraint(TExprContext& ctx) const;
    const TDistinctConstraintNode* MakeDistinctConstraint(TExprContext& ctx) const;
    void CopyConstraints(const TYqlRowSpecInfo& from);

    const TStructExprType* GetType() const {
        return Type;
    }

    NYT::TNode GetTypeNode(const NCommon::TStructMemberMapper& mapper = {}) const;

    const TMaybe<TColumnOrder>& GetColumnOrder() const {
        return Columns;
    }

    void SetType(const TStructExprType* type, TMaybe<ui64> nativeYtTypeFlags = Nothing());
    void SetColumnOrder(const TMaybe<TColumnOrder>& columnOrder);
    void SetConstraints(const TConstraintSet& constraints);
    TConstraintSet GetConstraints() const;
    TConstraintSet GetSomeConstraints(ui64 mask, TExprContext& ctx);
    TConstraintSet GetAllConstraints(TExprContext& ctx);

    void CopyType(const TYqlRowSpecInfo& from) {
        Type = from.Type;
        Columns = from.Columns;
        TypeNode = from.TypeNode;
        NativeYtTypeFlags = from.NativeYtTypeFlags;
    }

    void CopyTypeOrders(const NYT::TNode& typeNode);

    ui64 GetNativeYtTypeFlags(const NCommon::TStructMemberMapper& mapper = {}) const;

    TMaybe<NYT::TNode> GetNativeYtType(const NCommon::TStructMemberMapper& mapper = {}) const {
        return 0 != GetNativeYtTypeFlags(mapper) ? MakeMaybe(GetTypeNode(mapper)) : Nothing();
    }

    TVector<std::pair<TString, bool>> GetForeignSort() const;

    NNodes::TMaybeNode<NNodes::TExprBase> FromNode;

    bool StrictSchema = true;
    bool UniqueKeys = false;

    TVector<bool> SortDirections;
    TVector<TString> SortedBy;
    TVector<TString> SortMembers;
    TTypeAnnotationNode::TListType SortedByTypes;
    TMap<TString, TString> DefaultValues;
    TVector<TString> ExplicitYson;

private:
    void FillTypeTransform(NYT::TNode& attrs, TStringBuf typeNameAttr, const NCommon::TStructMemberMapper& mapper) const;
    void FillSort(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper = {}) const;
    void FillDefValues(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper = {}) const;
    void FillFlags(NYT::TNode& attrs) const;
    void FillConstraints(NYT::TNode& attrs) const;
    void FillExplicitYson(NYT::TNode& attrs, const NCommon::TStructMemberMapper& mapper) const;
    bool ParsePatched(const NYT::TNode& rowSpecAttr, const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos);
    bool ParseFull(const NYT::TNode& rowSpecAttr, const THashMap<TString, TString>& attrs, TExprContext& ctx, const TPositionHandle& pos);
    bool ParseType(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos);
    bool ParseSort(const NYT::TNode& rowSpecAttr, TExprContext& ctx, const TPositionHandle& pos);
    void ParseFlags(const NYT::TNode& rowSpecAttr);
    void ParseConstraints(const NYT::TNode& rowSpecAttr);
    void ParseConstraintsNode(TExprContext& ctx);
    void ParseDefValues(const NYT::TNode& rowSpecAttr);
    bool ValidateSort(const TYTSortInfo& sortInfo, TExprContext& ctx, const TPositionHandle& pos);

    NYT::TNode GetConstraintsNode() const;
    bool HasNonTrivialSort() const;
private:
    const TStructExprType* Type = nullptr;
    TMaybe<TColumnOrder> Columns;
    NYT::TNode TypeNode;
    ui64 NativeYtTypeFlags = 0ul;

    NYT::TNode ConstraintsNode;
    const TSortedConstraintNode* Sorted = nullptr;
    const TUniqueConstraintNode* Unique = nullptr;
    const TDistinctConstraintNode* Distinct = nullptr;
};

ui64 GetNativeYtTypeFlags(const TStructExprType& type, const NCommon::TStructMemberMapper& mapper = {});

}
