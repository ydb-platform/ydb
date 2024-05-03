#pragma once

#include <ydb/library/yql/providers/yt/lib/row_spec/yql_row_spec.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/ast/yql_constraint.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>

namespace NYql {

class TKeySelectorBuilder {
public:
    TKeySelectorBuilder(TPositionHandle pos, TExprContext& ctx, bool useNativeDescSort,
        const TTypeAnnotationNode* itemType = nullptr);

    void ProcessKeySelector(const TExprNode::TPtr& keySelectorLambda,
        const TExprNode::TPtr& sortDirections = {},
        bool unordered = false);
    void ProcessConstraint(const TSortedConstraintNode& sortConstraint);
    void ProcessRowSpec(const TYqlRowSpecInfo& rowSpec);

    bool NeedMap() const {
        return NeedMap_;
    }

    TExprNode::TPtr MakeRemapLambda(bool ordered = false) const;

    const TStructExprType* MakeRemapType() const {
        return Ctx_.MakeType<TStructExprType>(FieldTypes_);
    }

    const TVector<bool>& SortDirections() const {
        return SortDirections_;
    }

    const TVector<TString>& Columns() const {
        return Columns_;
    }

    const TVector<TString>& Members() const {
        return Members_;
    }

    TVector<std::pair<TString, bool>> ForeignSortColumns() const;

    void FillRowSpecSort(TYqlRowSpecInfo& rowSpec);

private:
    template <bool ComputedTuple, bool SingleColumn>
    void AddColumn(const TExprNode::TPtr& rootLambda, const TExprNode::TPtr& keyNode, bool ascending, size_t columnIndex, const TExprNode::TPtr& structArg, bool unordered);
    void AddColumn(const TStringBuf memberName, const TTypeAnnotationNode* columnType, bool ascending, bool unordered);

private:
    TPositionHandle Pos_;
    TExprContext& Ctx_;
    const TStructExprType* StructType = nullptr;
    TVector<const TItemExprType*> FieldTypes_;
    bool NeedMap_ = false;
    TExprNode::TPtr Arg_;
    TExprNode::TPtr LambdaBody_;
    TVector<bool> SortDirections_;
    TVector<bool> ForeignSortDirections_;
    TVector<TString> Columns_;
    TVector<TString> Members_;
    TTypeAnnotationNode::TListType ColumnTypes_;
    bool HasComputedColumn_ = false;
    ui32 Index_ = 0;
    THashSet<TString> UniqMemberColumns_;
    const bool NonStructInput;
    const bool UseNativeDescSort;
};

}
