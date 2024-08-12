#include "yql_yt_key_selector.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

#include <util/generic/strbuf.h>
#include <util/string/type.h>

namespace NYql {

using namespace NNodes;

static TMaybeNode<TCoLambda> GetEqualVisitKeyExtractorLambda(const TCoLambda& lambda) {
    const auto members = GetCommonKeysFromVariantSelector(lambda);
    if (!members.empty()) {
        auto maybeVisit = lambda.Body().Maybe<TCoVisit>();
        return TCoLambda(maybeVisit.Raw()->Child(2));
    }
    return {};
}

TKeySelectorBuilder::TKeySelectorBuilder(TPositionHandle pos, TExprContext& ctx, bool useNativeDescSort,
    const TTypeAnnotationNode* itemType)
    : Pos_(pos)
    , Ctx_(ctx)
    , Arg_(Build<TCoArgument>(ctx, pos).Name("item").Done().Ptr())
    , LambdaBody_(Arg_)
    , NonStructInput(itemType && itemType->GetKind() != ETypeAnnotationKind::Struct)
    , UseNativeDescSort(useNativeDescSort)
{
    if (itemType) {
        if (itemType->GetKind() == ETypeAnnotationKind::Struct) {
            StructType = itemType->Cast<TStructExprType>();
            FieldTypes_ = StructType->GetItems();
        }
        else {
            FieldTypes_.push_back(ctx.MakeType<TItemExprType>("_yql_original_row", itemType));
            LambdaBody_ = Build<TCoAsStruct>(Ctx_, Pos_)
                .Add<TExprList>()
                    .Add<TCoAtom>()
                        .Value("_yql_original_row")
                    .Build()
                    .Add(Arg_)
                .Build()
                .Done()
                .Ptr();
        }
    }
}

void TKeySelectorBuilder::ProcessKeySelector(const TExprNode::TPtr& keySelectorLambda, const TExprNode::TPtr& sortDirections, bool unordered) {
    YQL_ENSURE(!unordered || !sortDirections);
    auto lambda = TCoLambda(keySelectorLambda);
    if (auto maybeLambda = GetEqualVisitKeyExtractorLambda(lambda)) {
        lambda = maybeLambda.Cast();
    }

    auto keySelectorBody = lambda.Body();
    auto keySelectorArg = lambda.Args().Arg(0).Ptr();

    const bool allAscending = !sortDirections;
    const bool multiKey = sortDirections
        ? (sortDirections->IsList() && sortDirections->ChildrenSize() != 1)
        : keySelectorBody.Maybe<TExprList>() || keySelectorBody.Ref().GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple;

    if (multiKey) {
        if (keySelectorBody.Maybe<TExprList>()) {
            auto columnList = keySelectorBody.Cast<TExprList>();
            for (size_t i = 0; i < columnList.Size(); ++i) {
                AddColumn<false, false>(keySelectorLambda,
                    columnList.Item(i).Ptr(),
                    allAscending || IsTrue(TExprList(sortDirections).Item(i).Cast<TCoBool>().Literal().Value()),
                    i,
                    keySelectorArg,
                    unordered);
            }
        } else  {
            size_t tupleSize = keySelectorBody.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().size();
            for (size_t i = 0; i < tupleSize; ++i) {
                AddColumn<true, false>(keySelectorLambda,
                    keySelectorBody.Ptr(),
                    allAscending || IsTrue(TExprList(sortDirections).Item(i).Cast<TCoBool>().Literal().Value()),
                    i,
                    keySelectorArg,
                    unordered);
            }
        }
    } else {
        const bool ascending = allAscending || (
            sortDirections->IsList()
                ? IsTrue(TExprList(sortDirections).Item(0).Cast<TCoBool>().Literal().Value())
                : IsTrue(TCoBool(sortDirections).Literal().Value())
        );
        AddColumn<false, true>(keySelectorLambda,
            keySelectorBody.Ptr(),
            ascending,
            0,
            keySelectorArg,
            unordered);
    }
}

void TKeySelectorBuilder::ProcessConstraint(const TSortedConstraintNode& sortConstraint) {
    YQL_ENSURE(StructType);
    for (const auto& item : sortConstraint.GetContent()) {
        bool good = false;
        for (const auto& path : item.first) {
            if (path.size() == 1U) {
                const auto& column = path.front();
                const auto pos = StructType->FindItem(column);
                YQL_ENSURE(pos, "Column " << column << " is missing in struct type");
                AddColumn(column, StructType->GetItems()[*pos]->GetItemType(), item.second, false);
                good = true;
                break;
            }
        }
        if (!good)
            break;
    }
}

void TKeySelectorBuilder::ProcessRowSpec(const TYqlRowSpecInfo& rowSpec) {
    auto& columns = rowSpec.SortMembers;
    auto& dirs = rowSpec.SortDirections;
    YQL_ENSURE(columns.size() <= dirs.size());
    YQL_ENSURE(StructType);
    for (size_t i = 0; i < columns.size(); ++i) {
        auto pos = StructType->FindItem(columns[i]);
        YQL_ENSURE(pos, "Column " << columns[i] << " is missing in struct type");
        AddColumn(columns[i], StructType->GetItems()[*pos]->GetItemType(), dirs[i], false);
    }
}

TExprNode::TPtr TKeySelectorBuilder::MakeRemapLambda(bool ordered) const {
    return Build<TCoLambda>(Ctx_, Pos_)
        .Args({TStringBuf("stream")})
        .Body<TCoFlatMapBase>()
            .CallableName(ordered ? TCoOrderedFlatMap::CallableName() : TCoFlatMap::CallableName())
            .Input(TStringBuf("stream"))
            .Lambda()
                .Args(TCoArgument(Arg_))
                .Body<TCoJust>()
                    .Input(LambdaBody_)
                .Build()
            .Build()
        .Build()
        .Done()
        .Ptr();
}

template <bool ComputedTuple, bool SingleColumn>
void TKeySelectorBuilder::AddColumn(const TExprNode::TPtr& rootLambda, const TExprNode::TPtr& keyNode, bool ascending, size_t columnIndex,
    const TExprNode::TPtr& structArg, bool unordered) {
    const TTypeAnnotationNode* columnType = nullptr;
    if (ComputedTuple) {
        columnType = keyNode->GetTypeAnn()->Cast<TTupleExprType>()->GetItems()[columnIndex];
    } else {
        columnType = keyNode->GetTypeAnn();
    }

    auto presortColumnType = columnType;
    bool needPresort = false;
    if (ascending) {
        needPresort = RemoveOptionalType(columnType)->GetKind() != ETypeAnnotationKind::Data;
    } else {
        needPresort = true;
    }

    if (needPresort) {
        presortColumnType = Ctx_.MakeType<TDataExprType>(EDataSlot::String);
    }

    auto maybeMember = TMaybeNode<TCoMember>(keyNode);
    if (!ComputedTuple && maybeMember
        && (maybeMember.Cast().Struct().Raw() == structArg.Get()
            || maybeMember.Cast().Struct().Maybe<TCoVariantItem>().Variant().Raw() == structArg.Get()))
    {
        auto memberName = TString{maybeMember.Cast().Name().Value()};
        if (!HasComputedColumn_) {
            Members_.push_back(memberName);
        }
        if (!UniqMemberColumns_.insert(memberName).second) {
            return;
        }
        // Reset descending presort only for non-computed fields
        if (!ascending && UseNativeDescSort && RemoveOptionalType(columnType)->GetKind() == ETypeAnnotationKind::Data) {
            needPresort = false;
        }

        if (!needPresort) {
            Columns_.push_back(memberName);
            ColumnTypes_.push_back(columnType);
            SortDirections_.push_back(ascending);
            ForeignSortDirections_.push_back(ascending);
            if (NonStructInput) {
                FieldTypes_.push_back(Ctx_.MakeType<TItemExprType>(memberName, columnType));
                auto key = TExprBase(Ctx_.ReplaceNode(rootLambda->TailPtr(), rootLambda->Head().Head(), Arg_));
                if (!SingleColumn && rootLambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                    key = Build<TCoNth>(Ctx_, Pos_)
                        .Tuple(key)
                        .Index().Value(ToString(columnIndex)).Build()
                        .Done();
                }
                LambdaBody_ = Build<TCoAddMember>(Ctx_, Pos_)
                    .Struct(LambdaBody_)
                    .Name().Value(memberName).Build()
                    .Item(key)
                    .Done()
                    .Ptr();
            }
            return;
        }
    } else {
        HasComputedColumn_ = true;
    }

    NeedMap_ = true;

    auto key = TExprBase(Ctx_.ReplaceNode(rootLambda->TailPtr(), rootLambda->Head().Head(), Arg_));
    if (ComputedTuple || (!SingleColumn && rootLambda->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple)) {
        key = Build<TCoNth>(Ctx_, Pos_)
            .Tuple(key)
            .Index().Value(ToString(columnIndex)).Build()
            .Done();
    }
    if (needPresort) {
        if (unordered) {
            key = Build<TCoStablePickle>(Ctx_, Pos_)
                .Value(key)
                .Done();
        } else {
            if (ascending) {
                key = Build<TCoAscending>(Ctx_, Pos_)
                    .Input(key)
                    .Done();
            } else {
                key = Build<TCoDescending>(Ctx_, Pos_)
                    .Input(key)
                    .Done();
            }
        }
        columnType = presortColumnType;
    }

    TString column = TStringBuilder() << "_yql_column_" << Index_++;
    Columns_.push_back(column);
    ColumnTypes_.push_back(columnType);
    SortDirections_.push_back(ascending);
    ForeignSortDirections_.push_back(true);
    FieldTypes_.push_back(Ctx_.MakeType<TItemExprType>(column, presortColumnType));

    LambdaBody_ = Build<TCoAddMember>(Ctx_, Pos_)
        .Struct(LambdaBody_)
        .Name().Value(column).Build()
        .Item(key)
        .Done()
        .Ptr();
}

void TKeySelectorBuilder::AddColumn(const TStringBuf memberName, const TTypeAnnotationNode* columnType,
    bool ascending, bool unordered) {
    auto presortColumnType = columnType;
    bool needPresort = false;
    if (ascending) {
        needPresort = RemoveOptionalType(columnType)->GetKind() != ETypeAnnotationKind::Data;
    } else {
        needPresort = !UseNativeDescSort || RemoveOptionalType(columnType)->GetKind() != ETypeAnnotationKind::Data;
    }

    if (needPresort) {
        presortColumnType = Ctx_.MakeType<TDataExprType>(EDataSlot::String);
    }

    Members_.emplace_back(memberName);
    if (!UniqMemberColumns_.emplace(memberName).second) {
        return;
    }
    if (!needPresort) {
        Columns_.emplace_back(memberName);
        ColumnTypes_.push_back(columnType);
        SortDirections_.push_back(ascending);
        ForeignSortDirections_.push_back(ascending);
        return;
    }

    NeedMap_ = true;

    TExprBase key = Build<TCoMember>(Ctx_, Pos_)
        .Struct(Arg_)
        .Name()
            .Value(memberName)
        .Build()
        .Done();

    if (needPresort) {
        if (unordered) {
            key = Build<TCoStablePickle>(Ctx_, Pos_)
                .Value(key)
                .Done();
        } else {
            if (ascending) {
                key = Build<TCoAscending>(Ctx_, Pos_)
                    .Input(key)
                    .Done();
            } else {
                key = Build<TCoDescending>(Ctx_, Pos_)
                    .Input(key)
                    .Done();
            }
        }
        
        columnType = presortColumnType;
    }

    TString column = TStringBuilder() << "_yql_column_" << Index_++;
    Columns_.push_back(column);
    ColumnTypes_.push_back(columnType);
    SortDirections_.push_back(ascending);
    ForeignSortDirections_.push_back(true);
    FieldTypes_.push_back(Ctx_.MakeType<TItemExprType>(column, presortColumnType));

    LambdaBody_ = Build<TCoAddMember>(Ctx_, Pos_)
        .Struct(LambdaBody_)
        .Name().Value(column).Build()
        .Item(key)
        .Done()
        .Ptr();
}

TVector<std::pair<TString, bool>> TKeySelectorBuilder::ForeignSortColumns() const {
    TVector<std::pair<TString, bool>> res;
    YQL_ENSURE(ForeignSortDirections_.size() == Columns_.size());
    for (size_t i = 0; i < Columns_.size(); ++i) {
        res.emplace_back(Columns_[i], ForeignSortDirections_[i]);
    }
    return res;
}

void TKeySelectorBuilder::FillRowSpecSort(TYqlRowSpecInfo& rowSpec) {
    rowSpec.SortMembers = Members_;
    rowSpec.SortedBy = Columns_;
    rowSpec.SortedByTypes = ColumnTypes_;
    rowSpec.SortDirections = SortDirections_;
}

}
