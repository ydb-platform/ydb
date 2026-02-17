#include "type_ann_core.h"
#include "type_ann_impl.h"

#include <util/string/join.h>
#include <util/string/split.h>

#include <yql/essentials/core/type_ann/type_ann_types.h>
#include <yql/essentials/core/yql_join.h>
#include <yql/essentials/minikql/mkql_program_builder.h>


namespace NYql::NTypeAnnImpl {

    using namespace NNodes;

    IGraphTransformer::TStatus JoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);
        if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(input->Head(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureListType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto status = ConvertToLambda(input->ChildRef(2), ctx.Expr, 1);
        status = status.Combine(ConvertToLambda(input->ChildRef(3), ctx.Expr, 1));
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (!EnsureAtom(*input->Child(4), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto joinKind = input->Child(4)->Content();
        if (joinKind != "Inner" && joinKind != "Left" && joinKind != "Right" && joinKind != "Full") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(4)->Pos()), TStringBuilder() << "Unknown join kind: " << joinKind
                << ", supported: Inner, Right, Left, Full"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto leftItemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (leftItemType->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = leftItemType->Cast<TStructExprType>();
            if (AnyOf(structType->GetItems(), [](const TItemExprType* structItem) { return structItem->GetName().StartsWith("_yql_sys_"); })) {
                output = ctx.Expr.ChangeChild(*input, 0,
                    ctx.Expr.Builder(input->Child(0)->Pos())
                        .Callable("RemovePrefixMembers")
                            .Add(0, input->ChildPtr(0))
                            .List(1)
                                .Atom(0, "_yql_sys_", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Build()
                    );
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        const TTypeAnnotationNode* rightItemType = input->Child(1)->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (rightItemType->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = rightItemType->Cast<TStructExprType>();
            if (AnyOf(structType->GetItems(), [](const TItemExprType* structItem) { return structItem->GetName().StartsWith("_yql_sys_"); })) {
                output = ctx.Expr.ChangeChild(*input, 1,
                    ctx.Expr.Builder(input->Child(1)->Pos())
                        .Callable("RemovePrefixMembers")
                            .Add(0, input->ChildPtr(1))
                            .List(1)
                                .Atom(0, "_yql_sys_", TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Build()
                    );
                return IGraphTransformer::TStatus::Repeat;
            }
        }

        auto& lambda1 = input->ChildRef(2);
        if (!UpdateLambdaAllArgumentsTypes(lambda1, {leftItemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto& lambda2 = input->ChildRef(3);
        if (!UpdateLambdaAllArgumentsTypes(lambda2, {rightItemType}, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!lambda1->GetTypeAnn() || !lambda2->GetTypeAnn()) {
            return IGraphTransformer::TStatus::Repeat;
        }

        bool isUniversal1;
        if (!EnsureOneOrTupleOfDataOrOptionalOfData(*lambda1, ctx.Expr, isUniversal1)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isUniversal2;
        if (!EnsureOneOrTupleOfDataOrOptionalOfData(*lambda2, ctx.Expr, isUniversal2)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (isUniversal1 || isUniversal2) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        if (!EnsureComparableKey(lambda1->Pos(), lambda1->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureComparableKey(lambda2->Pos(), lambda2->GetTypeAnn(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!IsSameAnnotation(*lambda1->GetTypeAnn(), *lambda2->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "mismatch of key extractors types, "
                << *lambda1->GetTypeAnn() << " != " << *lambda2->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }

        TTypeAnnotationNode::TListType tupleItems(2);
        tupleItems[0] = leftItemType;
        tupleItems[1] = rightItemType;
        if (joinKind == "Right" || joinKind == "Full") {
            tupleItems[0] = ctx.Expr.MakeType<TOptionalExprType>(tupleItems[0]);
        }

        if (joinKind == "Left" || joinKind == "Full") {
            tupleItems[1] = ctx.Expr.MakeType<TOptionalExprType>(tupleItems[1]);
        }

        auto tupleType = ctx.Expr.MakeType<TTupleExprType>(tupleItems);
        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(tupleType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus JoinDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureMinMaxArgsCount(*input, 3U, 4U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto& left = input->Head();
        const auto& right = *input->Child(1U);
        const auto& kind = *input->Child(2U);

        if (!EnsureDictType(left, ctx.Expr) || !EnsureDictType(right, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(kind, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        bool leftUnique = false, rightUnique = false;
        if (input->ChildrenSize() > 3U) {
            if (!EnsureTupleOfAtoms(input->Tail(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            bool hasUnknown = false;
            input->Tail().ForEachChild([&](const TExprNode& flag) {
                if (const auto& content = flag.Content(); content == "LeftUnique")
                    leftUnique = true;
                else if (content == "RightUnique")
                    rightUnique = true;
                else {
                    hasUnknown = true;
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(flag.Pos()), TStringBuilder() << "Unknown flag " << content));
                }
            });
            if (hasUnknown)
                return IGraphTransformer::TStatus::Error;
        }

        const auto keyType = left.GetTypeAnn()->Cast<TDictExprType>()->GetKeyType();
        if (const auto rightKeyType = right.GetTypeAnn()->Cast<TDictExprType>()->GetKeyType(); !IsSameAnnotation(*keyType, *rightKeyType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch dict key types: " << *keyType << " and " << *rightKeyType));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureDryType(input->Pos(), *keyType, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto leftPayloadType = left.GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType();
        const auto rightPayloadType = right.GetTypeAnn()->Cast<TDictExprType>()->GetPayloadType();
        const TTypeAnnotationNode* outputItemType = nullptr;
        if (const auto joinKind = kind.Content(); joinKind == "LeftOnly" || joinKind == "LeftSemi") {
            if (!leftUnique && leftPayloadType->GetKind() != ETypeAnnotationKind::List) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(left.Pos()), TStringBuilder() << "Expected multi dict on left side but got " << *left.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
            if (rightPayloadType->GetKind() != ETypeAnnotationKind::Void) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(right.Pos()), TStringBuilder() << "Expected set on right side but got " << *right.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
            outputItemType = leftUnique ? leftPayloadType : leftPayloadType->Cast<TListExprType>()->GetItemType();
        } else if (joinKind == "RightOnly" || joinKind == "RightSemi") {
            if (leftPayloadType->GetKind() != ETypeAnnotationKind::Void) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(left.Pos()), TStringBuilder() << "Expected set on left side but got " << *left.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!rightUnique && rightPayloadType->GetKind() != ETypeAnnotationKind::List) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(right.Pos()), TStringBuilder() << "Expected multi dict on right side but got " << *right.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
            outputItemType = rightUnique ? rightPayloadType : rightPayloadType->Cast<TListExprType>()->GetItemType();
        } else if (joinKind == "Inner" || joinKind == "Left" || joinKind == "Right" || joinKind == "Full" || joinKind == "Exclusion") {
            if (!leftUnique && leftPayloadType->GetKind() != ETypeAnnotationKind::List) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(left.Pos()), TStringBuilder() << "Expected multi dict on left side but got " << *left.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!rightUnique && rightPayloadType->GetKind() != ETypeAnnotationKind::List) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(right.Pos()), TStringBuilder() << "Expected multi dict on right side but got " << *right.GetTypeAnn()));
                return IGraphTransformer::TStatus::Error;
            }

            TTypeAnnotationNode::TListType tupleItems = {
                leftUnique ? leftPayloadType : leftPayloadType->Cast<TListExprType>()->GetItemType(),
                rightUnique ? rightPayloadType : rightPayloadType->Cast<TListExprType>()->GetItemType()
            };

            if (joinKind == "Right" || joinKind == "Full" || joinKind == "Exclusion") {
                tupleItems.front() = ctx.Expr.MakeType<TOptionalExprType>(tupleItems.front());
            }
            if (joinKind == "Left" || joinKind == "Full" || joinKind == "Exclusion") {
                tupleItems.back() = ctx.Expr.MakeType<TOptionalExprType>(tupleItems.back());
            }

            outputItemType = ctx.Expr.MakeType<TTupleExprType>(tupleItems);
        } else {
            ctx.Expr.AddError(
                TIssue(ctx.Expr.GetPosition(kind.Pos()), TStringBuilder() << "Unsupported join kind: " << joinKind)
            );
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputItemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus EquiJoinWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const size_t numLists = input->ChildrenSize() - 2;

        auto optionsNode = input->Child(input->ChildrenSize() - 1);
        TJoinOptions options;
        auto status = ValidateEquiJoinOptions(input->Pos(), *optionsNode, options, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        TJoinLabels labels;
        TExprNode::TListType updatedChildren;
        for (ui32 idx = 0; idx < numLists; ++idx) {
            auto& listPair = *input->Child(idx);
            if (!EnsureTupleSize(listPair, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const auto& list = listPair.Head();
            if (list.GetTypeAnn() && list.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
                input->SetTypeAnn(list.GetTypeAnn());
                return IGraphTransformer::TStatus::Ok;
            }

            if (!EnsureListType(list, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            const TTypeAnnotationNode* itemType = list.GetTypeAnn()->Cast<TListExprType>()->GetItemType();
            if (itemType->GetKind() == ETypeAnnotationKind::Universal ||
                itemType->GetKind() == ETypeAnnotationKind::UniversalStruct) {
                input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
                return IGraphTransformer::TStatus::Ok;
            }

            if (itemType->GetKind() != ETypeAnnotationKind::Struct) {
                ctx.Expr.AddError(TIssue(
                    ctx.Expr.GetPosition(list.Pos()),
                    TStringBuilder() << "Expected list of struct"
                    ));
                return IGraphTransformer::TStatus::Error;
            }

            auto structType = itemType->Cast<TStructExprType>();
            if (auto err = labels.Add(ctx.Expr, *listPair.Child(1), structType)) {
                ctx.Expr.AddError(*err);
                ctx.Expr.AddError(TIssue(
                    ctx.Expr.GetPosition(input->Child(idx)->Pos()),
                    TStringBuilder() << "Failed to parse labels of struct as second element of " << idx << " argument"
                    ));
                return IGraphTransformer::TStatus::Error;
            }
        }
        if (!updatedChildren.empty()) {
            output = ctx.Expr.ChangeChildren(*input, std::move(updatedChildren));
            return IGraphTransformer::TStatus::Repeat;
        }

        auto joins = input->Child(input->ChildrenSize() - 2);
        const TStructExprType* resultType = nullptr;
        status = EquiJoinAnnotation(input->Pos(), resultType, labels, *joins, options, ctx.Expr);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(resultType));
        return IGraphTransformer::TStatus::Ok;
    }

    const TTypeAnnotationNode* GetFieldType(const TTupleExprType& tupleType, const ui32 position) {
        return tupleType.GetItems()[position];
    }

    const TTypeAnnotationNode* GetFieldType(const TMultiExprType& multiType, const ui32 position) {
        return multiType.GetItems()[position];
    }

    const TTypeAnnotationNode* GetFieldType(const TStructExprType& structType, const ui32 position) {
        return structType.GetItems()[position]->GetItemType();
    }

    template<class TLeftType>
    IGraphTransformer::TStatus MapJoinCoreWrapperT(const TExprNode::TPtr& input, const TLeftType& leftItemType, TContext& ctx) {
        constexpr bool ByStruct = std::is_same<TLeftType, TStructExprType>::value;
        const auto dictType = input->Child(1)->GetTypeAnn()->Cast<TDictExprType>();
        const auto dictPayloadType = dictType->GetPayloadType();

        if (!EnsureAtom(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto joinKind = input->Child(2)->Content();
        if (joinKind != "Inner" && joinKind != "Left" && joinKind != "LeftSemi" && joinKind != "LeftOnly") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(2)->Pos()), TStringBuilder() << "Unknown join kind: " << joinKind
                << ", supported: Inner, Left, LeftSemi, LeftOnly"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& child : input->Child(3)->Children()) {
            if (!GetFieldPosition(leftItemType, child->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown key column: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureTupleOfAtoms(*input->Child(4), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* rightStructType = nullptr;
        const TTupleExprType* rightTupleType = nullptr;
        if (joinKind != "LeftSemi" && joinKind != "LeftOnly") {
            auto singleItemType = dictPayloadType;
            if (dictPayloadType->GetKind() == ETypeAnnotationKind::List) {
                singleItemType = dictPayloadType->Cast<TListExprType>()->GetItemType();
            }

            switch (singleItemType->GetKind()) {
                case ETypeAnnotationKind::Struct:
                    rightStructType = singleItemType->Cast<TStructExprType>();
                    break;
                case ETypeAnnotationKind::Tuple:
                    rightTupleType = singleItemType->Cast<TTupleExprType>();
                    break;
                default:
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1U)->Pos()),
                        TStringBuilder() << "Expected tuple or struct payload type, but got: " << *singleItemType));
                    return IGraphTransformer::TStatus::Error;
            }
        }

        auto& leftRenames = *input->Child(5);
        auto& rightRenames = *input->Child(6);
        if (!EnsureTupleOfAtoms(leftRenames, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (leftRenames.ChildrenSize() % 2 != 0) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(leftRenames.Pos()), TStringBuilder() << "Expected even count of atoms"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto outputSize = (leftRenames.ChildrenSize() + rightRenames.ChildrenSize()) >> 1U;
        std::conditional_t<ByStruct, TVector<const TItemExprType*>, TVector<const TTypeAnnotationNode*>> resultItems;
        if constexpr (ByStruct)
            resultItems.reserve(outputSize);
        else
            resultItems.resize(outputSize);

        THashSet<TStringBuf> outputColumns;
        outputColumns.reserve(outputSize);
        for (ui32 i = 0; i < leftRenames.ChildrenSize(); i += 2) {
            const auto oldName = leftRenames.Child(i);
            const auto newName = leftRenames.Child(i + 1);

            const auto oldPos = GetFieldPosition(leftItemType, oldName->Content());
            if (!oldPos) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(oldName->Pos()), TStringBuilder() << "Unknown column: " << oldName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (newName->Content().empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), "Empty column is not allowed"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!outputColumns.emplace(newName->Content()).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Duplicate output field: " << newName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            const auto columnType = GetFieldType(leftItemType, *oldPos);

            if constexpr (ByStruct)
                resultItems.emplace_back(ctx.Expr.MakeType<TItemExprType>(newName->Content(), columnType));
            else {
                if (ui32 index; !TryFromString(newName->Content(), index) || index >= resultItems.size()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Invalid output field index: " << newName->Content()));
                    return IGraphTransformer::TStatus::Error;
                } else {
                    resultItems[index] = columnType;
                }
            }
        }

        if (rightStructType || rightTupleType) {
            if (!EnsureTupleOfAtoms(rightRenames, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (rightRenames.ChildrenSize() % 2 != 0) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(rightRenames.Pos()), TStringBuilder() << "Expected even count of atoms"));
                return IGraphTransformer::TStatus::Error;
            }

            for (ui32 i = 0; i < rightRenames.ChildrenSize(); i += 2) {
                const auto oldName = rightRenames.Child(i);
                const auto newName = rightRenames.Child(i + 1);

                const auto oldPos = rightStructType ? GetFieldPosition(*rightStructType, oldName->Content()) : GetFieldPosition(*rightTupleType, oldName->Content());
                if (!oldPos) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(oldName->Pos()), TStringBuilder() << "Unknown column: " << oldName->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (newName->Content().empty()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), "Empty column is not allowed"));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!outputColumns.emplace(newName->Content()).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Duplicate output field: " << newName->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                auto columnType = rightStructType ? GetFieldType(*rightStructType, *oldPos) : GetFieldType(*rightTupleType, *oldPos);
                if (joinKind == "Left" && !columnType->IsOptionalOrNull()) {
                    columnType = ctx.Expr.MakeType<TOptionalExprType>(columnType);
                }

                if constexpr (ByStruct)
                    resultItems.emplace_back(ctx.Expr.MakeType<TItemExprType>(newName->Content(), columnType));
                else {
                    if (ui32 index; !TryFromString(newName->Content(), index) || index >= resultItems.size()) {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Invalid output field index: " << newName->Content()));
                        return IGraphTransformer::TStatus::Error;
                    } else {
                        resultItems[index] = columnType;
                    }
                }
            }
        } else if (!EnsureTupleSize(rightRenames, 0, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto resultItemType = ctx.Expr.MakeType<TLeftType>(resultItems);
        if (!resultItemType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *resultItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus MapJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 9, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* leftItemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &leftItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureDictType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (leftItemType->GetKind()) {
            case ETypeAnnotationKind::Struct:
                return MapJoinCoreWrapperT(input, *leftItemType->Cast<TStructExprType>(), ctx);
            case ETypeAnnotationKind::Tuple:
                return MapJoinCoreWrapperT(input, *leftItemType->Cast<TTupleExprType>(), ctx);
            case ETypeAnnotationKind::Multi:
                return MapJoinCoreWrapperT(input, *leftItemType->Cast<TMultiExprType>(), ctx);
            default:
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected tuple or struct or multi item type, but got: " << *leftItemType));
                return IGraphTransformer::TStatus::Error;
        }
    }

    IGraphTransformer::TStatus GraceJoinCoreWrapperImp(const TExprNode::TPtr& input, const TMultiExprType& leftTupleType, const TMultiExprType& rightTupleType, TContext& ctx, int shift) {
        if (!EnsureAtom(*input->Child(shift), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto joinKind = input->Child(shift);

        auto& leftKeyColumns = *input->Child(shift + 1);
        auto& rightKeyColumns = *input->Child(shift + 2);

        if (!EnsureTupleOfAtoms(leftKeyColumns, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(rightKeyColumns, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }


        auto& leftRenames = *input->Child(shift + 3);
        auto& rightRenames = *input->Child(shift + 4);

        if (!EnsureTupleOfAtoms(leftRenames, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (leftRenames.ChildrenSize() % 2 != 0) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(leftRenames.Pos()), TStringBuilder() << "Expected even count of atoms"));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(rightRenames, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (rightRenames.ChildrenSize() % 2 != 0) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(rightRenames.Pos()), TStringBuilder() << "Expected even count of atoms"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto outputSize = (leftRenames.ChildrenSize() + rightRenames.ChildrenSize())  / 2;
        TVector<const TTypeAnnotationNode*> resultItems;
        resultItems.resize(outputSize);

        THashSet<TStringBuf> outputColumns;
        outputColumns.reserve(outputSize);

        for (ui32 i = 0; i < leftRenames.ChildrenSize(); i += 2) {
            const auto oldName = leftRenames.Child(i);
            const auto newName = leftRenames.Child(i + 1);

            const auto oldPos = GetFieldPosition(leftTupleType, oldName->Content());
            if (!oldPos) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(oldName->Pos()), TStringBuilder() << "Unknown column: " << oldName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (newName->Content().empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), "Empty column is not allowed"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!outputColumns.emplace(newName->Content()).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Duplicate output field: " << newName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            auto columnType = GetFieldType(leftTupleType, *oldPos);
            if (joinKind->IsAtom({"Right", "Full", "Exclusion"}) && !columnType->IsOptionalOrNull()) {
                columnType = ctx.Expr.MakeType<TOptionalExprType>(columnType);
            }

            if (ui32 index; !TryFromString(newName->Content(), index) || index >= resultItems.size()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Invalid output field index: " << newName->Content()));
                return IGraphTransformer::TStatus::Error;
            } else {
                    resultItems[index] = columnType;
            }

        }

        for (ui32 i = 0; i < rightRenames.ChildrenSize(); i += 2) {
            const auto oldName = rightRenames.Child(i);
            const auto newName = rightRenames.Child(i + 1);

            const auto oldPos =  GetFieldPosition(rightTupleType, oldName->Content());
            if (!oldPos) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(oldName->Pos()), TStringBuilder() << "Unknown column: " << oldName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (newName->Content().empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), "Empty column is not allowed"));
                return IGraphTransformer::TStatus::Error;
            }

            if (!outputColumns.emplace(newName->Content()).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Duplicate output field: " << newName->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            auto columnType =  GetFieldType(rightTupleType, *oldPos);
            if (joinKind->IsAtom({"Left", "Full", "Exclusion"}) && !columnType->IsOptionalOrNull()) {
                columnType = ctx.Expr.MakeType<TOptionalExprType>(columnType);
            }

            if (ui32 index; !TryFromString(newName->Content(), index) || index >= resultItems.size()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(newName->Pos()), TStringBuilder() << "Invalid output field index: " << newName->Content()));
                return IGraphTransformer::TStatus::Error;
            } else {
                resultItems[index] = columnType;
            }
        }

        for (auto i = 0U; i < input->Tail().ChildrenSize(); ++i) {
            if (const auto& flag = *input->Tail().Child(i); !flag.IsAtom({"LeftAny", "RightAny", "Broadcast"})) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(flag.Pos()), TStringBuilder() << "Unsupported grace join option: " << flag.Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto resultItemType = ctx.Expr.MakeType<TMultiExprType>(resultItems);
        if (!resultItemType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(resultItemType));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus GraceJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 10, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* leftItemType = nullptr;
        if (!EnsureNewSeqType<true>(*input->Child(0), ctx.Expr, &leftItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* rightItemType = nullptr;
        if (!EnsureNewSeqType<true>(*input->Child(1), ctx.Expr, &rightItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if ( !EnsureWideFlowType(*input->Child(0), ctx.Expr) ) {
            return IGraphTransformer::TStatus::Error;
        }

        if ( !EnsureWideFlowType(*input->Child(1), ctx.Expr) ) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isUniversal;
        if (const auto status = NormalizeTupleOfAtoms(input, 7U, output, ctx.Expr, isUniversal); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        return GraceJoinCoreWrapperImp(input, *leftItemType->Cast<TMultiExprType>(), *rightItemType->Cast<TMultiExprType>(), ctx, 2);
    }

    IGraphTransformer::TStatus GraceSelfJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        if (!EnsureArgsCount(*input, 9, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* leftItemType = nullptr;
        if (!EnsureNewSeqType<true>(*input->Child(0), ctx.Expr, &leftItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if ( !EnsureWideFlowType(*input->Child(0), ctx.Expr) ) {
            return IGraphTransformer::TStatus::Error;
        }

        bool isUniversal;
        if (const auto status = NormalizeTupleOfAtoms(input, 6U, output, ctx.Expr, isUniversal); status != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        if (isUniversal) {
            input->SetTypeAnn(ctx.Expr.MakeType<TUniversalExprType>());
            return IGraphTransformer::TStatus::Ok;
        }

        return GraceJoinCoreWrapperImp(input, *leftItemType->Cast<TMultiExprType>(), *leftItemType->Cast<TMultiExprType>(), ctx, 1);
    }

    template<class TInputType>
    IGraphTransformer::TStatus CommonJoinCoreWrapperT(const TExprNode::TPtr& input, const TInputType& inputItemType, TContext& ctx) {
        constexpr bool ByStruct = std::is_same<TInputType, TStructExprType>::value;

        const auto joinKind = input->Child(1)->Content();
        if (joinKind != "Inner" && joinKind != "Left" && joinKind != "Right" && joinKind != "Full"
            && joinKind != "LeftOnly" && joinKind != "RightOnly" && joinKind != "Exclusion"
            && joinKind != "LeftSemi" && joinKind != "RightSemi" && joinKind != "Cross") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1)->Pos()), TStringBuilder() << "Unknown join kind: " << joinKind
                << ", supported: Inner, Right, Left, Full, LeftOnly, RightOnly, Exclusion, LeftSemi, RightSemi, Cross"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto tableIndexFieldName = input->Tail().Content();
        const auto tableIndexPos = GetFieldPosition(inputItemType, tableIndexFieldName);
        if (!tableIndexPos) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Missing required field: " << tableIndexFieldName));
            return IGraphTransformer::TStatus::Error;
        }

        if (const auto tableIndexType = GetFieldType(inputItemType, *tableIndexPos); !EnsureSpecificDataType(input->Head().Pos(), *tableIndexType, EDataSlot::Uint32, ctx.Expr)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected type Uint32 for field " << tableIndexFieldName << ", but got: "
                << *tableIndexType));
            return IGraphTransformer::TStatus::Error;
        }

        THashSet<TStringBuf> leftColumns, rightColumns, fullColumns, requiredColumns, keyColumns;
        if (joinKind == "RightOnly" || joinKind == "RightSemi") {
            if (!EnsureTupleSize(*input->Child(2), 0, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureTuple(*input->Child(2), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            for (const auto& child : input->Child(2)->Children()) {
                if (!EnsureAtom(*child, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto pos = GetFieldPosition(inputItemType, child->Content());
                if (!pos) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown column: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!leftColumns.insert(child->Content()).second || !fullColumns.insert(child->Content()).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Duplication of column: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (const auto inputColumnType = GetFieldType(inputItemType, *pos); !inputColumnType->IsOptionalOrNull()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected optional or null type for column: " << child->Content() << ", but got: " << *inputColumnType));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (joinKind == "LeftOnly" || joinKind == "LeftSemi") {
            if (!EnsureTupleSize(*input->Child(3), 0, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureTuple(*input->Child(3), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            for (const auto& child : input->Child(3)->Children()) {
                if (!EnsureAtom(*child, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                const auto pos = GetFieldPosition(inputItemType, child->Content());
                if (!pos) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown column: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (!rightColumns.insert(child->Content()).second || !fullColumns.insert(child->Content()).second) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Duplication of column: " << child->Content()));
                    return IGraphTransformer::TStatus::Error;
                }

                if (const auto inputColumnType = GetFieldType(inputItemType, *pos); !inputColumnType->IsOptionalOrNull()) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Expected optional or null type for column: " << child->Content() << ", but got: " << *inputColumnType));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!EnsureTuple(*input->Child(4), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& child : input->Child(4)->Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!fullColumns.contains(child->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown column: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (!requiredColumns.insert(child->Content()).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Duplication of column: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }

            if (leftColumns.contains(child->Content())) {
                if (IsLeftJoinSideOptional(joinKind)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Required column " << child->Content() << " cannot be at the left side for the join kind: " << joinKind));
                    return IGraphTransformer::TStatus::Error;
                }
            } else {
                if (IsRightJoinSideOptional(joinKind)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Required column " << child->Content() << " cannot be at the right side for the join kind: " << joinKind));
                    return IGraphTransformer::TStatus::Error;
                }
            }
        }

        if (!EnsureTuple(*input->Child(5), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        for (const auto& child : input->Child(5)->Children()) {
            if (!EnsureAtom(*child, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (const auto pos = GetFieldPosition(inputItemType, child->Content()); !pos) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "Unknown column: " << child->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (!EnsureTuple(*input->Child(6), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TSet<TStringBuf> seenOptions;
        for (const auto& child : input->Child(6)->Children()) {
            if (!EnsureTupleMinSize(*child, 1, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureAtom(child->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (const auto optionName = child->Head().Content(); !seenOptions.insert(optionName).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Head().Pos()), TStringBuilder() <<
                    "Duplicate option: " << optionName));
                return IGraphTransformer::TStatus::Error;
            }
            else if (optionName == "sorted") {
                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(*child->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (const auto side = child->Child(1)->Content(); side != "left" && side != "right") {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Child(1)->Pos()), TStringBuilder() <<
                        "Unknown sorted side, expected left or right, but got: " << side));
                    return IGraphTransformer::TStatus::Error;
                }
            }
            else if (optionName == "memLimit") {
                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureAtom(*child->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (ui64 memLimit = 0ULL; !TryFromString(child->Child(1)->Content(), memLimit)) {
                    ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Child(1)->Pos()), TStringBuilder() <<
                        "Bad memLimit value: " << child->Child(1)->Content()));
                    return IGraphTransformer::TStatus::Error;
                }
            }
            else if (optionName == "any") {
                if (!EnsureTupleSize(*child, 2, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                if (!EnsureTuple(*child->Child(1), ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

                for (auto sideNode : child->Child(1)->Children()) {
                    if (!EnsureAtom(*sideNode, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }

                    auto side = sideNode->Content();
                    if (side != "left" && side != "right") {
                        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(sideNode->Pos()), TStringBuilder() <<
                            "Unknown any side, expected left or right, but got: " << side));
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }
            else if (optionName == "forceSortedMerge") {
                if (!EnsureTupleSize(*child, 1, ctx.Expr)) {
                    return IGraphTransformer::TStatus::Error;
                }

            }
            else if (optionName == "join_algo") {
                // do nothing
            }
            else if (optionName == "shuffle_lhs_by" || optionName == "shuffle_rhs_by") {
                for (ui64 i = 1; i < child->ChildrenSize(); ++i) {
                    const auto& shuffleColumn = child->Child(i);
                    if (!EnsureTupleSize(*shuffleColumn, 2, ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    if (!EnsureAtom(*shuffleColumn->Child(0), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                    if (!EnsureAtom(*shuffleColumn->Child(1), ctx.Expr)) {
                        return IGraphTransformer::TStatus::Error;
                    }
                }
            }
            else if (optionName == "compact") {
                if (!EnsureTupleSize(*child, 1, ctx.Expr)) {
                     return IGraphTransformer::TStatus::Error;
                 }
            }
            else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() <<
                    "Unknown option name: " << optionName));
                return IGraphTransformer::TStatus::Error;
            }
        }

        std::conditional_t<ByStruct, TVector<const TItemExprType*>, TVector<const TTypeAnnotationNode*>> resultItems;
        resultItems.reserve(fullColumns.size());

        for (const auto& child : input->Child(2)->Children()) {
            const auto pos = GetFieldPosition(inputItemType, child->Content());
            auto inputColumnType = GetFieldType(inputItemType, *pos);
            if (requiredColumns.contains(child->Content())) {
                inputColumnType = inputColumnType->template Cast<TOptionalExprType>()->GetItemType();
            }

            if constexpr (ByStruct)
                resultItems.emplace_back(ctx.Expr.MakeType<TItemExprType>(child->Content(), inputColumnType));
            else
                resultItems.emplace_back(inputColumnType);
        }

        for (const auto& child : input->Child(3)->Children()) {
            const auto pos = GetFieldPosition(inputItemType, child->Content());
            auto inputColumnType = GetFieldType(inputItemType, *pos);
            if (requiredColumns.contains(child->Content())) {
                inputColumnType = inputColumnType->template Cast<TOptionalExprType>()->GetItemType();
            }

            if constexpr (ByStruct)
                resultItems.emplace_back(ctx.Expr.MakeType<TItemExprType>(child->Content(), inputColumnType));
            else
                resultItems.emplace_back(inputColumnType);
        }

        const auto resultItemType = ctx.Expr.MakeType<TInputType>(resultItems);
        if (!resultItemType->Validate(input->Pos(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        input->SetTypeAnn(MakeSequenceType(input->Head().GetTypeAnn()->GetKind(), *resultItemType, ctx.Expr));
        return IGraphTransformer::TStatus::Ok;
    }

    IGraphTransformer::TStatus CommonJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
        if (!EnsureArgsCount(*input, 8U, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TTypeAnnotationNode* inputItemType = nullptr;
        if (!EnsureNewSeqType<false, false>(input->Head(), ctx.Expr, &inputItemType)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(input->Tail(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        switch (inputItemType->GetKind()) {
            case ETypeAnnotationKind::Struct:
                return CommonJoinCoreWrapperT(input, *inputItemType->Cast<TStructExprType>(), ctx);
            case ETypeAnnotationKind::Tuple:
                return CommonJoinCoreWrapperT(input, *inputItemType->Cast<TTupleExprType>(), ctx);
            case ETypeAnnotationKind::Multi:
                return CommonJoinCoreWrapperT(input, *inputItemType->Cast<TMultiExprType>(), ctx);
            default:
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected tuple or struct or multi item type, but got: " << *inputItemType));
                return IGraphTransformer::TStatus::Error;
        }
    }

    IGraphTransformer::TStatus BlockStorageWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<const TItemExprType*> structItems;
        if (!EnsureBlockListType(input->Head(), structItems, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        auto listItemType = input->Head().GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        input->SetTypeAnn(ctx.Expr.MakeType<TResourceExprType>(TStringBuilder() <<
                NKikimr::NMiniKQL::BlockStorageResourcePrefix << FormatType(listItemType)));
        return IGraphTransformer::TStatus::Ok;
    }

    bool EnsureBlockStorageResource(const TExprNode* resource, const TStructExprType*& listItemType, TExtContext& ctx) {
        using NKikimr::NMiniKQL::BlockStorageResourcePrefix;

        if (!EnsureResourceType(*resource, ctx.Expr)) {
            return false;
        }
        const auto resourceType = resource->GetTypeAnn()->Cast<TResourceExprType>();
        const auto resourceTag = resourceType->GetTag();
        if (!resourceTag.StartsWith(BlockStorageResourcePrefix)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(resource->Pos()), "Expected block storage resource"));
            return false;
        }

        auto typeString = TStringBuf(resourceTag.data() + BlockStorageResourcePrefix.size(), resourceTag.size() - BlockStorageResourcePrefix.size());
        auto typeNode = ctx.Expr.Builder(resource->Pos())
            .Callable("ParseType")
                .Atom(0, typeString)
            .Seal()
            .Build();

        auto status = ParseTypeWrapper(typeNode, typeNode, ctx);
        if (status == IGraphTransformer::TStatus::Error) {
            return false;
        }
        if (!EnsureType(*typeNode, ctx.Expr)) {
            return false;
        }

        listItemType = typeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->UserCast<TStructExprType>(ctx.Expr.GetPosition(resource->Pos()), ctx.Expr);
        return true;
    }

    IGraphTransformer::TStatus BlockMapJoinIndexWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);

        using NKikimr::NMiniKQL::BlockMapJoinIndexResourcePrefix;
        using NKikimr::NMiniKQL::BlockMapJoinIndexResourceSeparator;

        if (!EnsureArgsCount(*input, 4, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        const TStructExprType* expectedListItemType = nullptr;
        if (!EnsureBlockStorageResource(input->Child(0), expectedListItemType, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<const TItemExprType*> structItems;
        if (!EnsureType(*input->Child(1), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto inputType = input->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureBlockStructType(input->Child(1)->Pos(), *inputType, structItems, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto listItemType = inputType->Cast<TStructExprType>();

        if (!IsSameAnnotation(*listItemType, *expectedListItemType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch between provided list item type " << static_cast<const TTypeAnnotationNode&>(*listItemType)
                << "and block storage item type " << static_cast<const TTypeAnnotationNode&>(*expectedListItemType)));
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        TVector<TStringBuf> keyColumns;
        for (const auto& keyColumnNode : input->Child(2)->Children()) {
            if (!listItemType->FindItem(keyColumnNode->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyColumnNode->Pos()), TStringBuilder() << "Unknown key column: " << keyColumnNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            keyColumns.push_back(keyColumnNode->Content());
        }

        auto settingsValidator = [&](TStringBuf settingName, TExprNode& node, TExprContext& ctx) {
            if (settingName == "any") {
                if (node.ChildrenSize() != 1) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                        TStringBuilder() << "No extra parameters are expected by setting '" << settingName << "'"));
                    return false;
                }
            } else if (settingName == "rowCount") {
                if (node.ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Pos()),
                        TStringBuilder() << "Setting '" << settingName << "' requires 1 extra parameter"));
                    return false;
                }
                if (!node.Child(1)->IsAtom()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), "Expected atom"));
                    return false;
                }
            } else {
                YQL_ENSURE(false, "unknown setting");
            }
            return true;
        };
        if (!EnsureValidSettings(input->Tail(), {"any", "rowCount"}, settingsValidator, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        input->SetTypeAnn(ctx.Expr.MakeType<TResourceExprType>(TStringBuilder() <<
                BlockMapJoinIndexResourcePrefix << FormatType(listItemType) << BlockMapJoinIndexResourceSeparator << JoinSeq(",", keyColumns)));
        return IGraphTransformer::TStatus::Ok;
    }

    bool EnsureBlockMapJoinIndexResource(const TExprNode* resource, const TStructExprType*& listItemType, TVector<TStringBuf>& keyColumns, TExtContext& ctx) {
        using NKikimr::NMiniKQL::BlockMapJoinIndexResourcePrefix;
        using NKikimr::NMiniKQL::BlockMapJoinIndexResourceSeparator;

        if (!EnsureResourceType(*resource, ctx.Expr)) {
            return false;
        }
        const auto resourceType = resource->GetTypeAnn()->Cast<TResourceExprType>();
        const auto resourceTag = resourceType->GetTag();
        if (!resourceTag.StartsWith(BlockMapJoinIndexResourcePrefix)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(resource->Pos()), "Expected block map join index resource"));
            return false;
        }

        auto resourceIdentifier = TStringBuf(resourceTag.data() + BlockMapJoinIndexResourcePrefix.size(), resourceTag.size() - BlockMapJoinIndexResourcePrefix.size());
        TStringBuf typeString, keyColumnsString;
        Split(resourceIdentifier, BlockMapJoinIndexResourceSeparator, typeString, keyColumnsString);
        Split(keyColumnsString, ",", keyColumns);

        auto resourceTypeNode = ctx.Expr.Builder(resource->Pos())
            .Callable("ParseType")
                .Atom(0, typeString)
            .Seal()
            .Build();

        auto status = ParseTypeWrapper(resourceTypeNode, resourceTypeNode, ctx);
        if (status == IGraphTransformer::TStatus::Error) {
            return false;
        }
        if (!EnsureType(*resourceTypeNode, ctx.Expr)) {
            return false;
        }

        listItemType = resourceTypeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->UserCast<TStructExprType>(ctx.Expr.GetPosition(resource->Pos()), ctx.Expr);
        return true;
    }

    IGraphTransformer::TStatus BlockMapJoinCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
        Y_UNUSED(output);

        if (!EnsureArgsCount(*input, 8, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(3), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        const auto joinKind = input->Child(3)->Content();
        if (joinKind != "Inner" && joinKind != "Left" && joinKind != "LeftSemi" && joinKind != "LeftOnly" && joinKind != "Cross") {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(3)->Pos()), TStringBuilder() << "Unknown join kind: " << joinKind
                << ", supported: Inner, Left, LeftSemi, LeftOnly, Cross"));
            return IGraphTransformer::TStatus::Error;
        }

        TTypeAnnotationNode::TListType leftItemTypes;
        if (!EnsureWideStreamBlockType(input->Head(), leftItemTypes, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        leftItemTypes.pop_back();
        auto leftStreamItemType = input->Head().GetTypeAnn()->Cast<TStreamExprType>()->GetItemType()->Cast<TMultiExprType>();

        const TStructExprType* expectedRightListItemType = nullptr;
        TVector<TStringBuf> expectedRightKeyColumns;
        if (joinKind != "Cross") {
            if (!EnsureBlockMapJoinIndexResource(input->Child(1), expectedRightListItemType, expectedRightKeyColumns, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            if (!EnsureBlockStorageResource(input->Child(1), expectedRightListItemType, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        TVector<const TItemExprType*> rightStructItems;
        if (!EnsureType(*input->Child(2), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto rightInputType = input->Child(2)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureBlockStructType(input->Child(2)->Pos(), *rightInputType, rightStructItems, ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        auto rightListItemType = rightInputType->Cast<TStructExprType>();

        if (!IsSameAnnotation(*rightListItemType, *expectedRightListItemType)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                TStringBuilder() << "Mismatch between provided right list item type " << static_cast<const TTypeAnnotationNode&>(*rightListItemType)
                << "and right block storage item type " << static_cast<const TTypeAnnotationNode&>(*expectedRightListItemType)));
            return IGraphTransformer::TStatus::Error;
        }

        if (input->Child(4)->ChildrenSize() != input->Child(6)->ChildrenSize()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(6)->Pos()), TStringBuilder() << "Mismatch of key column count"));
            return IGraphTransformer::TStatus::Error;
        }

        for (size_t childIdx = 4; childIdx <= 7; childIdx++) {
            if (!EnsureTupleOfAtoms(*input->Child(childIdx), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
        }

        const auto& leftKeyColumnsNode = *input->Child(4);
        const auto& leftKeyDropsNode = *input->Child(5);
        const auto& rightKeyColumnsNode = *input->Child(6);
        const auto& rightKeyDropsNode = *input->Child(7);

        if (joinKind == "Cross") {
            if (!leftKeyColumnsNode.Children().empty() || !rightKeyColumnsNode.Children().empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Specifying key columns is not allowed for cross join"));
                return IGraphTransformer::TStatus::Error;
            }
            if (!leftKeyDropsNode.Children().empty() || !rightKeyDropsNode.Children().empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Specifying key drops is not allowed for cross join"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        std::unordered_set<ui32> leftKeyColumns;
        for (const auto& keyColumnNode : leftKeyColumnsNode.Children()) {
            auto position = GetWideBlockFieldPosition(*leftStreamItemType, keyColumnNode->Content());
            if (!position) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyColumnNode->Pos()), TStringBuilder() << "Unknown left key column: " << keyColumnNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            leftKeyColumns.insert(*position);
        }

        std::unordered_set<ui32> leftKeyDrops;
        for (const auto& keyDropNode : leftKeyDropsNode.Children()) {
            auto position = GetWideBlockFieldPosition(*leftStreamItemType, keyDropNode->Content());
            if (!position) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Unknown left key column: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!leftKeyColumns.contains(*position)) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Attempted to drop left non-key column: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!leftKeyDrops.insert(*position).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Duplicated left key drop: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        THashSet<TStringBuf> rightKeyColumns;
        for (const auto& keyColumnNode : rightKeyColumnsNode.Children()) {
            if (!rightListItemType->FindItem(keyColumnNode->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyColumnNode->Pos()), TStringBuilder() << "Unknown right key column: " << keyColumnNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            rightKeyColumns.insert(keyColumnNode->Content());
        }

        THashSet<TStringBuf> rightKeyDrops;
        for (const auto& keyDropNode : rightKeyDropsNode.Children()) {
            if (!rightListItemType->FindItem(keyDropNode->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Unknown right key column: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!rightKeyColumns.contains(keyDropNode->Content())) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Attempted to drop right non-key column: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
            if (!rightKeyDrops.insert(keyDropNode->Content()).second) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(keyDropNode->Pos()), TStringBuilder() << "Duplicated right key drop: " << keyDropNode->Content()));
                return IGraphTransformer::TStatus::Error;
            }
        }

        if (input->Child(6)->ChildrenSize() != expectedRightKeyColumns.size()) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                "Provided right key columns list differs from right block storage one"));
            return IGraphTransformer::TStatus::Error;
        }
        for (size_t i = 0; i < input->Child(6)->ChildrenSize(); i++) {
            if (input->Child(6)->Child(i)->Content() != expectedRightKeyColumns[i]) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()),
                    "Provided right key columns list differs from right block storage one"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        std::vector<const TTypeAnnotationNode*> resultItems;
        for (ui32 pos = 0; pos < leftItemTypes.size(); pos++) {
            if (leftKeyDrops.contains(pos)) {
                continue;
            }

            resultItems.push_back(ctx.Expr.MakeType<TBlockExprType>(leftItemTypes[pos]));
        }

        if (joinKind != "LeftSemi" && joinKind != "LeftOnly") {
            for (auto item : rightStructItems) {
                if (rightKeyDrops.contains(item->GetName())) {
                    continue;
                }

                auto columnType = item->GetItemType();
                if (joinKind == "Left" && !columnType->IsOptionalOrNull()) {
                    columnType = ctx.Expr.MakeType<TOptionalExprType>(columnType);
                }

                resultItems.push_back(ctx.Expr.MakeType<TBlockExprType>(columnType));
            }
        } else {
            if (!rightKeyDrops.empty()) {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(6)->Pos()), TStringBuilder() << "Right key drops are not allowed for semi/only join"));
                return IGraphTransformer::TStatus::Error;
            }
        }

        resultItems.push_back(ctx.Expr.MakeType<TScalarExprType>(ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
        input->SetTypeAnn(ctx.Expr.MakeType<TStreamExprType>(ctx.Expr.MakeType<TMultiExprType>(resultItems)));
        return IGraphTransformer::TStatus::Ok;
    }

} // namespace NYql::NTypeAnnImpl

