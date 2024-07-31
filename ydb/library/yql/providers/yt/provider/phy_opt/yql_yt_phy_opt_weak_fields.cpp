#include "yql_yt_phy_opt.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>

#include <ydb/library/yql/core/yql_opt_utils.h>

#include <util/generic/xrange.h>

namespace NYql {

using namespace NNodes;

TMaybeNode<TExprBase> TYtPhysicalOptProposalTransformer::WeakFields(TExprBase node, TExprContext& ctx, const TGetParents& getParents) const {
    auto op = node.Cast<TYtWithUserJobsOpBase>();

    if (op.Input().Size() > 1) {
        return node;
    }

    if (NYql::HasSetting(op.Settings().Ref(), EYtSettingType::WeakFields)) {
        return node;
    }

    auto section = op.Input().Item(0);
    auto inputType = section.Ref().GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (!inputType->FindItem(YqlOthersColumnName)) {
        return node;
    }

    for (auto path: section.Paths()) {
        TYtTableBaseInfo::TPtr info = TYtTableBaseInfo::Parse(path.Table());
        if (!info->RowSpec || info->RowSpec->StrictSchema || info->Meta->Attrs.contains(QB2Premapper)) {
            return node;
        }
    }

    TMaybeNode<TCoLambda> maybeMapper;
    if (auto map = op.Maybe<TYtMap>()) {
        maybeMapper = map.Mapper();
    } else {
        maybeMapper = op.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>();
    }
    if (!maybeMapper) {
        return node;
    }
    auto mapper = maybeMapper.Cast();
    auto mapperIdx = op.Maybe<TYtMap>() ? TYtMap::idx_Mapper : TYtMapReduce::idx_Mapper;

    auto lambdaBody = mapper.Body();
    if (!lambdaBody.Maybe<TCoFlatMapBase>() && !lambdaBody.Maybe<TCoCombineCore>()) {
        return node;
    }

    TVector<TExprBase> stack{lambdaBody};
    auto input = lambdaBody.Cast<TCoInputBase>().Input();
    while (input.Raw() != mapper.Args().Arg(0).Raw()) {
        TMaybe<THashSet<TStringBuf>> passthroughFields;
        if (!input.Maybe<TCoFlatMapBase>()
            || !IsPassthroughFlatMap(input.Cast<TCoFlatMapBase>(), &passthroughFields)
            || (passthroughFields && !passthroughFields->contains(YqlOthersColumnName)))
        {
            return node;
        }
        stack.push_back(input);
        input = input.Cast<TCoFlatMapBase>().Input();
    }

    auto getMemberColumn = [] (const TExprNode* node, const TExprNode* arg) {
        if (auto maybeMember = TMaybeNode<TCoMember>(node)) {
            if (maybeMember.Cast().Struct().Raw() == arg) {
                return maybeMember.Cast().Name().Value();
            }
        }
        return TStringBuf();
    };

    THashMap<const TExprNode*, TExprNode::TPtr> weaks; // map TryWeakMemberFromDict -> row argument
    THashSet<const TExprNode*> otherMembers;
    TSet<TStringBuf> finalWeakColumns;
    const TParentsMap* parentsMap = getParents();
    for (size_t pass: xrange(stack.size())) {
        auto consumer = stack[pass];

        TExprNode::TListType rowArgs;
        if (auto maybeCombineCore = consumer.Maybe<TCoCombineCore>()) {
            auto combineCore = maybeCombineCore.Cast();
            rowArgs.push_back(combineCore.KeyExtractor().Args().Arg(0).Ptr());
            rowArgs.push_back(combineCore.InitHandler().Args().Arg(1).Ptr());
            rowArgs.push_back(combineCore.UpdateHandler().Args().Arg(1).Ptr());
        }
        else {
            rowArgs.push_back(consumer.Cast<TCoFlatMapBase>().Lambda().Args().Arg(0).Ptr());
        }

        for (const auto& rowArg : rowArgs) {
            auto rowArgParentsIt = parentsMap->find(rowArg.Get());
            YQL_ENSURE(rowArgParentsIt != parentsMap->end());
            for (const auto& memberNode: rowArgParentsIt->second) {
                if (auto column = getMemberColumn(memberNode, rowArg.Get())) {
                    if (column != YqlOthersColumnName) {
                        continue;
                    }
                } else {
                    return node;
                }

                if (pass > 0) {
                    otherMembers.insert(memberNode);
                }

                auto justArgIt = parentsMap->find(memberNode);
                YQL_ENSURE(justArgIt != parentsMap->end());
                for (const auto& justNode: justArgIt->second) {
                    if (!justNode->IsCallable("Just") || justNode->Child(0) != memberNode) {
                        if (pass > 0) {
                            continue;
                        }
                        return node;
                    }

                    auto weakIt = parentsMap->find(justNode);
                    YQL_ENSURE(weakIt != parentsMap->end());
                    for (const auto& weakNode : weakIt->second) {
                        if (!weakNode->IsCallable("TryWeakMemberFromDict") || weakNode->Child(0) != justNode) {
                            if (pass > 0) {
                                continue;
                            }
                            return node;
                        }

                        weaks.insert(std::make_pair(weakNode, rowArg));
                        if (pass == 0) {
                            finalWeakColumns.insert(weakNode->Child(3)->Content());
                        }
                    }
                }
            }
        }
    }

    TSet<TStringBuf> filteredFields;
    for (auto item: inputType->GetItems()) {
        if (item->GetName() != YqlOthersColumnName) {
            filteredFields.insert(item->GetName());
        }
    }

    TSet<TStringBuf> weakFields;
    TExprNode::TPtr newLambda;
    TOptimizeExprSettings settings(State_->Types);
    settings.VisitChanges = true;
    auto status = OptimizeExpr(mapper.Ptr(), newLambda, [&](const TExprNode::TPtr& input, TExprContext& ctx) {
        if (auto maybeTryWeak = TMaybeNode<TCoTryWeakMemberFromDict>(input)) {
            auto it = weaks.find(input.Get());
            if (it == weaks.end()) {
                return input;
            }
            auto tryWeak = maybeTryWeak.Cast();
            auto weakName = tryWeak.Name().Value();
            if (!filteredFields.contains(weakName)) {
                weakFields.insert(weakName);
            }

            TExprBase member = Build<TCoMember>(ctx, input->Pos())
                .Struct(it->second)
                .Name(tryWeak.Name())
                .Done();

            const TStructExprType* structType = it->second->GetTypeAnn()->Cast<TStructExprType>();
            auto structMemberPos = structType->FindItem(weakName);
            bool notYsonMember = false;
            if (structMemberPos) {
                auto structMemberType = structType->GetItems()[*structMemberPos]->GetItemType();
                if (structMemberType->GetKind() == ETypeAnnotationKind::Optional) {
                    structMemberType = structMemberType->Cast<TOptionalExprType>()->GetItemType();
                }
                if (structMemberType->GetKind() != ETypeAnnotationKind::Data) {
                    notYsonMember = true;
                } else {
                    auto structMemberSlot = structMemberType->Cast<TDataExprType>()->GetSlot();
                    if (structMemberSlot != EDataSlot::Yson && structMemberSlot != EDataSlot::String) {
                        notYsonMember = true;
                    }
                }
            }

            TExprBase fromYson = (notYsonMember || tryWeak.Type().Value() == "Yson")
                ? member
                : Build<TCoFromYsonSimpleType>(ctx, input->Pos())
                    .Value(member)
                    .Type(tryWeak.Type())
                    .Done();

            if (tryWeak.RestDict().Maybe<TCoNothing>()) {
                return fromYson.Ptr();
            }

            return Build<TCoCoalesce>(ctx, input->Pos())
                .Predicate(fromYson)
                .Value<TCoTryWeakMemberFromDict>()
                    .InitFrom(tryWeak)
                    .OtherDict<TCoNull>()
                    .Build()
                .Build()
                .Done().Ptr();
        }

        if (stack.size() > 1) {
            if (auto maybeStruct = TMaybeNode<TCoAsStruct>(input)) {
                auto asStruct = maybeStruct.Cast();
                for (size_t i: xrange(asStruct.ArgCount())) {
                    auto list = asStruct.Arg(i);
                    if (list.Item(0).Cast<TCoAtom>().Value() == YqlOthersColumnName && otherMembers.contains(list.Item(1).Raw())) {
                        // rebuild AsStruct without other fields
                        auto row = list.Item(1).Cast<TCoMember>().Struct();
                        auto newChildren = input->ChildrenList();
                        newChildren.erase(newChildren.begin() + i);
                        // and with weak fields for combiner core
                        for (ui32 j = 0; j < newChildren.size(); ++j) {
                            finalWeakColumns.erase(newChildren[j]->Child(0)->Content());
                        }

                        for (auto column : finalWeakColumns) {
                            newChildren.push_back(Build<TExprList>(ctx, input->Pos())
                                .Add<TCoAtom>()
                                    .Value(column)
                                .Build()
                                .Add<TCoMember>()
                                    .Struct(row)
                                    .Name()
                                        .Value(column)
                                    .Build()
                                .Build()
                                .Done().Ptr());
                        }

                        return ctx.ChangeChildren(*input, std::move(newChildren));
                    }
                }

                return input;
            }
        }

        return input;
    }, ctx, settings);

    if (status.Level == IGraphTransformer::TStatus::Error) {
        return nullptr;
    }

    // refresh CombineCore lambdas
    auto child = newLambda->Child(TCoLambda::idx_Body);
    if (TCoCombineCore::Match(child)) {
        child->ChildRef(TCoCombineCore::idx_KeyExtractor) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_KeyExtractor));
        child->ChildRef(TCoCombineCore::idx_InitHandler) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_InitHandler));
        child->ChildRef(TCoCombineCore::idx_UpdateHandler) = ctx.DeepCopyLambda(*child->Child(TCoCombineCore::idx_UpdateHandler));
    }

    // refresh flatmaps too
    if (stack.size() > 1) {
        child = child->Child(TCoInputBase::idx_Input);
        for (size_t i = 1; i < stack.size(); ++i) {
            child->ChildRef(TCoFlatMapBase::idx_Lambda) = ctx.DeepCopyLambda(*child->Child(TCoFlatMapBase::idx_Lambda));
            child = child->Child(TCoInputBase::idx_Input);
        }
    }

    auto columnsBuilder = Build<TExprList>(ctx, op.Pos());
    for (auto& field: filteredFields) {
        columnsBuilder
            .Add<TCoAtom>()
                .Value(field)
            .Build();
    }
    for (auto& field: weakFields) {
        columnsBuilder
            .Add<TCoAtomList>()
                .Add()
                    .Value(field)
                .Build()
                .Add()
                    .Value("weak")
                .Build()
            .Build();
    }

    auto res = ctx.ChangeChild(op.Ref(), TYtWithUserJobsOpBase::idx_Input,
        Build<TYtSectionList>(ctx, op.Input().Pos())
            .Add(UpdateInputFields(section, columnsBuilder.Done(), ctx))
            .Done().Ptr());

    res = ctx.ChangeChild(*res, TYtWithUserJobsOpBase::idx_Settings, NYql::AddSetting(op.Settings().Ref(), EYtSettingType::WeakFields, {}, ctx));
    res = ctx.ChangeChild(*res, mapperIdx, std::move(newLambda));

    return TExprBase(res);
}

}  // namespace NYql
