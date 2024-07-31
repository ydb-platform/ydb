#include "type_ann_wide.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {
namespace NTypeAnnImpl {

namespace {

const TMultiExprType* GetWideLambdaOutputType(const TExprNode& lambda, TExprContext& ctx) {
    TTypeAnnotationNode::TListType types;
    types.reserve(lambda.ChildrenSize() - 1U);
    for (ui32 i = 1U; i < lambda.ChildrenSize(); ++i) {
        types.emplace_back(lambda.Child(i)->GetTypeAnn());
    }
    return ctx.MakeType<TMultiExprType>(types);
}

}

IGraphTransformer::TStatus ExpandMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();

    auto& lambda = input->TailRef();

    const auto status = ConvertToLambda(lambda, ctx.Expr, 1U);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, {itemType}, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(GetWideLambdaOutputType(*lambda, ctx.Expr)));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->TailRef();
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (const auto width = lambda->Head().ChildrenSize(); lambda->ChildrenSize() == width + 1U) {
        bool pass = true;
        for (auto i = 0U; pass && i < width; ++i)
            pass = lambda->Head().Child(i) == lambda->Child(i + 1U);

        if (pass) {
            output = input->HeadPtr();
            return IGraphTransformer::TStatus::Repeat;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(GetWideLambdaOutputType(*lambda, ctx.Expr)));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideChain1MapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& initLambda = input->ChildRef(1U);

    if (const auto status = ConvertToLambda(initLambda, ctx.Expr, multiType->GetSize()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(initLambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!initLambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    auto argTypes = multiType->GetItems();

    const auto initType = GetWideLambdaOutputType(*initLambda, ctx.Expr);
    const auto& stateTypes = initType->GetItems();
    argTypes.insert(argTypes.cend(), stateTypes.cbegin(), stateTypes.cend());

    auto& updateLambda = input->TailRef();
    if (const auto status = ConvertToLambda(updateLambda, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(updateLambda, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!updateLambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (const auto outputType = GetWideLambdaOutputType(*updateLambda, ctx.Expr); !IsSameAnnotation(*initType, *outputType)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch init and update handlers output types: "
            << *static_cast<const TTypeAnnotationNode*>(initType) << " and " << *static_cast<const TTypeAnnotationNode*>(outputType)));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(initType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideFilterWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->ChildrenSize() > 2U) {
        const auto expectedType = ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64);
        const auto convertStatus = TryConvertTo(input->TailRef(), *expectedType, ctx.Expr);
        if (convertStatus.Level == IGraphTransformer::TStatus::Error) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Tail().Pos()),
                TStringBuilder() << "Mismatch 'limit' type. Expected Uint64, got: " << *input->Tail().GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->ChildRef(1U);
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideWhileWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->TailRef();
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureSpecificDataType(*lambda, EDataSlot::Bool, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideCondense1Wrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 4U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& initLambda = input->ChildRef(1U);
    auto& switchLambda = input->ChildRef(2U);
    auto& updateLambda = input->ChildRef(3U);

    if (const auto status = ConvertToLambda(initLambda, ctx.Expr, multiType->GetSize()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(initLambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!initLambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const auto initType = GetWideLambdaOutputType(*initLambda, ctx.Expr);
    const auto& stateTypes = initType->GetItems();
    for (ui32 i = 1U; i < initLambda->ChildrenSize(); ++i) {
        if (!EnsureComputableType(initLambda->Child(i)->Pos(), *stateTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto argTypes = multiType->GetItems();
    argTypes.insert(argTypes.cend(), stateTypes.cbegin(), stateTypes.cend());

    if (const auto status = ConvertToLambda(switchLambda, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(switchLambda, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!switchLambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    bool isOptional;
    const TDataExprType* dataType;
    if (!EnsureDataOrOptionalOfData(switchLambda->Pos(), switchLambda->GetTypeAnn(), isOptional, dataType, ctx.Expr) || !EnsureSpecificDataType(switchLambda->Pos(), *dataType, EDataSlot::Bool, ctx.Expr)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(switchLambda->Pos()), TStringBuilder() << "Switch lambda returns " << *switchLambda->GetTypeAnn() << " instead of boolean."));
        return IGraphTransformer::TStatus::Error;
    }

    if (const auto status = ConvertToLambda(updateLambda, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(updateLambda, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!updateLambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (const auto outputType = GetWideLambdaOutputType(*updateLambda, ctx.Expr); !IsSameAnnotation(*initType, *outputType)) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), TStringBuilder() << "Mismatch init and update handlers output types: "
            << *static_cast<const TTypeAnnotationNode*>(initType) << " and " << *static_cast<const TTypeAnnotationNode*>(outputType)));
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(initType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideCombinerWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 6U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    if (!EnsureAtom(*input->Child(1U), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (const auto& limit = input->Child(1U)->Content(); !limit.empty()) {
        i64 memLimit = 0LL;
        if (!TryFromString(limit, memLimit)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1U)->Pos()), TStringBuilder() <<
                "Bad memLimit value: " << limit));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto& keyExtractor = input->ChildRef(2U);
    auto& initHandler = input->ChildRef(3U);
    auto& updateHandler = input->ChildRef(4U);
    auto& finishHandler = input->ChildRef(5U);

    if (const auto status = ConvertToLambda(keyExtractor, ctx.Expr, multiType->GetSize()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(keyExtractor, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!keyExtractor->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TTypeAnnotationNode::TListType keyTypes;
    keyTypes.reserve(keyExtractor->ChildrenSize() - 1U);
    for (ui32 i = 1U; i < keyExtractor->ChildrenSize(); ++i) {
        const auto child = keyExtractor->Child(i);
        keyTypes.emplace_back(child->GetTypeAnn());
        if (!EnsureHashableKey(child->Pos(), keyTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureEquatableKey(child->Pos(), keyTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto argTypes = multiType->GetItems();
    argTypes.insert(argTypes.cbegin(), keyTypes.cbegin(), keyTypes.cend());

    if (const auto status = ConvertToLambda(initHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(initHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!initHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TTypeAnnotationNode::TListType stateTypes;
    stateTypes.reserve(initHandler->ChildrenSize() - 1U);
    for (ui32 i = 1U; i < initHandler->ChildrenSize(); ++i) {
        const auto child = initHandler->Child(i);
        stateTypes.emplace_back(child->GetTypeAnn());
        if (!EnsureComputableType(child->Pos(), *stateTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    argTypes.insert(argTypes.cend(), stateTypes.cbegin(), stateTypes.cend());

    if (const auto status = ConvertToLambda(updateHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(updateHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!updateHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    for (auto i = updateHandler->ChildrenSize() - 1U; i;) {
        const auto child = updateHandler->Child(i);
        if (!IsSameAnnotation(*stateTypes[--i], *child->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "State type changed in update from "
                << *stateTypes[i] << " on " << *child->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
    }

    argTypes.erase(argTypes.cbegin() + keyTypes.size(), argTypes.cbegin() + keyTypes.size() + multiType->GetSize());

    if (const auto status = ConvertToLambda(finishHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(finishHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!finishHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(GetWideLambdaOutputType(*finishHandler, ctx.Expr)));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideCombinerWithSpillingWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 7U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    if (!EnsureAtom(*input->Child(1U), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (const auto& limit = input->Child(1U)->Content(); !limit.empty()) {
        i64 memLimit = 0LL;
        if (!TryFromString(limit, memLimit)) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Child(1U)->Pos()), TStringBuilder() <<
                "Bad memLimit value: " << limit));
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto& keyExtractor = input->ChildRef(2U);
    auto& initHandler = input->ChildRef(3U);
    auto& updateHandler = input->ChildRef(4U);
    auto& finishHandler = input->ChildRef(5U);
    auto& serdeHandler = input->ChildRef(6U);

    if (const auto status = ConvertToLambda(keyExtractor, ctx.Expr, multiType->GetSize()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(keyExtractor, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!keyExtractor->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TTypeAnnotationNode::TListType keyTypes;
    keyTypes.reserve(keyExtractor->ChildrenSize() - 1U);
    for (ui32 i = 1U; i < keyExtractor->ChildrenSize(); ++i) {
        const auto child = keyExtractor->Child(i);
        keyTypes.emplace_back(child->GetTypeAnn());
        if (!EnsureHashableKey(child->Pos(), keyTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (!EnsureEquatableKey(child->Pos(), keyTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto argTypes = multiType->GetItems();
    argTypes.insert(argTypes.cbegin(), keyTypes.cbegin(), keyTypes.cend());

    if (const auto status = ConvertToLambda(initHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(initHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!initHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TTypeAnnotationNode::TListType stateTypes;
    stateTypes.reserve(initHandler->ChildrenSize() - 1U);
    for (ui32 i = 1U; i < initHandler->ChildrenSize(); ++i) {
        const auto child = initHandler->Child(i);
        stateTypes.emplace_back(child->GetTypeAnn());
        if (!EnsureComputableType(child->Pos(), *stateTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    argTypes.insert(argTypes.cend(), stateTypes.cbegin(), stateTypes.cend());

    if (const auto status = ConvertToLambda(updateHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(updateHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!updateHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    for (auto i = updateHandler->ChildrenSize() - 1U; i;) {
        const auto child = updateHandler->Child(i);
        if (!IsSameAnnotation(*stateTypes[--i], *child->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(child->Pos()), TStringBuilder() << "State type changed in update from "
                << *stateTypes[i] << " on " << *child->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
    }

    argTypes.erase(argTypes.cbegin() + keyTypes.size(), argTypes.cbegin() + keyTypes.size() + multiType->GetSize());

    if (const auto status = ConvertToLambda(finishHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(finishHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!finishHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (const auto status = ConvertToLambda(serdeHandler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(serdeHandler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!serdeHandler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    auto retType = GetWideLambdaOutputType(*finishHandler, ctx.Expr);

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(retType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideChopperWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 4U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto itemType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();

    if (!EnsureMultiType(input->Head().Pos(), *itemType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = itemType->Cast<TMultiExprType>();

    auto& keyExtractor = input->ChildRef(1U);
    auto& groupSwitch = input->ChildRef(2U);
    auto& handler = input->TailRef();

    if (const auto status = ConvertToLambda(keyExtractor, ctx.Expr, multiType->GetSize()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(keyExtractor, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!keyExtractor->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    TTypeAnnotationNode::TListType keyTypes;
    keyTypes.reserve(keyExtractor->ChildrenSize() - 1U);
    for (ui32 i = 1U; i < keyExtractor->ChildrenSize(); ++i) {
        keyTypes.emplace_back(keyExtractor->Child(i)->GetTypeAnn());
        if (!EnsureComputableType(keyExtractor->Child(i)->Pos(), *keyTypes.back(), ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
    }

    auto argTypes = multiType->GetItems();
    argTypes.insert(argTypes.cbegin(), keyTypes.cbegin(), keyTypes.cend());

    if (const auto status = ConvertToLambda(groupSwitch, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(groupSwitch, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!groupSwitch->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureSpecificDataType(*groupSwitch, EDataSlot::Bool, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    argTypes.resize(keyTypes.size());
    argTypes.emplace_back(input->Head().GetTypeAnn());

    if (const auto status = ConvertToLambda(handler, ctx.Expr, argTypes.size()); status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(handler, argTypes, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!handler->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureFlowType(*handler, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMultiType(handler->Pos(), *handler->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(handler->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus NarrowMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->TailRef();
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const auto outputItemType = lambda->GetTypeAnn();
    if (!EnsureComputableType(lambda->Pos(), *outputItemType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus NarrowMultiMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->TailRef();
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (lambda->ChildrenSize() < 3U) {
        output = ctx.Expr.RenameNode(*input, "NarrowMap");
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const auto outputItemType = lambda->Tail().GetTypeAnn();
    if (!EnsureComputableType(lambda->Pos(), *outputItemType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    for (ui32 i = lambda->ChildrenSize() - 2U; i > 0U; --i) {
        if (!IsSameAnnotation(*outputItemType, *lambda->Child(i)->GetTypeAnn())) {
            ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), TStringBuilder() << "Mismatch of multi map lambda return types: "
                << *outputItemType << " and " << *lambda->Child(i)->GetTypeAnn()));
            return IGraphTransformer::TStatus::Error;
        }
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus NarrowFlatMapWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto multiType = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>();

    auto& lambda = input->TailRef();
    const auto status = ConvertToLambda(lambda, ctx.Expr, multiType->GetSize());
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, multiType->GetItems(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return IGraphTransformer::TStatus::Repeat;
    }

    const TTypeAnnotationNode* outputItemType = nullptr;
    if (!EnsureNewSeqType<true>(*lambda, ctx.Expr, &outputItemType)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureComputableType(lambda->Pos(), *outputItemType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputItemType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideTopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    if (!EnsureArgsCount(*input, 3U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureSpecificDataType(*input->Child(1U), EDataSlot::Uint64, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto& types = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    if (!ValidateWideTopKeys(input->Tail(), types, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    output = input;
    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus WideSortWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TContext& ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 2U, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureWideFlowType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto& types = input->Head().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetItems();
    if (!ValidateWideTopKeys(input->Tail(), types, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

bool ValidateWideTopKeys(TExprNode& keys, const TTypeAnnotationNode::TListType& types, TExprContext& ctx) {
    if (!(EnsureTupleMinSize(keys, 1U, ctx) && EnsureTupleMaxSize(keys, types.size(), ctx))) {
        return false;
    }

    std::unordered_set<ui32> indexes(keys.ChildrenSize());
    for (const auto& item : keys.Children()) {
        if (!EnsureTupleSize(*item, 2U, ctx)) {
            return false;
        }

        if (!EnsureAtom(item->Head(), ctx)) {
            return false;
        }

        if (ui32 index; TryFromString(item->Head().Content(), index)) {
            if (index >= types.size()) {
                ctx.AddError(TIssue(ctx.GetPosition(item->Head().Pos()),
                    TStringBuilder() << "Index too large: " << index));
                return false;
            } else if (!indexes.emplace(index).second) {
                ctx.AddError(TIssue(ctx.GetPosition(item->Head().Pos()),
                    TStringBuilder() << "Duplicate index: " << index));
                return false;
            }

            if (!EnsureComparableType(item->Pos(), *types[index], ctx)) {
                return false;
            }
        } else {
            ctx.AddError(TIssue(ctx.GetPosition(item->Head().Pos()),
                TStringBuilder() << "Invalid index value: " << item->Head().Content()));
            return false;
        }

        if (!EnsureSpecificDataType(item->Tail(), EDataSlot::Bool, ctx)) {
            return false;
        }
    }

    return true;
}

} // namespace NTypeAnnImpl
}
