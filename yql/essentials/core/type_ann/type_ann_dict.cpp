#include "type_ann_dict.h"
#include "type_ann_types.h"


namespace NYql::NTypeAnnImpl {

constexpr TStringBuf MutDictResourcePrefix = "_MutDict_";

namespace {

const TTypeAnnotationNode* ConvertDictTypeToMutDictType(const TDictExprType* dictType, TExprContext& ctx) {
    auto typeStr = FormatType(dictType);
    ctx.ParseTypeCache.emplace(typeStr, dictType);
    return ctx.MakeType<TLinearExprType>(
        ctx.MakeType<TResourceExprType>(MutDictResourcePrefix + typeStr));
}

bool ParseMutDictType(TPositionHandle pos, const TTypeAnnotationNode* type,
    const TDictExprType*& dictType, TExprContext& ctx, TTypeAnnotationContext& typeCtx) {
    bool isDynamic;
    auto innerType = GetLinearItemType(*type, isDynamic);
    auto resType = innerType->UserCast<TResourceExprType>(ctx.GetPosition(pos), ctx);
    if (!resType) {
        return false;
    }

    TStringBuf tag = resType->GetTag();
    if (!tag.SkipPrefix(MutDictResourcePrefix)) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Incorrect resource tag: " << tag));
        return false;
    }

    auto parsedType = ParseTypeCached(TString(tag), ctx, typeCtx);
    if (!parsedType) {
        return false;
    }

    dictType = parsedType->UserCast<TDictExprType>(ctx.GetPosition(pos), ctx);
    return dictType != nullptr;
}

}

const TDictExprType* GetCachedMutDictType(const TStringBuf& resourceTag, const TExprContext& ctx) {
    TStringBuf tag = resourceTag;
    YQL_ENSURE(tag.SkipPrefix(MutDictResourcePrefix));
    auto it = ctx.ParseTypeCache.find(TString(tag));
    YQL_ENSURE(it != ctx.ParseTypeCache.cend());
    return it->second->Cast<TDictExprType>();
}

IGraphTransformer::TStatus MutDictCreateWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (auto status = EnsureTypeRewrite(input->HeadRef(), ctx.Expr); status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    auto type = input->Head().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
    if (type->GetKind() != ETypeAnnotationKind::Dict) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder() << "Expected dict type, but got: "
            << *type));
        return IGraphTransformer::TStatus::Error;
    }

    auto dictType = type->Cast<TDictExprType>();
    auto status = EnsureDependsOnTailAndRewrite(input, output, ctx.Expr, ctx.Types, 1);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    input->SetTypeAnn(ConvertDictTypeToMutDictType(dictType, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus ToMutDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureMinArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureDictType(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto dictType = input->Head().GetTypeAnn()->Cast<TDictExprType>();
    auto status = EnsureDependsOnTailAndRewrite(input, output, ctx.Expr, ctx.Types, 1);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    input->SetTypeAnn(ConvertDictTypeToMutDictType(dictType, ctx.Expr));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus FromMutDictWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (input->Child(0)->GetTypeAnn() && input->Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(input->Child(0)->GetTypeAnn());
        return IGraphTransformer::TStatus::Ok;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    input->SetTypeAnn(dictType);
    return IGraphTransformer::TStatus::Ok;
}

template <bool WithPayload>
IGraphTransformer::TStatus MutDictBlindOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 2 + (WithPayload ? 1 : 0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto status = TryConvertTo(input->ChildRef(1), *dictType->GetKeyType(), ctx.Expr, ctx.Types);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if constexpr (WithPayload) {
        status = TryConvertTo(input->ChildRef(2), *dictType->GetPayloadType(), ctx.Expr, ctx.Types);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    input->SetTypeAnn(input->Child(0)->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictPopWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto status = TryConvertTo(input->ChildRef(1), *dictType->GetKeyType(), ctx.Expr, ctx.Types);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        ctx.Expr.MakeType<TOptionalExprType>(dictType->GetPayloadType())});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictContainsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 2, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto status = TryConvertTo(input->ChildRef(1), *dictType->GetKeyType(), ctx.Expr, ctx.Types);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        ctx.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool)});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictHasItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        ctx.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Bool)});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictLengthWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        ctx.Expr.MakeType<TDataExprType>(NUdf::EDataSlot::Uint64)});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictItemsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto list = ctx.Expr.MakeType<TListExprType>(
        ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            dictType->GetKeyType(),
            dictType->GetPayloadType()})
    );

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        list});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictKeysWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto list = ctx.Expr.MakeType<TListExprType>(
        dictType->GetKeyType()
    );

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        list});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MutDictPayloadsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 1, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureLinearType(*input->Child(0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const TDictExprType* dictType;
    if (!ParseMutDictType(input->Child(0)->Pos(), input->Child(0)->GetTypeAnn(), dictType, ctx.Expr, ctx.Types)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto list = ctx.Expr.MakeType<TListExprType>(
        dictType->GetPayloadType()
    );

    auto pair = ctx.Expr.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
        input->Child(0)->GetTypeAnn(),
        list});

    input->SetTypeAnn(pair);
    return IGraphTransformer::TStatus::Ok;
}

template <bool WithPayload>
IGraphTransformer::TStatus DictBlindOpWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx) {
    Y_UNUSED(output);
    if (!CheckLinearLangver(input->Pos(), ctx.Types.LangVer, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, 2 + (WithPayload ? 1 : 0), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (IsNull(input->Head())) {
        output = input->HeadPtr();
        return IGraphTransformer::TStatus::Repeat;
    }

    if (!EnsureComputable(input->Head(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    bool isOptional = false;
    auto type = input->Head().GetTypeAnn();
    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
        isOptional = true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Universal) {
        input->SetTypeAnn(type);
        return IGraphTransformer::TStatus::Ok;
    }

    if (type->GetKind() != ETypeAnnotationKind::Dict) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Head().Pos()), TStringBuilder()
            << "Expected dict or optional of dict, but got: " << *input->Head().GetTypeAnn()));
        return IGraphTransformer::TStatus::Error;
    }

    if (isOptional) {
        output = AddChildren(ctx.Expr.Builder(input->Pos())
            .Callable("Map")
                .Add(0, input->HeadPtr())
                .Lambda(1)
                    .Param("x")
                    .Callable(input->Content())
                        .Arg(0, "x"), 1U, input)
                    .Seal()
                .Seal()
            .Seal().Build();
        return IGraphTransformer::TStatus::Repeat;
    }

    auto dictType = type->Cast<TDictExprType>();
    auto status = TryConvertTo(input->ChildRef(1), *dictType->GetKeyType(), ctx.Expr, ctx.Types);
    if (status != IGraphTransformer::TStatus::Ok) {
        return status;
    }

    if constexpr (WithPayload) {
        status = TryConvertTo(input->ChildRef(2), *dictType->GetPayloadType(), ctx.Expr, ctx.Types);
        if (status != IGraphTransformer::TStatus::Ok) {
            return status;
        }
    }

    input->SetTypeAnn(input->Head().GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

template IGraphTransformer::TStatus MutDictBlindOpWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
template IGraphTransformer::TStatus MutDictBlindOpWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);

template IGraphTransformer::TStatus DictBlindOpWrapper<true>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);
template IGraphTransformer::TStatus DictBlindOpWrapper<false>(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExtContext& ctx);


} // namespace NYql::NTypeAnnImpl

