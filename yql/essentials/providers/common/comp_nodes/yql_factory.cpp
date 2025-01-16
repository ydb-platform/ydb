#include "yql_factory.h"
#include "yql_formatcode.h"
#include "yql_formattype.h"
#include "yql_formattypediff.h"
#include "yql_makecode.h"
#include "yql_maketype.h"
#include "yql_parsetypehandle.h"
#include "yql_reprcode.h"
#include "yql_serializetypehandle.h"
#include "yql_splittype.h"
#include "yql_typehandle.h"
#include "yql_typekind.h"

namespace NKikimr {
namespace NMiniKQL {

typedef IComputationNode* (*TYqlCallableComputationNodeBuilderFunc)(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex);
typedef THashMap<TString, TYqlCallableComputationNodeBuilderFunc> TYqlCallableComputationNodeBuilderFuncMap;

struct TYqlCallableComputationNodeBuilderFuncMapFiller {
    TYqlCallableComputationNodeBuilderFuncMap Map;

    TYqlCallableComputationNodeBuilderFuncMapFiller()
    {
        Map["FormatType"] = &WrapFormatType;
        Map["FormatTypeDiff"] = &WrapFormatTypeDiff;
        Map["ParseTypeHandle"] = &WrapParseTypeHandle;
        Map["SerializeTypeHandle"] = &WrapSerializeTypeHandle;
        Map["TypeHandle"] = &WrapTypeHandle;
        Map["TypeKind"] = &WrapTypeKind;
        Map["DataTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Data>;
        Map["DataTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Data>;
        Map["OptionalItemType"] = &WrapSplitType<NYql::ETypeAnnotationKind::Optional>;
        Map["OptionalTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Optional>;
        Map["ListItemType"] = &WrapSplitType<NYql::ETypeAnnotationKind::List>;
        Map["ListTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::List>;
        Map["StreamItemType"] = &WrapSplitType<NYql::ETypeAnnotationKind::Stream>;
        Map["StreamTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Stream>;
        Map["TupleTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Tuple>;
        Map["TupleTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Tuple>;
        Map["StructTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Struct>;
        Map["StructTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Struct>;
        Map["DictTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Dict>;
        Map["DictTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Dict>;
        Map["ResourceTypeTag"] = &WrapSplitType<NYql::ETypeAnnotationKind::Resource>;
        Map["ResourceTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Resource>;
        Map["TaggedTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Tagged>;
        Map["TaggedTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Tagged>;
        Map["VariantUnderlyingType"] = &WrapSplitType<NYql::ETypeAnnotationKind::Variant>;
        Map["VariantTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Variant>;
        Map["VoidTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Void>;
        Map["NullTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Null>;
        Map["EmptyListTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::EmptyList>;
        Map["EmptyDictTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::EmptyDict>;
        Map["CallableTypeComponents"] = &WrapSplitType<NYql::ETypeAnnotationKind::Callable>;
        Map["CallableTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Callable>;
        Map["PgTypeName"] = &WrapSplitType<NYql::ETypeAnnotationKind::Pg>;
        Map["PgTypeHandle"] = &WrapMakeType<NYql::ETypeAnnotationKind::Pg>;
        Map["FormatCode"] = &WrapFormatCode<false>;
        Map["FormatCodeWithPositions"] = &WrapFormatCode<true>;
        Map["SerializeCode"] = &WrapSerializeCode;
        Map["WorldCode"] = &WrapMakeCode<NYql::TExprNode::World>;
        Map["AtomCode"] = &WrapMakeCode<NYql::TExprNode::Atom>;
        Map["ListCode"] = &WrapMakeCode<NYql::TExprNode::List>;
        Map["FuncCode"] = &WrapMakeCode<NYql::TExprNode::Callable>;
        Map["LambdaCode"] = &WrapMakeCode<NYql::TExprNode::Lambda>;
        Map["ReprCode"] = &WrapReprCode;
    }
};

TComputationNodeFactory GetYqlFactory(ui32 exprCtxMutableIndex) {
    return [exprCtxMutableIndex](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        const auto& map = Singleton<TYqlCallableComputationNodeBuilderFuncMapFiller>()->Map;
        auto it = map.find(callable.GetType()->GetName());
        if (it == map.end())
            return nullptr;

        return it->second(callable, ctx, exprCtxMutableIndex);
    };
}

TComputationNodeFactory GetYqlFactory() {
    TComputationNodeFactory yqlFactory;
    return [yqlFactory]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) mutable -> IComputationNode* {
            if (!yqlFactory) {
                yqlFactory = GetYqlFactory(ctx.Mutables.CurValueIndex++);
            }
            return yqlFactory(callable, ctx);
        };
}

}
}
