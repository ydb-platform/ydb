#pragma once

#include "yql_graph_transformer.h"
#include "yql_type_annotation.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/minikql/mkql_type_ops.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <library/cpp/enumbitset/enumbitset.h>

#include <functional>

namespace NYql {

template <typename T>
T FromString(const TExprNode& node, NKikimr::NUdf::EDataSlot slot) {
    if (node.Flags() & TNodeFlags::BinaryContent) {
        if (node.Content().size() == sizeof(T)) {
            T ret = *(T*)(node.Content().data());
            if (NKikimr::NMiniKQL::IsValidValue(slot, NKikimr::NUdf::TUnboxedValuePod(ret))) {
                return ret;
            }
        }
    } else {
        if (const auto out = NKikimr::NMiniKQL::ValueFromString(slot, node.Content())) {
            return out.Get<T>();
        } else if (slot == NKikimr::NUdf::EDataSlot::Bool) {
            T value = T();
            if (node.Content() == TStringBuf("0")) {
                *(ui8*)&value = 0;
                return value;
            } else if (node.Content() == TStringBuf("1")) {
                *(ui8*)&value = 1;
                return value;
            }
        } else if (NKikimr::NUdf::GetDataTypeInfo(slot).Features &
            (NKikimr::NUdf::EDataTypeFeatures::DateType | NKikimr::NUdf::EDataTypeFeatures::TimeIntervalType)) {
            T ret;
            if (TryFromString<T>(node.Content(), ret) &&
                NKikimr::NMiniKQL::IsValidValue(slot, NKikimr::NUdf::TUnboxedValuePod(ret))) {
                return ret;
            }
        }
    }

    const auto typeName = NKikimr::NUdf::GetDataTypeInfo(slot).Name;
    ythrow TNodeException(node) << "Bad atom format for type: " << typeName << ", value: " << TString(node.Content()).Quote();
}

IGraphTransformer::TStatus CheckWholeProgramType(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);
void ClearExprTypeAnnotations(TExprNode& root);
bool AreAllNodesTypeAnnotated(const TExprNode& root);
void EnsureAllNodesTypeAnnotated(const TExprNode& root);
bool IsDataOrOptionalOfData(const TTypeAnnotationNode* typeAnnotation, bool& isOptional, const TDataExprType*& dataType);
bool IsDataOrOptionalOfData(const TTypeAnnotationNode* typeAnnotation);
bool IsPg(const TTypeAnnotationNode* typeAnnotation, const TPgExprType*& pgType);
bool IsDataOrOptionalOfDataOrPg(const TTypeAnnotationNode* typeAnnotation);
bool UpdateLambdaAllArgumentsTypes(TExprNode::TPtr& lambda, const std::vector<const TTypeAnnotationNode*>& argumentsAnnotations, TExprContext& ctx);
bool UpdateLambdaArgumentsType(const TExprNode& lambda, TExprContext& ctx);
bool EnsureArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx);
bool EnsureMinArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx);
bool EnsureMaxArgsCount(const TExprNode& node, ui32 expectedArgs, TExprContext& ctx);
bool EnsureMinMaxArgsCount(const TExprNode& node, ui32 minArgs, ui32 maxArgs, TExprContext& ctx);
bool EnsureCallableMinArgsCount(const TPositionHandle& pos, ui32 args, ui32 expectedArgs, TExprContext& ctx);
bool EnsureCallableMaxArgsCount(const TPositionHandle& pos, ui32 args, ui32 expectedArgs, TExprContext& ctx);
bool EnsureAtom(const TExprNode& node, TExprContext& ctx);
bool EnsureCallable(const TExprNode& node, TExprContext& ctx);
bool EnsureTuple(TExprNode& node, TExprContext& ctx);
bool EnsureTupleOfAtoms(TExprNode& node, TExprContext& ctx);

using TSettingNodeValidator = std::function<bool (TStringBuf name, TExprNode& setting, TExprContext& ctx)>;
bool EnsureValidSettings(TExprNode& node,
    const THashSet<TStringBuf>& supportedSettings,
    const TSettingNodeValidator& validator,
    TExprContext& ctx);
bool EnsureValidUserSchemaSetting(TExprNode& node, TExprContext& ctx);
TSettingNodeValidator RequireSingleValueSettings(const TSettingNodeValidator& validator);

bool EnsureLambda(const TExprNode& node, TExprContext& ctx);
IGraphTransformer::TStatus ConvertToLambda(TExprNode::TPtr& node, TExprContext& ctx, ui32 argumentsCount, ui32 maxArgumentsCount = Max<ui32>(),
    bool withTypes = true);
bool EnsureTupleSize(TExprNode& node, ui32 expectedSize, TExprContext& ctx);
bool EnsureTupleMinSize(TExprNode& node, ui32 minSize, TExprContext& ctx);
bool EnsureTupleMaxSize(TExprNode& node, ui32 maxSize, TExprContext& ctx);
bool EnsureTupleType(const TExprNode& node, TExprContext& ctx);
bool EnsureTupleType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureTupleTypeSize(const TExprNode& node, ui32 expectedSize, TExprContext& ctx);
bool EnsureTupleTypeSize(TPositionHandle position, const TTypeAnnotationNode* type, ui32 expectedSize, TExprContext& ctx);
bool EnsureMultiType(const TExprNode& node, TExprContext& ctx);
bool EnsureMultiType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureVariantType(const TExprNode& node, TExprContext& ctx);
bool EnsureVariantType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureDataOrPgType(const TExprNode& node, TExprContext& ctx);
bool EnsureDataOrPgType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsurePgType(const TExprNode& node, TExprContext& ctx);
bool EnsurePgType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureDataType(const TExprNode& node, TExprContext& ctx);
bool EnsureDataType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureSpecificDataType(const TExprNode& node, EDataSlot expectedDataSlot, TExprContext& ctx, bool allowOptional = false);
bool EnsureSpecificDataType(TPositionHandle position, const TTypeAnnotationNode& type, EDataSlot expectedDataSlot, TExprContext& ctx);
bool EnsureStringOrUtf8Type(const TExprNode& node, TExprContext& ctx);
bool EnsureStringOrUtf8Type(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureStructType(const TExprNode& node, TExprContext& ctx);
bool EnsureStructType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureStaticContainerType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureTypeWithStructType(const TExprNode& node, TExprContext& ctx);
bool EnsureComposable(const TExprNode& node, TExprContext& ctx);
bool EnsureComposableType(const TExprNode& node, TExprContext& ctx);
bool EnsureWorldType(const TExprNode& node, TExprContext& ctx);
bool EnsureDataSource(const TExprNode& node, TExprContext& ctx);
bool EnsureDataSink(const TExprNode& node, TExprContext& ctx);
bool EnsureDataProvider(const TExprNode& node, TExprContext& ctx);
bool EnsureSpecificDataSource(const TExprNode& node, TStringBuf expectedCategory, TExprContext& ctx);
bool EnsureSpecificDataSink(const TExprNode& node, TStringBuf expectedCategory, TExprContext& ctx);
bool EnsureListType(const TExprNode& node, TExprContext& ctx);
bool EnsureListType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureListOrEmptyType(const TExprNode& node, TExprContext& ctx);
bool EnsureListOrEmptyType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureListOfVoidType(const TExprNode& node, TExprContext& ctx);
bool EnsureListOfVoidType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureStreamType(const TExprNode& node, TExprContext& ctx);
bool EnsureStreamType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureFlowType(const TExprNode& node, TExprContext& ctx);
bool EnsureFlowType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureWideFlowType(const TExprNode& node, TExprContext& ctx);
bool EnsureWideFlowType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureWideStreamType(const TExprNode& node, TExprContext& ctx);
bool EnsureWideStreamType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool IsWideBlockType(const TTypeAnnotationNode& type);
bool IsWideSequenceBlockType(const TTypeAnnotationNode& type);
bool IsSupportedAsBlockType(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx, TTypeAnnotationContext& types, bool reportUnspported = false);
bool EnsureSupportedAsBlockType(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx, TTypeAnnotationContext& types);
bool EnsureWideBlockType(TPositionHandle position, const TTypeAnnotationNode& type, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar = true);
bool EnsureWideFlowBlockType(const TExprNode& node, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar = true);
bool EnsureWideStreamBlockType(const TExprNode& node, TTypeAnnotationNode::TListType& blockItemTypes, TExprContext& ctx, bool allowScalar = true);
bool EnsureOptionalType(const TExprNode& node, TExprContext& ctx);
bool EnsureOptionalType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureType(const TExprNode& node, TExprContext& ctx);
IGraphTransformer::TStatus EnsureTypeRewrite(TExprNode::TPtr& node, TExprContext& ctx);
IGraphTransformer::TStatus EnsureTypeOrAtomRewrite(TExprNode::TPtr& node, TExprContext& ctx);
bool EnsureTypePg(const TExprNode& node, TExprContext& ctx);
bool EnsureDryType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureDryType(const TExprNode& node, TExprContext& ctx);
bool EnsureDictType(const TExprNode& node, TExprContext& ctx);
bool EnsureDictType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureValidJsonPath(const TExprNode& node, TExprContext& ctx);

bool IsVoidType(const TExprNode& node, TExprContext& ctx);
bool EnsureVoidType(const TExprNode& node, TExprContext& ctx);
bool EnsureVoidLiteral(const TExprNode& node, TExprContext& ctx);
bool EnsureCallableType(const TExprNode& node, TExprContext& ctx);
bool EnsureCallableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureResourceType(const TExprNode& node, TExprContext& ctx);
bool EnsureTaggedType(const TExprNode& node, TExprContext& ctx);
bool EnsureTaggedType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);

bool EnsureComposableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureOneOrTupleOfDataOrOptionalOfData(const TExprNode& node, TExprContext& ctx);
bool EnsureOneOrTupleOfDataOrOptionalOfData(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureComparableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureComparableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx);
bool EnsureEquatableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureEquatableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx);
bool EnsureHashableKey(TPositionHandle position, const TTypeAnnotationNode* keyType, TExprContext& ctx);
bool EnsureDataOrOptionalOfData(const TExprNode& node, bool& isOptional, const TDataExprType*& dataType, TExprContext& ctx);
bool EnsureDataOrOptionalOfData(TPositionHandle position, const TTypeAnnotationNode* type, bool& isOptional, const TDataExprType*& dataType, TExprContext& ctx);
bool EnsurePersistable(const TExprNode& node, TExprContext& ctx);
bool EnsurePersistableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureComputable(const TExprNode& node, TExprContext& ctx);
bool EnsureComputableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureInspectable(const TExprNode& node, TExprContext& ctx);
bool EnsureInspectableType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureListOrOptionalType(const TExprNode& node, TExprContext& ctx);
bool EnsureListOrOptionalListType(const TExprNode& node, TExprContext& ctx);
bool EnsureStructOrOptionalStructType(const TExprNode& node, bool& isOptional, const TStructExprType*& structType, TExprContext& ctx);
bool EnsureStructOrOptionalStructType(TPositionHandle position, const TTypeAnnotationNode& type, bool& isOptional, const TStructExprType*& structType, TExprContext& ctx);
bool EnsureSeqType(const TExprNode& node, TExprContext& ctx, bool* isStream = nullptr);
bool EnsureSeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx, bool* isStream = nullptr);
bool EnsureSeqOrOptionalType(const TExprNode& node, TExprContext& ctx);
bool EnsureSeqOrOptionalType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
template <bool WithOptional, bool WithList = true, bool WithStream = true>
bool EnsureNewSeqType(const TExprNode& node, TExprContext& ctx, const TTypeAnnotationNode** itemType = nullptr);
template <bool WithOptional, bool WithList = true, bool WithStream = true>
bool EnsureNewSeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx, const TTypeAnnotationNode** itemType = nullptr);
bool EnsureAnySeqType(const TExprNode& node, TExprContext& ctx);
bool EnsureAnySeqType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureDependsOn(const TExprNode& node, TExprContext& ctx);
bool EnsureDependsOnTail(const TExprNode& node, TExprContext& ctx, unsigned requiredArgumentCount, unsigned requiredDependsOnCount = 0);

const TTypeAnnotationNode* MakeTypeHandleResourceType(TExprContext& ctx);
bool EnsureTypeHandleResourceType(const TExprNode& node, TExprContext& ctx);
const TTypeAnnotationNode* MakeCodeResourceType(TExprContext& ctx);
bool EnsureCodeResourceType(const TExprNode& node, TExprContext& ctx);

const TTypeAnnotationNode* MakeSequenceType(ETypeAnnotationKind sequenceKind, const TTypeAnnotationNode& itemType, TExprContext& ctx);

template <bool Strong>
NUdf::TCastResultOptions CastResult(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target);

enum class ECompareOptions {
    Null = -1,
    Uncomparable = 0,
    Comparable = 1,
    Optional = 2
};

template <bool Equality>
ECompareOptions CanCompare(const TTypeAnnotationNode* source, const TTypeAnnotationNode* target);

const TTypeAnnotationNode* DryType(const TTypeAnnotationNode* type, TExprContext& ctx);
const TTypeAnnotationNode* DryType(const TTypeAnnotationNode* type, bool& hasOptional, TExprContext& ctx);

// Key type for left or right join.
const TTypeAnnotationNode* JoinDryKeyType(bool outer, const TTypeAnnotationNode* primary, const TTypeAnnotationNode* secondary, TExprContext& ctx);
const TTypeAnnotationNode* JoinDryKeyType(const TTypeAnnotationNode* primary, const TTypeAnnotationNode* secondary, bool& hasOptional, TExprContext& ctx);
// Key type for inner or full join.
const TTypeAnnotationNode* JoinCommonDryKeyType(TPositionHandle position, bool outer, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx);

template <bool Strict, bool Silent = false> // Strict + DryType before - common type for join key.
const TTypeAnnotationNode* CommonType(TPositionHandle position, const TTypeAnnotationNode* one, const TTypeAnnotationNode* two, TExprContext& ctx, bool warn = false);

const TTypeAnnotationNode* CommonType(TPositionHandle position, const TTypeAnnotationNode::TSpanType& types, TExprContext& ctx, bool warn = false);
const TTypeAnnotationNode* CommonTypeForChildren(const TExprNode& node, TExprContext& ctx, bool warn = false);

size_t GetOptionalLevel(const TTypeAnnotationNode* type);

namespace NConvertFlags {

enum EFlags {
   DisableTruncation,
   AllowUnsafeConvert,
   Last
};

using TConvertFlags = TEnumBitSet<EFlags, DisableTruncation, Last>;

}

using NConvertFlags::TConvertFlags;

IGraphTransformer::TStatus TryConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& sourceType,
    const TTypeAnnotationNode& expectedType, TExprContext& ctx, TConvertFlags flags = {});
IGraphTransformer::TStatus TryConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& expectedType,
    TExprContext& ctx, TConvertFlags flags = {});
IGraphTransformer::TStatus TrySilentConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& expectedType, TExprContext& ctx,
    TConvertFlags flags = {});
IGraphTransformer::TStatus TrySilentConvertTo(TExprNode::TPtr& node, const TTypeAnnotationNode& sourceType,
    const TTypeAnnotationNode& expectedType, TExprContext& ctx, TConvertFlags flags = {});
TMaybe<EDataSlot> GetSuperType(EDataSlot dataSlot1, EDataSlot dataSlot2, bool warn = false, TExprContext* ctx = nullptr, TPositionHandle* pos = nullptr);
IGraphTransformer::TStatus SilentInferCommonType(TExprNode::TPtr& node1, TExprNode::TPtr& node2, TExprContext& ctx,
    const TTypeAnnotationNode*& commonType, TConvertFlags flags = {});
IGraphTransformer::TStatus SilentInferCommonType(TExprNode::TPtr& node1, const TTypeAnnotationNode& type1,
    TExprNode::TPtr& node2, const TTypeAnnotationNode& type2, TExprContext& ctx,
    const TTypeAnnotationNode*& commonType, TConvertFlags flags = {});
IGraphTransformer::TStatus ConvertChildrenToType(const TExprNode::TPtr& input,const TTypeAnnotationNode* targetType, TExprContext& ctx);

bool IsSqlInCollectionItemsNullable(const NNodes::TCoSqlIn& node);

bool IsDataTypeNumeric(EDataSlot dataSlot);
bool IsDataTypeFloat(EDataSlot dataSlot);
bool IsDataTypeIntegral(EDataSlot dataSlot);
bool IsDataTypeSigned(EDataSlot dataSlot);
bool IsDataTypeUnsigned(EDataSlot dataSlot);
bool IsDataTypeDate(EDataSlot dataSlot);
bool IsDataTypeBigDate(EDataSlot dataSlot);
bool IsDataTypeTzDate(EDataSlot dataSlot);
EDataSlot WithTzDate(EDataSlot dataSlot);
EDataSlot WithoutTzDate(EDataSlot dataSlot);
EDataSlot MakeSigned(EDataSlot dataSlot);
EDataSlot MakeUnsigned(EDataSlot dataSlot);
bool IsDataTypeDecimal(EDataSlot dataSlot);
bool IsDataTypeDateOrTzDateOrInterval(EDataSlot dataSlot);
bool IsDataTypeDateOrTzDate(EDataSlot dataSlot);
bool IsDataTypeInterval(EDataSlot dataSlot);
ui8 GetDecimalWidthOfIntegral(EDataSlot dataSlot);
TMaybe<ui32> GetDataFixedSize(const TTypeAnnotationNode* typeAnnotation);
bool IsFixedSizeData(const TTypeAnnotationNode* typeAnnotation);
ui32 GetNumericDataTypeLevel(EDataSlot dataSlot);
EDataSlot GetNumericDataTypeByLevel(ui32 level);
ui32 GetDateTypeLevel(EDataSlot dataSlot);
EDataSlot GetDateTypeByLevel(ui32 level);
bool IsPureIsolatedLambda(const TExprNode& lambdaBody, TSyncMap* syncList = nullptr);
TString GetIntegralAtomValue(ui64 value, bool hasSign);
bool AllowIntegralConversion(NNodes::TCoIntegralCtor node, bool negate, EDataSlot toType,
    TString* atomValue = nullptr);
void ExtractIntegralValue(const TExprNode& constructor, bool negate, bool& hasSign, bool& isSigned, ui64& value);
bool IsDataTypeString(EDataSlot dataSlot);
bool EnsureComparableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx);
bool EnsureEquatableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx);
bool EnsureHashableDataType(TPositionHandle position, EDataSlot dataSlot, TExprContext& ctx);
TMaybe<TIssue> NormalizeName(TPosition position, TString& name);
TString NormalizeName(const TStringBuf& name);

bool HasError(const TTypeAnnotationNode* type, TExprContext& ctx);
bool HasError(const TTypeAnnotationNode* type, TIssue& errIssue);
bool IsNull(const TExprNode& node);
bool IsNull(const TTypeAnnotationNode& type);
bool IsEmptyList(const TExprNode& node);
bool IsEmptyList(const TTypeAnnotationNode& type);
bool IsInstantEqual(const TTypeAnnotationNode& type);
bool IsFlowOrStream(const TTypeAnnotationNode& type);
bool IsFlowOrStream(const TExprNode& node);

bool IsBoolLike(const TTypeAnnotationNode& type);
bool IsBoolLike(const TExprNode& node);

TString GetTypeDiff(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right);
TString GetTypePrettyDiff(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right);
TExprNode::TPtr ExpandType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);

bool IsSystemMember(const TStringBuf& memberName);

template<bool Deduplicte = true, ui8 OrListsOfAtomsDepth = 0U>
IGraphTransformer::TStatus NormalizeTupleOfAtoms(const TExprNode::TPtr& input, ui32 index, TExprNode::TPtr& output, TExprContext& ctx);

IGraphTransformer::TStatus NormalizeKeyValueTuples(const TExprNode::TPtr& input, ui32 startIndex, TExprNode::TPtr& output,
    TExprContext& ctx, bool deduplicate = false);

std::optional<ui32> GetFieldPosition(const TMultiExprType& tupleType, const TStringBuf& field);
std::optional<ui32> GetFieldPosition(const TTupleExprType& tupleType, const TStringBuf& field);
std::optional<ui32> GetFieldPosition(const TStructExprType& structType, const TStringBuf& field);

bool ExtractPgType(const TTypeAnnotationNode* type, ui32& pgType, bool& convertToPg, TPositionHandle pos, TExprContext& ctx);
bool HasContextFuncs(const TExprNode& input);
IGraphTransformer::TStatus TryConvertToPgOp(TStringBuf op, const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

bool EnsureBlockOrScalarType(const TExprNode& node, TExprContext& ctx);
bool EnsureBlockOrScalarType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
bool EnsureScalarType(const TExprNode& node, TExprContext& ctx);
bool EnsureScalarType(TPositionHandle position, const TTypeAnnotationNode& type, TExprContext& ctx);
const TTypeAnnotationNode* GetBlockItemType(const TTypeAnnotationNode& type, bool& isScalar);

const TTypeAnnotationNode* AggApplySerializedStateType(const TExprNode::TPtr& input, TExprContext& ctx);
bool GetSumResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx);
bool GetAvgResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx);
bool GetAvgResultTypeOverState(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx);
bool GetMinMaxResultType(const TPositionHandle& pos, const TTypeAnnotationNode& inputType, const TTypeAnnotationNode*& retType, TExprContext& ctx);

IGraphTransformer::TStatus ExtractPgTypesFromMultiLambda(TExprNode::TPtr& lambda, TVector<ui32>& argTypes,
    bool& needRetype, TExprContext& ctx);

void AdjustReturnType(ui32& returnType, const TVector<ui32>& procArgTypes, ui32 procVariadicType, const TVector<ui32>& argTypes);
TExprNode::TPtr ExpandPgAggregationTraits(TPositionHandle pos, const NPg::TAggregateDesc& aggDesc, bool onWindow,
    const TExprNode::TPtr& lambda, const TVector<ui32>& argTypes, const TTypeAnnotationNode* itemType, TExprContext& ctx);

const TTypeAnnotationNode* GetOriginalResultType(TPositionHandle pos, bool isMany, const TTypeAnnotationNode* originalExtractorType, TExprContext& ctx);
bool ApplyOriginalType(TExprNode::TPtr input, bool isMany, const TTypeAnnotationNode* originalExtractorType, TExprContext& ctx);
TExprNode::TPtr ConvertToMultiLambda(const TExprNode::TPtr& lambda, TExprContext& ctx);

const TStringBuf BlockLengthColumnName = "_yql_block_length";

TStringBuf NormalizeCallableName(TStringBuf name);

void CheckExpectedTypeAndColumnOrder(const TExprNode& node, TExprContext& ctx, TTypeAnnotationContext& typesCtx);

}
