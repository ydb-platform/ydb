#include "kqp_read_table.h"
#include "kqp_scan_data.h"

#include <ydb/core/engine/mkql_keys.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {

namespace NKqp {

using namespace NMiniKQL;

TTableId ParseTableId(const TRuntimeNode& node) {
    auto tuple = AS_VALUE(TTupleLiteral, node);
    MKQL_ENSURE_S(tuple->GetValuesCount() >= 4);
    ui64 ownerId = AS_VALUE(TDataLiteral, tuple->GetValue(0))->AsValue().Get<ui64>();
    ui64 tableId = AS_VALUE(TDataLiteral, tuple->GetValue(1))->AsValue().Get<ui64>();
    TString sysViewInfo(AS_VALUE(TDataLiteral, tuple->GetValue(2))->AsValue().AsStringRef());
    ui64 schemeVersion = AS_VALUE(TDataLiteral, tuple->GetValue(3))->AsValue().Get<ui64>();
    return TTableId(ownerId, tableId, sysViewInfo, schemeVersion);
}

NUdf::TDataTypeId UnwrapDataTypeFromStruct(const TStructType& structType, ui32 index) {
    if (structType.GetMemberType(index)->GetKind() == TType::EKind::Optional) {
        auto type = AS_TYPE(TDataType, AS_TYPE(TOptionalType, structType.GetMemberType(index))->GetItemType());
        return type->GetSchemeType();
    } else {
        return AS_TYPE(TDataType, structType.GetMemberType(index))->GetSchemeType();
    }
}

} // namespace NKqp

namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

static TCell BuildKeyTupleCell(const TType* type, const TUnboxedValue& value, const TTypeEnvironment& env) {
    MKQL_ENSURE_S(type);

    auto keyType = type;
    auto keyValue = value;

    if (type->IsOptional()) {
        if (!value) {
            return TCell();
        }

        keyType = AS_TYPE(TOptionalType, type)->GetItemType();
        keyValue = value.GetOptionalValue();
    }

    return MakeCell(AS_TYPE(TDataType, keyType)->GetSchemeType(), keyValue, env, true);
}

void BuildKeyTupleCells(const TTupleType* tupleType, const TUnboxedValue& tupleValue, TVector<TCell>& cells,
    const TTypeEnvironment& env)
{
    MKQL_ENSURE_S(tupleType);

    cells.resize(tupleType->GetElementsCount());

    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        auto keyType = tupleType->GetElementType(i);
        auto keyValue = tupleValue.GetElement(i);
        cells[i] = BuildKeyTupleCell(keyType, keyValue, env);
    }
}

void ParseReadColumns(const TType* readType, const TRuntimeNode& tagsNode,
    TSmallVec<TKqpComputeContextBase::TColumn>& columns, TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns)
{
    MKQL_ENSURE_S(readType);
    MKQL_ENSURE_S(readType->GetKind() == TType::EKind::Flow);

    auto tags = AS_VALUE(TStructLiteral, tagsNode);
    MKQL_ENSURE_S(tags);

    auto itemType = AS_TYPE(TFlowType, readType)->GetItemType();
    MKQL_ENSURE_S(itemType->GetKind() == TType::EKind::Struct);

    auto structType = AS_TYPE(TStructType, itemType);
    MKQL_ENSURE_S(tags->GetValuesCount() == structType->GetMembersCount());

    columns.reserve(structType->GetMembersCount());

    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        auto memberType = structType->GetMemberType(i);
        if (memberType->GetKind() == TType::EKind::Optional) {
            memberType = AS_TYPE(TOptionalType, memberType)->GetItemType();
        }
        MKQL_ENSURE_S(memberType->GetKind() == TType::EKind::Data);
        NTable::TTag columnId = AS_VALUE(TDataLiteral, tags->GetValue(i))->AsValue().Get<ui32>();
        if (IsSystemColumn(columnId)) {
            systemColumns.push_back({columnId, AS_TYPE(TDataType, memberType)->GetSchemeType()});
        } else {
            columns.push_back({columnId, AS_TYPE(TDataType, memberType)->GetSchemeType()});
        }
    }
}

void ParseWideReadColumns(const TCallable& callable, const TRuntimeNode& tagsNode,
    TSmallVec<TKqpComputeContextBase::TColumn>& columns, TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns)
{
    auto tags = AS_VALUE(TStructLiteral, tagsNode);
    MKQL_ENSURE_S(tags);

    TType* returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE_S(returnType->GetKind() == TType::EKind::Flow);

    auto itemType = AS_TYPE(TFlowType, returnType)->GetItemType();
    MKQL_ENSURE_S(itemType->GetKind() == TType::EKind::Tuple);

    auto tupleType = AS_TYPE(TTupleType, itemType);
    MKQL_ENSURE_S(tags->GetValuesCount() == tupleType->GetElementsCount());

    columns.reserve(tupleType->GetElementsCount());

    for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
        auto memberType = tupleType->GetElementType(i);

        if (memberType->GetKind() == TType::EKind::Optional) {
            memberType = AS_TYPE(TOptionalType, memberType)->GetItemType();
        }

        MKQL_ENSURE_S(memberType->GetKind() == TType::EKind::Data);

        NTable::TTag columnId = AS_VALUE(TDataLiteral, tags->GetValue(i))->AsValue().Get<ui32>();

        if (IsSystemColumn(columnId)) {
            systemColumns.push_back({columnId, AS_TYPE(TDataType, memberType)->GetSchemeType()});
        } else {
            columns.push_back({columnId, AS_TYPE(TDataType, memberType)->GetSchemeType()});
        }
    }
}

TParseReadTableResult ParseWideReadTable(TCallable& callable) {
    MKQL_ENSURE_S(callable.GetInputsCount() >= 4);

    TParseReadTableResult result;

    result.CallableId = 0; // callable.GetUniqueId();

    auto tableNode = callable.GetInput(0);
    auto rangeNode = callable.GetInput(1);
    auto tagsNode = callable.GetInput(2);

    result.TableId = NKqp::ParseTableId(tableNode);

    auto range = AS_VALUE(TTupleLiteral, rangeNode);
    MKQL_ENSURE_S(range);
    MKQL_ENSURE_S(range->GetValuesCount() >= 4);

    result.FromTuple = AS_VALUE(TTupleLiteral, range->GetValue(0));
    MKQL_ENSURE_S(result.FromTuple);
    result.FromInclusive = AS_VALUE(TDataLiteral, range->GetValue(1))->AsValue().Get<bool>();
    result.ToTuple = AS_VALUE(TTupleLiteral, range->GetValue(2));
    MKQL_ENSURE_S(result.ToTuple);
    result.ToInclusive = AS_VALUE(TDataLiteral, range->GetValue(3))->AsValue().Get<bool>();

    ParseWideReadColumns(callable, tagsNode, result.Columns, result.SystemColumns);

    auto skipNullKeys = AS_VALUE(TListLiteral, callable.GetInput(3));
    result.SkipNullKeys.reserve(skipNullKeys->GetItemsCount());
    for (ui32 i = 0; i < skipNullKeys->GetItemsCount(); ++i) {
        result.SkipNullKeys.push_back(AS_VALUE(TDataLiteral, skipNullKeys->GetItems()[i])->AsValue().Get<bool>());
    }

    if (callable.GetInputsCount() >= 5) {
        auto node = callable.GetInput(4).GetNode();
        if (node->GetType()->GetKind() == TType::EKind::Callable) {
            MKQL_ENSURE_S(AS_TYPE(TDataType, AS_TYPE(TCallableType, node->GetType())->GetReturnType())->GetSchemeType()
                == NUdf::TDataType<ui64>::Id, "ItemsLimit must be () -> ui64");
            result.ItemsLimit = node;
        } else if (node->GetType()->GetKind() == TType::EKind::Data) {
            MKQL_ENSURE_S(AS_TYPE(TDataType, node->GetType())->GetSchemeType() == NUdf::TDataType<ui64>::Id,
                "ItemsLimit must be ui64");
            result.ItemsLimit = node;
        } else {
            MKQL_ENSURE_S(node->GetType()->GetKind() == TType::EKind::Null, "ItemsLimit expected to be Callable, Uint64 or Null");
        }
    }

    if (callable.GetInputsCount() >= 6) {
        result.Reverse = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<bool>();
    }

    return result;
}

TParseReadTableRangesResult ParseWideReadTableRanges(TCallable& callable) {
    MKQL_ENSURE_S(callable.GetInputsCount() == 5);

    TParseReadTableRangesResult result;

    result.CallableId = 0; // callable.GetUniqueId();

    auto tableNode = callable.GetInput(0);
    auto rangeNode = callable.GetInput(1);
    auto tagsNode = callable.GetInput(2);
    auto limit = callable.GetInput(3);
    auto reverse = callable.GetInput(4);

    result.TableId = NKqp::ParseTableId(tableNode);

    result.Ranges = AS_VALUE(TTupleLiteral, rangeNode);
    MKQL_ENSURE_S(result.Ranges);
    MKQL_ENSURE_S(result.Ranges->GetValuesCount() == 1);

    ParseWideReadColumns(callable, tagsNode, result.Columns, result.SystemColumns);

    auto limitNode = limit.GetNode();

    switch (limitNode->GetType()->GetKind()) {
        case TType::EKind::Callable:
            MKQL_ENSURE_S(
                AS_TYPE(TDataType, AS_TYPE(TCallableType, limitNode->GetType())->GetReturnType())->GetSchemeType()
                == NUdf::TDataType<ui64>::Id, "ItemsLimit must be () -> ui64"
            );
            result.ItemsLimit = limitNode;
            break;
        case TType::EKind::Data:
            MKQL_ENSURE_S(AS_TYPE(TDataType, limitNode->GetType())->GetSchemeType() == NUdf::TDataType<ui64>::Id,
                "ItemsLimit must be ui64");
            result.ItemsLimit = limitNode;
            break;
        case TType::EKind::Null:
            break;
        default:
            MKQL_ENSURE(false, "ItemsLimit expected to be Callable, Data or Null");
    }

    result.Reverse = AS_VALUE(TDataLiteral, reverse)->AsValue().Get<bool>();

    return result;
}

namespace {

class TKqpScanWideReadTableWrapperBase : public TStatelessWideFlowCodegeneratorNode<TKqpScanWideReadTableWrapperBase> {
    using TBase = TStatelessWideFlowCodegeneratorNode<TKqpScanWideReadTableWrapperBase>;
public:
    TKqpScanWideReadTableWrapperBase(TKqpScanComputeContext& computeCtx, std::vector<EValueRepresentation>&& representations)
        : TBase(this)
        , ComputeCtx(computeCtx)
        , Representations(std::move(representations)) {}

    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        Y_UNUSED(ctx);

        if (!TableReader) {
            TableReader = ComputeCtx.ReadTable(GetCallableId());
        }

        return TableReader->Next(output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();
        const auto size = GetAllColumnsSize();

        Row.resize(size);

        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtrType = PointerType::getUnqual(valueType);
        const auto valuesPtr = CastInst::Create(Instruction::IntToPtr,
            ConstantInt::get(Type::getInt64Ty(context), uintptr_t(Row.data())),
            valuePtrType, "values", &ctx.Func->getEntryBlock().back());

        ICodegeneratorInlineWideNode::TGettersList getters(size);
        const auto indexType = Type::getInt32Ty(context);
        for (auto i = 0U; i < size; ++i) {
            getters[i] = [i, valueType, valuesPtr, indexType] (const TCodegenContext&, BasicBlock*& block) {
                const auto loadPtr = GetElementPtrInst::Create(valueType, valuesPtr,
                    {ConstantInt::get(indexType, i)},
                    (TString("loadPtr_") += ToString(i)).c_str(),
                    block);
                return new LoadInst(loadPtr, "load", block);
            };
        }

        const auto fieldsType = ArrayType::get(valuePtrType, size);
        const auto fields = new AllocaInst(fieldsType, 0U, "fields", &ctx.Func->getEntryBlock().back());

        Value* init = UndefValue::get(fieldsType);
        for (auto i = 0U; i < size; ++i) {
            const auto pointer = GetElementPtrInst::Create(valueType, valuesPtr,
                    {ConstantInt::get(indexType, i)},
                    (TString("ptr_") += ToString(i)).c_str(),
                    &ctx.Func->getEntryBlock().back());

            init = InsertValueInst::Create(init, pointer, {i}, (TString("insert_") += ToString(i)).c_str(),
                &ctx.Func->getEntryBlock().back());
        }

        new StoreInst(init, fields, &ctx.Func->getEntryBlock().back());

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TKqpScanWideReadTableWrapperBase::DoCalculate));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto funcType = FunctionType::get(Type::getInt32Ty(context), { self->getType(), ctx.Ctx->getType(), fields->getType() }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto result = CallInst::Create(funcPtr, { self, ctx.Ctx, fields }, "fetch", block);

        return {result, std::move(getters)};
    }
#endif

    virtual ui32 GetCallableId() const = 0;
    virtual ui32 GetAllColumnsSize() const = 0;

private:
    TKqpScanComputeContext& ComputeCtx;
    mutable TIntrusivePtr<IKqpTableReader> TableReader;
    const std::vector<EValueRepresentation> Representations;
    mutable std::vector<NUdf::TUnboxedValue> Row;
};

class TKqpScanWideReadTableWrapper : public TKqpScanWideReadTableWrapperBase {
public:
    TKqpScanWideReadTableWrapper(TKqpScanComputeContext& computeCtx, const TParseReadTableResult& parseResult,
        IComputationNode* fromNode, IComputationNode* toNode, std::vector<EValueRepresentation>&& representations)
        : TKqpScanWideReadTableWrapperBase(computeCtx, std::move(representations))
        , FromNode(fromNode)
        , ToNode(toNode)
        , ParseResult(parseResult)
    {}

private:
    ui32 GetCallableId() const {
        return ParseResult.CallableId;
    }

    ui32 GetAllColumnsSize() const {
        return ParseResult.Columns.size() + ParseResult.SystemColumns.size();
    }

    void RegisterDependencies() const {
        FlowDependsOn(FromNode);
        FlowDependsOn(ToNode);
    }

private:
    IComputationNode* FromNode;
    IComputationNode* ToNode;
    TParseReadTableResult ParseResult;
};

class TKqpScanWideReadTableRangesWrapper : public TKqpScanWideReadTableWrapperBase {
public:
    TKqpScanWideReadTableRangesWrapper(TKqpScanComputeContext& computeCtx, const TParseReadTableRangesResult& parseResult,
        IComputationNode* rangesNode, std::vector<EValueRepresentation>&& representations)
        : TKqpScanWideReadTableWrapperBase(computeCtx, std::move(representations))
        , RangesNode(rangesNode)
        , ParseResult(parseResult)
    {}

private:
    ui32 GetCallableId() const {
        return ParseResult.CallableId;
    }

    ui32 GetAllColumnsSize() const {
        return ParseResult.Columns.size() + ParseResult.SystemColumns.size();
    }

    void RegisterDependencies() const {
        this->FlowDependsOn(RangesNode);
    }

private:
    IComputationNode* RangesNode;
    TParseReadTableRangesResult ParseResult;
};

} // namespace

IComputationNode* WrapKqpScanWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx)
{
    std::vector<EValueRepresentation> representations;

    auto parseResult = ParseWideReadTableRanges(callable);
    auto rangesNode = LocateNode(ctx.NodeLocator, *parseResult.Ranges);

    const auto type = callable.GetType()->GetReturnType();
    const auto returnItemType = type->IsFlow() ?
        AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType():
        AS_TYPE(TStreamType, callable.GetType()->GetReturnType())->GetItemType();

    const auto tupleType = AS_TYPE(TTupleType, returnItemType);

    representations.reserve(tupleType->GetElementsCount());
    for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i)
        representations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));

    return new TKqpScanWideReadTableRangesWrapper(computeCtx, parseResult, rangesNode, std::move(representations));
}

IComputationNode* WrapKqpScanWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx)
{
    std::vector<EValueRepresentation> representations;

    auto parseResult = ParseWideReadTable(callable);
    auto fromNode = LocateNode(ctx.NodeLocator, *parseResult.FromTuple);
    auto toNode = LocateNode(ctx.NodeLocator, *parseResult.ToTuple);

    const auto type = callable.GetType()->GetReturnType();
    const auto returnItemType = type->IsFlow() ?
        AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType():
        AS_TYPE(TStreamType, callable.GetType()->GetReturnType())->GetItemType();

    const auto tupleType = AS_TYPE(TTupleType, returnItemType);

    representations.reserve(tupleType->GetElementsCount());
    for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i)
        representations.emplace_back(GetValueRepresentation(tupleType->GetElementType(i)));

    return new TKqpScanWideReadTableWrapper(computeCtx, parseResult, fromNode, toNode, std::move(representations));
}

} // namespace NMiniKQL
} // namespace NKikimr
