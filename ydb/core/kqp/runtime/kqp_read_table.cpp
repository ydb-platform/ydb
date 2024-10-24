#include "kqp_read_table.h"
#include "kqp_scan_data.h"

#include <ydb/core/engine/mkql_keys.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <ydb/core/kqp/common/kqp_types.h>

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

NScheme::TTypeInfo UnwrapTypeInfoFromStruct(const TStructType& structType, ui32 index) {
    return NScheme::TypeInfoFromMiniKQLType(structType.GetMemberType(index));
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

    auto typeInfo = NScheme::TypeInfoFromMiniKQLType(keyType);
    return MakeCell(typeInfo, keyValue, env, true);
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
    TSmallVec<NTable::TTag>& columns, TSmallVec<NTable::TTag>& systemColumns)
{
    MKQL_ENSURE_S(readType);
    MKQL_ENSURE_S(readType->GetKind() == TType::EKind::Flow);

    auto tags = AS_VALUE(TStructLiteral, tagsNode);
    MKQL_ENSURE_S(tags);


    columns.reserve(tags->GetValuesCount());

    for (ui32 i = 0; i < tags->GetValuesCount(); ++i) {
        NTable::TTag columnId = AS_VALUE(TDataLiteral, tags->GetValue(i))->AsValue().Get<ui32>();
        if (IsSystemColumn(columnId)) {
            systemColumns.push_back(columnId);
        } else {
            columns.push_back(columnId);
        }
    }
}

void ParseWideReadColumns(const TRuntimeNode& tagsNode,
    TSmallVec<NTable::TTag>& columns, TSmallVec<NTable::TTag>& systemColumns)
{
    auto tags = AS_VALUE(TStructLiteral, tagsNode);
    MKQL_ENSURE_S(tags);

    columns.reserve(tags->GetValuesCount());

    for (ui32 i = 0; i < tags->GetValuesCount(); ++i) {


        NTable::TTag columnId = AS_VALUE(TDataLiteral, tags->GetValue(i))->AsValue().Get<ui32>();;

        if (IsSystemColumn(columnId)) {
            systemColumns.push_back(columnId);
        } else if (columnId != TKeyDesc::EColumnIdInvalid) {
            columns.push_back(columnId);
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

    ParseWideReadColumns(tagsNode, result.Columns, result.SystemColumns);

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

    ParseWideReadColumns(tagsNode, result.Columns, result.SystemColumns);

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
        auto& context = ctx.Codegen.GetContext();
        const auto size = Representations.size();

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
                return new LoadInst(valueType, loadPtr, "load", block);
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
        const auto result = CallInst::Create(funcType, funcPtr, { self, ctx.Ctx, fields }, "fetch", block);

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

class TKqpScanBlockReadTableWrapperBase : public TStatelessWideFlowCodegeneratorNode<TKqpScanBlockReadTableWrapperBase> {
    using TBase = TStatelessWideFlowCodegeneratorNode<TKqpScanBlockReadTableWrapperBase>;
public:
    TKqpScanBlockReadTableWrapperBase(TKqpScanComputeContext& computeCtx, std::vector<EValueRepresentation>&& representations)
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
        auto& context = ctx.Codegen.GetContext();
        const auto size = Representations.size();

        Block.resize(size);

        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtrType = PointerType::getUnqual(valueType);
        const auto valuesPtr = CastInst::Create(Instruction::IntToPtr,
            ConstantInt::get(Type::getInt64Ty(context), uintptr_t(Block.data())),
            valuePtrType, "values", &ctx.Func->getEntryBlock().back());

        ICodegeneratorInlineWideNode::TGettersList getters(size);
        const auto indexType = Type::getInt32Ty(context);
        for (auto i = 0U; i < size; ++i) {
            getters[i] = [i, valueType, valuesPtr, indexType] (const TCodegenContext&, BasicBlock*& block) {
                const auto loadPtr = GetElementPtrInst::Create(valueType, valuesPtr,
                    {ConstantInt::get(indexType, i)},
                    (TString("loadPtr_") += ToString(i)).c_str(),
                    block);
                return new LoadInst(valueType, loadPtr, "load", block);
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
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TKqpScanBlockReadTableWrapperBase::DoCalculate));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto funcType = FunctionType::get(Type::getInt32Ty(context), { self->getType(), ctx.Ctx->getType(), fields->getType() }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto result = CallInst::Create(funcType, funcPtr, { self, ctx.Ctx, fields }, "fetch", block);

        return {result, std::move(getters)};
    }
#endif

    virtual ui32 GetCallableId() const = 0;
    virtual ui32 GetAllColumnsSize() const = 0;

private:
    TKqpScanComputeContext& ComputeCtx;
    // Mutable is bad for computation pattern cache.
    // Probably this hack is necessary for LLVM. Need to review
    mutable TIntrusivePtr<IKqpTableReader> TableReader;
    const std::vector<EValueRepresentation> Representations;
    mutable std::vector<NUdf::TUnboxedValue> Block;
};

class TKqpScanBlockReadTableRangesWrapper : public TKqpScanBlockReadTableWrapperBase {
public:
    TKqpScanBlockReadTableRangesWrapper(TKqpScanComputeContext& computeCtx, const TParseReadTableRangesResult& parseResult,
        IComputationNode* rangesNode, std::vector<EValueRepresentation>&& representations)
        : TKqpScanBlockReadTableWrapperBase(computeCtx, std::move(representations))
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

std::vector<EValueRepresentation> BuildRepresentations(const TType* type) {
    std::vector<EValueRepresentation> representations;

    auto wideComponents = type->IsFlow() ?
        GetWideComponents(AS_TYPE(TFlowType, type)) :
        AS_TYPE(TTupleType, AS_TYPE(TStreamType, type)->GetItemType())->GetElements();

    representations.reserve(wideComponents.size());
    for (ui32 i = 0U; i < wideComponents.size(); ++i) {
        representations.emplace_back(GetValueRepresentation(wideComponents[i]));
    }

    return representations;
}

} // namespace

IComputationNode* WrapKqpScanWideReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTableRanges(callable);
    auto rangesNode = LocateNode(ctx.NodeLocator, *parseResult.Ranges);

    const auto type = callable.GetType()->GetReturnType();
    auto representations = BuildRepresentations(type);
    return new TKqpScanWideReadTableRangesWrapper(computeCtx, parseResult, rangesNode, std::move(representations));
}

IComputationNode* WrapKqpScanWideReadTable(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTable(callable);
    auto fromNode = LocateNode(ctx.NodeLocator, *parseResult.FromTuple);
    auto toNode = LocateNode(ctx.NodeLocator, *parseResult.ToTuple);

    const auto type = callable.GetType()->GetReturnType();
    auto representations = BuildRepresentations(type);

    return new TKqpScanWideReadTableWrapper(computeCtx, parseResult, fromNode, toNode, std::move(representations));
}

IComputationNode* WrapKqpScanBlockReadTableRanges(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpScanComputeContext& computeCtx)
{
    auto parseResult = ParseWideReadTableRanges(callable);
    auto rangesNode = LocateNode(ctx.NodeLocator, *parseResult.Ranges);

    const auto type = callable.GetType()->GetReturnType();
    auto representations = BuildRepresentations(type);
    return new TKqpScanBlockReadTableRangesWrapper(computeCtx, parseResult, rangesNode, std::move(representations));
}

} // namespace NMiniKQL
} // namespace NKikimr
