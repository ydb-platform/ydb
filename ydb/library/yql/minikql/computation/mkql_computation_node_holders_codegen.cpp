#include "mkql_computation_node_holders_codegen.h"
#include "mkql_computation_node_codegen.h" // Y_IGNORE
#include "mkql_computation_node_pack.h"
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

TContainerCacheOnContext::TContainerCacheOnContext(TComputationMutables& mutables)
    : Index(mutables.CurValueIndex++)
{
    ++++mutables.CurValueIndex;
}

NUdf::TUnboxedValuePod TContainerCacheOnContext::NewArray(TComputationContext& ctx, ui64 size, NUdf::TUnboxedValue*& items) const {
    if (!size)
        return ctx.HolderFactory.GetEmptyContainerLazy();

    auto& index = ctx.MutableValues[Index];
    if (index.IsInvalid())
        index = NUdf::TUnboxedValuePod::Zero();

    {
        auto& val = ctx.MutableValues[Index + (index.Get<bool>() ? 1U : 2U)];
        if (val.IsInvalid() || !val.UniqueBoxed()) {
            index = NUdf::TUnboxedValuePod(!index.Get<bool>());
            auto& value = ctx.MutableValues[Index + (index.Get<bool>() ? 1U : 2U)];
            if (value.IsInvalid() || !value.UniqueBoxed()) {
                return value = ctx.HolderFactory.CreateDirectArrayHolder(size, items);
            }
        }
    }

    auto& value = ctx.MutableValues[Index + (index.Get<bool>() ? 1U : 2U)];
    items = static_cast<const TDirectArrayHolderInplace*>(value.AsBoxed().Get())->GetPtr();
    std::fill_n(items, size, NUdf::TUnboxedValue());
    return value;
}

#ifndef MKQL_DISABLE_CODEGEN
namespace {

Value* GenerateCheckNotUniqueBoxed(Value* value, LLVMContext& context, Function* function, BasicBlock*& block) {
    const auto invalid = ConstantInt::get(value->getType(), 0xFFFFFFFFFFFFFFFFULL);
    const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, value, invalid, "empty", block);

    const auto have = BasicBlock::Create(context, "have", function);
    const auto done = BasicBlock::Create(context, "done", function);
    const auto result = PHINode::Create(empty->getType(), 2, "result", done);
    result->addIncoming(empty, block);
    BranchInst::Create(done, have, empty, block);

    block = have;
    const auto half = CastInst::Create(Instruction::Trunc, value, Type::getInt64Ty(context), "half", block);
    const auto type = StructType::get(context, {PointerType::getUnqual(StructType::get(context)), Type::getInt32Ty(context), Type::getInt16Ty(context)});
    const auto boxptr = CastInst::Create(Instruction::IntToPtr, half, PointerType::getUnqual(type), "boxptr", block);
    const auto cntptr = GetElementPtrInst::CreateInBounds(type, boxptr, {ConstantInt::get(Type::getInt32Ty(context), 0), ConstantInt::get(Type::getInt32Ty(context), 1)}, "cntptr", block);
    const auto refs = new LoadInst(Type::getInt32Ty(context), cntptr, "refs", block);
    const auto many = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, refs, ConstantInt::get(refs->getType(), 1U), "many", block);
    result->addIncoming(many, block);
    BranchInst::Create(done, block);

    block = done;
    return result;
}

}

Value* TContainerCacheOnContext::GenNewArray(ui64 sz, Value* items, const TCodegenContext& ctx, BasicBlock*& block) const {
    auto& context = ctx.Codegen.GetContext();
    const auto valueType = Type::getInt128Ty(context);
    const auto arrayType = ArrayType::get(valueType, sz);
    const auto pointerType = PointerType::getUnqual(arrayType);

    const auto idxType = Type::getInt32Ty(context);

    const auto values = ctx.GetMutables();

    const auto indexPtr = GetElementPtrInst::CreateInBounds(valueType, values, {ConstantInt::get(idxType, Index)}, "index_ptr", block);

    const auto raw = new LoadInst(valueType, indexPtr, "raw", block);

    const auto indb = GetterFor<bool>(raw, context, block);
    const auto indf = CastInst::Create(Instruction::ZExt, indb, idxType, "indf", block);
    const auto ind_one = BinaryOperator::CreateAdd(indf, ConstantInt::get(idxType, Index + 1U), "ind_one", block);

    const auto tpfirst = GetElementPtrInst::CreateInBounds(valueType, values, {ind_one}, "tpfirst", block);

    const auto tfirst = new LoadInst(valueType, tpfirst, "tfirst", block);
    const auto cfirst = GenerateCheckNotUniqueBoxed(tfirst, context, ctx.Func, block);

    const auto scnd = BasicBlock::Create(context, "scnd", ctx.Func);
    const auto make = BasicBlock::Create(context, "make", ctx.Func);
    const auto have = BasicBlock::Create(context, "have", ctx.Func);
    const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
    const auto result = PHINode::Create(valueType, 2, "result", exit);

    const auto has = PHINode::Create(tfirst->getType(), 2, "has", have);
    has->addIncoming(tfirst, block);

    BranchInst::Create(scnd, have, cfirst, block);

    block = scnd;

    const auto neg = BinaryOperator::CreateXor(indb, ConstantInt::get(indb->getType(), 1), "xor", block);
    const auto newInd = SetterFor<bool>(neg, context, block);
    new StoreInst(newInd, indexPtr, block);

    const auto inds = CastInst::Create(Instruction::ZExt, neg, idxType, "inds", block);

    const auto ind_two = BinaryOperator::CreateAdd(inds, ConstantInt::get(idxType, Index + 1U), "ind_two", block);

    const auto tpsecond = GetElementPtrInst::CreateInBounds(valueType, values, {ind_two}, "tpsecond", block);
    const auto tsecond = new LoadInst(valueType, tpsecond, "tsecond", block);
    const auto csecond = GenerateCheckNotUniqueBoxed(tsecond, context, ctx.Func, block);
    has->addIncoming(tsecond, block);
    BranchInst::Create(make, have, csecond, block);

    {
        block = make;
        ValueUnRef(EValueRepresentation::Boxed, tpsecond, ctx, block);

        const auto fact = ctx.GetFactory();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CreateDirectArrayHolder));
        const auto size = ConstantInt::get(Type::getInt64Ty(context), sz);

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {fact->getType(), size->getType(), items->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto array = CallInst::Create(funType, funcPtr, {fact, size, items}, "array", block);
            AddRefBoxed(array, ctx, block);
            result->addIncoming(array, block);
            new StoreInst(array, tpsecond, block);
        } else {
            const auto funType = FunctionType::get(Type::getVoidTy(context), {fact->getType(), tpsecond->getType(), size->getType(), items->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {fact, tpsecond, size, items}, "", block);
            const auto array = new LoadInst(valueType, tpsecond, "array", block);
            AddRefBoxed(array, ctx, block);
            result->addIncoming(array, block);
        }

        BranchInst::Create(exit, block);
    }

    {
        block = have;

        const auto half = CastInst::Create(Instruction::Trunc, has, Type::getInt64Ty(context), "half", block);

        const auto offs = BinaryOperator::CreateAdd(half, ConstantInt::get(half->getType(), sizeof(TDirectArrayHolderInplace)), "offs", block);

        const auto itemsPtr = CastInst::Create(Instruction::IntToPtr, offs, pointerType, "items_ptr", block);

        for (ui64 i = 0; i < sz; ++i) {
            const auto itemp = GetElementPtrInst::CreateInBounds(arrayType, itemsPtr, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, i)}, "itemp", block);
            ValueUnRef(EValueRepresentation::Any, itemp, ctx, block);
        }

        result->addIncoming(has, block);

        new StoreInst(ConstantAggregateZero::get(arrayType), itemsPtr, block);
        new StoreInst(itemsPtr, items, block);
        BranchInst::Create(exit, block);
    }

    block = exit;
    return result;
}
#endif

class TEmptyNode : public TMutableCodegeneratorNode<TEmptyNode> {
    typedef TMutableCodegeneratorNode<TEmptyNode> TBaseComputation;
public:
    TEmptyNode(TComputationMutables& mutables)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.GetEmptyContainerLazy();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::GetEmptyContainerLazy));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {factory->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto res = CallInst::Create(funType, funcPtr, {factory}, "res", block);
            return res;
        } else {
            const auto retPtr = new AllocaInst(valueType, 0U, "ret_ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr}, "", block);
            const auto res = new LoadInst(valueType, retPtr, "res", block);
            return res;
        }
    }
#endif
private:
    void RegisterDependencies() const final {}
};

class TOptionalNode: public TDecoratorCodegeneratorNode<TOptionalNode> {
    typedef TDecoratorCodegeneratorNode<TOptionalNode> TBaseComputation;
public:
    TOptionalNode(IComputationNode* itemNode)
        : TBaseComputation(itemNode)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return value.MakeOptional();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* arg, BasicBlock*& block) const {
        return MakeOptional(ctx.Codegen.GetContext(), arg, block);
    }
#endif
};

class TArrayNode: public TMutableCodegeneratorFallbackNode<TArrayNode> {
    typedef TMutableCodegeneratorFallbackNode<TArrayNode> TBaseComputation;
public:
    TArrayNode(TComputationMutables& mutables, TComputationNodePtrVector&& valueNodes)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , ValueNodes(std::move(valueNodes))
        , Cache(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        NUdf::TUnboxedValue *items = nullptr;
        const auto result = Cache.NewArray(ctx, ValueNodes.size(), items);
        if (!ValueNodes.empty()) {
            Y_ABORT_UNLESS(items);
            for (const auto& node : ValueNodes) {
                *items++ = node->GetValue(ctx);
            }
        }

        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        if (ValueNodes.size() > CodegenArraysFallbackLimit)
            return TBaseComputation::DoGenerateGetValue(ctx, block);

        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto idxType = Type::getInt32Ty(context);
        const auto type = ArrayType::get(valType, ValueNodes.size());
        const auto ptrType = PointerType::getUnqual(type);
        /// TODO: how to get computation context or other workaround
        const auto itms = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(ptrType, 0U, "itms", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(ptrType, 0U, "itms", block);
        const auto result = Cache.GenNewArray(ValueNodes.size(), itms, ctx, block);
        const auto itemsPtr = new LoadInst(ptrType, itms, "items", block);

        ui32 i = 0U;
        for (const auto node : ValueNodes) {
            const auto itemPtr = GetElementPtrInst::CreateInBounds(type, itemsPtr, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, i++)}, "item", block);
            GetNodeValue(itemPtr, node, ctx, block);
        }
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        std::for_each(ValueNodes.cbegin(), ValueNodes.cend(), std::bind(&TArrayNode::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector ValueNodes;
    const TContainerCacheOnContext Cache;
};

class TVariantNode : public TMutableCodegeneratorNode<TVariantNode> {
    typedef TMutableCodegeneratorNode<TVariantNode> TBaseComputation;
public:
    TVariantNode(TComputationMutables& mutables, IComputationNode* itemNode, ui32 index)
        : TBaseComputation(mutables, EValueRepresentation::Any)
        , ItemNode(itemNode)
        , Index(index)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        if (auto item = ItemNode->GetValue(ctx); item.TryMakeVariant(Index))
            return item.Release();
        else
            return ctx.HolderFactory.CreateBoxedVariantHolder(item.Release(), Index);
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        const auto value = GetNodeValue(ItemNode, ctx, block);
        return MakeVariant(value, ConstantInt::get(Type::getInt32Ty(ctx.Codegen.GetContext()), Index), ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(ItemNode);
    }

    IComputationNode *const ItemNode;
    const ui32 Index;
};

class TDictNode: public TMutableComputationNode<TDictNode> {
    typedef TMutableComputationNode<TDictNode> TBaseComputation;
public:
    TDictNode(TComputationMutables& mutables,
            std::vector<std::pair<IComputationNode*, IComputationNode*>>&& itemNodes,
            const TKeyTypes& types, bool isTuple, TType* encodedType,
            NUdf::IHash::TPtr hash, NUdf::IEquate::TPtr equate,
            NUdf::ICompare::TPtr compare, bool isSorted)
        : TBaseComputation(mutables)
        , ItemNodes(std::move(itemNodes))
        , Types(types)
        , IsTuple(isTuple)
        , EncodedType(encodedType)
        , Hash(hash)
        , Equate(equate)
        , Compare(compare)
        , IsSorted(isSorted)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TKeyPayloadPairVector items;
        items.reserve(ItemNodes.size());
        for (const auto& node : ItemNodes) {
            items.emplace_back(node.first->GetValue(ctx), node.second->GetValue(ctx));
        }

        std::optional<TValuePacker> packer;
        if (EncodedType) {
            packer.emplace(true, EncodedType);
        }

        if (IsSorted) {
            const TSortedDictFiller filler = [&](TKeyPayloadPairVector& values) {
                values = std::move(items);
            };

            return ctx.HolderFactory.CreateDirectSortedDictHolder(filler, Types, IsTuple, EDictSortMode::RequiresSorting,
                true, EncodedType, Compare.Get(), Equate.Get());
        } else {
            THashedDictFiller filler =
                    [&items, &packer](TValuesDictHashMap& map) {
                        for (auto& value : items) {
                            auto key = std::move(value.first);
                            if (packer) {
                                key = MakeString(packer->Pack(key));
                            }

                            map.emplace(std::move(key), std::move(value.second));
                        }
                    };

            return ctx.HolderFactory.CreateDirectHashedDictHolder(
                    filler, Types, IsTuple, true, EncodedType, Hash.Get(), Equate.Get());
        }
    }

private:
    void RegisterDependencies() const final {
        for (const auto& itemNode : ItemNodes) {
            DependsOn(itemNode.first);
            DependsOn(itemNode.second);
        }
    }

    const std::vector<std::pair<IComputationNode*, IComputationNode*>> ItemNodes;
    const TKeyTypes Types;
    const bool IsTuple;
    TType* EncodedType;
    NUdf::IHash::TPtr Hash;
    NUdf::IEquate::TPtr Equate;
    NUdf::ICompare::TPtr Compare;
    const bool IsSorted;
};

//////////////////////////////////////////////////////////////////////////////
// TNodeFactory
//////////////////////////////////////////////////////////////////////////////
TNodeFactory::TNodeFactory(TMemoryUsageInfo& memInfo, TComputationMutables& mutables)
    : MemInfo(memInfo)
    , Mutables(mutables)
{
}

IComputationNode* TNodeFactory::CreateEmptyNode() const
{
    return new TEmptyNode(Mutables);
}

IComputationNode* TNodeFactory::CreateOptionalNode(IComputationNode* item) const
{
    return item ? new TOptionalNode(item) : CreateImmutableNode(NUdf::TUnboxedValuePod());
}

IComputationNode* TNodeFactory::CreateArrayNode(TComputationNodePtrVector&& values) const
{
    if (values.empty()) {
        return new TEmptyNode(Mutables);
    }

    return new TArrayNode(Mutables, std::move(values));
}

IComputationNode* TNodeFactory::CreateDictNode(
        std::vector<std::pair<IComputationNode*, IComputationNode*>>&& items,
        const TKeyTypes& types, bool isTuple, TType* encodedType,
        NUdf::IHash::TPtr hash, NUdf::IEquate::TPtr equate, NUdf::ICompare::TPtr compare, bool isSorted) const
{
    if (items.empty()) {
        return new TEmptyNode(Mutables);
    }

    return new TDictNode(Mutables, std::move(items), types, isTuple, encodedType, hash, equate, compare, isSorted);
}

IComputationNode* TNodeFactory::CreateVariantNode(IComputationNode* item, ui32 index) const {
    return new TVariantNode(Mutables, item, index);
}

IComputationNode* TNodeFactory::CreateTypeNode(TType* type) const {
    return CreateImmutableNode(NUdf::TUnboxedValuePod(new TTypeHolder(&MemInfo, type)));
}

IComputationNode* TNodeFactory::CreateImmutableNode(NUdf::TUnboxedValue&& value) const {
    return new TUnboxedImmutableCodegeneratorNode(&MemInfo, std::move(value));
}

} // namespace NMiniKQL
} // namespace NKikimr
