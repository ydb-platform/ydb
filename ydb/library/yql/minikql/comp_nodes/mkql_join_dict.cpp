#include "mkql_join_dict.h"

#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <EJoinKind Kind>
struct TWrapTraits {
    static constexpr bool Wrap1 = IsLeftOptional(Kind);
    static constexpr bool Wrap2 = IsRightOptional(Kind);
};

template <bool KeyTuple>
class TJoinDictWrapper : public TMutableCodegeneratorPtrNode<TJoinDictWrapper<KeyTuple>> {
    typedef TMutableCodegeneratorPtrNode<TJoinDictWrapper<KeyTuple>> TBaseComputation;
public:
    TJoinDictWrapper(TComputationMutables& mutables, IComputationNode* dict1, IComputationNode* dict2,
        bool isMulti1, bool isMulti2, EJoinKind joinKind, std::vector<ui32>&& indexes = std::vector<ui32>())
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Dict1(dict1)
        , Dict2(dict2)
        , IsMulti1(isMulti1)
        , IsMulti2(isMulti2)
        , JoinKind(joinKind)
        , OptIndicies(std::move(indexes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& dict1 = Dict1->GetValue(ctx);
        const auto& dict2 = Dict2->GetValue(ctx);
        return JoinDicts(ctx, dict1, dict2);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto joinFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TJoinDictWrapper::JoinDicts));
        const auto joinFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);

        const auto one = GetNodeValue(Dict1, ctx, block);
        const auto two = GetNodeValue(Dict2, ctx, block);

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto joinFuncType = FunctionType::get(Type::getInt128Ty(context),
                { joinFuncArg->getType(), ctx.Ctx->getType(), one->getType(), two->getType() }, false);
            const auto joinFuncPtr = CastInst::Create(Instruction::IntToPtr, joinFunc, PointerType::getUnqual(joinFuncType), "cast", block);
            const auto join = CallInst::Create(joinFuncType, joinFuncPtr, { joinFuncArg, ctx.Ctx, one, two }, "join", block);
            AddRefBoxed(join, ctx, block);
            new StoreInst(join, pointer, block);
        } else {
            const auto onePtr = new AllocaInst(one->getType(), 0U, "one_ptr", block);
            const auto twoPtr = new AllocaInst(two->getType(), 0U, "two_ptr", block);
            new StoreInst(one, onePtr, block);
            new StoreInst(two, twoPtr, block);

            const auto joinFuncType = FunctionType::get(Type::getVoidTy(context),
                { joinFuncArg->getType(), pointer->getType(), ctx.Ctx->getType(), onePtr->getType(), twoPtr->getType() }, false);

            const auto joinFuncPtr = CastInst::Create(Instruction::IntToPtr, joinFunc, PointerType::getUnqual(joinFuncType), "cast", block);
            CallInst::Create(joinFuncType, joinFuncPtr, { joinFuncArg, pointer, ctx.Ctx, onePtr, twoPtr }, "", block);
            const auto join = new LoadInst(Type::getInt128Ty(context), pointer, "join", block);
            AddRefBoxed(join, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Dict1);
        this->DependsOn(Dict2);
    }

    bool HasNullInKey(const NUdf::TUnboxedValue& key) const {
        if (!key) {
            return true;
        }

        if constexpr (KeyTuple) {
            for (ui32 index : OptIndicies) {
                if (!key.GetElement(index)) {
                    return true;
                }
            }
        }

        return false;
    }

    template <EJoinKind Kind>
    void WriteValuesImpl(const NUdf::TUnboxedValuePod& payload1, const NUdf::TUnboxedValuePod& payload2,
        TDefaultListRepresentation& resList, TComputationContext& ctx) const {
        WriteValues<TWrapTraits<Kind>::Wrap1, TWrapTraits<Kind>::Wrap2>(payload1, payload2, resList, ctx);
    }

    template <bool WrapAsOptional1, bool WrapAsOptional2>
    void WriteValues(const NUdf::TUnboxedValuePod& payload1, const NUdf::TUnboxedValuePod& payload2,
        TDefaultListRepresentation& resList, TComputationContext& ctx) const {
        const bool isMulti1 = IsMulti1 && bool(payload1);
        const bool isMulti2 = IsMulti2 && bool(payload2);
        if (!isMulti1 && !isMulti2) {
            WriteTuple<WrapAsOptional1, WrapAsOptional2>(payload1, payload2, resList, ctx);
        } else if (isMulti1 && !isMulti2) {
            const auto it = payload1.GetListIterator();
            for (NUdf::TUnboxedValue item1; it.Next(item1);) {
                WriteTuple<WrapAsOptional1, WrapAsOptional2>(item1, payload2, resList, ctx);
            }
        } else if (!isMulti1 && isMulti2) {
            const auto it = payload2.GetListIterator();
            for (NUdf::TUnboxedValue item2; it.Next(item2);) {
                WriteTuple<WrapAsOptional1, WrapAsOptional2>(payload1, item2, resList, ctx);
            }
        } else {
            const auto it1 = payload1.GetListIterator();
            for (NUdf::TUnboxedValue item1; it1.Next(item1);) {
                const auto it2 = payload2.GetListIterator();
                for (NUdf::TUnboxedValue item2; it2.Next(item2);) {
                    WriteTuple<WrapAsOptional1, WrapAsOptional2>(item1, item2, resList, ctx);
                }
            }
        }
    }

    template <bool WrapAsOptional1, bool WrapAsOptional2>
    void WriteTuple(const NUdf::TUnboxedValuePod& val1, const NUdf::TUnboxedValuePod& val2,
        TDefaultListRepresentation& resList, TComputationContext& ctx) const {
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto tuple = ctx.HolderFactory.CreateDirectArrayHolder(2, itemsPtr);
        itemsPtr[0] = val1 ? val1.MakeOptionalIf<WrapAsOptional1>() : NUdf::TUnboxedValuePod(val1);
        itemsPtr[1] = val2 ? val2.MakeOptionalIf<WrapAsOptional2>() : NUdf::TUnboxedValuePod(val2);
        resList = resList.Append(std::move(tuple));
    }

    NUdf::TUnboxedValuePod JoinDicts(TComputationContext& ctx, const NUdf::TUnboxedValuePod dict1, const NUdf::TUnboxedValuePod dict2) const {
        TDefaultListRepresentation resList;
        switch (JoinKind) {
        case EJoinKind::Inner:
            if (dict1.GetDictLength() < dict2.GetDictLength()) {
                // traverse dict1, lookup dict2
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    Y_DEBUG_ABORT_UNLESS(!HasNullInKey(key1));
                    if (const auto lookup2 = dict2.Lookup(key1)) {
                        WriteValuesImpl<EJoinKind::Inner>(payload1, lookup2, resList, ctx);
                    }
                }
            } else {
                // traverse dict2, lookup dict1
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    Y_DEBUG_ABORT_UNLESS(!HasNullInKey(key2));
                    if (const auto lookup1 = dict1.Lookup(key2)) {
                        WriteValuesImpl<EJoinKind::Inner>(lookup1, payload2, resList, ctx);
                    }
                }
            }
            break;

        case EJoinKind::Left: {
            // traverse dict1, lookup dict2
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    auto lookup2 = HasNullInKey(key1) ? NUdf::TUnboxedValue() : dict2.Lookup(key1);
                    lookup2 = lookup2 ? lookup2.Release().GetOptionalValue() : NUdf::TUnboxedValuePod();
                    WriteValuesImpl<EJoinKind::Left>(payload1, lookup2, resList, ctx);
                }
            }
            break;

        case EJoinKind::Right: {
            // traverse dict2, lookup dict1
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    auto lookup1 = HasNullInKey(key2) ? NUdf::TUnboxedValue() : dict1.Lookup(key2);
                    lookup1 = lookup1 ? lookup1.Release().GetOptionalValue() : NUdf::TUnboxedValuePod();
                    WriteValuesImpl<EJoinKind::Right>(lookup1, payload2, resList, ctx);
                }
            }
            break;

        case EJoinKind::Full: {
            // traverse dict1, lookup dict2 - as Left
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    auto lookup2 = HasNullInKey(key1) ? NUdf::TUnboxedValue() : dict2.Lookup(key1);
                    lookup2 = lookup2 ? lookup2.Release().GetOptionalValue() : NUdf::TUnboxedValuePod();
                    WriteValuesImpl<EJoinKind::Full>(payload1, lookup2, resList, ctx);
                }
            }
            {
            // traverse dict2, lookup dict1 - avoid Inner
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    if (HasNullInKey(key2) || !dict1.Contains(key2)) {
                        WriteValuesImpl<EJoinKind::Full>(NUdf::TUnboxedValuePod(), payload2, resList, ctx);
                    }
                }
            }
            break;

        case EJoinKind::LeftOnly: {
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    if (HasNullInKey(key1) || !dict2.Contains(key1)) {
                        if (IsMulti1) {
                            TThresher<false>::DoForEachItem(payload1,
                                [&resList] (NUdf::TUnboxedValue&& item) {
                                    resList = resList.Append(std::move(item));
                                }
                            );
                        } else {
                            resList = resList.Append(std::move(payload1));
                        }
                    }
                }
            }
            break;

        case EJoinKind::RightOnly: {
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    if (HasNullInKey(key2) || !dict1.Contains(key2)) {
                        if (IsMulti2) {
                            TThresher<false>::DoForEachItem(payload2,
                                [&resList] (NUdf::TUnboxedValue&& item) {
                                    resList = resList.Append(std::move(item));
                                }
                            );
                        } else {
                            resList = resList.Append(std::move(payload2));
                        }
                    }
                }
            }
            break;

        case EJoinKind::Exclusion: {
            // traverse dict1, lookup dict2 - avoid Inner
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    if (HasNullInKey(key1) || !dict2.Contains(key1)) {
                        WriteValuesImpl<EJoinKind::Exclusion>(payload1, NUdf::TUnboxedValuePod(), resList, ctx);
                    }
                }
           }
           {
            // traverse dict2, lookup dict1 - avoid Inner
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    if (HasNullInKey(key2) || !dict1.Contains(key2)) {
                        WriteValuesImpl<EJoinKind::Exclusion>(NUdf::TUnboxedValuePod(), payload2, resList, ctx);
                    }
                }
            }
            break;

        case EJoinKind::LeftSemi: {
                const auto it = dict1.GetDictIterator();
                for (NUdf::TUnboxedValue key1, payload1; it.NextPair(key1, payload1);) {
                    Y_DEBUG_ABORT_UNLESS(!HasNullInKey(key1));
                    if (dict2.Contains(key1)) {
                        if (IsMulti1) {
                            TThresher<false>::DoForEachItem(payload1,
                                [&resList] (NUdf::TUnboxedValue&& item) {
                                    resList = resList.Append(std::move(item));
                                }
                            );
                        } else {
                            resList = resList.Append(std::move(payload1));
                        }
                    }
                }
            }
            break;

        case EJoinKind::RightSemi: {
                const auto it = dict2.GetDictIterator();
                for (NUdf::TUnboxedValue key2, payload2; it.NextPair(key2, payload2);) {
                    Y_DEBUG_ABORT_UNLESS(!HasNullInKey(key2));
                    if (dict1.Contains(key2)) {
                        if (IsMulti2) {
                            TThresher<false>::DoForEachItem(payload2,
                                [&resList] (NUdf::TUnboxedValue&& item) {
                                    resList = resList.Append(std::move(item));
                                }
                            );
                        } else {
                            resList = resList.Append(std::move(payload2));
                        }
                    }
                }
            }
            break;

        default:
            Y_ABORT("Unknown kind");
        }

        return ctx.HolderFactory.CreateDirectListHolder(std::move(resList));
    }

    IComputationNode* const Dict1;
    IComputationNode* const Dict2;
    const bool IsMulti1;
    const bool IsMulti2;
    const EJoinKind JoinKind;
    const std::vector<ui32> OptIndicies;
};

}

IComputationNode* WrapJoinDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto dict1node = callable.GetInput(0);
    const auto dict2node = callable.GetInput(1);
    const auto isMulti1Node = callable.GetInput(2);
    const auto isMulti2Node = callable.GetInput(3);
    const auto joinKindNode = callable.GetInput(4);

    const auto dict1type = AS_TYPE(TDictType, dict1node);
    const auto dict2type = AS_TYPE(TDictType, dict2node);
    const auto keyType = dict1type->GetKeyType();
    MKQL_ENSURE(keyType->IsSameType(*dict2type->GetKeyType()), "Dict key types must be the same");

    const bool multi1 = AS_VALUE(TDataLiteral, isMulti1Node)->AsValue().Get<bool>();
    const bool multi2 = AS_VALUE(TDataLiteral, isMulti2Node)->AsValue().Get<bool>();
    const ui32 rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();

    const auto dict1 = LocateNode(ctx.NodeLocator, callable, 0);
    const auto dict2 = LocateNode(ctx.NodeLocator, callable, 1);

    if (keyType->IsTuple()) {
        const auto tupleType = AS_TYPE(TTupleType, keyType);
        std::vector<ui32> indicies;
        indicies.reserve(tupleType->GetElementsCount());
        for (ui32 i = 0U; i < tupleType->GetElementsCount(); ++i) {
            if (tupleType->GetElementType(i)->IsOptional()) {
                indicies.emplace_back(i);
            }
        }

        return new TJoinDictWrapper<true>(ctx.Mutables, dict1, dict2, multi1, multi2, GetJoinKind(rawKind), std::move(indicies));
    } else {
        return new TJoinDictWrapper<false>(ctx.Mutables, dict1, dict2, multi1, multi2, GetJoinKind(rawKind));
    }
}

}
}
