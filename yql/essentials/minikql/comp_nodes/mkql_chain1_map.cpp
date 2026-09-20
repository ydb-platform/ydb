#include "mkql_chain1_map.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TComputationNodes {
    IComputationExternalNode* const ItemArg;
    IComputationExternalNode* const StateArg;
    IComputationNode* const InitItem;
    IComputationNode* const InitState;
    IComputationNode* const UpdateItem;
    IComputationNode* const UpdateState;
};

class TFold1MapFlowWrapper: public TStatefulFlowCodegeneratorNode<TFold1MapFlowWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TFold1MapFlowWrapper>;

public:
    TFold1MapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,
                         IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
                         IComputationNode* initItem, IComputationNode* initState,
                         IComputationNode* updateItem, IComputationNode* updateState)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded)
        ,
        Flow_(flow)
        , ComputationNodes_({.ItemArg = itemArg, .StateArg = stateArg, .InitItem = initItem, .InitState = initState, .UpdateItem = updateItem, .UpdateState = updateState})

    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        auto item = Flow_->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }

        ComputationNodes_.ItemArg->SetValue(ctx, std::move(item));

        const bool init = state.IsInvalid();
        const auto value = (init ? ComputationNodes_.InitItem : ComputationNodes_.UpdateItem)->GetValue(ctx);
        ComputationNodes_.StateArg->SetValue(ctx, (init ? ComputationNodes_.InitState : ComputationNodes_.UpdateState)->GetValue(ctx));

        if (init) {
            state = NUdf::TUnboxedValuePod(true);
        }

        return value;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes_.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes_.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", done);

        const auto item = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(done, good, IsSpecial(item, block, context), block);

        block = good;
        codegenItemArg->CreateSetValue(ctx, block, item);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto state = new LoadInst(valueType, statePtr, "load", block);
        BranchInst::Create(init, next, IsInvalid(state, block, context), block);

        block = init;
        const auto one = GetNodeValue(ComputationNodes_.InitItem, ctx, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(ComputationNodes_.InitState, ctx, block));
        result->addIncoming(one, block);
        new StoreInst(GetTrue(context), statePtr, block);
        BranchInst::Create(done, block);

        block = next;
        const auto two = GetNodeValue(ComputationNodes_.UpdateItem, ctx, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(ComputationNodes_.UpdateState, ctx, block));
        result->addIncoming(two, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            DependsOn(flow, ComputationNodes_.InitItem);
            DependsOn(flow, ComputationNodes_.InitState);
            DependsOn(flow, ComputationNodes_.UpdateItem);
            DependsOn(flow, ComputationNodes_.UpdateState);
            Own(flow, ComputationNodes_.ItemArg);
            Own(flow, ComputationNodes_.StateArg);
        }
    }

    IComputationNode* const Flow_;
    const TComputationNodes ComputationNodes_;
};

template <bool IsStream>
class TBaseChain1MapWrapper {
public:
    class TListValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, const TComputationNodes& computationNodes)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx_(compCtx)
                , Iter_(std::move(iter))
                , ComputationNodes_(computationNodes)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (Length_ > 0) {
                    ComputationNodes_.StateArg->SetValue(CompCtx_, std::move(PreservedState_));
                }

                NYql::NUdf::TUnboxedValue fetchResult;
                if (!Iter_.Next(fetchResult)) {
                    return false;
                }
                ComputationNodes_.ItemArg->SetValue(CompCtx_, std::move(fetchResult));
                ++Length_;

                auto itemNode = Length_ == 1 ? ComputationNodes_.InitItem : ComputationNodes_.UpdateItem;
                auto stateNode = Length_ == 1 ? ComputationNodes_.InitState : ComputationNodes_.UpdateState;
                value = itemNode->GetValue(CompCtx_);
                PreservedState_ = stateNode->GetValue(CompCtx_);
                return true;
            }

            TComputationContext& CompCtx_;
            const NUdf::TUnboxedValue Iter_;
            const TComputationNodes& ComputationNodes_;
            ui64 Length_ = 0;
            NUdf::TUnboxedValue PreservedState_;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, const TComputationNodes& computationNodes)
            : TCustomListValue(memInfo)
            , CompCtx_(compCtx)
            , List_(std::move(list))
            , ComputationNodes_(computationNodes)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx_.HolderFactory.Create<TIterator>(CompCtx_, List_.GetListIterator(), ComputationNodes_);
        }

        ui64 GetListLength() const final {
            if (!Length_) {
                Length_ = List_.GetListLength();
            }

            return *Length_;
        }

        bool HasListItems() const final {
            if (!HasItems_) {
                HasItems_ = List_.HasListItems();
            }

            return *HasItems_;
        }

        TComputationContext& CompCtx_;
        const NUdf::TUnboxedValue List_;
        const TComputationNodes& ComputationNodes_;
    };

    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, const TComputationNodes& computationNodes)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , List_(std::move(list))
            , ComputationNodes_(computationNodes)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) final {
            NYql::NUdf::TUnboxedValue fetchResult;
            const auto status = List_.Fetch(fetchResult);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            ComputationNodes_.ItemArg->SetValue(CompCtx_, std::move(fetchResult));

            ++Length_;

            auto itemNode = Length_ == 1 ? ComputationNodes_.InitItem : ComputationNodes_.UpdateItem;
            auto stateNode = Length_ == 1 ? ComputationNodes_.InitState : ComputationNodes_.UpdateState;
            value = itemNode->GetValue(CompCtx_);
            ComputationNodes_.StateArg->SetValue(CompCtx_, stateNode->GetValue(CompCtx_));
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx_;
        const NUdf::TUnboxedValue List_;
        const TComputationNodes& ComputationNodes_;
        ui64 Length_ = 0;
    };

    TBaseChain1MapWrapper(IComputationNode* list, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
                          IComputationNode* initItem, IComputationNode* initState,
                          IComputationNode* updateItem, IComputationNode* updateState)
        : List(list)
        , ComputationNodes({.ItemArg = itemArg, .StateArg = stateArg, .InitItem = initItem, .InitState = initState, .UpdateItem = updateItem, .UpdateState = updateState})
    {
    }

#ifndef MKQL_DISABLE_CODEGEN
    template <bool IsFirst>
    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto newItem = IsFirst ? ComputationNodes.InitItem : ComputationNodes.UpdateItem;
        const auto newState = IsFirst ? ComputationNodes.InitState : ComputationNodes.UpdateState;

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = IsStream ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto funcType = IsStream
                                  ? FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType)}, /*isVarArg=*/false)
                                  : FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto stateArg = IsStream ? nullptr : &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = static_cast<Value*>(containerArg);

        if constexpr (IsStream) {
            Y_ABORT_UNLESS(stateArg == nullptr);
        } else {
            if constexpr (!IsFirst) {
                codegenStateArg->CreateSetValue(ctx, block, stateArg);
            }
        }

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto [status, itemPtr] = RefValueWithCallResult(codegenItemArg, ctx, block, [&](Value* itemPtr) {
            return IsStream ? CallBoxedValueFetch(container, ctx, block, itemPtr) : CallBoxedValueNext(container, ctx, block, itemPtr);
        });

        const auto icmp = IsStream ? CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block) : CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::getFalse(context), "cond", block);

        BranchInst::Create(done, good, icmp, block);
        block = good;

        SafeUnRefUnboxedOne(valuePtr, ctx, block);
        GetNodeValue(valuePtr, newItem, ctx, block);

        const auto nextState = GetNodeValue(newState, ctx, block);

        if constexpr (IsStream) {
            codegenStateArg->CreateSetValue(ctx, block, nextState);
        } else {
            ValueUnRef(EValueRepresentation::Any, stateArg, ctx, block);
            new StoreInst(nextState, stateArg, block);
            ValueAddRef(EValueRepresentation::Any, stateArg, ctx, block);
        }

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TChainMapPtr = std::conditional_t<IsStream, TStreamCodegenValueOne::TFetchPtr, TListCodegenValueOne::TNextPtr>;

    Function* MapFuncOne = nullptr;
    Function* MapFuncTwo = nullptr;

    TChainMapPtr MapOne = nullptr;
    TChainMapPtr MapTwo = nullptr;
#endif

    IComputationNode* const List;
    const TComputationNodes ComputationNodes;
};

class TStreamChain1MapWrapper: public TCustomValueCodegeneratorNode<TStreamChain1MapWrapper>, private TBaseChain1MapWrapper<true> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TStreamChain1MapWrapper>;
    using TBaseWrapper = TBaseChain1MapWrapper<true>;

public:
    TStreamChain1MapWrapper(TComputationMutables& mutables, IComputationNode* list,
                            IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
                            IComputationNode* initItem, IComputationNode* initState,
                            IComputationNode* updateItem, IComputationNode* updateState)
        : TBaseComputation(mutables)
        , TBaseWrapper(list, itemArg, stateArg, initItem, initState, updateItem, updateState)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && MapOne && MapTwo) {
            return ctx.HolderFactory.Create<TStreamCodegenValueOne>(MapOne, MapTwo, &ctx, List->GetValue(ctx));
        }
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, List->GetValue(ctx), ComputationNodes);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(ComputationNodes.InitItem);
        DependsOn(ComputationNodes.InitState);
        DependsOn(ComputationNodes.UpdateItem);
        DependsOn(ComputationNodes.UpdateState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        MapFuncOne = GenerateMapper<true>(codegen, TBaseComputation::MakeName("Fetch_One"));
        MapFuncTwo = GenerateMapper<false>(codegen, TBaseComputation::MakeName("Fetch_Two"));
        codegen.ExportSymbol(MapFuncOne);
        codegen.ExportSymbol(MapFuncTwo);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (MapFuncOne) {
            MapOne = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncOne));
        }
        if (MapFuncTwo) {
            MapTwo = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncTwo));
        }
    }
#endif
};

class TListChain1MapWrapper: public TBothWaysCodegeneratorNode<TListChain1MapWrapper>, private TBaseChain1MapWrapper<false> {
    using TBaseComputation = TBothWaysCodegeneratorNode<TListChain1MapWrapper>;
    using TBaseWrapper = TBaseChain1MapWrapper<false>;

public:
    TListChain1MapWrapper(TComputationMutables& mutables, IComputationNode* list,
                          IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
                          IComputationNode* initItem, IComputationNode* initState,
                          IComputationNode* updateItem, IComputationNode* updateState)
        : TBaseComputation(mutables)
        , TBaseWrapper(list, itemArg, stateArg, initItem, initState, updateItem, updateState)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(size, items);
            if (size) {
                ComputationNodes.ItemArg->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                *items++ = ComputationNodes.InitItem->GetValue(ctx);
                ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.InitState->GetValue(ctx));
                while (--size) {
                    ComputationNodes.ItemArg->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                    *items++ = ComputationNodes.UpdateItem->GetValue(ctx);
                    ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.UpdateState->GetValue(ctx));
                }
            }
            return result;
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), ComputationNodes);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<TListCodegenValueOne>(MapOne, MapTwo, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto list = GetNodeValue(List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto map = PHINode::Create(list->getType(), 3U, "map", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

            const auto itemsPtr = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(elementsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()) : new AllocaInst(elementsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, size, itemsPtr, block);
            const auto items = new LoadInst(elementsType, itemsPtr, "items", block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

            const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 0), "good", block);
            map->addIncoming(array, block);

            BranchInst::Create(init, done, good, block);

            block = init;
            const auto head = new LoadInst(list->getType(), elements, "head", block);
            codegenItemArg->CreateSetValue(ctx, block, head);
            GetNodeValue(items, ComputationNodes.InitItem, ctx, block);
            const auto state = GetNodeValue(ComputationNodes.InitState, ctx, block);
            codegenStateArg->CreateSetValue(ctx, block, state);

            const auto index = PHINode::Create(size->getType(), 2U, "index", loop);
            index->addIncoming(ConstantInt::get(size->getType(), 1), block);
            BranchInst::Create(loop, block);

            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, index, "more", block);
            BranchInst::Create(next, stop, more, block);

            block = next;
            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "src", block);
            const auto item = new LoadInst(list->getType(), src, "item", block);
            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {index}, "dst", block);
            GetNodeValue(dst, ComputationNodes.UpdateItem, ctx, block);
            const auto newState = GetNodeValue(ComputationNodes.UpdateState, ctx, block);
            codegenStateArg->CreateSetValue(ctx, block, newState);
            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(size->getType(), 1), "plus", block);
            index->addIncoming(plus, block);
            BranchInst::Create(loop, block);

            block = stop;
            if (List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }
            map->addIncoming(array, block);
            BranchInst::Create(done, block);
        }

        {
            block = lazy;

            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            const auto value = EmitFunctionCall<&TListChain1MapWrapper::MakeLazyList>(list->getType(), {self, ctx.Ctx, list}, ctx, block);
            map->addIncoming(value, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return map;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(ComputationNodes.InitItem);
        DependsOn(ComputationNodes.InitState);
        DependsOn(ComputationNodes.UpdateItem);
        DependsOn(ComputationNodes.UpdateState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChain1MapWrapper>::GenerateFunctions(codegen);
        MapFuncOne = GenerateMapper<true>(codegen, TBaseComputation::MakeName("Next_One"));
        MapFuncTwo = GenerateMapper<false>(codegen, TBaseComputation::MakeName("Next_Two"));
        codegen.ExportSymbol(MapFuncOne);
        codegen.ExportSymbol(MapFuncTwo);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChain1MapWrapper>::FinalizeFunctions(codegen);
        if (MapFuncOne) {
            MapOne = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncOne));
        }
        if (MapFuncTwo) {
            MapTwo = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncTwo));
        }
    }
#endif
};

} // namespace

IComputationNode* WrapChain1Map(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initItem = LocateNode(ctx.NodeLocator, callable, 2);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 3);
    const auto updateItem = LocateNode(ctx.NodeLocator, callable, 5);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 6);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto stateArg = LocateExternalNode(ctx.NodeLocator, callable, 4);
    if (type->IsFlow()) {
        return new TFold1MapFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    } else if (type->IsStream()) {
        return new TStreamChain1MapWrapper(ctx.Mutables, flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    } else if (type->IsList()) {
        return new TListChain1MapWrapper(ctx.Mutables, flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

} // namespace NKikimr::NMiniKQL
