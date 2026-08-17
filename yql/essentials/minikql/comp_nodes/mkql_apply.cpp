#include "mkql_apply.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <library/cpp/containers/stack_array/stack_array.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

class TApplyWrapper: public TMutableCodegeneratorPtrNode<TApplyWrapper> {
    using TBaseComputation = TMutableCodegeneratorPtrNode<TApplyWrapper>;

public:
    struct TKernelState: public arrow::compute::KernelState {
        explicit TKernelState(ui32 argsCount)
            : Alloc(__LOCATION__)
            , MemInfo("Apply")
            , HolderFactory(Alloc.Ref(), MemInfo)
            , ValueBuilder(HolderFactory, NUdf::EValidatePolicy::Exception)
            , Args(argsCount)
        {
            Alloc.Ref().EnableArrowTracking = false;
            Alloc.Release();
        }

        ~TKernelState() override {
            Alloc.Acquire();
        }

        TScopedAlloc Alloc;
        TMemoryUsageInfo MemInfo;
        THolderFactory HolderFactory;
        TDefaultValueBuilder ValueBuilder;
        TVector<NUdf::TUnboxedValue> Args;
    };

    class TArrowNode: public IArrowKernelComputationNode {
    public:
        TArrowNode(const TApplyWrapper* parent, NUdf::TUnboxedValue callable, TType* returnType, const TVector<TType*>& argsTypes)
            : Parent_(parent)
            , Callable_(std::move(callable))
            , ArgsValuesDescr_(ToValueDescr(argsTypes))
            , Kernel_(ConvertToInputTypes(argsTypes), ConvertToOutputType(returnType), [this](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                auto& state = dynamic_cast<TKernelState&>(*ctx->state());
                auto guard = Guard(state.Alloc);
                Y_ENSURE(batch.values.size() == state.Args.size());
                for (ui32 i = 0; i < batch.values.size(); ++i) {
                    state.Args[i] = state.HolderFactory.CreateArrowBlock(arrow::Datum(batch.values[i]), NYql::EDatumValidationMode::None);
                }

                const auto& ret = Callable_.Run(&state.ValueBuilder, state.Args.data());
                *res = TArrowBlock::From(ret).GetDatum();
                return arrow::Status::OK();
            })
        {
            Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
            Kernel_.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
            Kernel_.init = [argsCount = argsTypes.size()](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
                auto state = std::make_unique<TKernelState>(argsCount);
                return arrow::Result(std::move(state));
            };
        }

        TStringBuf GetKernelName() const final {
            return "Apply";
        }

        const arrow::compute::ScalarKernel& GetArrowKernel() const override {
            return Kernel_;
        }

        const std::vector<arrow::ValueDescr>& GetArgsDesc() const override {
            return ArgsValuesDescr_;
        }

        const IComputationNode* GetArgument(ui32 index) const override {
            return Parent_->ArgNodes_[index];
        }

    private:
        const TApplyWrapper* Parent_;
        const NUdf::TUnboxedValue Callable_;
        const std::vector<arrow::ValueDescr> ArgsValuesDescr_;
        arrow::compute::ScalarKernel Kernel_;
    };
    friend class TArrowNode;

    TApplyWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* callableNode,
                  TComputationNodePtrVector&& argNodes, ui32 usedArgs, const NUdf::TSourcePosition& pos, TCallableType* callableType)
        : TBaseComputation(mutables, kind)
        , CallableNode_(callableNode)
        , ArgNodes_(std::move(argNodes))
        , UsedArgs_(usedArgs)
        , Position_(pos)
        , CallableType_(callableType)
    {
        Stateless_ = false;
    }

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final {
        if (UsedArgs_ != CallableType_->GetArgumentsCount()) {
            return {};
        }

        std::shared_ptr<arrow::DataType> t;
        if (!CallableType_->GetReturnType()->IsBlock() ||
            !ConvertArrowType(AS_TYPE(TBlockType, CallableType_->GetReturnType())->GetItemType(), t)) {
            return {};
        }

        TVector<TType*> argsTypes;
        for (ui32 i = 0; i < CallableType_->GetArgumentsCount(); ++i) {
            argsTypes.push_back(CallableType_->GetArgumentType(i));
            if (!CallableType_->GetArgumentType(i)->IsBlock() ||
                !ConvertArrowType(AS_TYPE(TBlockType, CallableType_->GetArgumentType(i))->GetItemType(), t)) {
                return {};
            }
        }

        const auto callable = CallableNode_->GetValue(ctx);
        return std::make_unique<TArrowNode>(this, callable, CallableType_->GetReturnType(), argsTypes);
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        NStackArray::TStackArray<NUdf::TUnboxedValue> values(ALLOC_ON_STACK(NUdf::TUnboxedValue, UsedArgs_));
        for (size_t i = 0; i < UsedArgs_; ++i) {
            if (const auto valueNode = ArgNodes_[i]) {
                values[i] = valueNode->GetValue(ctx);
            }
        }

        const auto callable = CallableNode_->GetValue(ctx);
        const auto prev = ctx.CalleePosition;
        ctx.CalleePosition = &Position_;
        const auto ret = callable.Run(ctx.Builder, values.data());
        ctx.CalleePosition = prev;
        return ret;
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto idxType = Type::getInt32Ty(context);
        const auto valType = Type::getInt128Ty(context);
        const auto arrayType = ArrayType::get(valType, ArgNodes_.size());
        const auto args = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(arrayType, 0U, "args", &ctx.Func->getEntryBlock().back()) : new AllocaInst(arrayType, 0U, "args", block);

        ui32 i = 0;
        std::vector<std::pair<Value*, EValueRepresentation>> argsv;
        argsv.reserve(ArgNodes_.size());
        for (const auto node : ArgNodes_) {
            const auto argPtr = GetElementPtrInst::CreateInBounds(arrayType, args, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, i++)}, "arg_ptr", block);
            if (node) {
                GetNodeValue(argPtr, node, ctx, block);
                argsv.emplace_back(argPtr, node->GetRepresentation());
            } else {
                new StoreInst(ConstantInt::get(valType, 0), argPtr, block);
            }
        }

        if (const auto codegen = dynamic_cast<ICodegeneratorRunNode*>(CallableNode_)) {
            codegen->CreateRun(ctx, block, pointer, args);
        } else {
            const auto callable = GetNodeValue(CallableNode_, ctx, block);
            // XXX: Since <GetNodeValue> method releases the
            // UnboxedValue, obtained via <GetValue>, the only
            // reference to this UnboxedValue remains in mutables.
            // However, it might be invalidated within its <Run>
            // method, so anchor the callable value to prevent its
            // destruction while running its <Run> method.
            ValueAddRef(CallableNode_->GetRepresentation(), callable, ctx, block);
            const auto calleePtr = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(idxType, 0), ConstantInt::get(idxType, 6)}, "callee_ptr", block);
            const auto previous = new LoadInst(PointerType::getUnqual(GetSourcePosType(context)), calleePtr, "previous", block);
            const auto callee = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), ui64(&Position_)), previous->getType(), "callee", block);
            new StoreInst(callee, calleePtr, block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Run>(pointer, callable, ctx.Codegen, block, ctx.GetBuilder(), args);
            new StoreInst(previous, calleePtr, block);
            // XXX: Release the anchor to the callable, taken above.
            ValueUnRef(CallableNode_->GetRepresentation(), callable, ctx, block);
        }
        for (const auto& arg : argsv) {
            ValueUnRef(arg.second, arg.first, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(CallableNode_);
        for (const auto node : ArgNodes_) {
            if (node) {
                DependsOn(node);
            }
        }
    }

    IComputationNode* const CallableNode_;
    const TComputationNodePtrVector ArgNodes_;
    const ui32 UsedArgs_;
    const NUdf::TSourcePosition Position_;
    TCallableType* CallableType_;
};

} // namespace

IComputationNode* WrapApply(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 5, "Expected at least 5 arguments");
    constexpr size_t posArgs = 3;

    const auto function = callable.GetInput(0);
    MKQL_ENSURE(!function.IsImmediate() && function.GetNode()->GetType()->IsCallable(),
                "First argument of Apply must be a callable");

    const auto functionCallable = static_cast<TCallable*>(function.GetNode());
    const auto returnType = functionCallable->GetType()->GetReturnType();
    MKQL_ENSURE(returnType->IsCallable(), "Expected callable as return type");

    const TStringBuf file = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();
    const ui32 row = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<ui32>();
    const ui32 column = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<ui32>();

    const ui32 inputsCount = callable.GetInputsCount() - posArgs;
    const ui32 argsCount = inputsCount - 2;

    const ui32 dependentCount = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    MKQL_ENSURE(dependentCount <= argsCount, "Too many dependent nodes");
    const ui32 usedArgs = argsCount - dependentCount;

    auto callableType = static_cast<TCallableType*>(returnType);
    MKQL_ENSURE(usedArgs <= callableType->GetArgumentsCount(), "Too many arguments");
    MKQL_ENSURE(usedArgs >= callableType->GetArgumentsCount() - callableType->GetOptionalArgumentsCount(), "Too few arguments");

    TComputationNodePtrVector argNodes(callableType->GetArgumentsCount() + dependentCount);
    for (ui32 i = 2; i < 2 + usedArgs; ++i) {
        argNodes[i - 2] = LocateNode(ctx.NodeLocator, callable, i + posArgs);
    }

    for (ui32 i = 2 + usedArgs; i < inputsCount; ++i) {
        argNodes[callableType->GetArgumentsCount() + i - 2 - usedArgs] = LocateNode(ctx.NodeLocator, callable, i + posArgs);
    }

    auto functionNode = LocateNode(ctx.NodeLocator, callable, 0);
    return new TApplyWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), functionNode, std::move(argNodes),
                             callableType->GetArgumentsCount(), NUdf::TSourcePosition(row, column, file), callableType);
}

} // namespace NKikimr::NMiniKQL
