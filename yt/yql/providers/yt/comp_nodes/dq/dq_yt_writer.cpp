#include "dq_yt_writer.h"

#include <library/cpp/yson/node/node_io.h>
#include <util/generic/size_literals.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/yql/providers/yt/codec/yt_codec_io.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yt/yql/providers/yt/common/yql_names.h>

#include <utility>

namespace NYql::NDqs {
namespace {

using namespace NKikimr::NMiniKQL;
using namespace NYT;

class TYtDqWideWriteWrapper final : public TStatefulFlowCodegeneratorNode<TYtDqWideWriteWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TYtDqWideWriteWrapper>;

    class TWriterState : public TComputationValue<TWriterState> {
    public:
        TWriterState(
            TMemoryUsageInfo* memInfo,
            IClientPtr&& client,
            ITransactionPtr&& transaction,
            THolder<TMkqlIOSpecs>&& specs,
            TRawTableWriterPtr&& outStream,
            THolder<TMkqlWriterImpl>&& writer,
            size_t width,
            IComputationWideFlowNode* flow
        )
            : TComputationValue<TWriterState>(memInfo)
            , Client_(std::move(client)), Transaction_(std::move(transaction))
            , Specs_(std::move(specs)), OutStream_(std::move(outStream)), Writer_(std::move(writer))
            , Values_(width), Fields_(GetPointers(Values_)), Flow_(flow)
        {}

        ~TWriterState() override {
            try {
                Finish();
            } catch (...) {
            }
        }

        EFetchResult FetchRead(TComputationContext& ctx) {
            switch(Flow_->FetchValues(ctx, Fields_.data())) {
            case EFetchResult::One:
                Writer_->AddFlatRow(Values_.data());
                return EFetchResult::One;
            case EFetchResult::Yield:
                return EFetchResult::Yield;
            case EFetchResult::Finish:
                Finish();
                return EFetchResult::Finish;
            }
        }

        void Finish() {
            if (!Finished_) {
                Writer_->Finish();
                OutStream_->Finish();
                Values_.clear();
                Fields_.clear();
            }
            Finished_ = true;
        }
    private:
        bool Finished_ = false;
        const IClientPtr Client_;
        const ITransactionPtr Transaction_;
        const THolder<TMkqlIOSpecs> Specs_;
        const TRawTableWriterPtr OutStream_;
        const THolder<TMkqlWriterImpl> Writer_;
        std::vector<NUdf::TUnboxedValue> Values_;
        std::vector<NUdf::TUnboxedValue*> Fields_;
        IComputationWideFlowNode* Flow_;
    };

public:
    TYtDqWideWriteWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        std::vector<EValueRepresentation>&& representations,
        const std::string_view& clusterName,
        const std::string_view& token,
        const TRichYPath& path,
        const NYT::TNode& outSpec,
        const NYT::TNode& writerOptions,
        THolder<NCommon::TCodecContext>&& codecCtx
    )
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded, EValueRepresentation::Boxed)
        , Flow(flow)
        , Representations(std::move(representations))
        , ClusterName(clusterName)
        , Token(token)
        , Path(path)
        , OutSpec(outSpec)
        , WriterOptions(writerOptions)
        , CodecCtx(std::move(codecCtx))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        } else if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        auto result = static_cast<TWriterState*>(state.AsBoxed().Get())->FetchRead(ctx);
        switch (result) {
            case EFetchResult::One:
                return NUdf::TUnboxedValuePod::Void();
            case EFetchResult::Yield:
                return NUdf::TUnboxedValuePod::MakeYield();
            case EFetchResult::Finish:
                state = NUdf::TUnboxedValuePod::MakeFinish();
                return state;
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto returnOne = BasicBlock::Create(context, "returnOne", ctx.Func);
        const auto returnYield = BasicBlock::Create(context, "returnYield", ctx.Func);
        const auto returnFinish = BasicBlock::Create(context, "returnFinish", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto output = PHINode::Create(valueType, 2U, "output", exit);
        const auto check = new LoadInst(valueType, statePtr, "check", block);
        const auto choise = SwitchInst::Create(check, next, 2U, block);
        // if (state.IsFinish()) => goto returnFinish
        choise->addCase(GetFinish(context), returnFinish);
        // if (state.IsInvalid()) => goto MakeState(ctx, state)
        choise->addCase(GetInvalid(context), init);
        // Calling MakeState
        {
            block = init;

            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TYtDqWideWriteWrapper::MakeState>());
            const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
            const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "make_state", block);
            CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
            BranchInst::Create(next, block);
        }

        {
            // Calling state->FetchRead()
            block = next;
            const auto state = new LoadInst(valueType, statePtr, "state", block);
            const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
            const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, structPtrType, "state_arg", block);

            const auto statusType = Type::getInt32Ty(context);

            const auto fetchFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TWriterState::FetchRead>());
            const auto fetchType = FunctionType::get(statusType, {stateArg->getType(), ctx.Ctx->getType()}, false);
            const auto fetchFunPtr = CastInst::Create(Instruction::IntToPtr, fetchFunc, PointerType::getUnqual(fetchType), "fetch_function", block);
            const auto fetchResult = CallInst::Create(fetchType, fetchFunPtr, {stateArg, ctx.Ctx}, "call_fetch_fun", block);

            const auto way = SwitchInst::Create(fetchResult, returnOne, 2U, block);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), returnYield);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), done);
        }
        {
            block = returnOne;
            output->addIncoming(GetFalse(context), block);
            BranchInst::Create(exit, block);
        }
        {
            block = returnYield;
            output->addIncoming(GetYield(context), block);
            BranchInst::Create(exit, block);
        }
        {
            block = returnFinish;
            output->addIncoming(GetFinish(context), block);
            BranchInst::Create(exit, block);
        }
        {
            block = done;
            // state = MakeFinish()
            UnRefBoxed(statePtr, ctx, block);
            new StoreInst(GetFinish(context), statePtr, block);
            BranchInst::Create(returnFinish, block);
        }

        block = exit;
        return output;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        TString token;
        if (NUdf::TStringRef tokenRef(Token); ctx.Builder->GetSecureParam(tokenRef, tokenRef)) {
            token = tokenRef;
        }

        NYT::TCreateClientOptions createOpts;
        if (token) {
            createOpts.Token(token);
        }

        auto client = NYT::CreateClient(ClusterName, createOpts);
        auto transaction = client->AttachTransaction(Path.TransactionId_.GetRef());
        auto specs = MakeHolder<TMkqlIOSpecs>();
        specs->SetUseSkiff("");
        specs->Init(*CodecCtx, OutSpec);
        auto path = Path;
        path.TransactionId_.Clear();
        NYT::TTableWriterOptions options;
        options.Config(WriterOptions);
        auto outStream = transaction->CreateRawWriter(path, specs->MakeOutputFormat(), options);

        auto writer = MakeHolder<TMkqlWriterImpl>(outStream, 4_MB);
        writer->SetSpecs(*specs);

        state = ctx.HolderFactory.Create<TWriterState>(std::move(client), std::move(transaction), std::move(specs), std::move(outStream), std::move(writer), Representations.size(), Flow);
    }

    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    static std::vector<NUdf::TUnboxedValue*> GetPointers(std::vector<NUdf::TUnboxedValue>& array) {
        std::vector<NUdf::TUnboxedValue*> pointers;
        pointers.reserve(array.size());
        std::transform(array.begin(), array.end(), std::back_inserter(pointers), [](NUdf::TUnboxedValue& v) { return std::addressof(v); });
        return pointers;
    }

    IComputationWideFlowNode* const Flow;
    const std::vector<EValueRepresentation> Representations;

    const TString ClusterName;
    const TString Token;
    const TRichYPath Path;
    const NYT::TNode OutSpec;
    const NYT::TNode WriterOptions;
    const THolder<NCommon::TCodecContext> CodecCtx;

    std::vector<NUdf::TUnboxedValue> Values;
    const std::vector<NUdf::TUnboxedValue*> Fields;
};

}

IComputationNode* WrapYtDqRowsWideWrite(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    YQL_ENSURE(callable.GetInputsCount() == 6, "Expected six args.");

    const auto& clusterName = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef();
    const auto& token = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef();

    TRichYPath richYPath;
    Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().AsStringRef())));

    const NYT::TNode outSpec(NYT::NodeFromYsonString((AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().AsStringRef())));
    const NYT::TNode writerOptions(NYT::NodeFromYsonString((AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().AsStringRef())));
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);

    std::vector<EValueRepresentation> representations;
    auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType()));
    representations.reserve(wideComponents.size());
    for (ui32 i = 0U; i < wideComponents.size(); ++i) {
        representations.emplace_back(GetValueRepresentation(wideComponents[i]));
    }

    auto codecCtx = MakeHolder<NCommon::TCodecContext>(ctx.Env, ctx.FunctionRegistry, &ctx.HolderFactory);
    return new TYtDqWideWriteWrapper(ctx.Mutables, static_cast<IComputationWideFlowNode*>(node), std::move(representations), clusterName, token, richYPath, outSpec, writerOptions, std::move(codecCtx));
}

} // NYql
