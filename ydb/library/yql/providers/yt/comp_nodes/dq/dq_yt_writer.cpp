#include "dq_yt_writer.h"

#include <library/cpp/yson/node/node_io.h>
#include <util/generic/size_literals.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>

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
            THolder<TMkqlWriterImpl>&& writer
        )
            : TComputationValue<TWriterState>(memInfo)
            , Client(std::move(client)), Transaction(std::move(transaction))
            , Specs(std::move(specs)), OutStream(std::move(outStream)), Writer(std::move(writer))
        {}

        ~TWriterState() override {
            try {
                Finish();
            } catch (...) {
            }
        }

        void AddRow(const NUdf::TUnboxedValuePod* row) const {
            Writer->AddFlatRow(row);
        }

        void Finish() {
            if (!Finished) {
                Writer->Finish();
                OutStream->Finish();
            }
            Finished = true;
        }
    private:
        bool Finished = false;
        const IClientPtr Client;
        const ITransactionPtr Transaction;
        const THolder<TMkqlIOSpecs> Specs;
        const TRawTableWriterPtr OutStream;
        const THolder<TMkqlWriterImpl> Writer;
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
        , Values(Representations.size()), Fields(GetPointers(Values))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        } else if (state.IsInvalid())
            MakeState(ctx, state);

        switch (const auto ptr = static_cast<TWriterState*>(state.AsBoxed().Get()); Flow->FetchValues(ctx, Fields.data())) {
            case EFetchResult::One:
                ptr->AddRow(Values.data());
                return NUdf::TUnboxedValuePod::Void();
            case EFetchResult::Yield:
                return NUdf::TUnboxedValuePod::MakeYield();
            case EFetchResult::Finish:
                ptr->Finish();
                state = NUdf::TUnboxedValuePod::MakeFinish();
                return NUdf::TUnboxedValuePod::MakeFinish();
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto arrayType = ArrayType::get(valueType, Representations.size());
        const auto values = new AllocaInst(arrayType, 0U, "values", &ctx.Func->getEntryBlock().back());

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto output = PHINode::Create(valueType, 4U, "output", exit);
        output->addIncoming(GetFinish(context), block);

        const auto check = new LoadInst(valueType, statePtr, "check", block);
        const auto choise = SwitchInst::Create(check, next, 2U, block);
        choise->addCase(GetInvalid(context), init);
        choise->addCase(GetFinish(context), exit);

        block = init;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtDqWideWriteWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);

        BranchInst::Create(next, block);

        block = next;

        const auto result = GetNodeValues(Flow, ctx, block);

        output->addIncoming(GetYield(context), block);

        const auto way = SwitchInst::Create(result.first, work, 2U, block);
        way->addCase(ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Yield)), exit);
        way->addCase(ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Finish)), done);

        {
            block = work;

            TSmallVec<Value*> fields;
            fields.reserve(Representations.size());
            for (ui32 i = 0U; i < Representations.size(); ++i) {
                const auto pointer = GetElementPtrInst::CreateInBounds(arrayType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                fields.emplace_back(result.second[i](ctx, block));
                new StoreInst(fields.back(), pointer, block);
            }

            const auto state = new LoadInst(valueType, statePtr, "state", block);
            const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
            const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, structPtrType, "state_arg", block);

            const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWriterState::AddRow));
            const auto addType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), values->getType()}, false);
            const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "write", block);
            CallInst::Create(addType, addPtr, {stateArg, values}, "", block);

            for (ui32 i = 0U; i < Representations.size(); ++i) {
                ValueCleanup(Representations[i], fields[i], ctx, block);
            }

            output->addIncoming(GetFalse(context), block);
            BranchInst::Create(exit, block);
        }

        {
            block = done;

            const auto state = new LoadInst(valueType, statePtr, "state", block);
            const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
            const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, structPtrType, "state_arg", block);

            const auto finishFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWriterState::Finish));
            const auto finishType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType()}, false);
            const auto finishPtr = CastInst::Create(Instruction::IntToPtr, finishFunc, PointerType::getUnqual(finishType), "finish", block);
            CallInst::Create(finishType, finishPtr, {stateArg}, "", block);

            UnRefBoxed(statePtr, ctx, block);
            new StoreInst(GetFinish(context), statePtr, block);

            output->addIncoming(GetFinish(context), block);
            BranchInst::Create(exit, block);
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

        state = ctx.HolderFactory.Create<TWriterState>(std::move(client), std::move(transaction), std::move(specs), std::move(outStream), std::move(writer));
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
