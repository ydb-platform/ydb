#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/minikql/async/compute/compute.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_terminator.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <atomic>
#include <thread>
#include <deque>

namespace NKikimr::NMiniKQL::NAsync {
namespace {

struct TProvider final: IProvider {
    std::vector<std::shared_ptr<ICompletion>> Calls;
    std::vector<TString> Payloads;
    size_t Cancelled = 0;
    bool Inline = false;
    std::function<void()> OnStart;
    void Start(TOperationId, std::shared_ptr<const TRequest> request, std::shared_ptr<ICompletion> completion) override {
        Calls.push_back(completion);
        Payloads.push_back(request->Payload);
        if (OnStart) {
            OnStart();
        }
        if (Inline) {
            Reply(Calls.size() - 1);
        }
    }
    void Cancel(TOperationId) noexcept override {
        ++Cancelled;
    }
    void Reply(size_t i) {
        Calls.at(i)->Complete(Payloads.at(i));
        Calls.at(i)->Release();
    }
};

TExecutionSettings DeterministicSettings() {
    TExecutionSettings settings;
    settings.CpuBudgetUs = 0; // functional tests use deterministic step quanta
    return settings;
}

// A real serializable source cursor, not an Iterator restarted at row zero.
// The engine adapter is responsible for establishing this input cut in D6.
class TCheckpointSource final: public TStatefulSourceComputationNode<TCheckpointSource, true> {
    using TBase = TStatefulSourceComputationNode<TCheckpointSource, true>;
    class TStream final: public TComputationValue<TStream> {
    public:
        TStream(TMemoryUsageInfo* memory, ui64 count, std::shared_ptr<TExecutionServices> services)
            : TComputationValue<TStream>(memory)
            , Count(count)
            , Services(std::move(services))
        {
        }
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& out) override {
            if (Services->IsInputPaused()) {
                return NUdf::EFetchStatus::Yield;
            }
            if (Position == Count) {
                return NUdf::EFetchStatus::Finish;
            }
            out = NUdf::TUnboxedValuePod(++Position);
            return NUdf::EFetchStatus::Ok;
        }
        bool HasListItems() const override {
            return false;
        }
        NUdf::TUnboxedValue Save() const override {
            TString bytes;
            WriteUi64(bytes, Position);
            return MakeString(bytes);
        }
        bool Load2(const NUdf::TUnboxedValue& value) override {
            TString bytes;
            auto it = value.GetListIterator();
            NUdf::TUnboxedValue chunk;
            while (it.Next(chunk)) {
                bytes += chunk.AsStringRef();
            }
            TStringBuf in(bytes);
            Position = ReadUi64(in);
            MKQL_ENSURE(Position <= Count && in.empty(), "Invalid test source state");
            return true;
        }
        ui64 Position = 0;
        const ui64 Count;
        const std::shared_ptr<TExecutionServices> Services;
    };

public:
    TCheckpointSource(TComputationMutables& mutables, ui64 count)
        : TBase(mutables)
        , Count(count)
    {
    }
    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const override {
        auto& value = ValueRef(ctx);
        if (value.IsInvalid()) {
            value = ctx.HolderFactory.Create<TStream>(Count, ctx.AsyncServices);
        } else if (value.HasListItems()) {
            NUdf::TUnboxedValue stream = ctx.HolderFactory.Create<TStream>(Count, ctx.AsyncServices);
            stream.Load2(value);
            value = std::move(stream);
        }
        return value;
    }

private:
    void RegisterDependencies() const override {
    }
    const ui64 Count;
};
IComputationNode* CheckpointTestFactory(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    if (callable.GetType()->GetName() == "CheckpointTestSource") {
        return new TCheckpointSource(ctx.Mutables, AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().Get<ui64>());
    }
    return nullptr;
}
struct TEchoReplayCodec final: IReplayCodec {
    TString Contract() const override {
        return "test-echo/v1";
    }
    TString Encode(const TRequest& request) const override {
        UNIT_ASSERT_VALUES_EQUAL(request.Operation, "echo");
        return request.Payload;
    }
    TRequest Decode(TStringBuf descriptor) const override {
        return {.Operation = "echo", .Payload = TString(descriptor), .MaxResponseBytes = 1024};
    }
};
TExecutionSettings CheckpointSettings(ECheckpointMode mode) {
    auto settings = DeterministicSettings();
    settings.Checkpoint.Mode = mode;
    settings.Checkpoint.PlanFingerprint = "test-graph/v1";
    return settings;
}

struct TFixture {
    TSetup<false> Setup;
    std::atomic<size_t> Events = 0;
    std::shared_ptr<TExecutionServices> Services;
    std::shared_ptr<TProvider> Provider = std::make_shared<TProvider>();
    TCallableRegistry Registry;
    THolder<IComputationGraph> Graph;
    NUdf::TUnboxedValue Stream;
    std::vector<NUdf::TUnboxedValue> Output;
    bool Finished = false;
    bool AckOutputs = true;
    std::deque<ui64> Unacked;

    explicit TFixture(size_t quantum = 1000, TLimits limits = {}, TExecutionSettings settings = DeterministicSettings())
        : Services(std::make_shared<TExecutionServices>([this](ui64) { ++Events; }, quantum, limits, std::move(settings)))
    {
        Services->RegisterProvider("native", Provider);
        auto* type = Setup.PgmBuilder->NewDataLiteral<ui64>(0).GetStaticType();
        Registry.Register({"Echo", type, type, "native", "echo", 1024});
    }
    ~TFixture() {
        Services->Shutdown();
    }
    TProgramBuilder& Pb() {
        return *Setup.PgmBuilder;
    }
    TRuntimeNode Echo(TRuntimeNode arg) {
        return Registry.Call(Pb(), "Echo", arg);
    }

    template <class F>
    void Build(std::vector<ui64> input, F body, ui32 window = 16, bool flow = false, bool serialize = false) {
        std::vector<TRuntimeNode> rows;
        auto* type = Pb().NewDataLiteral<ui64>(0).GetStaticType();
        for (auto value : input) {
            rows.push_back(Pb().NewDataLiteral<ui64>(value));
        }
        auto source = Pb().Iterator(Pb().NewList(type, rows), {});
        if (flow) {
            source = Pb().ToFlow(source, {});
        }
        auto row = Pb().Arg(type);
        auto program = BuildAsyncMap(Pb(), source, row, body(row), Registry, window);
        if (flow) {
            program = Pb().FromFlow(program);
        }
        if (serialize) {
            program = DeserializeRuntimeNode(SerializeRuntimeNode(program, *Setup.Env), *Setup.Env);
        }
        BuildProgram(program);
    }
    void BuildProgram(TRuntimeNode program, TString llvm = "OFF", bool materialize = true) {
        Setup.Explorer.Walk(program.GetNode(), Setup.Env->GetNodeStack());
        TComputationPatternOpts opts(Setup.Alloc.Ref(), *Setup.Env, GetTestFactory(CheckpointTestFactory), Setup.FunctionRegistry.Get(), NUdf::EValidateMode::Greedy, NUdf::EValidatePolicy::Exception, llvm, EGraphPerProcess::Multi);
        Setup.Pattern = MakeComputationPattern(Setup.Explorer, program, {}, opts);
        auto execution = opts.ToComputationOptions(*Setup.RandomProvider, *Setup.TimeProvider);
        execution.AsyncServices = Services;
        if (Services->GetCheckpointMode() != ECheckpointMode::Disabled) {
            execution.AsyncCheckpoint = Services;
        }
        Graph = Setup.Pattern->Clone(execution);
        Graph->Prepare();
        if (materialize) {
            Stream = Graph->GetValue();
        }
    }
    template <class F>
    void BuildCheckpoint(ui64 count, F body, bool materialize = true, ui32 window = 16, ui32 depth = 1, bool flow = false) {
        auto* type = Pb().NewDataLiteral<ui64>(0).GetStaticType();
        TCallableBuilder source(Pb().GetTypeEnvironment(), "CheckpointTestSource", TStreamType::Create(type, Pb().GetTypeEnvironment()), true);
        source.Add(Pb().NewDataLiteral<ui64>(count));
        auto program = TRuntimeNode(source.Build(), false);
        if (flow) {
            program = Pb().ToFlow(program, {});
        }
        for (ui32 i = 0; i < depth; ++i) {
            auto row = Pb().Arg(type);
            program = BuildAsyncMap(Pb(), program, row, body(row), Registry, window);
        }
        if (flow) {
            program = Pb().FromFlow(program);
        }
        BuildProgram(DeserializeRuntimeNode(SerializeRuntimeNode(program, *Setup.Env), *Setup.Env), "OFF", materialize);
    }
    void EnableReplay() {
        Services->RegisterReplayCodec("native", std::make_shared<TEchoReplayCodec>());
    }
    void AckAll() {
        while (!Unacked.empty()) {
            Services->OutputAcknowledged(Services->GetGeneration(), Unacked.front());
            Unacked.pop_front();
        }
    }
    void Cut(ui64 id = 1) {
        Services->BeginCheckpoint(id);
        Services->EstablishInputCut(id);
    }
    void Restore(TStringBuf snapshot) {
        Graph->LoadGraphState(snapshot);
        UNIT_ASSERT(Provider->Calls.empty());
        Stream = Graph->GetValue();
        UNIT_ASSERT(Provider->Calls.empty());
        Services->CompleteRestore();
        UNIT_ASSERT(Provider->Calls.empty());
    }
    void BuildNested(std::vector<ui64> input, ui32 depth, bool flow = false) {
        std::vector<TRuntimeNode> rows;
        auto* type = Pb().NewDataLiteral<ui64>(0).GetStaticType();
        for (auto value : input) {
            rows.push_back(Pb().NewDataLiteral<ui64>(value));
        }
        auto program = Pb().Iterator(Pb().NewList(type, rows), {});
        if (flow) {
            program = Pb().ToFlow(program, {});
        }
        for (ui32 i = 0; i < depth; ++i) {
            auto row = Pb().Arg(type);
            program = BuildAsyncMap(Pb(), program, row, Echo(row), Registry, 4);
        }
        if (flow) {
            program = Pb().FromFlow(program);
        }
        BuildProgram(DeserializeRuntimeNode(SerializeRuntimeNode(program, *Setup.Env), *Setup.Env));
    }
    TRuntimeNode EchoMessage(TStringBuf text) {
        auto message = Pb().NewDataLiteral<NUdf::EDataSlot::String>(text);
        Registry.Register({"EchoString", message.GetStaticType(), message.GetStaticType(), "native", "message", 1024});
        return Registry.Call(Pb(), "EchoString", message);
    }
    void Turn() {
        Services->BeginTurn();
        for (size_t i = 0; i != 1000; ++i) {
            NUdf::TUnboxedValue value;
            auto status = Stream.Fetch(value);
            if (status == NUdf::EFetchStatus::Ok) {
                Output.push_back(std::move(value));
                if (Services->GetCheckpointMode() != ECheckpointMode::Disabled) {
                    Unacked.push_back(Services->OutputProduced());
                    if (AckOutputs) {
                        AckAll();
                    }
                }
                continue;
            }
            Finished = status == NUdf::EFetchStatus::Finish;
            Services->EndTurn();
            return;
        }
        UNIT_FAIL("Fetch failed to yield within the quantum");
    }
    size_t Pump() {
        size_t turns = 0;
        while (Events.exchange(0)) {
            UNIT_ASSERT_C(++turns < 10000, "Waiting execution self-polls");
            Turn();
            if (Finished) {
                break;
            }
        }
        return turns;
    }
    void Check(std::initializer_list<ui64> expected) {
        UNIT_ASSERT_VALUES_EQUAL(Output.size(), expected.size());
        size_t i = 0;
        for (auto value : expected) {
            UNIT_ASSERT_VALUES_EQUAL(Output[i++].Get<ui64>(), value);
        }
    }
};

struct TYqlFixture {
    NYql::TExprContext Ctx;
    NYql::TTypeAnnotationContext Types;
    NYql::NCommon::TMkqlCommonCallableCompiler Compiler;
    TYqlFixture() {
        const auto* type = Ctx.MakeType<NYql::TDataExprType>(NUdf::EDataSlot::Uint64);
        Types.AsyncFunctions.emplace("Echo", NYql::TAsyncCallableDescriptor{type, type, "native", "echo", 1024});
    }
    NYql::TExprNode::TPtr Number(TStringBuf value) {
        return Ctx.NewCallable(NYql::TPositionHandle{}, "Uint64", {Ctx.NewAtom(NYql::TPositionHandle{}, value)});
    }
    NYql::TExprNode::TPtr Echo(NYql::TExprNode::TPtr value) {
        return Ctx.NewCallable(NYql::TPositionHandle{}, "AsyncInvoke", {Ctx.NewAtom(NYql::TPositionHandle{}, "Echo"), value});
    }
    NYql::TExprNode::TPtr Projection(bool distinctCalls) {
        auto row = Ctx.NewArgument(NYql::TPositionHandle{}, "row");
        auto call = Echo(row);
        auto body = Ctx.NewCallable(NYql::TPositionHandle{}, "Add", {call, distinctCalls ? Echo(row) : call});
        auto lambda = Ctx.NewLambda(NYql::TPositionHandle{}, Ctx.NewArguments(NYql::TPositionHandle{}, {row}), std::move(body));
        auto source = Ctx.NewCallable(NYql::TPositionHandle{}, "Iterator", {Ctx.NewCallable(NYql::TPositionHandle{}, "AsList", {Number("1"), Number("2")})});
        return Ctx.NewCallable(NYql::TPositionHandle{}, "AsyncMap", {source, lambda});
    }
    void Annotate(NYql::TExprNode::TPtr& expression) {
        UNIT_ASSERT_C(NYql::InstantAnnotateTypes(expression, Ctx, false, Types), Ctx.IssueManager.GetIssues().ToString());
    }
    TRuntimeNode Compile(NYql::TExprNode::TPtr expression, TFixture& f) {
        NYql::NCommon::TMkqlBuildContext ctx(Compiler, f.Pb(), Ctx);
        ctx.AsyncTypes = &Types;
        return NYql::NCommon::MkqlBuildExpr(*expression, ctx);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAsyncCheckpoint) {
Y_UNIT_TEST(DrainWaitsForRpcAndSlowDownstream) {
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Drain));
        f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); });
        f.Turn();
        UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 16);
        f.Cut();
        UNIT_ASSERT(!f.Services->CanSaveCheckpoint());
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->SaveGraphState(), yexception, "consistent input/output cut");
        f.AckOutputs = false;
        for (size_t i = 0; i < 16; ++i) {
            f.Provider->Reply(i);
        }
        f.Pump();
        UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
        UNIT_ASSERT(!f.Services->CanSaveCheckpoint());
        UNIT_ASSERT(!f.Services->CanForwardWatermark());
        f.AckAll();
        UNIT_ASSERT(f.Services->CanSaveCheckpoint());
        snapshot = f.Graph->SaveGraphState();
        UNIT_ASSERT(!f.Services->CanForwardBoundary());
        f.Services->SnapshotStored(f.Services->GetSnapshotId());
        UNIT_ASSERT(f.Services->CanForwardBoundary());
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Drain));
    f.Provider->Inline = true;
    f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(snapshot);
    UNIT_ASSERT(f.Services->CanForwardBoundary());
    f.Services->ReleaseBoundary();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 184);
    for (ui64 i = 0; i < 184; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 17);
    }
}
Y_UNIT_TEST(Replay200RowsPendingAndCompletedWithoutApplyingWithLateOldCallbacks) {
    TString snapshot;
    std::shared_ptr<TProvider> old;
    ui64 generation;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); });
        f.Turn();
        old = f.Provider;
        generation = f.Services->GetGeneration();
        for (size_t i = 1; i < 16; i += 2) {
            old->Reply(i);
        }
        // Owner has received results but has not resumed any frame.
        f.Services->BeginTurn();
        f.Services->EndTurn();
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
        f.Services->SnapshotStored(f.Services->GetSnapshotId());
        UNIT_ASSERT(!f.Services->CanForwardBoundary());
        UNIT_ASSERT(!f.Services->CanForwardWatermark());
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.Services->ReleaseBoundary(), yexception, "cannot overtake");
        auto immutable = snapshot;
        old->Reply(0);
        UNIT_ASSERT_VALUES_EQUAL(snapshot, immutable);
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(snapshot);
    UNIT_ASSERT(f.Services->GetGeneration() != generation);
    for (size_t i = 0; i < old->Calls.size(); ++i) {
        old->Reply(i);
    }
    f.AckOutputs = false;
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 8); // only unfinished calls replay
    UNIT_ASSERT(!f.Services->CanForwardBoundary());
    f.Services->OutputAcknowledged(generation, 1); // stale generation cannot release output
    UNIT_ASSERT(!f.Services->CanForwardWatermark());
    f.AckAll();
    UNIT_ASSERT(!f.Services->CanForwardBoundary()); // the old pending image is stale
    UNIT_ASSERT(!f.Graph->SaveGraphState().empty());
    f.Services->SnapshotStored(f.Services->GetSnapshotId());
    UNIT_ASSERT(f.Services->CanForwardBoundary());
    f.Services->ReleaseBoundary();
    f.AckOutputs = true;
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 200);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 192);
    for (ui64 i = 0; i < 200; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
    }
}
Y_UNIT_TEST(ReplayNestedFramesAtCpuYield) {
    TString snapshot;
    {
        TFixture f(1, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, true, 4, 3);
        f.Turn();
        f.Pump();
        UNIT_ASSERT(!f.Provider->Calls.empty());
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
    }
    TFixture f(1, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, false, 4, 3);
    f.Restore(snapshot);
    f.Pump();
    UNIT_ASSERT(!f.Services->CanForwardBoundary());
    UNIT_ASSERT(!f.Graph->SaveGraphState().empty());
    f.Services->SnapshotStored(f.Services->GetSnapshotId());
    UNIT_ASSERT(f.Services->CanForwardBoundary());
    f.Services->ReleaseBoundary();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 200);
    for (ui64 i = 0; i < 200; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
    }
}
Y_UNIT_TEST(ReplayRequiresCodecAndExplicitInputCut) {
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); }), yexception, "explicit provider codec");
    UNIT_ASSERT(f.Provider->Calls.empty());
}
Y_UNIT_TEST(SnapshotRequiresInputCut) {
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Drain));
    f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); });
    f.Services->BeginCheckpoint(1);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->SaveGraphState(), yexception, "consistent input/output cut");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Services->EstablishInputCut(2), yexception, "Invalid input cut");
    f.Services->EstablishInputCut(1);
    UNIT_ASSERT(!f.Graph->SaveGraphState().empty());
}
Y_UNIT_TEST(InvalidSnapshotPlanModeVersionAndTruncationFailWithoutIo) {
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); });
        f.Turn();
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->LoadGraphState(snapshot), yexception, "fresh execution generation");
    }
    for (ui32 variant = 0; variant < 4; ++variant) {
        auto settings = CheckpointSettings(variant == 0 ? ECheckpointMode::Drain : ECheckpointMode::Replay);
        if (variant == 1) {
            settings.Checkpoint.PlanFingerprint = "other-plan";
        }
        TFixture f(1000, {}, settings);
        f.EnableReplay();
        f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); }, false);
        auto bad = snapshot;
        if (variant == 2) {
            bad[bad.find("DQ-ASYNC")] = 'X';
        }
        if (variant == 3) {
            bad.resize(bad.size() - 1);
        }
        UNIT_ASSERT_EXCEPTION(f.Graph->LoadGraphState(bad), yexception);
        UNIT_ASSERT(f.Provider->Calls.empty());
        UNIT_ASSERT_EXCEPTION(f.Services->BeginTurn(), yexception);
    }
}
Y_UNIT_TEST(RestoreMustCompleteBeforeResume) {
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); });
        f.Turn();
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); }, false);
    f.Graph->LoadGraphState(snapshot);
    UNIT_ASSERT_EXCEPTION(f.Services->BeginTurn(), yexception);
    f.Stream = f.Graph->GetValue();
    UNIT_ASSERT_EXCEPTION(f.Turn(), yexception);
    f.Services->CompleteRestore();
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 16);
}
Y_UNIT_TEST(SnapshotMemoryAndSizeLimitsReleaseOnFailure) {
    auto settings = CheckpointSettings(ECheckpointMode::Replay);
    settings.Checkpoint.MaxSnapshotBytes = 64;
    TFixture f(1000, {}, settings);
    f.EnableReplay();
    f.BuildCheckpoint(20, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Cut();
    auto before = f.Services->GetSchedulerStats().MemoryBytes;
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->SaveGraphState(), yexception, "size limit");
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetSchedulerStats().MemoryBytes, before);
    UNIT_ASSERT(f.Services->CanSaveCheckpoint());
}
Y_UNIT_TEST(CompletedBranchIsNotReevaluatedOnReplay) {
    TString snapshot;
    auto body = [](TFixture& f, TRuntimeNode row) {
        return f.Pb().If(f.Pb().Equals(f.Echo(row), f.Pb().NewDataLiteral<ui64>(1)),
                         f.Echo(f.Pb().Add(row, f.Pb().NewDataLiteral<ui64>(10))),
                         f.Echo(f.Pb().Add(row, f.Pb().NewDataLiteral<ui64>(100))));
    };
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(1, [&](auto row) { return body(f, row); });
        f.Turn();
        f.Provider->Reply(0);
        f.Pump();
        UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(1, [&](auto row) { return body(f, row); }, false);
    f.Restore(snapshot);
    f.Pump();
    f.Check({11});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
}
Y_UNIT_TEST(SavedTerminalErrorIsNotRetriedOrDisclosed) {
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(1, [&](auto row) { return f.Echo(row); });
        f.Turn();
        f.Provider->Calls[0]->Fail("sensitive-provider-error");
        f.Provider->Calls[0]->Release();
        f.Services->BeginTurn();
        f.Services->EndTurn();
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
        UNIT_ASSERT(!snapshot.Contains("sensitive-provider-error"));
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.BuildCheckpoint(1, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(snapshot);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Turn(), yexception, "Async operation failed, outcome=1, code=1");
    UNIT_ASSERT(f.Provider->Calls.empty());
}
Y_UNIT_TEST(LegacySynchronousGraphStateHasNoAsyncEnvelope) {
    TString snapshot;
    {
        TFixture f;
        // A checkpointable synchronous source exercises the legacy graph format.
        TCallableBuilder source(f.Pb().GetTypeEnvironment(), "CheckpointTestSource",
                                TStreamType::Create(f.Pb().NewDataLiteral<ui64>(0).GetStaticType(), f.Pb().GetTypeEnvironment()), true);
        source.Add(f.Pb().NewDataLiteral<ui64>(3));
        f.BuildProgram(TRuntimeNode(source.Build(), false));
        NUdf::TUnboxedValue value;
        UNIT_ASSERT(f.Stream.Fetch(value) == NUdf::EFetchStatus::Ok);
        snapshot = f.Graph->SaveGraphState();
        TStringBuf in(snapshot);
        const auto size = ReadUi64(in);
        UNIT_ASSERT_VALUES_EQUAL(size, in.size());
        UNIT_ASSERT_VALUES_EQUAL(ReadUi64(in), 1);
        UNIT_ASSERT(in.empty());
    }
    TFixture f;
    TCallableBuilder source(f.Pb().GetTypeEnvironment(), "CheckpointTestSource",
                            TStreamType::Create(f.Pb().NewDataLiteral<ui64>(0).GetStaticType(), f.Pb().GetTypeEnvironment()), true);
    source.Add(f.Pb().NewDataLiteral<ui64>(3));
    f.BuildProgram(TRuntimeNode(source.Build(), false), "OFF", false);
    f.Graph->LoadGraphState(snapshot);
    f.Stream = f.Graph->GetValue();
    f.Turn();
    f.Check({2, 3});
}
Y_UNIT_TEST(RestoredPendingOperationsAreRebatched) {
    struct TBatch final: IBatchProvider {
        std::vector<std::vector<TBatchItem>> Batches;
        bool Inline = false;
        TString GetBatchKey(const TRequest&) const override {
            return "compatible";
        }
        size_t GetMaxBatchSize() const noexcept override {
            return 8;
        }
        void StartBatch(std::vector<TBatchItem> items) override {
            Batches.push_back(std::move(items));
            if (Inline) {
                for (const auto& item : Batches.back()) {
                    item.Completion->Complete(item.Request->Payload);
                    item.Completion->Release();
                }
            }
        }
        void Cancel(TOperationId) noexcept override {
        }
    };
    auto setup = [](TFixture& f, std::shared_ptr<TBatch> batch, bool materialize) {
        f.Services->RegisterProvider("batch", batch);
        f.Services->RegisterReplayCodec("batch", std::make_shared<TEchoReplayCodec>());
        auto* type = f.Pb().NewDataLiteral<ui64>(0).GetStaticType();
        f.Registry.Register({"BatchEcho", type, type, "batch", "echo", 1024});
        f.BuildCheckpoint(16, [&](auto row) { return f.Registry.Call(f.Pb(), "BatchEcho", row); }, materialize);
    };
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        auto batch = std::make_shared<TBatch>();
        setup(f, batch, true);
        f.Turn();
        UNIT_ASSERT_VALUES_EQUAL(batch->Batches.size(), 2);
        for (const auto& b : batch->Batches) {
            UNIT_ASSERT_VALUES_EQUAL(b.size(), 8);
        }
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    auto batch = std::make_shared<TBatch>();
    batch->Inline = true;
    setup(f, batch, false);
    f.Restore(snapshot);
    UNIT_ASSERT(batch->Batches.empty());
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(batch->Batches.size(), 2);
    for (const auto& b : batch->Batches) {
        UNIT_ASSERT_VALUES_EQUAL(b.size(), 8);
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
    for (ui64 i = 0; i < 16; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
    }
}

Y_UNIT_TEST(ReplayFlowAndOneStepContinuation) {
    for (bool pending : {false, true}) {
        TString snapshot;
        {
            TFixture f(1, {}, CheckpointSettings(ECheckpointMode::Replay));
            f.EnableReplay();
            f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, true, 4, 2, true);
            f.Turn();
            if (pending) {
                f.Pump();
            }
            f.Cut();
            snapshot = f.Graph->SaveGraphState();
        }
        TFixture f(1, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.Provider->Inline = true;
        f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, false, 4, 2, true);
        f.Restore(snapshot);
        f.Pump();
        if (f.Services->CanSaveCheckpoint()) {
            f.Graph->SaveGraphState();
            f.Services->SnapshotStored(f.Services->GetSnapshotId());
        }
        UNIT_ASSERT(f.Services->CanForwardBoundary());
        f.Services->ReleaseBoundary();
        f.Pump();
        UNIT_ASSERT(f.Finished);
        UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 200);
        for (ui64 i = 0; i < 200; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
        }
    }
}
Y_UNIT_TEST(QueuedDescriptorsRestoreUnderConcurrencyLimit) {
    auto settings = CheckpointSettings(ECheckpointMode::Replay);
    settings.Scheduler.MaxInFlight = 1;
    TString snapshot;
    {
        TFixture f(1000, {}, settings);
        f.EnableReplay();
        f.BuildCheckpoint(16, [&](auto row) { return f.Echo(row); });
        f.Turn();
        UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Services->GetSchedulerStats().Queued, 15);
        f.Cut();
        snapshot = f.Graph->SaveGraphState();
    }
    TFixture f(1000, {}, settings);
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(16, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(snapshot);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 16);
    for (ui64 i = 0; i < 16; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
    }
}
Y_UNIT_TEST(StaleReplayImageCannotCommitLaterOutputAndNewImageRestoresExactlyOnce) {
    TString committed;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); });
        f.Turn();
        f.Cut();
        f.Graph->SaveGraphState();
        const auto memory = f.Services->GetSchedulerStats().MemoryBytes;
        UNIT_ASSERT_EXCEPTION(f.Graph->SaveGraphState(), yexception);
        UNIT_ASSERT_VALUES_EQUAL(f.Services->GetSchedulerStats().MemoryBytes, memory);
        UNIT_ASSERT_EXCEPTION(f.Services->BeginTurn(), yexception); // snapshot not handed off yet
        f.Services->SnapshotStored(f.Services->GetSnapshotId());
        for (size_t i = 0; i < 16; ++i) {
            f.Provider->Reply(i);
        }
        f.Pump();
        UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
        UNIT_ASSERT(!f.Services->CanForwardBoundary());
        UNIT_ASSERT_EXCEPTION(f.Services->ReleaseBoundary(), yexception);
        committed = f.Graph->SaveGraphState();
        f.Services->SnapshotStored(f.Services->GetSnapshotId());
        UNIT_ASSERT(f.Services->CanForwardBoundary());
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(200, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(committed);
    UNIT_ASSERT(f.Services->CanForwardBoundary());
    f.Services->ReleaseBoundary();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 184);
    for (ui64 i = 0; i < 184; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 17);
    }
}

Y_UNIT_TEST(CompletionThreadRacesImmutableSnapshot) {
    TString snapshot;
    {
        TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
        f.EnableReplay();
        f.BuildCheckpoint(16, [&](auto row) { return f.Echo(row); });
        f.Turn();
        f.Cut();
        std::atomic<bool> start = false;
        std::thread callback([&] {
            while (!start.load()) {
                std::this_thread::yield();
            }
            for (size_t i = 0; i < 16; ++i) {
                f.Provider->Reply(i);
            }
        });
        start.store(true);
        try {
            snapshot = f.Graph->SaveGraphState();
        } catch (...) {
            callback.join();
            throw;
        }
        callback.join();
        // Completions were never drained by the owner: their replay is explicit.
        UNIT_ASSERT(f.Output.empty());
        f.Services->SnapshotStored(f.Services->GetSnapshotId());
    }
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.Provider->Inline = true;
    f.BuildCheckpoint(16, [&](auto row) { return f.Echo(row); }, false);
    f.Restore(snapshot);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 16);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 16);
    for (ui64 i = 0; i < 16; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i + 1);
    }
}

Y_UNIT_TEST(LateStorageAcknowledgementCannotConfirmAnotherSnapshot) {
    TFixture f(1000, {}, CheckpointSettings(ECheckpointMode::Replay));
    f.EnableReplay();
    f.BuildCheckpoint(16, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Cut();
    f.Graph->SaveGraphState();
    const auto old = f.Services->GetSnapshotId();
    UNIT_ASSERT(f.Services->SnapshotStored(old));
    for (size_t i = 0; i < 16; ++i) {
        f.Provider->Reply(i);
    }
    f.Pump();
    f.Graph->SaveGraphState();
    const auto fresh = f.Services->GetSnapshotId();
    UNIT_ASSERT(fresh != old);
    const auto memory = f.Services->GetSchedulerStats().MemoryBytes;
    UNIT_ASSERT(!f.Services->SnapshotStored(old));
    auto wrongGeneration = fresh;
    ++wrongGeneration.Generation;
    UNIT_ASSERT(!f.Services->SnapshotStored(wrongGeneration));
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetSchedulerStats().MemoryBytes, memory);
    UNIT_ASSERT(!f.Services->CanForwardBoundary());
    UNIT_ASSERT(f.Services->SnapshotStored(fresh));
    UNIT_ASSERT(f.Services->CanForwardBoundary());
}

} // Y_UNIT_TEST_SUITE(TAsyncCheckpoint)

Y_UNIT_TEST_SUITE(TResumableComputation) {
Y_UNIT_TEST(CpuQuantumMakesProgressWhenFirstCheckIsExpired) {
    ui64 cpu = 0;
    TExecutionSettings settings;
    settings.CpuBudgetUs = 1;
    settings.CpuClock = [&] { return cpu++; };
    TFixture f(10000, {}, settings);
    f.Provider->Inline = true;
    f.Build({7}, [&](auto row) { return f.Echo(row); });
    for (size_t turn = 0; turn < 100 && !f.Finished; ++turn) {
        f.Events.exchange(0);
        f.Turn();
    }
    UNIT_ASSERT_C(f.Finished, "100 owner turns without progress when CPU expires before the first instruction");
    f.Check({7});
    UNIT_ASSERT(f.Services->GetComputeStats().CpuBudgetYields > 0);
}

Y_UNIT_TEST(NestedFramePathFailsWhenLocalQuotaCannotFit) {
    ui64 singleFrameMemory = 0;
    {
        TFixture f(1);
        f.BuildNested({1}, 1);
        f.Turn();
        singleFrameMemory = f.Services->GetSchedulerStats().MemoryBytes;
        UNIT_ASSERT(f.Provider->Calls.empty());
    }
    auto settings = DeterministicSettings();
    settings.Scheduler.MaxMemoryBytes = singleFrameMemory;
    TFixture f(1, {}, settings);
    f.BuildNested({1}, 2);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Turn(), yexception, "cannot fit resource limits");
    UNIT_ASSERT(f.Provider->Calls.empty());
}
Y_UNIT_TEST(NestedMapsConsumeReadyResultsAtMemoryLimit) {
    for (ui32 depth : {2u, 4u}) {
        for (bool flow : {false, true}) {
            ui64 required = 0;
            {
                TFixture f(10000);
                f.BuildNested({1, 2, 3, 4}, depth, flow);
                f.Turn();
                UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 4);
                required = f.Services->GetSchedulerStats().MemoryBytes;
            }
            auto quota = std::make_shared<TMemoryQuota>(required);
            {
                auto settings = DeterministicSettings();
                settings.Scheduler.Quota = quota;
                TFixture f(10000, {}, settings);
                f.BuildNested({1, 2, 3, 4}, depth, flow);
                f.Turn();
                UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 4);
                UNIT_ASSERT_VALUES_EQUAL(quota->GetUsed(), required);
                size_t replied = 0;
                for (ui32 stage = 0; stage < depth; ++stage) {
                    const size_t end = f.Provider->Calls.size();
                    UNIT_ASSERT_VALUES_EQUAL(end, (stage + 1) * 4);
                    while (replied < end) {
                        f.Provider->Reply(replied++);
                    }
                    f.Pump();
                }
                UNIT_ASSERT(f.Finished);
                f.Check({1, 2, 3, 4});
                UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), depth * 4);
            }
            UNIT_ASSERT_VALUES_EQUAL(quota->GetUsed(), 0);
        }
    }
}

Y_UNIT_TEST(GraphFormsRealBatchesAndFlushesPartialTail) {
    struct TBatch final: IBatchProvider {
        std::shared_ptr<TProvider> Scalar;
        std::vector<size_t> Sizes;
        TString GetBatchKey(const TRequest&) const override {
            return "context";
        }
        size_t GetMaxBatchSize() const noexcept override {
            return 64;
        }
        void StartBatch(std::vector<TBatchItem> items) override {
            Sizes.push_back(items.size());
            for (auto& item : items) {
                Scalar->Start(item.Id, item.Request, item.Completion);
            }
        }
        void Cancel(TOperationId id) noexcept override {
            Scalar->Cancel(id);
        }
    };
    TExecutionSettings settings;
    settings.Scheduler.MaxBatchSize = 8;
    settings.CpuBudgetUs = 0;
    TFixture f(10000, {}, settings);
    auto batch = std::make_shared<TBatch>();
    batch->Scalar = f.Provider;
    f.Services->RegisterProvider("batch", batch);
    auto* type = f.Pb().NewDataLiteral<ui64>(0).GetStaticType();
    f.Registry.Register({"BatchEcho", type, type, "batch", "echo", 1024});
    std::vector<ui64> input;
    for (ui64 i = 0; i < 19; ++i) {
        input.push_back(i);
    }
    f.Provider->Inline = true;
    f.Build(input, [&](auto row) { return f.Registry.Call(f.Pb(), "BatchEcho", row); }, 64);
    f.Turn();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 19);
    UNIT_ASSERT_VALUES_EQUAL(batch->Sizes.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(batch->Sizes[0], 8);
    UNIT_ASSERT_VALUES_EQUAL(batch->Sizes[1], 8);
    UNIT_ASSERT_VALUES_EQUAL(batch->Sizes[2], 3);
    for (ui64 i = 0; i < 19; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i);
    }
}
Y_UNIT_TEST(MeasuredCpuYieldLetsAnotherTaskRunAfterLastCompletion) {
    ui64 cpu = 0;
    TExecutionSettings settings;
    settings.CpuBudgetUs = 5;
    settings.CpuClock = [&] { return cpu++; };
    TFixture f(10000, {}, settings);
    f.Build({7}, [&](auto row) {
        auto value = f.Echo(row);
        for (ui32 i = 0; i < 40; ++i) {
            value = f.Pb().Add(value, f.Pb().NewDataLiteral<ui64>(1));
        }
        return value;
    });
    // Prime explicitly: disabling reruns must fail after the last response,
    // rather than while reaching the first submission.
    for (size_t i = 0; f.Provider->Calls.empty() && i < 500; ++i) {
        f.Events.exchange(0);
        f.Turn();
    }
    f.Events.exchange(0);
    f.Turn(); // consume source EOF before the last completion
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    f.Provider->Reply(0); // Last external event. A single worker serves both tasks.
    std::deque<bool> runnable{true, false};
    bool coldRanBeforeHotFinished = false;
    size_t turns = 0;
    while (!runnable.empty()) {
        UNIT_ASSERT(++turns < 1000);
        const bool hot = runnable.front();
        runnable.pop_front();
        if (!hot) {
            coldRanBeforeHotFinished = !f.Finished;
            continue;
        }
        f.Events.exchange(0);
        f.Turn();
        if (f.Events.load() && !f.Finished) {
            runnable.push_back(true);
        }
    }
    UNIT_ASSERT(coldRanBeforeHotFinished);
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT(f.Services->GetComputeStats().CpuBudgetYields > 1);
    f.Check({47});
}
Y_UNIT_TEST(FrameRoundRobinDoesNotStarveShortContinuation) {
    TExecutionSettings settings;
    settings.CpuBudgetUs = 0;
    TFixture f(1, {}, settings);
    f.Build({1, 2}, [&](auto row) {
        auto value = f.Echo(row);
        for (ui32 i = 0; i < 40; ++i) {
            value = f.Pb().Add(value, f.Pb().NewDataLiteral<ui64>(1));
        }
        return f.Pb().If(f.Pb().Equals(row, f.Pb().NewDataLiteral<ui64>(1)), value, f.Echo(f.Echo(row)));
    });
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    bool shortStartedBeforeFirstOutput = false;
    f.Provider->OnStart = [&] { if (f.Provider->Calls.size() == 3){ shortStartedBeforeFirstOutput = f.Output.empty();
} };
    f.Provider->Reply(0);
    f.Provider->Reply(1);
    f.Pump();
    UNIT_ASSERT(shortStartedBeforeFirstOutput);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 3);
    f.Provider->Reply(2);
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({41, 2});
}
Y_UNIT_TEST(SlowDownstreamRetainsBoundedResultsWithoutSelfPolling) {
    TFixture f;
    f.Build({1, 2, 3, 4, 5, 6, 7, 8}, [&](auto row) { return f.Echo(row); }, 4);
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
        f.Provider->Reply(i);
    }
    f.Events.exchange(0);
    f.Services->BeginTurn(); // output blocked: owner drains, but does not Fetch
    f.Services->EndTurn();
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 4);
    UNIT_ASSERT(f.Services->GetSchedulerStats().MemoryBytes > 0);
    f.Turn(); // output capacity becomes available
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 8);
    for (size_t i = 4; i < 8; ++i) {
        f.Provider->Reply(i);
    }
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({1, 2, 3, 4, 5, 6, 7, 8});
}
Y_UNIT_TEST(GraphMemoryLimitFailsBeforeProviderAndReleasesOnDestruction) {
    auto quota = std::make_shared<TMemoryQuota>(1);
    {
        TExecutionSettings settings;
        settings.Scheduler.Quota = quota;
        TFixture f(1000, {}, settings);
        f.Build({1}, [&](auto row) { return f.Echo(row); });
        UNIT_ASSERT_EXCEPTION_CONTAINS(f.Turn(), yexception, "memory limit");
        UNIT_ASSERT(f.Provider->Calls.empty());
    }
    UNIT_ASSERT_VALUES_EQUAL(quota->GetUsed(), 0);
}
Y_UNIT_TEST(CpuAccountingExcludesOtherTasksBetweenTurnsAndShutdownRejectsRerun) {
    ui64 cpu = 0;
    size_t events = 0;
    TExecutionSettings settings;
    settings.CpuClock = [&] { return cpu; };
    settings.CpuBudgetUs = 5;
    TExecutionServices services([&](ui64) { ++events; }, 1000, {}, settings);
    services.BeginTurn();
    cpu = 3;
    UNIT_ASSERT(services.TryStep());
    services.EndTurn();
    cpu = 1000;
    services.BeginTurn();
    cpu = 1002;
    UNIT_ASSERT(services.TryStep());
    services.EndTurn();
    UNIT_ASSERT_VALUES_EQUAL(services.GetComputeStats().CpuTimeUs, 5);
    services.BeginTurn();
    UNIT_ASSERT(services.TryStep()); // every turn guarantees one progress step
    cpu += 5;
    UNIT_ASSERT(!services.TryStep());
    services.RequestRerun();
    services.RequestRerun();
    UNIT_ASSERT_VALUES_EQUAL(events, 1);
    services.EndTurn();
    services.Shutdown();
    UNIT_ASSERT_EXCEPTION_CONTAINS(services.BeginTurn(), yexception, "closed");
    services.RequestRerun();
    UNIT_ASSERT_VALUES_EQUAL(events, 1);
}
Y_UNIT_TEST(NestedAsyncMapsProgressWithSingleStepQuantum) {
    for (auto depth : {2u, 4u}) {
        for (bool flow : {false, true}) {
            TFixture f(1);
            f.Provider->Inline = true;
            f.BuildNested({7, 8, 9}, depth, flow);
            f.Turn();
            f.Pump();
            UNIT_ASSERT(f.Finished);
            f.Check({7, 8, 9});
            UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 3 * depth);
        }
    }
}
Y_UNIT_TEST(NestedAsyncMapsWaitWithoutPollingAndResumeAfterLastReply) {
    TFixture f(1);
    f.BuildNested({7}, 3);
    f.Turn();
    f.Pump();
    for (size_t i = 0; i < 3; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), i + 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Pump(), 0);
        UNIT_ASSERT(f.Output.empty());
        f.Provider->Reply(i);
        f.Pump();
    }
    UNIT_ASSERT(f.Finished);
    f.Check({7});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 3);
}
Y_UNIT_TEST(NestedAsyncMapsProcessTwoHundredRowsWithSingleStepQuantum) {
    TFixture f(1);
    f.Provider->Inline = true;
    std::vector<ui64> rows;
    for (ui64 i = 0; i < 200; ++i) {
        rows.push_back(i);
    }
    f.BuildNested(rows, 3);
    f.Turn();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 600);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 200);
    for (ui64 i = 0; i < 200; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), i);
    }
}
Y_UNIT_TEST(PresentUnwrapSkipsAsyncDiagnostic) {
    TFixture f(1);
    auto message = f.EchoMessage("unused diagnostic");
    f.Build({7, 9}, [&](auto row) {
        return f.Pb().Unwrap(f.Pb().NewOptional(row), message, "unwrap.sql", 12, 34);
    }, 2, true, true);
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 0);
    UNIT_ASSERT(f.Finished);
    f.Check({7, 9});
}
Y_UNIT_TEST(UnwrapResumesOptionalWithoutEvaluatingDiagnostic) {
    TFixture f(1);
    auto message = f.EchoMessage("unused diagnostic");
    f.Build({7, 9}, [&](auto row) {
        return f.Pb().Unwrap(f.Pb().NewOptional(f.Echo(row)), message, "unwrap.sql", 12, 34);
    });
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    f.Pump();
    UNIT_ASSERT(f.Output.empty());
    f.Provider->Reply(0);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    UNIT_ASSERT(f.Finished);
    f.Check({7, 9});
}
Y_UNIT_TEST(UnwrapPresentOuterOptionalPreservesEmptyInnerOptional) {
    TFixture f(1);
    auto message = f.EchoMessage("unused diagnostic");
    f.Build({7}, [&](auto) {
        auto inner = f.Pb().NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);
        return f.Pb().Unwrap(f.Pb().NewOptional(inner), message, "unwrap.sql", 12, 34);
    });
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 0);
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 1);
    UNIT_ASSERT(!f.Output[0]);
}
Y_UNIT_TEST(EmptyUnwrapAwaitsDiagnosticOnceAndPreservesErrorPosition) {
    TOnlyThrowingBindTerminator terminator;
    TFixture f(1);
    auto message = f.EchoMessage("deferred diagnostic");
    f.Build({7}, [&](auto) {
        return f.Pb().Unwrap(f.Pb().NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id), message, "unwrap.sql", 12, 34);
    }, 2, false, true);
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT(f.Output.empty());
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    f.Turn(); // An unrelated event must not submit the diagnostic twice.
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    f.Provider->Reply(0);
    try {
        f.Pump();
        UNIT_FAIL("Expected empty optional error");
    } catch (const yexception& e) {
        UNIT_ASSERT(TString(e.what()).Contains("unwrap.sql:12:34"));
        UNIT_ASSERT(TString(e.what()).Contains("deferred diagnostic"));
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().ReservedPayloadBytes, 0);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Turn(), yexception, "execution has failed");
}
Y_UNIT_TEST(SuspendResumeDoesNotResubmit) {
    TFixture f;
    f.Build({7}, [&](auto row) { return f.Pb().Add(f.Echo(row), f.Pb().NewDataLiteral<ui64>(2)); });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    UNIT_ASSERT(f.Output.empty());
    f.Turn(); // unrelated source event cannot resubmit an operation
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    std::thread callback([&] { f.Provider->Reply(0); });
    callback.join();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({9});
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().ReservedPayloadBytes, 0);
}
Y_UNIT_TEST(IfEvaluatesOnlyChosenBranch) {
    TFixture f;
    f.Build({0, 1}, [&](auto row) {
        return f.Pb().If(f.Pb().Equals(row, f.Pb().NewDataLiteral<ui64>(1)), f.Echo(row), f.Pb().NewDataLiteral<ui64>(42));
    });
    f.Turn();
    f.Check({42});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    f.Provider->Reply(0);
    f.Pump();
    f.Check({42, 1});
}
Y_UNIT_TEST(CasePreservesLazyBranches) {
    TFixture f;
    f.Build({2}, [&](auto row) {
        return f.Pb().If(f.Pb().Equals(row, f.Pb().NewDataLiteral<ui64>(0)), f.Echo(f.Pb().NewDataLiteral<ui64>(10)),
                         f.Pb().If(f.Pb().Equals(row, f.Pb().NewDataLiteral<ui64>(2)), f.Echo(row), f.Echo(f.Pb().NewDataLiteral<ui64>(30))));
    });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    f.Provider->Reply(0);
    f.Pump();
    f.Check({2});
}
Y_UNIT_TEST(NullConditionDoesNotInvokeEitherBranch) {
    TFixture f;
    f.Build({2}, [&](auto row) {
        return f.Pb().If(f.Pb().NewEmptyOptionalDataLiteral(NUdf::TDataType<bool>::Id), f.Echo(row), f.Echo(row));
    });
    f.Turn();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 1);
    UNIT_ASSERT(!f.Output[0]);
    UNIT_ASSERT(f.Provider->Calls.empty());
}
Y_UNIT_TEST(DependentCallsAndSharedOccurrence) {
    TFixture f;
    f.Build({5}, [&](auto row) {
        auto first = f.Echo(row);
        return f.Pb().Add(first, f.Echo(f.Pb().Add(first, f.Pb().NewDataLiteral<ui64>(1))));
    });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    f.Provider->Reply(0);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    f.Pump();
    f.Check({11});
}
Y_UNIT_TEST(DistinctOccurrencesAreNotMerged) {
    TFixture f;
    f.Build({5}, [&](auto row) { return f.Pb().Add(f.Echo(row), f.Echo(row)); });
    f.Turn();
    f.Provider->Reply(0);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    f.Pump();
    f.Check({10});
}
Y_UNIT_TEST(FramesIsolateArgumentsAndPreserveOrder) {
    TFixture f;
    f.Build({1, 2, 3}, [&](auto row) { return f.Pb().Add(row, f.Echo(row)); });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 3);
    f.Provider->Reply(2);
    f.Provider->Reply(1);
    f.Pump();
    UNIT_ASSERT(f.Output.empty());
    f.Provider->Reply(0);
    f.Pump();
    f.Check({2, 4, 6});
}
Y_UNIT_TEST(SharedVolatileValueSurvivesSuspendAndInterleaving) {
    TFixture f;
    f.Build({1, 2}, [&](auto row) {
        auto random = f.Pb().RandomNumber({row});
        return f.Pb().NewTuple({random, f.Echo(random), random});
    });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    f.Pump();
    f.Provider->Reply(0);
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 2);
    for (auto& row : f.Output) {
        UNIT_ASSERT_VALUES_EQUAL(row.GetElement(0).Get<ui64>(), row.GetElement(1).Get<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(row.GetElement(0).Get<ui64>(), row.GetElement(2).Get<ui64>());
    }
    UNIT_ASSERT(f.Output[0].GetElement(0).Get<ui64>() != f.Output[1].GetElement(0).Get<ui64>());
}
Y_UNIT_TEST(LastCompletionThenMultipleCpuQuantaMakeProgressWithoutExternalEvents) {
    TFixture f(1);
    f.Build({7}, [&](auto row) {
        auto value = f.Echo(row);
        for (ui32 i = 0; i < 25; ++i) {
            value = f.Pb().Add(value, f.Pb().NewDataLiteral<ui64>(1));
        }
        return value;
    });
    // Prime the operation explicitly, so a negative control disabling budget
    // reruns can fail only AFTER the last response, not while reaching Submit.
    for (size_t i = 0; f.Provider->Calls.empty() && i < 200; ++i) {
        f.Events.exchange(0);
        f.Turn();
    }
    // Explicitly consume source EOF too: after Reply there are no remaining
    // source events or source reads that could accidentally drive progress.
    f.Events.exchange(0);
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    f.Provider->Reply(0); // LAST external event. Everything below is local work.
    UNIT_ASSERT(f.Pump() > 10);
    UNIT_ASSERT(f.Finished);
    f.Check({32});
}
Y_UNIT_TEST(WaitingHasNoSelfWakeupsEvenAtBudgetBoundary) {
    TFixture f(1);
    f.Build({7}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    UNIT_ASSERT(!f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Pump(), 0);
}
Y_UNIT_TEST(InlineCompletionAndBudgetRerunsCoalesce) {
    TFixture f(1);
    f.Provider->Inline = true;
    f.Build({1, 2, 3}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({1, 2, 3});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 3);
}
Y_UNIT_TEST(AdmissionResumesOnCleanupCreditsWithoutPolling) {
    TFixture f(1000, {.MaxOperations = 1, .MaxPayloadBytes = 4096});
    f.Build({1, 2}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    f.Provider->Calls[0]->Complete(f.Provider->Payloads[0]);
    f.Pump();
    f.Check({1});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    f.Provider->Calls[0]->Release();
    f.Pump();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    f.Pump();
    f.Check({1, 2});
}
Y_UNIT_TEST(FlowYieldAndSerializedProgram) {
    TFixture f;
    f.Build({9}, [&](auto row) { return f.Echo(row); }, 2, true, true);
    f.Turn();
    f.Provider->Reply(0);
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({9});
}
Y_UNIT_TEST(ShutdownIgnoresLateCompletion) {
    TFixture f;
    f.Build({9}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Services->Shutdown();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Cancelled, 1);
    f.Provider->Reply(0);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 0);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Services->BeginTurn(), yexception, "closed");
}
Y_UNIT_TEST(ProviderErrorsAreNotRetriedOrLeaked) {
    TFixture f;
    f.Build({9}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Provider->Calls[0]->Fail("secret-marker");
    f.Provider->Calls[0]->Release();
    try {
        f.Pump();
        UNIT_FAIL("Expected failure");
    } catch (const yexception& e) {
        UNIT_ASSERT(TString(e.what()).Contains("Async operation failed"));
        UNIT_ASSERT(!TString(e.what()).Contains("secret-marker"));
    }
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().ReservedPayloadBytes, 0);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 1);
}
Y_UNIT_TEST(UnsupportedContextsAndTypeMismatchesFailPreparation) {
    TFixture f;
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Build({1}, [&](auto) { return f.Pb().Arg(f.Pb().NewResourceType("test")); }), yexception, "Resource");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Echo(f.Pb().NewDataLiteral<bool>(true)), yexception, "type mismatch");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Build({1}, [&](auto row) { return f.Pb().NewList(row.GetStaticType(), {f.Echo(row)}); }), yexception, "higher-order");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Build({1}, [&](auto row) { return f.Pb().Coalesce(f.Pb().NewOptional(f.Echo(row)), row); }), yexception, "Unsupported callable");
}
Y_UNIT_TEST(TwoHundredRowsWithInlineCompletionAndSmallQuanta) {
    TFixture f(3);
    f.Provider->Inline = true;
    std::vector<ui64> rows;
    for (ui64 i = 0; i != 200; ++i) {
        rows.push_back(i);
    }
    f.Build(rows, [&](auto row) { return f.Pb().Add(f.Echo(row), row); }, 4);
    f.Turn();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    UNIT_ASSERT_VALUES_EQUAL(f.Output.size(), 200);
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 200);
    for (ui64 i = 0; i != 200; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(f.Output[i].Get<ui64>(), 2 * i);
    }
}
Y_UNIT_TEST(DroppingGraphCancelsFramesWithoutWaitingForProvider) {
    TFixture f;
    f.Build({1, 2}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Stream.Clear();
    f.Graph.Destroy();
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Cancelled, 2);
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().CleanupPending, 2);
    f.Provider->Reply(0);
    f.Provider->Reply(1);
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().ReservedPayloadBytes, 0);
}
Y_UNIT_TEST(UnconfiguredCheckpointAndLlvmAreExplicitlyRejected) {
    TFixture f;
    f.Build({1}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->SaveGraphState(), yexception, "not configured");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->LoadGraphState(""), yexception, "not configured");
    f.Graph->SetExecuteLLVM(true);
    // A late switch cannot bypass the runtime check on a fresh stream.
    f.Stream.Clear();
    f.Graph->Invalidate();
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->GetValue(), yexception, "interpreted");
}
Y_UNIT_TEST(YqlRegistrationTypingEffectsCseAndExecution) {
    TFixture f;
    TYqlFixture yql;
    auto root = yql.Projection(true);
    yql.Annotate(root);
    const auto effects = NYql::GetAsyncExpressionEffects(root->Tail());
    UNIT_ASSERT(effects.MaySuspend && effects.HasExternalEffects);
    UNIT_ASSERT(!NYql::IsPureIsolatedLambda(root->Tail().Tail()));
    auto constraints = NYql::CreateConstraintTransformer(yql.Types, true, true);
    UNIT_ASSERT(NYql::SyncTransform(*constraints, root, yql.Ctx) == NYql::IGraphTransformer::TStatus::Ok);
    NYql::TExprNode::TPtr optimized;
    UNIT_ASSERT(NYql::UpdateCompletness(root, optimized, yql.Ctx) == NYql::IGraphTransformer::TStatus::Ok);
    UNIT_ASSERT(NYql::EliminateCommonSubExpressions(root, optimized, yql.Ctx, true, *yql.Types.ColumnOrderStorage).Level != NYql::IGraphTransformer::TStatus::Error);
    if (optimized) {
        root = optimized;
    }
    f.BuildProgram(yql.Compile(root, f));
    f.Provider->Inline = true;
    f.Turn();
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({2, 4});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 4);
}
Y_UNIT_TEST(YqlSharedOccurrenceExecutesOncePerRow) {
    TFixture f;
    TYqlFixture yql;
    auto root = yql.Projection(false);
    yql.Annotate(root);
    f.BuildProgram(yql.Compile(root, f));
    f.Provider->Inline = true;
    f.Turn();
    f.Pump();
    f.Check({2, 4});
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
}
Y_UNIT_TEST(YqlScalarAsyncAndUnknownRegistrationFailPreparation) {
    TFixture f;
    TYqlFixture yql;
    auto scalar = yql.Echo(yql.Number("1"));
    yql.Annotate(scalar);
    UNIT_ASSERT_EXCEPTION_CONTAINS(yql.Compile(scalar, f), yexception, "explicit AsyncMap");
    yql.Types.AsyncFunctions.clear();
    auto unknown = yql.Echo(yql.Number("2"));
    UNIT_ASSERT(!NYql::InstantAnnotateTypes(unknown, yql.Ctx, false, yql.Types));
}

Y_UNIT_TEST(CompletionAfterDrainBeforeWaitingIsNotLost) {
    TFixture f;
    f.Build({4}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Services->BeginTurn(); // drained before the response is published
    std::thread response([&] { f.Provider->Reply(0); });
    response.join();
    NUdf::TUnboxedValue value;
    UNIT_ASSERT(f.Stream.Fetch(value) == NUdf::EFetchStatus::Yield);
    UNIT_ASSERT_VALUES_EQUAL(f.Events.load(), 1);
    f.Pump();
    UNIT_ASSERT(f.Finished);
    f.Check({4});
}
Y_UNIT_TEST(SynchronousMapNeedsNoAsyncServices) {
    TFixture f;
    auto row = f.Pb().NewDataLiteral<ui64>(4);
    auto source = f.Pb().Iterator(f.Pb().NewList(row.GetStaticType(), {row}), {});
    auto map = f.Pb().Map(source, [&](auto arg) { return f.Pb().Add(arg, row); });
    auto graph = f.Setup.BuildGraph(map);
    graph->Prepare();
    UNIT_ASSERT(!graph->GetContext().AsyncServices);
    auto stream = graph->GetValue();
    NUdf::TUnboxedValue value;
    UNIT_ASSERT(stream.Fetch(value) == NUdf::EFetchStatus::Ok);
    UNIT_ASSERT_VALUES_EQUAL(value.Get<ui64>(), 8);
    UNIT_ASSERT(stream.Fetch(value) == NUdf::EFetchStatus::Finish);
}
Y_UNIT_TEST(FailureCancelsOtherFramesAndCannotResubmit) {
    TFixture f;
    f.Build({1, 2}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    f.Provider->Calls[0]->Fail("hidden");
    f.Provider->Calls[0]->Release();
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Pump(), yexception, "Async operation failed");
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Cancelled, 1);
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Turn(), yexception, "execution has failed");
    UNIT_ASSERT_VALUES_EQUAL(f.Provider->Calls.size(), 2);
    f.Provider->Reply(1);
    UNIT_ASSERT_VALUES_EQUAL(f.Services->GetStats().ReservedPayloadBytes, 0);
}

} // Y_UNIT_TEST_SUITE(TResumableComputation)

} // namespace NKikimr::NMiniKQL::NAsync
