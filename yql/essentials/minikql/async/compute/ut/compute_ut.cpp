#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/minikql/async/compute/compute.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <atomic>
#include <thread>

namespace NKikimr::NMiniKQL::NAsync {
namespace {

struct TProvider final: IProvider {
    std::vector<std::shared_ptr<ICompletion>> Calls;
    std::vector<TString> Payloads;
    size_t Cancelled = 0;
    bool Inline = false;
    void Start(TOperationId, std::shared_ptr<const TRequest> request, std::shared_ptr<ICompletion> completion) override {
        Calls.push_back(completion);
        Payloads.push_back(request->Payload);
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

    explicit TFixture(size_t quantum = 1000, TLimits limits = {})
        : Services(std::make_shared<TExecutionServices>([this](ui64) { ++Events; }, quantum, limits))
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
    void BuildProgram(TRuntimeNode program, TString llvm = "OFF") {
        Setup.Explorer.Walk(program.GetNode(), Setup.Env->GetNodeStack());
        TComputationPatternOpts opts(Setup.Alloc.Ref(), *Setup.Env, GetTestFactory(), Setup.FunctionRegistry.Get(), NUdf::EValidateMode::Greedy, NUdf::EValidatePolicy::Exception, llvm, EGraphPerProcess::Multi);
        Setup.Pattern = MakeComputationPattern(Setup.Explorer, program, {}, opts);
        auto execution = opts.ToComputationOptions(*Setup.RandomProvider, *Setup.TimeProvider);
        execution.AsyncServices = Services;
        Graph = Setup.Pattern->Clone(execution);
        Graph->Prepare();
        Stream = Graph->GetValue();
    }
    void Turn() {
        Services->BeginTurn();
        for (size_t i = 0; i != 1000; ++i) {
            NUdf::TUnboxedValue value;
            auto status = Stream.Fetch(value);
            if (status == NUdf::EFetchStatus::Ok) {
                Output.push_back(std::move(value));
                continue;
            }
            Finished = status == NUdf::EFetchStatus::Finish;
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

Y_UNIT_TEST_SUITE(TResumableComputation) {
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
Y_UNIT_TEST(CheckpointAndLlvmAreExplicitlyRejected) {
    TFixture f;
    f.Build({1}, [&](auto row) { return f.Echo(row); });
    f.Turn();
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->SaveGraphState(), yexception, "until D4");
    UNIT_ASSERT_EXCEPTION_CONTAINS(f.Graph->LoadGraphState(""), yexception, "until D4");
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
