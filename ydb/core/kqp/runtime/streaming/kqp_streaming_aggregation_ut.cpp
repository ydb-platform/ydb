#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_program_builder.h>
#include <ydb/core/kqp/runtime/streaming/kqp_streaming_aggregation.h>

#include <yql/essentials/ast/yql_ast_escaping.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <library/cpp/threading/future/future.h>

#include <util/stream/str.h>
#include <util/string/escape.h>

#include <bit>
#include <deque>
#include <map>

namespace NKikimr::NMiniKQL {

namespace {

TString CheckQuotedIdentifier(NSQLTranslation::ILexer& lexer, const TStringBuf& name) {
    const auto quoted = NPrivate::QuoteStreamingAggregationIdentifier(name);
    const TString query = TStringBuilder() << "SELECT " << quoted << " FROM input_table; SELECT 42;";
    NSQLTranslation::TParsedTokenList tokens;
    NYql::TIssues issues;
    UNIT_ASSERT_C(NSQLTranslation::Tokenize(lexer, query, "quoted identifier", tokens, issues, 10),
        EscapeC(name) << ": " << issues.ToString());

    TVector<TString> tokenNames;
    TVector<TString> tokenContents;
    for (const auto& token : tokens) {
        if (token.Name != "WS") {
            tokenNames.push_back(token.Name);
            tokenContents.push_back(token.Content);
        }
    }
    const TVector<TString> expectedNames = {
        "SELECT", "ID_QUOTED", "FROM", "ID_PLAIN", "SEMICOLON", "SELECT", "DIGITS", "SEMICOLON", "EOF"
    };
    UNIT_ASSERT_VALUES_EQUAL_C(tokenNames.size(), expectedNames.size(), EscapeC(name));
    for (size_t i = 0; i < tokenNames.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(tokenNames[i], expectedNames[i], EscapeC(name));
    }
    UNIT_ASSERT_VALUES_EQUAL(tokenContents.at(1), quoted);
    UNIT_ASSERT_VALUES_EQUAL(tokenContents.at(3), "input_table");
    UNIT_ASSERT_VALUES_EQUAL(tokenContents.at(6), "42");
    return quoted;
}

class TOutputTableAggregationRuntime {
    class TInput final : public TComputationValue<TInput> {
    public:
        explicit TInput(TMemoryUsageInfo* const memInfo)
            : TComputationValue<TInput>(memInfo)
        {}

        std::deque<NUdf::TUnboxedValue> Rows;
        bool Finished = false;

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) final {
            if (Rows.empty()) {
                return Finished ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
            }
            value = std::move(Rows.front());
            Rows.pop_front();
            return NUdf::EFetchStatus::Ok;
        }
    };

public:
    TOutputTableAggregationRuntime()
        : Setup([this](TCallable& callable, const TComputationNodeFactoryContext& ctx) {
            auto* const node = GetKqpBaseComputeFactory(&ComputeCtx)(callable, ctx);
            if (callable.GetType()->GetName() == "KqpStreamingAggregation") {
                AggregationNode = node;
            }
            return node;
        })
    {
        ComputeCtx.SetCheckpointContext(Checkpoints);
        TKqpProgramBuilder pb(*Setup.Env, *Setup.FunctionRegistry);
        const auto zero = pb.NewDataLiteral<ui64>(0);
        const auto row = pb.NewStruct({{"key", zero}, {"value", zero}});
        const auto input = pb.Arg(TStreamType::Create(row.GetStaticType(), *Setup.Env));
        const auto key = pb.NewStruct({{"key", zero}});
        const auto saved = pb.NewStruct({{"total", zero}});
        KeyType = key.GetStaticType();
        SavedType = saved.GetStaticType();
        const auto binding = pb.NewTuple({pb.NewDataLiteral<NUdf::EDataSlot::String>("/Root/result"),
            pb.NewStruct({{"key", pb.NewDataLiteral<NUdf::EDataSlot::String>("key")},
                {"total", pb.NewDataLiteral<NUdf::EDataSlot::String>("total")}})});
        const auto aggregation = pb.KqpStreamingAggregation(pb.ToFlow(input, {}),
            [&](TRuntimeNode item) { return pb.NewStruct({{"key", pb.Member(item, "key")}}); },
            [&](TRuntimeNode item) { return pb.NewStruct({{"total", pb.Member(item, "value")}}); },
            [&](TRuntimeNode state, TRuntimeNode item) {
                return pb.NewStruct({{"total", pb.Add(pb.Member(state, "total"), pb.Member(item, "value"))}});
            },
            [&](TRuntimeNode key, TRuntimeNode state) {
                return pb.NewTuple({pb.Member(key, "key"), pb.Member(state, "total")});
            }, binding, {}, {}, [&](TRuntimeNode state, TRuntimeNode other) {
                return pb.NewStruct({{"total", pb.Add(pb.Member(state, "total"), pb.Member(other, "total"))}});
            });
        Graph = Setup.BuildGraph(pb.FromFlow(aggregation), {input.GetNode()});
        InputValue = Graph->GetHolderFactory().Create<TInput>();
        Input = static_cast<TInput*>(InputValue.AsBoxed().Get());
        Graph->GetEntryPoint(0, true)->SetValue(Graph->GetContext(), NUdf::TUnboxedValue(InputValue));
        Stream = Graph->GetValue();
    }

    void Seed(const std::vector<std::pair<ui64, ui64>>& rows) {
        auto& ctx = Graph->GetContext();
        TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, 2, ctx);
        out.Write<ui64>(1);
        out.Write<ui64>(1);
        out.Write<ui64>(rows.size());
        const TValuePacker keyPacker(false, KeyType);
        const TValuePacker statePacker(false, SavedType);
        for (const auto& [key, value] : rows) {
            out.WriteUnboxedValue(keyPacker, Array({key}));
            out.WriteUnboxedValue(statePacker, Array({value}));
        }
        out.Write<ui64>(0); // Pending lookups.
        ctx.MutableValues[AggregationNode->GetIndex()] = out.MakeState();
    }

    void Add(ui64 key, ui64 value) {
        Input->Rows.push_back(Array({key, value}));
    }

    void FinishInput() {
        Input->Finished = true;
    }

    TString Save(ui64 generation, ui64 id) {
        Checkpoints->PendingSaveCheckpoint = Checkpoint(generation, id);
        auto result = Graph->SaveGraphState();
        Checkpoints->PendingSaveCheckpoint.Clear();
        return result;
    }

    void Restore(const TString& checkpoint) {
        Checkpoints->LastCommittedCheckpoint.Clear();
        Graph->LoadGraphState(checkpoint);
    }

    void Commit(ui64 generation, ui64 id) {
        Checkpoints->LastCommittedCheckpoint = Checkpoint(generation, id);
    }

    std::pair<ui64, ui64> FetchRow() {
        NUdf::TUnboxedValue row;
        UNIT_ASSERT_VALUES_EQUAL(Stream.Fetch(row), NUdf::EFetchStatus::Ok);
        return {row.GetElement(0).Get<ui64>(), row.GetElement(1).Get<ui64>()};
    }

    void ExpectRow(ui64 key, ui64 value) {
        const auto row = FetchRow();
        UNIT_ASSERT_VALUES_EQUAL(row.first, key);
        UNIT_ASSERT_VALUES_EQUAL(row.second, value);
    }

    void ExpectStatus(NUdf::EFetchStatus status = NUdf::EFetchStatus::Yield) {
        NUdf::TUnboxedValue row;
        UNIT_ASSERT_VALUES_EQUAL(Stream.Fetch(row), status);
    }

    std::map<ui64, ui64> Drain() {
        std::map<ui64, ui64> result;
        NUdf::TUnboxedValue row;
        while (Stream.Fetch(row) == NUdf::EFetchStatus::Ok) {
            UNIT_ASSERT(result.emplace(row.GetElement(0).Get<ui64>(), row.GetElement(1).Get<ui64>()).second);
        }
        return result;
    }

private:
    static NYql::NDqProto::TCheckpoint Checkpoint(ui64 generation, ui64 id) {
        NYql::NDqProto::TCheckpoint result;
        result.SetGeneration(generation);
        result.SetId(id);
        return result;
    }

    NUdf::TUnboxedValue Array(std::initializer_list<ui64> values) {
        NUdf::TUnboxedValue* fields = nullptr;
        auto result = Graph->GetHolderFactory().CreateDirectArrayHolder(values.size(), fields);
        for (const auto value : values) {
            *fields++ = NUdf::TUnboxedValuePod(value);
        }
        return result;
    }

    TKqpComputeContextBase ComputeCtx;
    const TIntrusivePtr<NYql::NDq::TCheckpointContext> Checkpoints = MakeIntrusive<NYql::NDq::TCheckpointContext>();
    IComputationNode* AggregationNode = nullptr;
    TSetup<false> Setup;
    TType* KeyType = nullptr;
    TType* SavedType = nullptr;
    THolder<IComputationGraph> Graph;
    NUdf::TUnboxedValue InputValue;
    TInput* Input = nullptr;
    NUdf::TUnboxedValue Stream;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpStreamingAggregationRuntime) {
    Y_UNIT_TEST(OutputTableFrozenSnapshotAndNewerLive) {
        TOutputTableAggregationRuntime test;
        test.Seed({{1, 10}});
        test.ExpectRow(1, 10);
        test.Add(1, 5);
        test.ExpectStatus();
        test.Save(1, 10);
        test.Add(1, 7);
        test.ExpectStatus();
        test.Commit(1, 10);
        test.ExpectRow(1, 15);
        test.ExpectStatus();
        test.Save(1, 11);
        test.Commit(1, 11);
        test.ExpectRow(1, 22);
        test.ExpectStatus();
        test.Save(1, 12);
        test.Commit(1, 12);
        test.ExpectStatus(); // Evict before emitting this unchanged snapshot again.
        test.FinishInput();
        test.ExpectStatus(NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_TWIN(OutputTableUpdateCancelsEviction, AfterSave) {
        TOutputTableAggregationRuntime test;
        test.Seed({{1, 10}});
        test.ExpectRow(1, 10);
        test.ExpectStatus();
        if constexpr (AfterSave) {
            test.Save(1, 11);
        }
        test.Add(1, 7);
        test.ExpectStatus();
        if constexpr (!AfterSave) {
            test.Save(1, 11);
        }
        test.Commit(1, 11);
        test.ExpectRow(1, AfterSave ? 10 : 17);
        test.ExpectStatus();
        test.Save(1, 12);
        test.Commit(1, 12);
        if constexpr (AfterSave) {
            test.ExpectRow(1, 17);
        }
        test.ExpectStatus();
        test.Save(1, 13);
        test.Commit(1, 13);
        test.FinishInput();
        test.ExpectStatus(NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST(OutputTableAbortedSaveAndCompletedRecovery) {
        TOutputTableAggregationRuntime test;
        test.Seed({{1, 10}, {2, 20}});
        const std::map<ui64, ui64> initial = {{1, 10}, {2, 20}};
        UNIT_ASSERT(test.Drain() == initial);
        test.Save(1, 10);
        test.Add(1, 7);
        test.ExpectStatus();
        const auto checkpoint = test.Save(1, 12); // Checkpoint 10 was aborted.
        test.ExpectStatus();
        test.Restore(checkpoint); // Completed restoration does not resend commit 12.
        const std::map<ui64, ui64> expected = {{1, 17}, {2, 20}};
        // A restored durable batch emits without a new save or commit notification.
        UNIT_ASSERT(test.Drain() == expected);
        test.FinishInput();
        test.ExpectStatus();
        test.Save(2, 1);
        test.Commit(1, 100); // A stale generation must not enable output or eviction.
        test.ExpectStatus();
        test.Commit(2, 1);
        test.ExpectStatus(NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST(OutputTableDrainsBeforeInputAndSurvivesRehash) {
        TOutputTableAggregationRuntime test;
        std::vector<std::pair<ui64, ui64>> rows;
        std::map<ui64, ui64> expected;
        for (ui64 i = 0; i < 4096; ++i) {
            rows.emplace_back(i, i);
            expected.emplace(i, i);
        }
        test.Seed(rows);
        UNIT_ASSERT(test.Drain() == expected);
        const auto checkpoint = test.Save(1, 10);
        test.Restore(checkpoint);
        test.Add(1, 7);
        const auto first = test.FetchRow();
        UNIT_ASSERT_VALUES_EQUAL(expected.at(first.first), first.second);
        expected.erase(first.first);
        // A late restored commit must not restart or evict the batch being drained.
        test.Commit(1, 10);
        UNIT_ASSERT(test.Drain() == expected);
        test.Save(1, 11);
        test.Commit(1, 11);
        test.ExpectRow(1, 8);
        test.ExpectStatus();
        test.Save(1, 12);
        test.Commit(1, 12);
        test.ExpectStatus();
    }

    Y_UNIT_TEST(OutputTableFinishedInputWaitsForDurability) {
        TOutputTableAggregationRuntime test;
        test.Seed({{1, 10}});
        test.ExpectRow(1, 10);
        test.FinishInput();
        test.ExpectStatus();
        const auto checkpoint = test.Save(1, 10);
        test.Restore(checkpoint);
        test.ExpectRow(1, 10);
        test.ExpectStatus(); // The restored input flow reports Finish again.
        test.Commit(1, 10);
        test.ExpectStatus(); // The restored commit cannot evict newly re-emitted rows.
        test.Save(1, 11);
        test.Commit(1, 11);
        test.ExpectStatus(NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_TWIN(QuoteIdentifierSqlSyntax, Ansi) {
        const auto factory = Ansi ? NSQLTranslationV1::MakeAntlr4AnsiLexerFactory() : NSQLTranslationV1::MakeAntlr4LexerFactory();
        const auto lexer = factory->MakeLexer();
        const TVector<TString> names = {
            "", "normal", "select", "column with spaces", "a`b", "``", "\\", "\\`", "\\\\`", "ends-with-\\",
            "` FROM input_table; SELECT 123; --", "`; DROP TABLE input_table; /*", "'\"; -- /* */",
            "line\n\r\t--!ansi_lexer\nSELECT 123;", R"(\x60\u0060\140)", "?" "?/`", "ключ"
        };
        for (const auto& name : names) {
            const auto quoted = CheckQuotedIdentifier(*lexer, name);
            TString decoded;
            TStringOutput output(decoded);
            size_t readBytes = 0;
            const auto result = NYql::UnescapeArbitraryAtom(TStringBuf(quoted).SubStr(1), '`', &output, &readBytes);
            UNIT_ASSERT_C(result == NYql::EUnescapeResult::OK, NYql::UnescapeResultToString(result));
            UNIT_ASSERT_VALUES_EQUAL(readBytes, quoted.size() - 1);
            UNIT_ASSERT_VALUES_EQUAL_C(decoded, name, EscapeC(name));
        }
    }

    Y_UNIT_TEST_TWIN(QuoteIdentifierArbitraryBytes, Ansi) {
        const auto factory = Ansi ? NSQLTranslationV1::MakeAntlr4AnsiLexerFactory() : NSQLTranslationV1::MakeAntlr4LexerFactory();
        const auto lexer = factory->MakeLexer();
        TString allBytes;
        for (ui32 first = 0; first < 256; ++first) {
            const char byte = static_cast<char>(first);
            allBytes.push_back(byte);
            CheckQuotedIdentifier(*lexer, TStringBuf(&byte, 1));
            for (ui32 second = 0; second < 256; ++second) {
                const char pair[] = {byte, static_cast<char>(second)};
                CheckQuotedIdentifier(*lexer, TStringBuf(pair, sizeof(pair)));
            }
        }
        CheckQuotedIdentifier(*lexer, allBytes);
        // Exercise longer runs of backslashes next to a delimiter and SQL-looking text.
        for (ui32 count = 0; count <= 16; ++count) {
            CheckQuotedIdentifier(*lexer, TStringBuilder() << TString(count, '\\') << "`; SELECT 123; --");
        }
    }

    Y_UNIT_TEST(InMemoryStateUsesMiniKqlAllocator) {
        constexpr ui64 keyCount = 65536;
        TKqpComputeContextBase computeCtx;
        TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
        TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
        const auto one = pb.NewDataLiteral<ui64>(1);
        const auto flow = pb.ToFlow(pb.ListFromRange(
            pb.NewDataLiteral<ui64>(0), pb.NewDataLiteral<ui64>(keyCount), one), {});
        const auto aggregation = pb.KqpStreamingAggregation(flow,
            [&](TRuntimeNode item) { return pb.NewTuple({item}); },
            [&](TRuntimeNode) { return one; },
            [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.Add(pb.Nth(key, 0), state); },
            pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        auto graph = setup.BuildGraph(pb.FromFlow(aggregation));
        const auto stream = graph->GetValue();
        const auto usedBefore = setup.Alloc.GetUsed();
        NUdf::TUnboxedValue item;
        for (ui64 i = 0; i < keyCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), i + 1);
        }

        // The input is lazy and outputs/states are embedded integers. Retained
        // map entries, rather than collected results, must account for this growth.
        const auto usedAfter = setup.Alloc.GetUsed();
        UNIT_ASSERT_C(usedAfter >= usedBefore + keyCount * sizeof(NUdf::TUnboxedValue),
            "State map storage is not charged to MiniKQL: before=" << usedBefore << ", after=" << usedAfter);
        UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST(InMemoryKeyPayloadUsesMiniKqlAllocator) {
        constexpr ui64 keyCount = 1024;
        constexpr ui64 keySize = 4096;
        TKqpComputeContextBase computeCtx;
        TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
        TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
        const auto one = pb.NewDataLiteral<ui64>(1);
        const auto count = pb.NewDataLiteral<ui64>(keyCount);
        const auto prefix = pb.NewDataLiteral<NUdf::EDataSlot::String>(TString(keySize, 'x'));
        const auto flow = pb.ToFlow(pb.ListFromRange(
            pb.NewDataLiteral<ui64>(0), pb.NewDataLiteral<ui64>(2 * keyCount), one), {});
        const auto aggregation = pb.KqpStreamingAggregation(flow,
            [&](TRuntimeNode item) {
                return pb.NewStruct({{"key", pb.Concat(prefix, pb.ToString(pb.Mod(item, count)))}});
            },
            [&](TRuntimeNode) { return one; },
            [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
            [&](TRuntimeNode, TRuntimeNode state) { return state; },
            pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        auto graph = setup.BuildGraph(pb.FromFlow(aggregation));
        const auto stream = graph->GetValue();
        const auto usedBefore = setup.Alloc.GetUsed();
        NUdf::TUnboxedValue item;
        for (ui64 i = 0; i < 2 * keyCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), i / keyCount + 1);
        }

        // Each input constructs a fresh long string. Retaining only packed TString keys
        // would leave this payload growth outside the MiniKQL allocator.
        const auto usedAfter = setup.Alloc.GetUsed();
        UNIT_ASSERT_C(usedAfter >= usedBefore + keyCount * keySize,
            "Key payloads are not charged to MiniKQL: before=" << usedBefore << ", after=" << usedAfter);
        UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_TWIN(TypedKeysAndCheckpointRecovery, LegacyCheckpoint) {
        for (const auto keyKind : {TType::EKind::Data, TType::EKind::Tuple, TType::EKind::Struct}) {
            TKqpComputeContextBase computeCtx;
            TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
            TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
            const auto one = pb.NewDataLiteral<ui64>(1);
            const auto zero = pb.NewDataLiteral<double>(0.0);
            const auto nan = std::bit_cast<double>(ui64(0x7ff8000000000001));
            const auto otherNan = std::bit_cast<double>(ui64(0xfff8000000000002));
            const auto flow = pb.ToFlow(pb.AsList({zero, pb.NewDataLiteral<double>(-0.0),
                pb.NewDataLiteral<double>(nan), pb.NewDataLiteral<double>(otherNan),
                pb.NewDataLiteral<double>(1.0), pb.NewDataLiteral<double>(nan), zero}), {});
            const auto keyExtractor = [&](TRuntimeNode item) {
                if (keyKind == TType::EKind::Tuple) {
                    return pb.NewTuple({item});
                }
                if (keyKind == TType::EKind::Struct) {
                    return pb.NewStruct({{"key", item}});
                }
                return item;
            };
            const auto aggregation = pb.KqpStreamingAggregation(flow, keyExtractor,
                [&](TRuntimeNode) { return one; },
                [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
                [&](TRuntimeNode, TRuntimeNode state) { return state; },
                pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
            const auto program = pb.FromFlow(aggregation);
            TString checkpoint;
            {
                auto graph = setup.BuildGraph(program);
                const auto stream = graph->GetValue();
                NUdf::TUnboxedValue item;
                for (const ui64 expected : {1, 2, 1, 2, 1, 3, 3}) {
                    UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Ok);
                    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), expected);
                }

                if constexpr (LegacyCheckpoint) {
                    // The previous implementation wrote packed keys as TString values.
                    auto& ctx = graph->GetContext();
                    TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, 1, ctx);
                    out.Write<ui64>(3);
                    const TValuePacker keyPacker(true, keyExtractor(zero).GetStaticType());
                    const TValuePacker statePacker(false, one.GetStaticType());
                    for (const double value : {0.0, nan, 1.0}) {
                        NUdf::TUnboxedValue key{NUdf::TUnboxedValuePod(value)};
                        if (keyKind != TType::EKind::Data) {
                            NUdf::TUnboxedValue* fields = nullptr;
                            key = ctx.HolderFactory.CreateDirectArrayHolder(1, fields);
                            fields[0] = NUdf::TUnboxedValuePod(value);
                        }
                        out(TString(keyPacker.Pack(key)));
                        out.WriteUnboxedValue(statePacker, NUdf::TUnboxedValuePod(ui64(value == 1.0 ? 1 : 3)));
                    }
                    TString bytes;
                    const auto chunks = out.MakeState();
                    const auto iterator = chunks.GetListIterator();
                    NUdf::TUnboxedValue chunk;
                    while (iterator.Next(chunk)) {
                        const auto data = chunk.AsStringRef();
                        bytes.AppendNoAlias(data.Data(), data.Size());
                    }
                    TNodeStateHelper::AddNodeState(checkpoint, bytes);
                } else {
                    checkpoint = graph->SaveGraphState();
                }
            }

            auto graph = setup.BuildGraph(program);
            graph->LoadGraphState(checkpoint);
            const auto stream = graph->GetValue();
            NUdf::TUnboxedValue item;
            for (const ui64 expected : {4, 5, 4, 5, 2, 6, 6}) {
                UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), expected);
            }
            UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Finish);
        }
    }

    Y_UNIT_TEST(PatternIsNotCacheable) {
        for (const TStringBuf stateTablePath : {"", "/Root/state"}) {
            TKqpComputeContextBase computeCtx;
            TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
            TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
            const auto one = pb.NewDataLiteral<ui64>(1);
            const auto two = pb.NewDataLiteral<ui64>(2);
            const auto flow = pb.ToFlow(pb.AsList({one, one, two}), {});
            const auto aggregation = pb.KqpStreamingAggregation(flow,
                [&](TRuntimeNode item) { return pb.NewTuple({item}); },
                [&](TRuntimeNode) { return one; },
                [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
                [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewTuple({key, state}); },
                pb.NewDataLiteral<NUdf::EDataSlot::String>(stateTablePath));
            auto graph = setup.BuildGraph(pb.Collect(aggregation));
            UNIT_ASSERT(!setup.Pattern->GetSuitableForCache());

            // The in-memory backend still works without an actor or wakeup callback.
            if (stateTablePath.empty()) {
                const auto result = graph->GetValue();
                const auto iterator = result.GetListIterator();
                NUdf::TUnboxedValue item;
                for (const ui64 expected : {1, 2, 1}) {
                    UNIT_ASSERT(iterator.Next(item));
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<ui64>(), expected);
                }
                UNIT_ASSERT(!iterator.Next(item));
            }
        }
    }

    Y_UNIT_TEST(WakeupCallbackOutlivesContext) {
        for (const bool readyBeforeSubscribe : {false, true}) {
            for (const bool failed : {false, true}) {
                auto promise = NThreading::NewPromise<void>();
                const auto complete = [&]() {
                    if (failed) {
                        promise.SetException("test failure");
                    } else {
                        promise.SetValue();
                    }
                };
                if (readyBeforeSubscribe) {
                    complete();
                }

                ui32 wakeups = 0;
                {
                    TKqpComputeContextBase computeCtx;
                    UNIT_ASSERT(!computeCtx.GetWakeupCallback());
                    computeCtx.SetWakeupCallback([&]() { ++wakeups; });
                    const auto wakeupCallback = computeCtx.GetWakeupCallback();
                    promise.GetFuture().Subscribe([wakeupCallback](const auto&) { wakeupCallback(); });
                    computeCtx.SetWakeupCallback({});
                    UNIT_ASSERT_VALUES_EQUAL(wakeups, readyBeforeSubscribe ? 1 : 0);
                }

                // The subscription owns the callback independently of the KQP context.
                if (!readyBeforeSubscribe) {
                    complete();
                }
                UNIT_ASSERT_VALUES_EQUAL(wakeups, 1);
            }
        }
    }
}

} // namespace NKikimr::NMiniKQL
