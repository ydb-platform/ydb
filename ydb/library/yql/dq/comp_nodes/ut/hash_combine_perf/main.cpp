#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_setup.h>

#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <algorithm>
#include <ctime>
#include <vector>

#if defined(_linux_)
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace NKikimr::NMiniKQL {
namespace {

enum class EMode { Combine, Aggregate };
enum class EKey { Int64, OptionalInt64, String };
enum class EState { Uint64, OptionalUint64, Tuple, List };

struct TCase {
    EMode Mode = EMode::Combine;
    EKey Key = EKey::Int64;
    EState State = EState::Uint64;
    size_t States = 1;
    size_t Rows = 4'000'000;
    size_t Distinct = 100;
};

const char* ToName(EMode mode) {
    return mode == EMode::Combine ? "combine" : "aggregate";
}

const char* ToName(EKey key) {
    switch (key) {
        case EKey::Int64: return "int64";
        case EKey::OptionalInt64: return "optional-int64";
        case EKey::String: return "string";
    }
    Y_UNREACHABLE();
}

const char* ToName(EState state) {
    switch (state) {
        case EState::Uint64: return "uint64";
        case EState::OptionalUint64: return "optional-uint64";
        case EState::Tuple: return "tuple";
        case EState::List: return "list";
    }
    Y_UNREACHABLE();
}

ui64 SplitMix64(ui64 x) {
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

// Rows of (key, 1); keys are uniform over [0, Distinct), or all unique when Distinct is 0
class TInputStream: public NUdf::TBoxedValue {
public:
    explicit TInputStream(const TCase& testCase)
        : Case(testCase)
    {
    }

private:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
        Y_ENSURE(width == 2);
        if (Row == Case.Rows) {
            return NUdf::EFetchStatus::Finish;
        }
        const ui64 key = Case.Distinct ? SplitMix64(Row) % Case.Distinct : Row * 0x9E3779B97F4A7C15ull;
        if (Case.Key == EKey::String) {
            const TString text = Sprintf("%016llx", static_cast<unsigned long long>(key));
            result[0] = MakeString(NUdf::TStringRef(text.data(), text.size()));
        } else {
            result[0] = NUdf::TUnboxedValuePod(static_cast<i64>(key));
        }
        result[1] = NUdf::TUnboxedValuePod(ui64{1});
        ++Row;
        return NUdf::EFetchStatus::Ok;
    }

    const TCase Case;
    size_t Row = 0;
};

// User-mode cycles and instructions of the calling thread; unavailable when perf_event_open is not permitted
class TCounters {
public:
    TCounters() {
#if defined(_linux_)
        Fds[0] = Open(PERF_COUNT_HW_CPU_CYCLES);
        Fds[1] = Open(PERF_COUNT_HW_INSTRUCTIONS);
#endif
    }

    ~TCounters() {
#if defined(_linux_)
        for (int fd : Fds) {
            if (fd >= 0) {
                close(fd);
            }
        }
#endif
    }

    bool Available() const {
        return Fds[0] >= 0 && Fds[1] >= 0;
    }

    void Start() {
#if defined(_linux_)
        for (int fd : Fds) {
            if (fd >= 0) {
                ioctl(fd, PERF_EVENT_IOC_RESET, 0);
                ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
            }
        }
#endif
    }

    void Stop(ui64& cycles, ui64& instructions) {
        cycles = Read(Fds[0]);
        instructions = Read(Fds[1]);
    }

private:
#if defined(_linux_)
    static int Open(ui64 config) {
        perf_event_attr attr = {};
        attr.type = PERF_TYPE_HARDWARE;
        attr.size = sizeof(attr);
        attr.config = config;
        attr.disabled = 1;
        attr.exclude_kernel = 1;
        attr.exclude_hv = 1;
        return syscall(__NR_perf_event_open, &attr, 0, -1, -1, 0);
    }
#endif

    static ui64 Read(int fd) {
        ui64 value = 0;
#if defined(_linux_)
        if (fd >= 0) {
            ioctl(fd, PERF_EVENT_IOC_DISABLE, 0);
            Y_ENSURE(read(fd, &value, sizeof(value)) == sizeof(value));
        }
#else
        Y_UNUSED(fd);
#endif
        return value;
    }

    int Fds[2] = {-1, -1};
};

TDuration ThreadCpuTime() {
    timespec ts;
    Y_ENSURE(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0);
    return TDuration::Seconds(ts.tv_sec) + TDuration::MicroSeconds(ts.tv_nsec / 1000);
}

struct TRunResult {
    bool Bypass = false;
    size_t OutputRows = 0;
    TDuration Cpu;
    ui64 Cycles = 0;
    ui64 Instructions = 0;
    bool HasCounters = false;
};

template <bool LLVM>
TRunResult RunOnce(const TCase& testCase) {
    TDqSetup<LLVM> setup(GetDqNodeFactory());
    setup.Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    TDqProgramBuilder& pb = setup.GetDqProgramBuilder();

    TType* keyType = pb.NewDataType(testCase.Key == EKey::String ? NUdf::EDataSlot::String : NUdf::EDataSlot::Int64);
    if (testCase.Key == EKey::OptionalInt64) {
        keyType = pb.NewOptionalType(keyType);
    }
    TType* valueType = pb.NewDataType(NUdf::EDataSlot::Uint64);
    if (testCase.State == EState::OptionalUint64) {
        valueType = pb.NewOptionalType(valueType);
    }
    const auto source = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode",
        pb.NewStreamType(pb.NewMultiType({keyType, valueType}))).Build();

    const auto extractKey = [](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {items[0]};
    };
    const auto init = [&](TRuntimeNode::TList, TRuntimeNode::TList items) -> TRuntimeNode::TList {
        switch (testCase.State) {
            case EState::Tuple:
                return {pb.NewTuple({items[1], pb.NewDataLiteral<ui64>(1)})};
            case EState::List:
                return {pb.AsList(items[1])};
            default:
                return TRuntimeNode::TList(testCase.States, items[1]);
        }
    };
    const auto update = [&](TRuntimeNode::TList, TRuntimeNode::TList items, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        switch (testCase.State) {
            case EState::Tuple:
                return {pb.NewTuple({
                    pb.AggrAdd(pb.Nth(state[0], 0), items[1]),
                    pb.AggrAdd(pb.Nth(state[0], 1), pb.NewDataLiteral<ui64>(1))})};
            case EState::List:
                return {pb.Append(state[0], items[1])};
            default: {
                TRuntimeNode::TList result;
                for (const auto& item : state) {
                    result.push_back(pb.AggrAdd(item, items[1]));
                }
                return result;
            }
        }
    };
    // Every output row ends with a count, so the counts of all rows add up to the input row count
    const auto finish = [&](TRuntimeNode::TList keys, TRuntimeNode::TList state) -> TRuntimeNode::TList {
        switch (testCase.State) {
            case EState::Tuple:
                return {keys[0], pb.Nth(state[0], 1)};
            case EState::List:
                return {keys[0], pb.Length(state[0])};
            default: {
                TRuntimeNode::TList result = keys;
                result.insert(result.end(), state.begin(), state.end());
                return result;
            }
        }
    };

    const TRuntimeNode input(source, false);
    const auto root = testCase.Mode == EMode::Aggregate
        ? pb.DqHashAggregate(input, false, extractKey, init, update, finish)
        : pb.DqHashCombine(input, 128_MB, extractKey, init, update, finish);
    auto graph = setup.BuildGraph(root, {source});

    TRunResult result;
    for (auto& node : graph->GetNodes()) {
        if (auto* testPoints = dynamic_cast<TDqHashCombineTestPoints*>(node.Get())) {
            testPoints->SetTestStateCallback([&result](const TDqHashCombineTestState& state) {
                result.Bypass = result.Bypass || state.BypassActivated;
            });
        }
    }
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(),
        NUdf::TUnboxedValuePod(new TInputStream(testCase)));

    const size_t width = testCase.State == EState::Tuple || testCase.State == EState::List ? 2 : 1 + testCase.States;
    std::vector<NUdf::TUnboxedValue> output(width);
    ui64 count = 0;

    TCounters counters;
    const auto cpuStart = ThreadCpuTime();
    counters.Start();
    {
        auto stream = graph->GetValue();
        for (;;) {
            const auto status = stream.WideFetch(output.data(), output.size());
            if (status == NUdf::EFetchStatus::Finish) {
                break;
            }
            Y_ENSURE(status == NUdf::EFetchStatus::Ok);
            ++result.OutputRows;
            count += output[1].Get<ui64>();
        }
    }
    counters.Stop(result.Cycles, result.Instructions);
    result.Cpu = ThreadCpuTime() - cpuStart;
    result.HasCounters = counters.Available();

    Y_ENSURE(count == testCase.Rows, "Output counts add up to " << count << " instead of " << testCase.Rows);
    return result;
}

template <typename T>
T Median(std::vector<T> values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

template <bool LLVM>
void Report(const TCase& testCase, size_t repeats) {
    RunOnce<LLVM>(testCase);

    std::vector<TRunResult> runs;
    for (size_t i = 0; i < repeats; ++i) {
        runs.push_back(RunOnce<LLVM>(testCase));
    }

    std::vector<TDuration> cpu;
    std::vector<ui64> cycles;
    std::vector<ui64> instructions;
    bool bypass = false;
    for (const auto& run : runs) {
        cpu.push_back(run.Cpu);
        cycles.push_back(run.Cycles);
        instructions.push_back(run.Instructions);
        bypass = bypass || run.Bypass;
    }

    TStringBuilder line;
    line << "mode=" << ToName(testCase.Mode)
         << " key=" << ToName(testCase.Key)
         << " state=" << ToName(testCase.State)
         << " states=" << (testCase.State == EState::Tuple || testCase.State == EState::List ? 1 : testCase.States)
         << " rows=" << testCase.Rows
         << " distinct=" << (testCase.Distinct ? ToString(testCase.Distinct) : TString("unique"))
         << " llvm=" << (LLVM ? 1 : 0)
         << " bypass=" << (bypass ? 1 : 0)
         << " out_rows=" << runs.front().OutputRows
         << " cpu_ms_best=" << Sprintf("%.1f", std::min_element(cpu.begin(), cpu.end())->MicroSeconds() / 1000.0)
         << " cpu_ms_median=" << Sprintf("%.1f", Median(cpu).MicroSeconds() / 1000.0);
    if (runs.front().HasCounters) {
        line << " mcycles_best=" << *std::min_element(cycles.begin(), cycles.end()) / 1'000'000
             << " mcycles_median=" << Median(cycles) / 1'000'000
             << " minstr_median=" << Median(instructions) / 1'000'000;
    } else {
        line << " mcycles=n/a";
    }
    Cout << line << Endl;
}

std::vector<TCase> FullMatrix() {
    std::vector<TCase> cases;
    const auto add = [&](EMode mode, EKey key, EState state, size_t states, size_t distinct, size_t rows = 4'000'000) {
        cases.push_back({.Mode = mode, .Key = key, .State = state, .States = states, .Rows = rows, .Distinct = distinct});
    };

    for (size_t distinct : {size_t(0), size_t(1'000'000), size_t(100'000), size_t(100)}) {
        add(EMode::Combine, EKey::OptionalInt64, EState::Uint64, 1, distinct);
        add(EMode::Combine, EKey::Int64, EState::Uint64, 1, distinct);
        add(EMode::Combine, EKey::Int64, EState::OptionalUint64, 1, distinct);
    }
    add(EMode::Combine, EKey::OptionalInt64, EState::Uint64, 5, 100);
    add(EMode::Combine, EKey::Int64, EState::Uint64, 5, 100);
    add(EMode::Combine, EKey::Int64, EState::OptionalUint64, 5, 100);
    add(EMode::Combine, EKey::String, EState::Uint64, 1, 0);

    for (size_t distinct : {size_t(1'000'000), size_t(100'000), size_t(100)}) {
        for (size_t states : {size_t(1), size_t(5)}) {
            add(EMode::Aggregate, EKey::OptionalInt64, EState::Uint64, states, distinct);
            add(EMode::Aggregate, EKey::Int64, EState::Uint64, states, distinct);
            add(EMode::Aggregate, EKey::Int64, EState::OptionalUint64, states, distinct);
        }
    }

    for (EMode mode : {EMode::Combine, EMode::Aggregate}) {
        add(mode, EKey::Int64, EState::Tuple, 1, 1);
        add(mode, EKey::Int64, EState::Tuple, 1, 100);
        add(mode, EKey::Int64, EState::Tuple, 1, 100'000);
        add(mode, EKey::Int64, EState::List, 1, 1, 2'000'000);
        add(mode, EKey::Int64, EState::List, 1, 100, 2'000'000);
    }
    return cases;
}

} // namespace
} // namespace NKikimr::NMiniKQL

int main(int argc, const char* argv[]) {
    using namespace NKikimr::NMiniKQL;

    TCase testCase;
    size_t repeats = 3;
    bool llvm = false;
    bool all = false;

    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');
    opts.SetFreeArgsNum(0);
    opts.AddLongOption("mode")
        .Choices({"combine", "aggregate"})
        .DefaultValue("combine")
        .Handler1([&](const NLastGetopt::TOptsParser* parser) {
            testCase.Mode = TStringBuf(parser->CurVal()) == "aggregate" ? EMode::Aggregate : EMode::Combine;
        })
        .Help("combine: DqHashCombine with a 128 MiB limit (bypass possible); aggregate: DqHashAggregate without spilling");
    opts.AddLongOption("key")
        .Choices({"int64", "optional-int64", "string"})
        .DefaultValue("int64")
        .Handler1([&](const NLastGetopt::TOptsParser* parser) {
            const TStringBuf value = parser->CurVal();
            testCase.Key = value == "string" ? EKey::String : value == "optional-int64" ? EKey::OptionalInt64 : EKey::Int64;
        })
        .Help("Key column type");
    opts.AddLongOption("state")
        .Choices({"uint64", "optional-uint64", "tuple", "list"})
        .DefaultValue("uint64")
        .Handler1([&](const NLastGetopt::TOptsParser* parser) {
            const TStringBuf value = parser->CurVal();
            testCase.State = value == "tuple" ? EState::Tuple
                : value == "list" ? EState::List
                : value == "optional-uint64" ? EState::OptionalUint64
                : EState::Uint64;
        })
        .Help("uint64 and optional-uint64: SUM states; tuple: (sum, count); list: AGG_LIST-like append");
    opts.AddLongOption("states")
        .StoreResult(&testCase.States)
        .DefaultValue(testCase.States)
        .Help("Number of SUM states (uint64 and optional-uint64 only)");
    opts.AddLongOption("rows")
        .StoreResult(&testCase.Rows)
        .DefaultValue(testCase.Rows)
        .Help("Input rows");
    opts.AddLongOption("distinct")
        .StoreResult(&testCase.Distinct)
        .DefaultValue(testCase.Distinct)
        .Help("Distinct keys, uniformly distributed; 0 makes every key unique");
    opts.AddLongOption("repeats")
        .StoreResult(&repeats)
        .DefaultValue(repeats)
        .Help("Measured runs after one warm-up run; best and median are reported");
    opts.AddLongOption("llvm")
        .NoArgument()
        .SetFlag(&llvm)
        .Help("Build the graph with LLVM");
    opts.AddLongOption("all")
        .NoArgument()
        .SetFlag(&all)
        .Help("Run the predefined matrix of shapes instead of a single case");

    NLastGetopt::TOptsParseResult parsedOpts(&opts, argc, argv);
    Y_ENSURE(repeats > 0, "--repeats must be positive");
    Y_ENSURE(testCase.States > 0, "--states must be positive");
    Y_ENSURE(testCase.Rows > 0, "--rows must be positive");

    const std::vector<TCase> cases = all ? FullMatrix() : std::vector<TCase>{testCase};
    for (const auto& item : cases) {
        if (llvm) {
            Report<true>(item, repeats);
        } else {
            Report<false>(item, repeats);
        }
    }
    return 0;
}
