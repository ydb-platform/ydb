#include "joins.h"
#include "construct_join_graph.h"

#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_factories.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/utils.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_list.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/generic/algorithm.h>
#include <util/string/printf.h>
#include <util/system/datetime.h>

#include <cmath>
#include <format>
#include <optional>
#include <random>
#include <span>

using namespace NKikimr::NMiniKQL;
namespace NUdf = NYql::NUdf;

namespace {
struct TGeneratedKeys {
    TVector<ui64> Left;
    TVector<ui64> Right;
};

// Both sides are generated together, because selectivity is a property of the
// pair. Every complete group on the right contains exactly dupsPerKey copies;
// a final incomplete group deterministically contains the remainder. The left
// side has a rounded, exact number of matching rows and only draws matching
// keys from groups that were actually emitted on the right.
TGeneratedKeys GenerateKeys(TTableSizes sizes, TSelectivity selectivity, int seed) {
    const double matchRate = selectivity.MatchRate;
    const int dupsPerKey = selectivity.DupsPerKey;
    Y_ABORT_IF(dupsPerKey < 1, "dups per key must be at least 1");
    Y_ABORT_IF(matchRate < 0.0 || matchRate > 1.0, "match rate must be within [0, 1]");

    std::default_random_engine eng;
    eng.seed(seed);

    TGeneratedKeys keys;
    keys.Right.reserve(sizes.Right);
    const ui64 completeKeys = sizes.Right / dupsPerKey;
    const ui64 remainder = sizes.Right % dupsPerKey;
    const ui64 presentKeys = completeKeys + (remainder != 0);
    for (ui64 key = 0; key < completeKeys; ++key) {
        keys.Right.insert(keys.Right.end(), dupsPerKey, key);
    }
    keys.Right.insert(keys.Right.end(), remainder, completeKeys);
    std::shuffle(keys.Right.begin(), keys.Right.end(), eng);

    const size_t matchingRows = static_cast<size_t>(std::llround(matchRate * sizes.Left));
    Y_ABORT_IF(matchingRows != 0 && presentKeys == 0, "cannot generate matching rows with an empty right side");
    std::uniform_int_distribution<ui64> matching(0, presentKeys ? presentKeys - 1 : 0);
    std::uniform_int_distribution<ui64> missing(0, Max<ui64>(1, sizes.Left) - 1);

    keys.Left.reserve(sizes.Left);
    std::generate_n(std::back_inserter(keys.Left), matchingRows, [&]() { return matching(eng); });
    std::generate_n(std::back_inserter(keys.Left), sizes.Left - matchingRows,
                    [&]() { return presentKeys + missing(eng); });
    std::shuffle(keys.Left.begin(), keys.Left.end(), eng);
    return keys;
}

// Every key column past the first is a function of the same key. Integer and
// string columns are bijective, so widening the key changes how much comparing
// and hashing cost without changing which rows match.
ui64 KeyComponent(ui64 key, int col) { return col == 0 ? key : key * (7 + static_cast<ui64>(col)) + 1; }

TString FormatStringKey(ui64 key, int col, int stringBytes) {
    const ui64 num = KeyComponent(key, col) + 1234567;
    TString result = std::format("{:08}.{:08}.{:08}.", num, num, num);
    Y_ABORT_IF(std::ssize(result) > stringBytes, "string key length is too small");
    result.resize(stringBytes, 'x');
    return result;
}

NUdf::TUnboxedValue MakeKeyValue(ETestedJoinKeyType keyType, ui64 key, int col, int stringBytes) {
    const ui64 value = KeyComponent(key, col);
    switch (keyType) {
    case ETestedJoinKeyType::kString:
        return MakeString(FormatStringKey(key, col, stringBytes));
    case ETestedJoinKeyType::kInteger:
        return NUdf::TUnboxedValuePod(value);
    }
    Y_ABORT("unreachable");
}

bool SupportsJoinFilters(ETestedJoinAlgo algo) {
    return algo == ETestedJoinAlgo::kBlockHash || algo == ETestedJoinAlgo::kScalarHash;
}

bool SupportsJoinKind(ETestedJoinAlgo algo, EJoinKind kind) {
    const bool leftOriented = kind == EJoinKind::Inner || kind == EJoinKind::Left || kind == EJoinKind::LeftSemi ||
                              kind == EJoinKind::LeftOnly;
    switch (algo) {
    case ETestedJoinAlgo::kScalarMap:
    case ETestedJoinAlgo::kBlockMap:
        return leftOriented;
    case ETestedJoinAlgo::kBlockHash:
    case ETestedJoinAlgo::kScalarHash:
        return leftOriented || kind == EJoinKind::Cross;
    case ETestedJoinAlgo::kScalarGrace:
        return kind != EJoinKind::Cross;
    }
    Y_ABORT("unreachable");
}

TDqProgramBuilder::TJoinFilterLambda SideFilter(TDqSetup<false, true>* setup, ETestedJoinKeyType type, bool keepHigh,
                                                ui64 midKey, int stringBytes) {
    return [setup, type, keepHigh, midKey, stringBytes](TRuntimeNode::TList row) -> TRuntimeNode {
        auto& pb = setup->GetDqProgramBuilder();
        const TRuntimeNode col = row[0];
        TRuntimeNode pred;
        switch (type) {
        case ETestedJoinKeyType::kInteger: {
            const auto bit = pb.BitAnd(col, pb.NewDataLiteral<ui64>(1));
            const auto even = pb.Equals(bit, pb.NewDataLiteral<ui64>(0));
            pred = keepHigh ? even : pb.Not(even);
            break;
        }
        case ETestedJoinKeyType::kString: {
            const auto lit =
                pb.NewDataLiteral<NUdf::EDataSlot::String>(FormatStringKey(midKey, /* col */ 0, stringBytes));
            pred = keepHigh ? pb.Greater(col, lit) : pb.Less(col, lit);
            break;
        }
        }
        return pb.Coalesce(pred, pb.NewDataLiteral<bool>(false));
    };
}

void AttachFilters(TJoinDescription& descr, TDqSetup<false, true>* setup, const TKeySchema& keySchema,
                   ETestedFilter filter, ui64 midKey, int stringBytes) {
    if (filter == ETestedFilter::kNone) {
        return;
    }
    const auto type = keySchema.front();
    if (filter == ETestedFilter::kLeft || filter == ETestedFilter::kAll) {
        descr.LeftFilter = SideFilter(setup, type, true, midKey, stringBytes);
    }
    if (filter == ETestedFilter::kRight || filter == ETestedFilter::kAll) {
        descr.RightFilter = SideFilter(setup, type, false, midKey, stringBytes);
    }
    if (filter == ETestedFilter::kCommon || filter == ETestedFilter::kAll) {
        descr.CommonFilter = [setup, type, midKey, stringBytes](TRuntimeNode::TList left,
                                                               TRuntimeNode::TList) -> TRuntimeNode {
            auto& pb = setup->GetDqProgramBuilder();
            TRuntimeNode pred;
            switch (type) {
            case ETestedJoinKeyType::kInteger:
                pred = pb.Equals(pb.BitAnd(left[0], pb.NewDataLiteral<ui64>(1)), pb.NewDataLiteral<ui64>(0));
                break;
            case ETestedJoinKeyType::kString:
                pred = pb.Less(
                    left[0], pb.NewDataLiteral<NUdf::EDataSlot::String>(FormatStringKey(midKey, 0, stringBytes)));
                break;
            }
            return pb.Coalesce(pred, pb.NewDataLiteral<bool>(false));
        };
    }
}

TType* KeyColumnType(TDqProgramBuilder& pb, ETestedJoinKeyType keyType) {
    switch (keyType) {
    case ETestedJoinKeyType::kString:
        return pb.NewDataType(NUdf::EDataSlot::String);
    case ETestedJoinKeyType::kInteger:
        return pb.NewDataType(NUdf::TDataType<ui64>::Id);
    }
    Y_ABORT("unreachable");
}

int PayloadColumnCount(ETestedPayload payload, int widePayloadColumns) {
    switch (payload) {
    case ETestedPayload::kNarrow:
        return 1;
    case ETestedPayload::kWide:
        return widePayloadColumns;
    }
    Y_ABORT("unreachable");
}

// Narrow keeps the shape this benchmark always had: one integer column on the
// left and one string column on the right. Wide adds integer columns only, to
// grow rows without also changing how payload values are compared and copied.
TType* PayloadColumnType(TDqProgramBuilder& pb, ETestedPayload payload, bool leftSide) {
    if (payload == ETestedPayload::kNarrow && !leftSide) {
        return pb.NewDataType(NUdf::EDataSlot::String);
    }
    return pb.NewDataType(NUdf::TDataType<ui64>::Id);
}

NUdf::TUnboxedValue MakePayloadValue(ETestedPayload payload, bool leftSide, ui64 row, int col) {
    if (payload == ETestedPayload::kNarrow) {
        return leftSide ? NUdf::TUnboxedValuePod(ui64{111}) : MakeString("woo");
    }
    const ui64 value = row * (17 + static_cast<ui64>(col)) + (leftSide ? 3 : 11);
    return NUdf::TUnboxedValuePod(value);
}

struct TSideData {
    TVector<TType*> ColumnTypes;
    NUdf::TUnboxedValue Values;
};

TSideData BuildSide(TDqSetup<false, true>& setup, const THolderFactory& factory, const TVector<ui64>& keys,
                    const TKeySchema& keySchema, ETestedPayload payload, bool leftSide, int stringBytes,
                    int widePayloadColumns) {
    TDqProgramBuilder& pb = setup.GetDqProgramBuilder();
    const int keyCols = std::ssize(keySchema);
    const int payloadCols = PayloadColumnCount(payload, widePayloadColumns);

    TSideData side;
    for (int col = 0; col < keyCols; ++col) {
        side.ColumnTypes.push_back(KeyColumnType(pb, keySchema[col]));
    }
    for (int col = 0; col < payloadCols; ++col) {
        side.ColumnTypes.push_back(PayloadColumnType(pb, payload, leftSide));
    }

    TDefaultListRepresentation list;
    for (size_t row = 0; row < keys.size(); ++row) {
        NUdf::TUnboxedValue* items = nullptr;
        auto tuple = factory.CreateDirectArrayHolder(keyCols + payloadCols, items);
        for (int col = 0; col < keyCols; ++col) {
            items[col] = MakeKeyValue(keySchema[col], keys[row], col, stringBytes);
        }
        for (int col = 0; col < payloadCols; ++col) {
            items[keyCols + col] = MakePayloadValue(payload, leftSide, row, col);
        }
        list = list.Append(std::move(tuple));
    }
    side.Values = factory.CreateDirectListHolder(std::move(list));
    return side;
}

struct TInputs {
    TSideData Left;
    TSideData Right;
};

TInputs PrepareInputs(TDqSetup<false, true>& setup, const TBenchmarkSettings& params, const TKeySchema& keySchema,
                      ETestedPayload payload, TSelectivity selectivity, TTableSizes sizes) {
    auto& pb = setup.GetDqProgramBuilder();
    auto dummy = setup.BuildGraph(pb.NewDataLiteral<ui64>(0));
    const auto& factory = dummy->GetHolderFactory();

    const TGeneratedKeys keys = GenerateKeys(sizes, selectivity, params.Seed);

    TInputs inputs;
    inputs.Left =
        BuildSide(setup, factory, keys.Left, keySchema, payload, true, params.StringBytes, params.WidePayloadColumns);
    inputs.Right =
        BuildSide(setup, factory, keys.Right, keySchema, payload, false, params.StringBytes, params.WidePayloadColumns);
    return inputs;
}

// Converting inputs to blocks is not part of what we measure, so it is done
// once per case and the result is reused by every run. Doing it per run would
// also churn the arena between samples.
void MaterializeBlocks(TDqSetup<false, true>& setup, TInputs& inputs, const TVector<int>& blockSizes) {
    TVector<size_t> lengths(blockSizes.begin(), blockSizes.end());
    auto& pb = setup.GetDqProgramBuilder();
    auto dummy = setup.BuildGraph(pb.NewDataLiteral<ui64>(0));
    auto& ctx = dummy->GetContext();
    auto leftBlocks = ToBlocks(ctx, lengths, inputs.Left.ColumnTypes, inputs.Left.Values);
    auto rightBlocks = ToBlocks(ctx, lengths, inputs.Right.ColumnTypes, inputs.Right.Values);
    inputs.Left.Values = std::move(leftBlocks);
    inputs.Right.Values = std::move(rightBlocks);
}

i64 LineSize(ETestedJoinAlgo algo, std::span<const NYql::NUdf::TUnboxedValue> line) {
    if (IsBlockJoin(algo)) {
        return TArrowBlock::From(line.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }
    return 1;
}

struct TSample {
    TDuration Cpu;
    TDuration Wall;
    i64 OutputRows = 0;
    int Iters = 1;
};

TSample RunOnce(ETestedJoinAlgo algo, EJoinKind joinKind, const TJoinDescription& descr, ui32 cols) {
    descr.Setup->Alloc.Ref().ForcefullySetMemoryYellowZone(false);
    THolder<IComputationGraph> graph = ConstructJoinGraphStream(joinKind, algo, descr);
    graph->GetContext().LogProvider = nullptr;

    NYql::NUdf::TUnboxedValue stream = graph->GetValue();
    TVector<NYql::NUdf::TUnboxedValue> buf(cols);
    i64 rows = 0;

    const ui64 cpuStart = ThreadCPUTime();
    const TInstant wallStart = TInstant::Now();
    NYql::NUdf::EFetchStatus status;
    while ((status = stream.WideFetch(buf.data(), cols)) != NYql::NUdf::EFetchStatus::Finish) {
        if (status == NYql::NUdf::EFetchStatus::Ok) {
            rows += LineSize(algo, {buf.data(), cols});
        }
    }

    TSample sample;
    sample.Cpu = TDuration::MicroSeconds(ThreadCPUTime() - cpuStart);
    sample.Wall = TInstant::Now() - wallStart;
    sample.OutputRows = rows;
    return sample;
}

TSample RunSample(const TBenchmarkSettings& params, ETestedJoinAlgo algo, EJoinKind joinKind,
                  const TJoinDescription& descr, ui32 cols) {
    TDuration totalCpu;
    TDuration totalWall;
    i64 outputRows = -1;
    int iters = 0;
    do {
        TSample one = RunOnce(algo, joinKind, descr, cols);
        Y_ENSURE(outputRows < 0 || outputRows == one.OutputRows, "non-deterministic output row count");
        outputRows = one.OutputRows;
        totalCpu += one.Cpu;
        totalWall += one.Wall;
        ++iters;
    } while (totalWall.MilliSeconds() < static_cast<ui64>(params.MinSampleMs) && iters < params.MaxItersPerSample);

    TSample sample;
    sample.Cpu = totalCpu / iters;
    sample.Wall = totalWall / iters;
    sample.OutputRows = outputRows;
    sample.Iters = iters;
    return sample;
}

TDuration Median(TVector<TDuration> values) {
    Y_ENSURE(!values.empty());
    Sort(values);
    const size_t n = values.size();
    if (n % 2 == 1) {
        return values[n / 2];
    }
    return (values[n / 2 - 1] + values[n / 2]) / 2;
}

TDuration Mean(const TVector<TDuration>& values) {
    ui64 sum = 0;
    for (auto value : values) {
        sum += value.MicroSeconds();
    }
    return TDuration::MicroSeconds(sum / values.size());
}

TDuration Stdev(const TVector<TDuration>& values, TDuration mean) {
    if (values.size() < 2) {
        return TDuration::Zero();
    }
    double acc = 0;
    for (auto value : values) {
        const double diff = static_cast<double>(value.MicroSeconds()) - static_cast<double>(mean.MicroSeconds());
        acc += diff * diff;
    }
    return TDuration::MicroSeconds(static_cast<ui64>(std::sqrt(acc / static_cast<double>(values.size() - 1))));
}

TBenchmarkCaseResult Summarize(const TVector<TSample>& samples) {
    Y_ENSURE(!samples.empty());
    TVector<TDuration> cpu;
    TVector<TDuration> wall;
    cpu.reserve(samples.size());
    wall.reserve(samples.size());
    const i64 outputRows = samples.front().OutputRows;
    int minIters = samples.front().Iters;
    int maxIters = samples.front().Iters;
    for (const auto& sample : samples) {
        Y_ENSURE(sample.OutputRows == outputRows, "non-deterministic output row count across samples");
        cpu.push_back(sample.Cpu);
        wall.push_back(sample.Wall);
        minIters = Min(minIters, sample.Iters);
        maxIters = Max(maxIters, sample.Iters);
    }

    TBenchmarkCaseResult result;
    result.RunDuration = Median(cpu);
    result.MinCpu = *MinElement(cpu.begin(), cpu.end());
    result.MaxCpu = *MaxElement(cpu.begin(), cpu.end());
    result.MeanCpu = Mean(cpu);
    result.StdevCpu = Stdev(cpu, result.MeanCpu);
    result.MedianWall = Median(wall);
    result.CvPercent = result.MeanCpu.MicroSeconds() == 0
                           ? 0.0
                           : 100.0 * static_cast<double>(result.StdevCpu.MicroSeconds()) /
                                 static_cast<double>(result.MeanCpu.MicroSeconds());
    result.Samples = static_cast<int>(samples.size());
    result.MinItersPerSample = minIters;
    result.MaxItersPerSample = maxIters;
    result.OutputRows = outputRows;
    return result;
}

} // namespace

void NKikimr::NMiniKQL::RunJoinsBench(const TBenchmarkSettings& params, const TBenchmarkResultConsumer& consume) {
    for (const auto& keySchema : params.KeySchemas) {
        Y_ABORT_IF(keySchema.empty(), "at least one key column is required");
        TVector<const ui32> keyColumns;
        for (int col = 0; col < std::ssize(keySchema); ++col) {
            keyColumns.push_back(col);
        }

        for (auto payload : params.Payloads) {
            for (auto flavour : params.Flavours) {
                for (auto tableSizes : params.Preset.Sizes) {
                    Y_ABORT_IF(flavour == ETestedInputFlavour::kLittleRightTable && params.Scale < 128,
                               "little right table preset requires scale to be at least 128");
                    tableSizes.Left *= params.Scale;
                    tableSizes.Right *= params.Scale;
                    if (flavour == ETestedInputFlavour::kLittleRightTable) {
                        tableSizes.Right /= 128;
                    }

                    for (auto selectivity : params.Selectivities) {
                        // A fresh setup per data set, so that one case cannot inherit a
                        // fragmented arena from the previous one.
                        TDqSetup<false, true> setup{GetDqNodeFactory()};
                        TInputs scalarInputs =
                            PrepareInputs(setup, params, keySchema, payload, selectivity, tableSizes);
                        std::optional<TInputs> blockInputs;
                        if (AnyOf(params.Algorithms, IsBlockJoin)) {
                            blockInputs = scalarInputs;
                            MaterializeBlocks(setup, *blockInputs, params.BlockSizes);
                        }

                        const ui64 filterMidKey =
                            Max<ui64>(1, static_cast<ui64>(tableSizes.Right) / (2ull * selectivity.DupsPerKey));
                        for (auto joinKind : params.JoinKinds) {
                            for (auto filter : params.Filters) {
                                if (joinKind == EJoinKind::Cross && filter != ETestedFilter::kNone) {
                                    Cerr << "skipping cross join: join filters are not supported" << Endl;
                                    continue;
                                }
                                for (auto algo : params.Algorithms) {
                                    if (!SupportsJoinKind(algo, joinKind)) {
                                        Cerr << "skipping " << AlgoOptionName(algo) << ": join kind "
                                             << JoinKindOptionName(joinKind) << " is not supported" << Endl;
                                        continue;
                                    }
                                    if (filter != ETestedFilter::kNone && !SupportsJoinFilters(algo)) {
                                        Cerr << "skipping " << AlgoOptionName(algo)
                                             << ": it does not support join filters" << Endl;
                                        continue;
                                    }
                                    const TInputs& inputs = IsBlockJoin(algo) ? *blockInputs : scalarInputs;

                                    TJoinDescription descr;
                                    descr.Setup = &setup;
                                    descr.InputsAreBlocks = IsBlockJoin(algo);
                                    descr.LeftSource.ColumnTypes = inputs.Left.ColumnTypes;
                                    descr.RightSource.ColumnTypes = inputs.Right.ColumnTypes;
                                    if (joinKind != EJoinKind::Cross) {
                                        descr.LeftSource.KeyColumnIndexes = keyColumns;
                                        descr.RightSource.KeyColumnIndexes = keyColumns;
                                    }
                                    descr.LeftSource.ValuesList = inputs.Left.Values;
                                    descr.RightSource.ValuesList = inputs.Right.Values;
                                    AttachFilters(descr, &setup, keySchema, filter, filterMidKey, params.StringBytes);

                                    const TString caseName = CaseName(algo, keySchema, payload, selectivity, joinKind,
                                                                      filter, flavour, params, tableSizes);
                                    const ui32 cols = ResultColumnCount(algo, joinKind, descr);
                                    Cerr << "Compute graph result for case '" << caseName << "'" << Endl;

                                    for (int warmup = 0; warmup < params.Warmup; ++warmup) {
                                        RunOnce(algo, joinKind, descr, cols);
                                    }

                                    TVector<TSample> samples;
                                    samples.reserve(params.Samples);
                                    for (int sample = 0; sample < params.Samples; ++sample) {
                                        samples.push_back(RunSample(params, algo, joinKind, descr, cols));
                                        Cerr << Sprintf("  sample %i/%i: cpu %ims, wall %ims, "
                                                        "iters %i, output line "
                                                        "count: %li", sample + 1, params.Samples,
                                                        samples.back().Cpu.MilliSeconds(),
                                                        samples.back().Wall.MilliSeconds(), samples.back().Iters,
                                                        samples.back().OutputRows)
                                             << Endl;
                                    }

                                    TBenchmarkCaseResult result = Summarize(samples);
                                    result.CaseName = caseName;
                                    result.Algo = algo;
                                    result.KeySchema = keySchema;
                                    result.Payload = payload;
                                    result.Flavour = flavour;
                                    result.Sizes = tableSizes;
                                    result.Selectivity = selectivity;
                                    result.JoinKind = joinKind;
                                    result.Filter = filter;
                                    if (IsBlockJoin(algo)) {
                                        result.LeftBlocks = inputs.Left.Values.GetListLength();
                                        result.RightBlocks = inputs.Right.Values.GetListLength();
                                    }
                                    consume(result);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
