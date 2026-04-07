#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>


namespace {

using namespace NKikimr::NArrow::NMerger;

// PK: (timestamp_us, utf8, utf8), version: int64
std::shared_ptr<arrow::Schema> MakeSortSchema() {
    return arrow::schema({
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
    });
}

std::shared_ptr<arrow::Schema> MakeFullSchema() {
    return arrow::schema({
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
        arrow::field("ver", arrow::int64()),
    });
}

struct TBatchParams {
    int NumRows;
    int SourceIdx;        // задаёт версию и смещение ключей
    int NumSources;       // сколько источников всего — для расчёта перекрытия
    double OverlapFactor; // 0.0 = нет дублей, 1.0 = все ключи одинаковые
};

// Генерирует батч с отсортированными строками.
// ts    = base_ts + row * step
// a     = "aval_{row % domain}"  где domain управляет перекрытием
// b     = "bval_{row}"
// ver   = sourceIdx
std::shared_ptr<arrow::RecordBatch> MakeBatch(const TBatchParams& p) {
    arrow::TimestampBuilder tsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder aBuilder, bBuilder;
    arrow::Int64Builder verBuilder;

    const int64_t baseTs = 1'000'000'000LL * p.SourceIdx;
    // чем больше overlap, тем меньше уникальных значений a
    const int aDomain = std::max(1, (int)(p.NumRows / (p.NumSources * p.OverlapFactor + 1e-9)));

    for (int i = 0; i < p.NumRows; ++i) {
        Y_ABORT_UNLESS(tsBuilder.Append(baseTs + (int64_t)i * 1000).ok());
        Y_ABORT_UNLESS(aBuilder.Append(std::to_string(i % aDomain)).ok());
        Y_ABORT_UNLESS(bBuilder.Append(std::to_string(i)).ok());
        Y_ABORT_UNLESS(verBuilder.Append(p.SourceIdx).ok());
    }

    std::shared_ptr<arrow::Array> tsArr, aArr, bArr, verArr;
    Y_ABORT_UNLESS(tsBuilder.Finish(&tsArr).ok());
    Y_ABORT_UNLESS(aBuilder.Finish(&aArr).ok());
    Y_ABORT_UNLESS(bBuilder.Finish(&bArr).ok());
    Y_ABORT_UNLESS(verBuilder.Finish(&verArr).ok());

    return arrow::RecordBatch::Make(MakeFullSchema(), p.NumRows, {tsArr, aArr, bArr, verArr});
}

// Готовит векторы батчей один раз до начала замеров
struct TFixture {
    std::vector<std::shared_ptr<arrow::RecordBatch>> Batches;

    TFixture(int numSources, int rowsPerSource, double overlapFactor) {
        Batches.reserve(numSources);
        for (int i = 0; i < numSources; ++i) {
            Batches.push_back(MakeBatch({rowsPerSource, i, numSources, overlapFactor}));
        }
    }
};

void RunMerge(const TFixture& f, NBench::NCpu::TParams& iface) {
    auto sortSchema = MakeSortSchema();
    auto fullSchema = MakeFullSchema();

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TMergePartialStream merger(sortSchema, fullSchema, /*reverse=*/false,
            {"ver"}, std::nullopt, std::nullopt);

        for (auto& batch : f.Batches) {
            merger.AddSource(batch, nullptr, TIterationOrder::Forward(0));
        }

        TRecordBatchBuilder builder(fullSchema->fields());
        merger.DrainAll(builder);

        auto result = builder.Finalize();
        Y_DO_NOT_OPTIMIZE_AWAY(result.get());
    }
}

// ---- фикстуры: (источников, строк на источник, перекрытие) ----

static const TFixture F_2src_10k_noOverlap    {  2, 10'000, 0.0 };
static const TFixture F_2src_10k_halfOverlap  {  2, 10'000, 0.5 };
static const TFixture F_2src_10k_fullOverlap  {  2, 10'000, 1.0 };

static const TFixture F_10src_10k_noOverlap   { 10, 10'000, 0.0 };
static const TFixture F_10src_10k_halfOverlap { 10, 10'000, 0.5 };
static const TFixture F_10src_10k_fullOverlap { 10, 10'000, 1.0 };

static const TFixture F_100src_1k_noOverlap   {100,  1'000, 0.0 };
static const TFixture F_100src_1k_halfOverlap {100,  1'000, 0.5 };
static const TFixture F_100src_1k_fullOverlap {100,  1'000, 1.0 };

} // namespace

// 2 источника
Y_CPU_BENCHMARK(Merge_2src_10k_noOverlap,    iface) { RunMerge(F_2src_10k_noOverlap,    iface); }
Y_CPU_BENCHMARK(Merge_2src_10k_halfOverlap,  iface) { RunMerge(F_2src_10k_halfOverlap,  iface); }
Y_CPU_BENCHMARK(Merge_2src_10k_fullOverlap,  iface) { RunMerge(F_2src_10k_fullOverlap,  iface); }

// 10 источников
Y_CPU_BENCHMARK(Merge_10src_10k_noOverlap,   iface) { RunMerge(F_10src_10k_noOverlap,   iface); }
Y_CPU_BENCHMARK(Merge_10src_10k_halfOverlap, iface) { RunMerge(F_10src_10k_halfOverlap, iface); }
Y_CPU_BENCHMARK(Merge_10src_10k_fullOverlap, iface) { RunMerge(F_10src_10k_fullOverlap, iface); }

// 100 источников
Y_CPU_BENCHMARK(Merge_100src_1k_noOverlap,   iface) { RunMerge(F_100src_1k_noOverlap,   iface); }
Y_CPU_BENCHMARK(Merge_100src_1k_halfOverlap, iface) { RunMerge(F_100src_1k_halfOverlap, iface); }
Y_CPU_BENCHMARK(Merge_100src_1k_fullOverlap, iface) { RunMerge(F_100src_1k_fullOverlap, iface); }
