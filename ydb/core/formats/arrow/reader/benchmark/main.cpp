#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>


namespace {

using namespace NKikimr::NArrow::NMerger;

// PK: (timestamp_us, utf8, utf8), version: int64
std::shared_ptr<arrow::Schema> MakeSortSchema() {
    // arrow::schema()
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
    bool SameTs = false;  // все источники используют одинаковый диапазон ts (полное пересечение)
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

    const int64_t baseTs = p.SameTs ? 0 : 1'000'000'000LL * p.SourceIdx;
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

    TFixture(int numSources, int rowsPerSource, double overlapFactor, bool sameTs = false) {
        Batches.reserve(numSources);
        for (int i = 0; i < numSources; ++i) {
            Batches.push_back(MakeBatch({rowsPerSource, i, numSources, overlapFactor, sameTs}));
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
// Используем function-local static чтобы избежать static initialization order fiasco
// (Arrow memory pool должен быть инициализирован до первого использования билдеров)

const TFixture& Get_2src_10k_noOverlap()    { static TFixture f{  2, 10'000, 0.0 }; return f; }
const TFixture& Get_2src_10k_halfOverlap()  { static TFixture f{  2, 10'000, 0.5 }; return f; }
const TFixture& Get_2src_10k_fullOverlap()  { static TFixture f{  2, 10'000, 1.0 }; return f; }

const TFixture& Get_10src_10k_noOverlap()   { static TFixture f{ 10, 10'000, 0.0 }; return f; }
const TFixture& Get_10src_10k_halfOverlap() { static TFixture f{ 10, 10'000, 0.5 }; return f; }
const TFixture& Get_10src_10k_fullOverlap() { static TFixture f{ 10, 10'000, 1.0 }; return f; }

const TFixture& Get_100src_1k_noOverlap()   { static TFixture f{100,  1'000, 0.0 }; return f; }
const TFixture& Get_100src_1k_halfOverlap() { static TFixture f{100,  1'000, 0.5 }; return f; }
const TFixture& Get_100src_1k_fullOverlap() { static TFixture f{100,  1'000, 1.0 }; return f; }

// 10 источников с полным пересечением по ts (все источники имеют одинаковый диапазон ключей)
const TFixture& Get_10src_10k_trueOverlap() {
    static TFixture f{ 10, 10'000, 1.0, /*SameTs=*/true };
    return f;
}

} // namespace

// 2 источника
Y_CPU_BENCHMARK(Merge_2src_10k_noOverlap,    iface) { RunMerge(Get_2src_10k_noOverlap(),    iface); }
Y_CPU_BENCHMARK(Merge_2src_10k_halfOverlap,  iface) { RunMerge(Get_2src_10k_halfOverlap(),  iface); }
Y_CPU_BENCHMARK(Merge_2src_10k_fullOverlap,  iface) { RunMerge(Get_2src_10k_fullOverlap(),  iface); }

// 10 источников
Y_CPU_BENCHMARK(Merge_10src_10k_noOverlap,   iface) { RunMerge(Get_10src_10k_noOverlap(),   iface); }
Y_CPU_BENCHMARK(Merge_10src_10k_halfOverlap, iface) { RunMerge(Get_10src_10k_halfOverlap(), iface); }
Y_CPU_BENCHMARK(Merge_10src_10k_fullOverlap, iface) { RunMerge(Get_10src_10k_fullOverlap(), iface); }

// 100 источников
Y_CPU_BENCHMARK(Merge_100src_1k_noOverlap,   iface) { RunMerge(Get_100src_1k_noOverlap(),   iface); }
Y_CPU_BENCHMARK(Merge_100src_1k_halfOverlap, iface) { RunMerge(Get_100src_1k_halfOverlap(), iface); }
Y_CPU_BENCHMARK(Merge_100src_1k_fullOverlap, iface) { RunMerge(Get_100src_1k_fullOverlap(), iface); }

// 10 источников, полное пересечение по ts (все источники с одинаковым диапазоном ключей)
Y_CPU_BENCHMARK(Merge_10src_10k_trueOverlap, iface) { RunMerge(Get_10src_10k_trueOverlap(), iface); }
