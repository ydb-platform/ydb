#pragma once
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>


#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernel.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/registry.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/row/grouper.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/type_fwd.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h>



using namespace NKikimr::NArrow::NMerger;

// PK: (timestamp_us, utf8, utf8), version: int64
inline std::shared_ptr<arrow::Schema> MakeSortSchema() {
    return arrow::schema({
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
    });
}

inline std::shared_ptr<arrow::Schema> MakeFullSchema() {
    return arrow::schema({
        arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("a", arrow::utf8()),
        arrow::field("b", arrow::utf8()),
        arrow::field("ver", arrow::int64()),
    });
}

// Целевая картина: все источники имеют идентичный диапазон PK (полное перекрытие).
// Построчно одинаковый контент (ts/a/b зависят только от i), отличается только ver = sourceIdx.
// Колонка a имеет повторы (домен = NumRows/10) — имитация низкокардинального ключа.
inline std::shared_ptr<arrow::RecordBatch> MakeBatch(int numRows, int sourceIdx) {
    arrow::TimestampBuilder tsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder aBuilder, bBuilder;
    arrow::Int64Builder verBuilder;

    const int aDomain = std::max(1, numRows / 10);
    const int shift = sourceIdx;
    for (int i = 0; i < numRows; ++i) {
        Y_ABORT_UNLESS(tsBuilder.Append(shift + (int64_t)i * 1000).ok());
        Y_ABORT_UNLESS(aBuilder.Append(std::to_string(i % aDomain)).ok());
        Y_ABORT_UNLESS(bBuilder.Append(std::to_string(i)).ok());
        Y_ABORT_UNLESS(verBuilder.Append(sourceIdx).ok());
    }

    std::shared_ptr<arrow::Array> tsArr, aArr, bArr, verArr;
    Y_ABORT_UNLESS(tsBuilder.Finish(&tsArr).ok());
    Y_ABORT_UNLESS(aBuilder.Finish(&aArr).ok());
    Y_ABORT_UNLESS(bBuilder.Finish(&bArr).ok());
    Y_ABORT_UNLESS(verBuilder.Finish(&verArr).ok());

    return arrow::RecordBatch::Make(MakeFullSchema(), numRows, {tsArr, aArr, bArr, verArr});
}

struct TFixture {
    std::vector<std::shared_ptr<arrow::RecordBatch>> Batches;

    TFixture(int numSources) {
        Batches.reserve(numSources);
        for (int i = 0; i < numSources; ++i) {
            Batches.push_back(MakeBatch(10000, i));
        }
    }
};

inline std::shared_ptr<arrow::RecordBatch> MergeOnce(const TFixture& f) {
    auto sortSchema = MakeSortSchema();
    auto fullSchema = MakeFullSchema();

    TMergePartialStream merger(sortSchema, fullSchema, /*reverse=*/false,
        {"ver"}, std::nullopt, std::nullopt);

    for (auto& batch : f.Batches) {
        merger.AddSource(batch, nullptr, TIterationOrder::Forward(0));
    }

    TRecordBatchBuilder builder(fullSchema->fields());
    merger.DrainAll(builder);
    return builder.Finalize();
}

void fooo() {
    int size = 5;
    void* arr = new int[5];
    int write_index = 1; 
    // arr[0] = arr[0];
    for (int i = 1; i < size; ++i) {
        if (arr[i] != arr[i-1]) {
            arr[write_index++] = arr[i];
        }
    }
}

inline std::shared_ptr<arrow::RecordBatch> MergeOnce(const TFixture& f) {
    auto sortSchema = MakeSortSchema();
    auto fullSchema = MakeFullSchema();

    arrow::compute::SortOptions sortOptions({
        arrow::compute::SortKey("ts", arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("a",  arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("b",  arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("ver", arrow::compute::SortOrder::Descending),
    });
    auto tableRes = arrow::Table::FromRecordBatches(fullSchema, f.Batches);
    Y_ABORT_UNLESS(tableRes.ok());
    auto table = *tableRes;

    auto indicesRes = arrow::compute::SortIndices(arrow::Datum(table), sortOptions);
    Y_ABORT_UNLESS(indicesRes.ok());
    auto indices = *indicesRes;


    auto all_indices_but_first = indices->SliceSafe(1).ValueOrDie();
    auto all_indices_but_last = indices->SliceSafe(0, indices->length() - 1).ValueOrDie();
    
    // arrow::VisitArrayInline(indices, nullptr);

    auto takenRes = arrow::compute::Take(arrow::Datum(table), arrow::Datum(indices));
    Y_ABORT_UNLESS(takenRes.ok());
    auto result = takenRes->table();



    return result;

}



// ---- фикстуры: (источников, строк на источник) ----
// Function-local static — Arrow memory pool должен быть инициализирован до первого использования билдеров.

inline const TFixture& Get_2src_10k()   { static TFixture f{ 2 }; return f; }
inline const TFixture& Get_5src_10k()   { static TFixture f{ 5 }; return f; }
inline const TFixture& Get_10src_10k()  { static TFixture f{ 10 }; return f; }
inline const TFixture& Get_20src_10k()  { static TFixture f{ 20 }; return f; }

// ============================================================================
// Третий алгоритм: arrow_next (arrow 20) — sort + Grouper + hash_first.
// Логически эквивалентен мерж-with-dedup TMergePartialStream:
//   1) concat всех источников
//   2) sort по (ts ASC, a ASC, b ASC, ver DESC) — внутри каждой (ts,a,b) первой
//      идёт строка с максимальным ver
//   3) Grouper строит group_ids по (ts,a,b)
//   4) hash_first каждой колонке (включая ver) — берёт первую строку каждой группы
// ============================================================================

inline std::shared_ptr<arrow20::Schema> MakeFullSchema20() {
    return arrow20::schema({
        arrow20::field("ts", arrow20::timestamp(arrow20::TimeUnit::MICRO)),
        arrow20::field("a", arrow20::utf8()),
        arrow20::field("b", arrow20::utf8()),
        arrow20::field("ver", arrow20::int64()),
    });
}

inline std::shared_ptr<arrow20::RecordBatch> MakeBatch20(int numRows, uint32_t sourceIdx) {
    arrow20::TimestampBuilder tsBuilder(arrow20::timestamp(arrow20::TimeUnit::MICRO),
                                        arrow20::default_memory_pool());
    arrow20::StringBuilder aBuilder, bBuilder;
    arrow20::Int64Builder verBuilder;

    const int aDomain = std::max(1, numRows / 10);
    int shift = sourceIdx;
    
    for (int i = 0; i < numRows; ++i) {
        
        Y_ABORT_UNLESS(tsBuilder.Append(shift+(int64_t)i * 1000).ok());
        Y_ABORT_UNLESS(aBuilder.Append(std::to_string(i % aDomain)).ok());
        Y_ABORT_UNLESS(bBuilder.Append(std::to_string(i)).ok());
        Y_ABORT_UNLESS(verBuilder.Append(sourceIdx).ok());
    }

    std::shared_ptr<arrow20::Array> tsArr, aArr, bArr, verArr;
    Y_ABORT_UNLESS(tsBuilder.Finish(&tsArr).ok());
    Y_ABORT_UNLESS(aBuilder.Finish(&aArr).ok());
    Y_ABORT_UNLESS(bBuilder.Finish(&bArr).ok());
    Y_ABORT_UNLESS(verBuilder.Finish(&verArr).ok());

    return arrow20::RecordBatch::Make(MakeFullSchema20(), numRows, {tsArr, aArr, bArr, verArr});
}

struct TFixture20 {
    std::vector<std::shared_ptr<arrow20::RecordBatch>> Batches;

    TFixture20(int numSources) {
        Batches.reserve(numSources);
        for (int i = 0; i < numSources; ++i) {
            Batches.push_back(MakeBatch20(10000, i));
        }
    }
};

// В arrow 20 нет Acero в этой сборке, и публичного GroupBy тоже нет.
// hash_first как HASH_AGGREGATE не вызывается через CallFunction —
// его надо прогонять напрямую через kernel-протокол Init/Resize/Consume/Finalize.
inline arrow20::Datum HashFirstPerGroup(const arrow20::Datum& values,
                                 const arrow20::Datum& groupIds,
                                 int64_t numGroups,
                                 arrow20::compute::ExecContext* ctx) {
    static auto* registry = arrow20::compute::GetFunctionRegistry();
    static auto fn = registry->GetFunction("hash_first").ValueOrDie();

    std::vector<arrow20::TypeHolder> inputTypes{values.type(), groupIds.type()};
    auto* kernel = static_cast<const arrow20::compute::HashAggregateKernel*>(
        fn->DispatchExact(inputTypes).ValueOrDie());

    arrow20::compute::ScalarAggregateOptions options;
    arrow20::compute::KernelContext kctx(ctx, kernel);
    arrow20::compute::KernelInitArgs initArgs{kernel, inputTypes, &options};
    auto state = kernel->init(&kctx, initArgs).ValueOrDie();
    kctx.SetState(state.get());

    Y_ABORT_UNLESS(kernel->resize(&kctx, numGroups).ok());

    arrow20::compute::ExecBatch eb({values, groupIds}, values.length());
    arrow20::compute::ExecSpan span(eb);
    Y_ABORT_UNLESS(kernel->consume(&kctx, span).ok());

    arrow20::Datum out;
    Y_ABORT_UNLESS(kernel->finalize(&kctx, &out).ok());
    return out;
}

inline std::shared_ptr<arrow20::RecordBatch> HashFirstMergeOnce(const TFixture20& f) {
    static const arrow20::compute::SortOptions sortOptions({
        arrow20::compute::SortKey("ts",  arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("a",   arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("b",   arrow20::compute::SortOrder::Ascending),
        arrow20::compute::SortKey("ver", arrow20::compute::SortOrder::Descending),
    });
    auto* ctx = arrow20::compute::default_exec_context();

    // 1) concat → один batch
    auto combined = arrow20::ConcatenateRecordBatches(f.Batches).ValueOrDie();

    // 2) sort: (ts,a,b ASC, ver DESC)
    auto sortedIdx = arrow20::compute::SortIndices(arrow20::Datum(combined), sortOptions).ValueOrDie();
    auto sorted = arrow20::compute::Take(arrow20::Datum(combined), arrow20::Datum(sortedIdx))
                      .ValueOrDie().record_batch();

    // 3) group по (ts,a,b)
    std::vector<arrow20::TypeHolder> keyTypes{
        sorted->column(0)->type(), sorted->column(1)->type(), sorted->column(2)->type(),
    };
    auto grouper = arrow20::compute::Grouper::Make(keyTypes, ctx).ValueOrDie();

    arrow20::compute::ExecBatch keysBatch({
        arrow20::Datum(sorted->column(0)),
        arrow20::Datum(sorted->column(1)),
        arrow20::Datum(sorted->column(2)),
    }, sorted->num_rows());
    arrow20::compute::ExecSpan keysSpan(keysBatch);
    auto groupIds = grouper->Consume(keysSpan).ValueOrDie();
    const int64_t numGroups = grouper->num_groups();

    // 4) hash_first каждой колонке (включая ver — там окажется максимальный)
    std::vector<std::shared_ptr<arrow20::Array>> outCols;
    outCols.reserve(sorted->num_columns());
    for (int c = 0; c < sorted->num_columns(); ++c) {
        auto out = HashFirstPerGroup(arrow20::Datum(sorted->column(c)), groupIds, numGroups, ctx);
        outCols.push_back(out.make_array());
    }

    return arrow20::RecordBatch::Make(sorted->schema(), numGroups, outCols);
}


inline const TFixture20& Get20_2src_10k()  { static TFixture20 f{  2}; return f; }
inline const TFixture20& Get20_5src_10k()  { static TFixture20 f{  5}; return f; }
inline const TFixture20& Get20_10src_10k() { static TFixture20 f{ 10}; return f; }
inline const TFixture20& Get20_20src_10k() { static TFixture20 f{ 20}; return f; }

