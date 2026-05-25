
#include <library/cpp/testing/benchmark/bench.h>

#include "merges.h"


void RunMerge(const TFixture& f, NBench::NCpu::TParams& iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto result = MergeOnce(f);
        Y_DO_NOT_OPTIMIZE_AWAY(result.get());
    }
}

// Альтернативный алгоритм: concat всех источников + arrow::compute::SortIndices + Take.
// Не делает дедупликацию по версии — голый sort+gather для сравнения стоимости сортировки.
void RunSortIndicesMerge(const TFixture& f, NBench::NCpu::TParams& iface) {
    auto fullSchema = MakeFullSchema();

    arrow::compute::SortOptions sortOptions({
        arrow::compute::SortKey("ts", arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("a",  arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("b",  arrow::compute::SortOrder::Ascending),
        arrow::compute::SortKey("ver", arrow::compute::SortOrder::Descending),
    });

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto tableRes = arrow::Table::FromRecordBatches(fullSchema, f.Batches);
        Y_ABORT_UNLESS(tableRes.ok());
        auto table = *tableRes;

        auto indicesRes = arrow::compute::SortIndices(arrow::Datum(table), sortOptions);
        Y_ABORT_UNLESS(indicesRes.ok());
        auto indices = *indicesRes;



        auto takenRes = arrow::compute::Take(arrow::Datum(table), arrow::Datum(indices));
        Y_ABORT_UNLESS(takenRes.ok());
        auto result = takenRes->table();
        Y_DO_NOT_OPTIMIZE_AWAY(result.get());
    }
}

void RunHashFirstMerge(const TFixture20& f, NBench::NCpu::TParams& iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto result = HashFirstMergeOnce(f);
        Y_DO_NOT_OPTIMIZE_AWAY(result.get());
    }
}


// ---- arrow_next 20: sort + Grouper + hash_first (с дедупликацией по max(ver)) ----

Y_CPU_BENCHMARK(HashFirst_2src_10k,  iface) { RunHashFirstMerge(Get20_2src_10k(),  iface); }
Y_CPU_BENCHMARK(HashFirst_5src_10k,  iface) { RunHashFirstMerge(Get20_5src_10k(),  iface); }
Y_CPU_BENCHMARK(HashFirst_10src_10k, iface) { RunHashFirstMerge(Get20_10src_10k(), iface); }
Y_CPU_BENCHMARK(HashFirst_20src_10k, iface) { RunHashFirstMerge(Get20_20src_10k(), iface); }

// ---- TMergePartialStream ----
Y_CPU_BENCHMARK(Merge_2src_10k,  iface) { RunMerge(Get_2src_10k(),  iface); }
Y_CPU_BENCHMARK(Merge_5src_10k,  iface) { RunMerge(Get_5src_10k(),  iface); }
Y_CPU_BENCHMARK(Merge_10src_10k, iface) { RunMerge(Get_10src_10k(), iface); }
Y_CPU_BENCHMARK(Merge_20src_10k, iface) { RunMerge(Get_20src_10k(), iface); }

// // ---- arrow::compute::SortIndices + Take ----
Y_CPU_BENCHMARK(SortIndices_2src_10k,  iface) { RunSortIndicesMerge(Get_2src_10k(),  iface); }
Y_CPU_BENCHMARK(SortIndices_5src_10k,  iface) { RunSortIndicesMerge(Get_5src_10k(),  iface); }
Y_CPU_BENCHMARK(SortIndices_10src_10k, iface) { RunSortIndicesMerge(Get_10src_10k(), iface); }
Y_CPU_BENCHMARK(SortIndices_20src_10k, iface) { RunSortIndicesMerge(Get_20src_10k(), iface); }
