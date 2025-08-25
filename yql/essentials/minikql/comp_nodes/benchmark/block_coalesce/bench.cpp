#include <benchmark/benchmark.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/computation/mkql_block_item.h>
#include <yql/essentials/minikql/comp_nodes/mkql_block_coalesce.h>
#include <yql/essentials/minikql/comp_nodes/mkql_block_coalesce_blending_helper.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_block_builder.h>
#include <yql/essentials/ast/yql_expr_builder.h>
#include <yql/essentials/public/udf/arrow/memory_pool.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>
#include <yql/essentials/core/arrow_kernels/registry/registry.h>

#include <arrow/compute/exec_internal.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {

template <typename T>
static void BenchmarkFixedSizeCoalesce(benchmark::State& state) {
    NYql::TExprContext exprCtx;
    TSetup<false> setup;
    bool secondIsScalar = state.range(1);
    const auto type = setup.PgmBuilder->NewDataType(NUdf::TDataType<T>::Id);
    auto* typeNode = exprCtx.template MakeType<NYql::TBlockExprType>(
        exprCtx.template MakeType<NYql::TDataExprType>(NUdf::TDataType<T>::Slot));
    auto* optTypeNode = exprCtx.template MakeType<NYql::TBlockExprType>(
        exprCtx.template MakeType<NYql::TOptionalExprType>(
            exprCtx.template MakeType<NYql::TDataExprType>(NUdf::TDataType<T>::Slot)));
    auto getBlockItem = [&](int64_t value) {
        return TBlockItem(static_cast<T>(value));
    };

    auto arrow_type = std::make_shared<typename TPrimitiveDataType<T>::TResult>();
    int arrayLength = state.range(0);
    constexpr int batchSize = 30720;

    arrow::compute::ExecContext execCtx;
    const auto drng = CreateDeterministicRandomProvider(1);
    TTypeInfoHelper tif;

    auto getArray = [&](TType* type, bool isOptional) {
        auto array_builder = MakeArrayBuilder(tif, type, *NYql::NUdf::GetYqlMemoryPool(), arrayLength, nullptr);
        for (int i = 0; i < arrayLength; i++) {
            if (!isOptional) {
                array_builder->Add(getBlockItem(drng->GenRand64()));
            } else {
                array_builder->Add(drng->GenRand64() % 2 == 0 ? TBlockItem() : getBlockItem(drng->GenRand64()));
            }
        }
        return array_builder->Build(/*finish=*/true);
    };

    arrow::compute::KernelContext ctx(&execCtx);

    const auto optType = setup.PgmBuilder->NewOptionalType(type);
    auto left = getArray(optType, /*isOptional=*/true);
    arrow::Datum right;
    if (secondIsScalar) {
        right = MakeScalarDatum<T>(drng->GenRand64());
    } else {
        right = getArray(type, /*isOptional=*/false);
    }
    auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
    NYql::TKernelRequestBuilder b(*registry);

    b.AddBinaryOp(NYql::TKernelRequestBuilder::EBinaryOp::Coalesce, optTypeNode, typeNode, typeNode);
    auto serializedNode = b.Serialize();
    auto nodeFactory = GetBuiltinFactory();
    auto kernel = NYql::LoadKernels(serializedNode, *registry, nodeFactory);
    Y_ENSURE(kernel.size() == 1);

    for (auto _ : state) {
        auto bi = ARROW_RESULT(arrow::compute::detail::ExecBatchIterator::Make({left, right}, batchSize));

        arrow::compute::ExecBatch batch;
        while (bi->Next(&batch)) {
            arrow::Datum out;
            Y_ENSURE(kernel[0]->exec(&ctx, batch, &out).ok());
            benchmark::DoNotOptimize(out);
        }
    }
}

} // namespace NKikimr::NMiniKQL

static void CustomArguments(benchmark::internal::Benchmark* b) {
    b->Args({9000000, 0});
    b->Args({9000000, 1});
}

BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<ui8>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<ui16>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<ui32>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<ui64>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<float>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
BENCHMARK(NKikimr::NMiniKQL::BenchmarkFixedSizeCoalesce<double>)->Unit(benchmark::kMillisecond)->Apply(CustomArguments);
