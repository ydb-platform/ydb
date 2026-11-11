#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <benchmark/benchmark.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

namespace {

constexpr size_t LIST_SIZE = 5000000UL;

struct TPackerFixture {
    TPackerFixture()
        : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , Alloc(__LOCATION__)
        , Env(Alloc)
        , MemInfo("bench")
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , ProgramBuilder(Env, *FunctionRegistry)
        , ListType(ProgramBuilder.NewListType(ProgramBuilder.NewTupleType({ProgramBuilder.NewOptionalType(ProgramBuilder.NewDataType(EDataSlot::Uint64)),
                                                                           ProgramBuilder.NewDataType(EDataSlot::Uint64)})))
    {
        for (ui64 i = 0; i < LIST_SIZE; ++i) {
            TUnboxedValueVector tupleItems;
            if (i % 2 == 0) {
                tupleItems.emplace_back();
            } else {
                tupleItems.emplace_back(TUnboxedValuePod(i));
            }
            tupleItems.emplace_back(TUnboxedValuePod(i));
            ListItems.emplace_back(HolderFactory.VectorAsArray(tupleItems));
        }
        List = HolderFactory.VectorAsArray(ListItems);
    }

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TProgramBuilder ProgramBuilder;
    const NKikimr::NMiniKQL::TType* ListType;
    TUnboxedValueVector ListItems;
    TUnboxedValue List;
};

void ValidateList(const TUnboxedValue& list) {
    auto iterator = list.GetListIterator();
    TUnboxedValue item;
    ui64 index = 0;
    while (iterator.Next(item)) {
        Y_ENSURE(index < LIST_SIZE, "Unpacked list has too many items");
        const auto* tuple = item.GetElements();
        Y_ENSURE(tuple, "Unpacked tuple has no elements");
        Y_ENSURE(bool(tuple[0]) == (index % 2 != 0), "Optional mismatch at " << index);
        if (index % 2 != 0) {
            Y_ENSURE(tuple[0].Get<ui64>() == index, "Optional value mismatch at " << index);
        }
        Y_ENSURE(tuple[1].Get<ui64>() == index, "Value mismatch at " << index);
        ++index;
    }
    Y_ENSURE(index == LIST_SIZE, "Unpacked list size is " << index);
}

void Pack(benchmark::State& state) {
    TPackerFixture fixture;
    for (auto _ : state) {
        // A fresh packer preserves the original cold buffer; its lifetime is outside the timer.
        state.PauseTiming();
        {
            TValuePacker packer(/*stable=*/true, fixture.ListType);
            state.ResumeTiming();
            TStringBuf packed = packer.Pack(fixture.List);
            benchmark::DoNotOptimize(packed);
            state.PauseTiming();
        }
        state.ResumeTiming();
    }
}
BENCHMARK(Pack);

void Unpack(benchmark::State& state) {
    TPackerFixture fixture;
    TValuePacker packer(/*stable=*/true, fixture.ListType);
    const TStringBuf packed = packer.Pack(fixture.List);
    for (auto _ : state) {
        auto unpacked = packer.Unpack(packed, fixture.HolderFactory);
        benchmark::DoNotOptimize(unpacked);
        // The original timer stopped before destroying the unpacked list.
        state.PauseTiming();
        unpacked.Clear();
        state.ResumeTiming();
    }
    ValidateList(packer.Unpack(packed, fixture.HolderFactory));
}
BENCHMARK(Unpack);

} // namespace
