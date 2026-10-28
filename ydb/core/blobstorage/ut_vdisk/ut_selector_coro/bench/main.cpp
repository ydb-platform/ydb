#include "../selector_test_helpers.h"

#include <util/string/cast.h>
#include <util/stream/output.h>

#include <chrono>
#include <future>
#include <sys/resource.h>

namespace {
using namespace NKikimr;
using namespace NKikimr::NHullComp;
using namespace NKikimr::NSelectorTest;
using TBenchmarkInput = NSelectorTest::TInput<TKeyLogoBlob, TMemRecLogoBlob>;
using TInputs = std::vector<std::unique_ptr<TBenchmarkInput>>;

ui64 ProcessCpuUs() {
    rusage usage{};
    Y_ABORT_UNLESS(getrusage(RUSAGE_SELF, &usage) == 0);
    return ui64(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1'000'000
        + usage.ru_utime.tv_usec + usage.ru_stime.tv_usec;
}

template<template<class, class> class TSelector>
class TBatch : public TActorBootstrapped<TBatch<TSelector>> {
    using TThis = TBatch<TSelector>;
    TInputs& Inputs;
    std::promise<void> Done;
    ui32 Remaining;

    STFUNC(Receive) {
        const auto* selected = ev->Get<TSelected<TKeyLogoBlob, TMemRecLogoBlob>>();
        Y_ABORT_UNLESS(selected->Action == ActNothing);
        if (!--Remaining) {
            Done.set_value();
            this->PassAway();
        }
    }

public:
    TBatch(TInputs& inputs, std::promise<void>&& done)
        : Inputs(inputs)
        , Done(std::move(done))
        , Remaining(inputs.size())
    {}

    void Bootstrap() {
        this->Become(&TThis::Receive);
        for (auto& input : Inputs) {
            this->Register(new TSelector<TKeyLogoBlob, TMemRecLogoBlob>(input->HullCtx, input->Params,
                input->Index->GetIndexSnapshot(), input->Ds->Barriers->GetIndexSnapshot(), this->SelfId(),
                std::make_unique<TTask<TKeyLogoBlob, TMemRecLogoBlob>>(), false));
        }
    }
};

template<template<class, class> class TSelector>
void Measure(TEnvironment& env, TInputs& inputs, bool records, ui32 size, ui32 threads,
        const char* actor, ui32 iteration, bool report) {
    // Input construction and invalidating the ratio cache are outside the timed region.
    if (records) {
        for (auto& input : inputs) {
            auto ratio = input->Ssts.front()->StorageRatio.Get();
            ratio->Time = TInstant::Zero();
            input->Ssts.front()->StorageRatio.SetCalculationTime(TInstant::Zero());
        }
    }
    std::promise<void> done;
    auto result = done.get_future();
    const ui64 cpu = ProcessCpuUs();
    const auto start = TMonotonic::Now();
    env.System->Register(new TBatch<TSelector>(inputs, std::move(done)));
    Y_ABORT_UNLESS(result.wait_for(std::chrono::seconds(120)) == std::future_status::ready);
    result.get();
    const ui64 wallUs = (TMonotonic::Now() - start).MicroSeconds();
    const ui64 cpuUs = ProcessCpuUs() - cpu;
    if (records) {
        for (const auto& input : inputs) {
            const auto ratio = input->Ssts.front()->StorageRatio.Get();
            Y_ABORT_UNLESS(ratio->IndexItemsTotal == size && ratio->IndexItemsKeep == size);
        }
    }
    if (report) {
        Cout << actor << ',' << (records ? "records" : "ssts") << ',' << threads << ',' << inputs.size()
            << ',' << size << ',' << iteration << ',' << cpuUs << ',' << wallUs << ','
            << double(size) * inputs.size() * 1'000'000 / wallUs << Endl;
    }
}
} // namespace

int main(int argc, char** argv) {
    if (argc != 6 && argc != 7) {
        Cerr << "Usage: selector_coro_bench THREADS SELECTORS SIZE ROUNDS records|ssts [both|sync|coro]" << Endl;
        return 1;
    }
    const ui32 threads = FromString<ui32>(argv[1]);
    const ui32 selectors = FromString<ui32>(argv[2]);
    const ui32 size = FromString<ui32>(argv[3]);
    const ui32 rounds = FromString<ui32>(argv[4]);
    const TString shape = argv[5];
    Y_ABORT_UNLESS(threads && selectors && size && rounds && (shape == "records" || shape == "ssts"));
    const bool records = shape == "records";
    const TString mode = argc == 7 ? argv[6] : "both";
    Y_ABORT_UNLESS(mode == "both" || mode == "sync" || mode == "coro");
    TEnvironment env(threads);
    TInputs inputs;
    for (ui32 i = 0; i < selectors; ++i) {
        auto input = std::make_unique<TBenchmarkInput>(env.System.get());
        if (records) {
            input->AddSst(0, 1, size, 1);
        } else {
            input->Params.EmergencyMode = true;
            input->HullCtx->VCfg->HullCompEmergencyMaxSsts = 0;
            for (ui32 j = 1; j <= size; ++j) {
                input->AddSst(LastLevel, j, j, j);
            }
        }
        inputs.push_back(std::move(input));
    }
    if (mode != "coro") {
        Measure<TSelectorActor>(env, inputs, records, size, threads, "sync", 0, false);
    }
    if (mode != "sync") {
        Measure<TSelectorActorCoro>(env, inputs, records, size, threads, "coro", 0, false);
    }
    Cout << "actor,shape,threads,selectors,size,iteration,cpu_us,wall_us,items_per_second" << Endl;
    for (ui32 i = 0; i < rounds; ++i) {
        // Alternate the order to avoid consistently charging one actor for a colder input.
        if (mode == "sync") {
            Measure<TSelectorActor>(env, inputs, records, size, threads, "sync", i, true);
        } else if (mode == "coro") {
            Measure<TSelectorActorCoro>(env, inputs, records, size, threads, "coro", i, true);
        } else if (i % 2) {
            Measure<TSelectorActorCoro>(env, inputs, records, size, threads, "coro", i, true);
            Measure<TSelectorActor>(env, inputs, records, size, threads, "sync", i, true);
        } else {
            Measure<TSelectorActor>(env, inputs, records, size, threads, "sync", i, true);
            Measure<TSelectorActorCoro>(env, inputs, records, size, threads, "coro", i, true);
        }
    }
}
