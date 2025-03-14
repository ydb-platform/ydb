#include <ydb/core/kqp/tools/combiner_perf/simple_last.h>
#include <ydb/core/kqp/tools/combiner_perf/simple.h>
#include <ydb/core/kqp/tools/combiner_perf/tpch_last.h>

#include <library/cpp/lfalloc/alloc_profiler/profiler.h>

#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/system/compiler.h>

void DoFullPass(bool withSpilling)
{
    using namespace NKikimr::NMiniKQL;

    TRunParams runParams;

    runParams.NumRuns = 100;
    runParams.RowsPerRun = 1'000'000;
    runParams.MaxKey = 1ull << 16;
    runParams.LongStringKeys = true;

    if (false) {
        Cerr << "Simple, -llvm, -spilling" << Endl;
        NKikimr::NMiniKQL::RunTestSimple<false>();

        Cerr << "Simple, +llvm, -spilling" << Endl;
        NKikimr::NMiniKQL::RunTestSimple<true>();
    }

    auto doSimpleLast = [](const TRunParams& params) {
        Cerr << "LastSimple, -llvm, -spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastSimple<false, false>(params);

        if (false) {
            Cerr << "LastSimple, +llvm, -spilling" << Endl;
            NKikimr::NMiniKQL::RunTestLastSimple<true, false>(params);
        }
    };

    doSimpleLast(runParams);

    if (false) {
        TRunParams manyKeysParams = runParams;
        manyKeysParams.MaxKey = (1ull << 16) - 1;

        doSimpleLast(manyKeysParams);

        manyKeysParams.MaxKey = (1ull << 24) - 1;

        doSimpleLast(manyKeysParams);
    }

    if (false) {
        Cerr << "LastTpch, -llvm, -spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastTpch<false, false>();

        Cerr << "LastTpch, +llvm, -spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastTpch<true, false>();
    }

    if (withSpilling) {
        Cerr << "LastSimple, -llvm, +spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastSimple<false, true>(runParams);

        Cerr << "LastSimple, +llvm, +spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastSimple<true, true>(runParams);

        Cerr << "LastTpch, -llvm, +spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastTpch<false, true>();

        Cerr << "LastTpch, +llvm, +spilling" << Endl;
        NKikimr::NMiniKQL::RunTestLastTpch<true, true>();
    }
}

int main(int argc, const char* argv[])
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    if (false) {
        NAllocProfiler::StartAllocationSampling(true);
    }

    constexpr int NumIterations = 1;

    for (int i = 0; i < NumIterations; ++i) {
        DoFullPass(false);
    }

    if (false) {
        TFileOutput out("memory_profile");
        NAllocProfiler::StopAllocationSampling(out, 10000);
    }
}
