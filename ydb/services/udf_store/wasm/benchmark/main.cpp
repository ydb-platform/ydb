#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/prefer_wasm_stats.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <ydb/services/udf_store/wasm/wasm_string.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/data_transfer.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/system/thread.h>

#include <thread>

// Cost of feeding one string column value to a wasm UDF, per path:
//
//   Host     (EnableWasmUdfResidentStringColumns off): the scan builds a host
//            string (copy #1), then every UDF call copies it into the
//            compartment (copy #2, #3, ...).
//   Resident (on): the scan writes the value into the compartment once
//            (copy #1), and every UDF call passes the same offset.
//
// Run: ./ya make -r ydb/services/udf_store/wasm/benchmark &&
//      ./ydb/services/udf_store/wasm/benchmark/benchmark

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

using NYql::NUdf::TStringRef;
using NYql::NUdf::TUnboxedValue;

namespace {

//! Allocator over 2 MiB of linear memory handing out the same block every time:
//! the measured loops keep at most one live allocation, and a bump allocator
//! would run out of memory after a few thousand iterations.
constexpr TStringBuf SdkStubWast = R"(
    (module
        (import "env" "memory" (memory i64 32 2097152))
        (func $malloc (param $n i64) (result i64)
            (i64.const 65536)
        )
        (func $free (param $p i64))
        (export "malloc" (func $malloc))
        (export "free" (func $free))
    )
)";

//! Intentionally never destroyed: a WAVM teardown at exit races the harness.
//! Shared-owned like in a compute actor, so materialization registers a keep-alive
//! on the handle and the measured cost matches production.
TQueryCompartmentHandle& QueryCompartment() {
    static auto* handle = new TQueryCompartmentHandlePtr([] {
        const auto objectCode = CompileModuleObjectCode(SdkStubWast, EBytecodeFormat::HumanReadable);
        auto h = std::make_shared<TQueryCompartmentHandle>();
        h->Compartment = CreateEmptyImage();
        h->Compartment->AddSdk(
            MakeModuleBytecode(SdkStubWast, objectCode, EBytecodeFormat::HumanReadable));
        h->Generation = 1;
        return h;
    }());
    return **handle;
}

TString MakeBlob(size_t size) {
    TString blob = TString::Uninitialized(size);
    for (size_t i = 0; i < size; ++i) {
        blob[i] = 'a' + (i % 26);
    }
    return blob;
}

void FeedUdfCalls(const TUnboxedValue& value, size_t callsPerRow) {
    auto* compartment = QueryCompartment().Compartment.get();
    for (size_t call = 0; call < callsPerRow; ++call) {
        TUnversionedValue arg{};
        TCopyGuard guard;
        TWasmStringValue::FillAbiStringArg(compartment, value, arg, guard);
        Y_DO_NOT_OPTIMIZE_AWAY(arg.Data.String);
    }
}

enum class EPath {
    Host,
    Resident,
};

void Run(size_t iterations, size_t blobSize, size_t callsPerRow, EPath path) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    // The compute actor keeps the query compartment installed for the whole
    // activation in both cases; only value materialization differs.
    TCurrentQueryCompartmentGuard queryGuard(&QueryCompartment());

    const TString blob = MakeBlob(blobSize);
    const TStringRef ref(blob.data(), blob.size());

    for (size_t i = 0; i < iterations; ++i) {
        TUnboxedValue value(path == EPath::Resident
            ? TWasmStringValue::MakePreferWasm(ref)
            : NKikimr::NMiniKQL::MakeString(ref));
        FeedUdfCalls(value, callsPerRow);
    }
}

} // namespace

#define DEFINE_ROW_BENCHMARK(name, blobSize, callsPerRow)                     \
    Y_CPU_BENCHMARK(Host_##name, iface) {                                     \
        Run(iface.Iterations(), blobSize, callsPerRow, EPath::Host);           \
    }                                                                         \
    Y_CPU_BENCHMARK(Resident_##name, iface) {                                  \
        Run(iface.Iterations(), blobSize, callsPerRow, EPath::Resident);       \
    }

// Cost breakdown of the pieces the two paths are built from.
Y_CPU_BENCHMARK(Part_GuestMallocFree, iface) {
    auto* compartment = QueryCompartment().Compartment.get();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto offset = compartment->AllocateBytes(4096);
        Y_DO_NOT_OPTIMIZE_AWAY(offset);
        compartment->FreeBytes(offset);
    }
}

//! Guest function entry asks for the host stack bounds to guard against stack
//! overflow; this is what that query costs when it is not cached. On the main
//! thread glibc walks /proc/self/maps for it, on a pthread it is a
//! sched_getaffinity syscall - the gap between the two numbers is why the
//! uncached version looked catastrophic in a benchmark and merely wasteful in
//! the server, where UDFs run on actor threads.
Y_CPU_BENCHMARK(Part_StackBoundsQuery_MainThread, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TCurrentThreadLimits limits;
        Y_DO_NOT_OPTIMIZE_AWAY(limits.StackBegin);
    }
}

Y_CPU_BENCHMARK(Part_StackBoundsQuery_WorkerThread, iface) {
    const size_t iterations = iface.Iterations();
    std::thread worker([iterations] {
        for (size_t i = 0; i < iterations; ++i) {
            TCurrentThreadLimits limits;
            Y_DO_NOT_OPTIMIZE_AWAY(limits.StackBegin);
        }
    });
    worker.join();
}

//! Export lookup only, to tell a slow name lookup from a slow guest call.
Y_CPU_BENCHMARK(Part_ExportLookup, iface) {
    auto* compartment = QueryCompartment().Compartment.get();
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto* function = compartment->GetFunction("malloc");
        Y_DO_NOT_OPTIMIZE_AWAY(function);
    }
}

Y_CPU_BENCHMARK(Part_HostMakeString_4KB, iface) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    const TString blob = MakeBlob(4096);
    const TStringRef ref(blob.data(), blob.size());
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TUnboxedValue value(NKikimr::NMiniKQL::MakeString(ref));
        Y_DO_NOT_OPTIMIZE_AWAY(value.AsStringRef().Data());
    }
}

Y_CPU_BENCHMARK(Part_CopyIntoCompartment_4KB, iface) {
    auto* compartment = QueryCompartment().Compartment.get();
    const TString blob = MakeBlob(4096);
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto guard = CopyIntoCompartment(TStringBuf(blob), compartment);
        Y_DO_NOT_OPTIMIZE_AWAY(guard.GetCopiedOffset());
    }
}

Y_CPU_BENCHMARK(Part_MakePreferWasm_4KB, iface) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    TCurrentQueryCompartmentGuard queryGuard(&QueryCompartment());
    const TString blob = MakeBlob(4096);
    const TStringRef ref(blob.data(), blob.size());
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TUnboxedValue value(TWasmStringValue::MakePreferWasm(ref));
        Y_DO_NOT_OPTIMIZE_AWAY(value.AsStringRef().Data());
    }
}

Y_CPU_BENCHMARK(Part_FillArgFromResident_4KB, iface) {
    NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
    TCurrentQueryCompartmentGuard queryGuard(&QueryCompartment());
    const TString blob = MakeBlob(4096);
    TUnboxedValue value(TWasmStringValue::MakePreferWasm(TStringRef(blob.data(), blob.size())));
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        FeedUdfCalls(value, 1);
    }
}

DEFINE_ROW_BENCHMARK(64B_1call, 64, 1)
DEFINE_ROW_BENCHMARK(4KB_1call, 4096, 1)
DEFINE_ROW_BENCHMARK(8KB_1call, 8192, 1)
DEFINE_ROW_BENCHMARK(16KB_1call, 16384, 1)
DEFINE_ROW_BENCHMARK(32KB_1call, 32768, 1)
DEFINE_ROW_BENCHMARK(64KB_1call, 65536, 1)
DEFINE_ROW_BENCHMARK(256KB_1call, 262144, 1)
DEFINE_ROW_BENCHMARK(4KB_2calls, 4096, 2)
DEFINE_ROW_BENCHMARK(64KB_4calls, 65536, 4)

#undef DEFINE_ROW_BENCHMARK
