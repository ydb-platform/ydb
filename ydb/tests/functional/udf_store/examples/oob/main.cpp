#include <ydb/services/udf_store/wasm/abi/udf_cpp_abi.h>

#include <cstdint>

using namespace NYdb::NUdfStore::NAbi;

//! Nested helpers with stable wasm export names for a readable WAVM call stack
//! when the trap is translated by InvokeUdfExport (describeException → "Call stack:").
extern "C" {

//! Load from an address far past linear memory → outOfBoundsMemoryAccess.
__attribute__((visibility("default"))) void oob_leaf() {
    // Guest pointers are linear-memory offsets. A huge offset cannot be mapped.
    volatile char* p = reinterpret_cast<volatile char*>(static_cast<uintptr_t>(1ULL << 40));
    (void)*p;
}

__attribute__((visibility("default"))) void oob_middle() {
    oob_leaf();
}

//! Oob::crash() — intentional memory trap to demo stack printing.
__attribute__((visibility("default"))) void crash(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/)
{
    oob_middle();
}

//! Oob::bad_index() — classic C-style out-of-bounds on a small local buffer.
//! Index is volatile so the compiler cannot fold the access away.
__attribute__((visibility("default"))) void bad_index(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/)
{
    char buf[8] = {};
    volatile int idx = 1 << 28;
    volatile char sink = buf[idx];
    (void)sink;
}

// ---------------------------------------------------------------------------
// Null / broken reference
//
// In WASM, guest nullptr is linear-memory offset 0, and that page is mapped —
// so a plain `*nullptr` usually does NOT trap. To still demo a null-based
// crash we index far from a null base (null + large offset → OOB).
// ---------------------------------------------------------------------------

struct TPoisoned {
    char Pad[1 << 20];
    int Tail;
};

__attribute__((visibility("default"))) void null_leaf() {
    TPoisoned* p = nullptr;
    // Effective address ≈ 1MiB. Default empty/sdk images start with a small
    // linear memory → outOfBoundsMemoryAccess (not a silent load from 0).
    volatile int sink = p->Tail;
    (void)sink;
}

__attribute__((visibility("default"))) void null_middle() {
    null_leaf();
}

//! Oob::null_deref() — null base + field access (trap + stack).
__attribute__((visibility("default"))) void null_deref(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/)
{
    null_middle();
}

__attribute__((visibility("default"))) void bad_ref_leaf() {
    // "Broken reference": bind a C++ reference to a non-object at a poison address.
    TPoisoned& broken = *reinterpret_cast<TPoisoned*>(static_cast<uintptr_t>(0xDeadBeefULL << 12));
    volatile int sink = broken.Tail;
    (void)sink;
}

__attribute__((visibility("default"))) void bad_ref_middle() {
    bad_ref_leaf();
}

//! Oob::bad_ref() — use of a poisoned / dangling-style reference (trap + stack).
__attribute__((visibility("default"))) void bad_ref(
    TExpressionContext* /*context*/,
    TUnversionedValue* /*result*/)
{
    bad_ref_middle();
}

} // extern "C"
