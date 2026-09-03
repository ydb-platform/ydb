#include <ydb/services/udf_store/wasm/bridge_node_table.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/bridge_types.h>
#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/query_compartment_scope.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/pointer.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <bit>
#include <optional>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;
using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;

namespace {

constexpr TStringBuf SdkStubWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (global $break (mut i64) (i64.const 65536))
        (func $sbrk (param $n i64) (result i64)
            (local $old i64)
            (local $new i64)
            (local $pages i64)
            (local.set $old (global.get $break))
            (local.set $new
                (i64.and
                    (i64.add (i64.add (local.get $old) (local.get $n)) (i64.const 7))
                    (i64.const -8)))
            (local.set $pages
                (i64.sub
                    (i64.shr_u
                        (i64.add (local.get $new) (i64.const 65535))
                        (i64.const 16))
                    (memory.size)))
            (if (i64.gt_s (local.get $pages) (i64.const 0))
                (then
                    (if (i64.eq (memory.grow (local.get $pages)) (i64.const -1))
                        (then (return (i64.const -1))))))
            (global.set $break (local.get $new))
            (local.get $old)
        )
        (func $malloc (param $n i64) (result i64)
            (call $sbrk (local.get $n))
        )
        (func $free (param $p i64))
        (export "sbrk" (func $sbrk))
        (export "malloc" (func $malloc))
        (export "free" (func $free))
    )
)";

//! Fresh MiniKQL string on every call. That is the host allocation a guest
//! that never Unrefs would leak, one per row, unless the Run scope drops it.
constexpr TStringBuf BridgeMakeStringWast = R"(
    (module
        (import "env" "memory" (memory i64 8 2097152))
        (import "env" "BridgeMakeString" (func $make (param i64 i64) (result i64)))
        (data (i64.const 1024) "leak-probe-payload-long-enough-for-refcount!!")
        (func $make_str (param $ctx i64) (param $result i64)
            (i64.store (local.get $result)
                (call $make (i64.const 1024) (i64.const 45)))
        )
        (export "make_str" (func $make_str))
    )
)";

TNamedModuleBytecode MakeNamedLibrary(TStringBuf name, TStringBuf wast) {
    const auto objectCode = CompileModuleObjectCode(wast, EBytecodeFormat::HumanReadable);
    return TNamedModuleBytecode{
        .Name = TString(name),
        .Bytecode = MakeModuleBytecode(wast, objectCode, EBytecodeFormat::HumanReadable),
    };
}

//! Boxed value that reports its own death. Node counts and byte counters tell
//! you the bridge forgot a bookkeeping entry; this tells you whether the value
//! behind it was actually released, which is the part that leaks a query's
//! worth of MiniKQL memory when it goes wrong. Allocated through
//! TWithMiniKQLAlloc like every other MiniKQL value, so creating and
//! destroying one both need the allocator bound.
class TLifetimeProbe: public TComputationValue<TLifetimeProbe> {
public:
    TLifetimeProbe(TMemoryUsageInfo* memInfo, i64* live)
        : TComputationValue(memInfo)
        , Live_(live)
    {
        ++*Live_;
    }

    ~TLifetimeProbe() {
        --*Live_;
    }

private:
    i64* const Live_;
};

//! One-off allocator plus the value machinery on top of it, in the shape the
//! compute actors build it: created unacquired, so every access has to bind it
//! explicitly and teardown outside a bind is reproducible.
struct TDetachedMiniKqlEnv {
    std::shared_ptr<TScopedAlloc> Alloc;
    TMemoryUsageInfo MemInfo;
    std::optional<THolderFactory> HolderFactory;
    std::optional<TDefaultValueBuilder> ValueBuilder;

    TDetachedMiniKqlEnv()
        : Alloc(std::make_shared<TScopedAlloc>(
              __LOCATION__,
              NKikimr::TAlignedPagePoolCounters(),
              /*supportsSizedAllocators=*/true,
              /*initiallyAcquired=*/false))
        , MemInfo("bridge_leak_ut")
    {
        auto guard = Guard(*Alloc);
        HolderFactory.emplace(Alloc->Ref(), MemInfo);
        ValueBuilder.emplace(*HolderFactory);
    }

    ~TDetachedMiniKqlEnv() {
        auto guard = Guard(*Alloc);
        ValueBuilder.reset();
        HolderFactory.reset();
    }

    TUnboxedValue MakeProbe(i64* live) {
        return TUnboxedValue(TUnboxedValuePod(new TLifetimeProbe(&MemInfo, live)));
    }

    TUnboxedValue MakeString(TStringBuf bytes) {
        return ValueBuilder->NewString(TStringRef(bytes.data(), bytes.size()));
    }
};

//! MiniKQL carves small objects out of 64 KiB pool pages, so "used" only falls
//! back to the exact byte when the last object on a page dies. One page of
//! slack keeps this assert about the multi-MiB payloads it exists to catch,
//! while TLifetimeProbe covers the small ones exactly.
void AssertUsedBackToBaseline(TScopedAlloc& alloc, ui64 baseline) {
    auto guard = Guard(alloc);
    const ui64 used = alloc.GetUsed();
    UNIT_ASSERT_C(
        used <= baseline + (64u << 10),
        TStringBuilder() << "MiniKQL used " << used << " bytes, baseline was " << baseline);
}

std::unique_ptr<TQueryCompartmentHandle> MakeCompartmentHandle(ui64 generation) {
    auto compartment = CreateEmptyImage();
    compartment->AddSdk(MakeNamedLibrary("sdk", SdkStubWast).Bytecode);

    auto handle = std::make_unique<TQueryCompartmentHandle>();
    handle->Generation = generation;
    handle->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(handle->Generation);
    handle->Compartment = std::move(compartment);
    handle->Resident = std::make_unique<TCompartmentResidentCache>(handle->Compartment.get());
    return handle;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmBridgeLeakTest) {

Y_UNIT_TEST(TableDeathReleasesNodesTheGuestNeverUnrefd) {
    // BridgeRef is the guest's way to keep a handle past the row, and nothing
    // makes it pair that with BridgeUnref. Such a node stays alive to the end
    // of the query by design, so the table dying is the only thing that can
    // release its value.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);
    i64 live = 0;

    {
        TWasmBridgeNodeTable table(/*generation=*/3);
        const ui64 handle = table.Register(
            EBridgeNodeKind::Resource,
            EBridgeValueKind::Resource,
            nullptr,
            mkql.MakeProbe(&live));
        table.Ref(handle);
        table.Ref(handle);
        UNIT_ASSERT_VALUES_EQUAL(live, 1);
        UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    }

    UNIT_ASSERT_VALUES_EQUAL(live, 0);
}

Y_UNIT_TEST(RunScopeEndReleasesTheValueAndNotJustTheHandle) {
    // Dropping the node is not enough: the identity map is keyed by the boxed
    // pointer, so an entry left behind would both keep the value alive for the
    // whole query and hand its handle to a later row.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);
    i64 live = 0;

    TWasmBridgeNodeTable table(/*generation=*/4);
    {
        TBridgeRunScopeGuard scope(table);
        table.RegisterOrReuse(
            EBridgeNodeKind::Resource,
            EBridgeValueKind::Resource,
            nullptr,
            mkql.MakeProbe(&live));
        UNIT_ASSERT_VALUES_EQUAL(live, 1);
    }

    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(live, 0);
}

Y_UNIT_TEST(RowLoopDoesNotAccumulateValuesPerRow) {
    // A guest that never unrefs anything must cost one live value at a time,
    // not one per row: this is the shape a scan over a computed column takes,
    // and accumulating there would exhaust the query's MiniKQL budget long
    // before the scan ends.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);
    i64 live = 0;
    i64 peak = 0;

    TWasmBridgeNodeTable table(/*generation=*/5);
    for (int row = 0; row < 256; ++row) {
        TBridgeRunScopeGuard scope(table);
        table.RegisterOrReuse(
            EBridgeNodeKind::Resource,
            EBridgeValueKind::Resource,
            nullptr,
            mkql.MakeProbe(&live));
        peak = Max(peak, live);
    }

    UNIT_ASSERT_VALUES_EQUAL(peak, 1);
    UNIT_ASSERT_VALUES_EQUAL(live, 0);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(EvictedPinReleasesItsOwner) {
    // Eviction recycles the linear-memory block, which the arena counters
    // already show. The pin also holds the MiniKQL value the bytes came from,
    // and keeping that would grow the host heap for the whole query even
    // though linear memory stays flat.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);

    constexpr ui64 kBudget = 4ull << 20;
    constexpr size_t kBlob = 1ull << 20;

    auto handle = MakeCompartmentHandle(/*generation=*/6);
    TCompartmentResidentCache resident(handle->Compartment.get(), kBudget);

    ui64 baseline = mkql.Alloc->GetUsed();
    for (int row = 0; row < 16; ++row) {
        resident.BeginRun();
        const TString payload(kBlob, static_cast<char>('a' + row));
        // A fresh column value per row, so every pin is a new identity and the
        // cache has to evict rather than reuse.
        const TUnboxedValue column = mkql.MakeString(payload);
        UNIT_ASSERT(resident.Pin(BridgeIdentityKey(column), column, column.AsStringRef()) != 0);
    }

    UNIT_ASSERT(resident.EvictionCount() > 0);
    // Only the pins still inside the budget may hold a value.
    UNIT_ASSERT(
        mkql.Alloc->GetUsed() <= baseline + kBudget + (64u << 10));
}

Y_UNIT_TEST(ResidentDeathReleasesEverythingItOwns) {
    // Pins and guest user-state both keep a TUnboxedValue alive so their
    // identity key stays valid. Both are keyed by identity rather than by node
    // precisely so they outlive the nodes, which leaves the cache as the only
    // thing that can release them.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);
    i64 live = 0;

    auto handle = MakeCompartmentHandle(/*generation=*/7);
    {
        TCompartmentResidentCache resident(handle->Compartment.get());

        const TUnboxedValue column = mkql.MakeString(TString(1ull << 20, 'P'));
        UNIT_ASSERT(resident.Pin(BridgeIdentityKey(column), column, column.AsStringRef()) != 0);

        const TUnboxedValue stateOwner = mkql.MakeProbe(&live);
        resident.SetUserData(BridgeIdentityKey(stateOwner), stateOwner, 0xB0FFE7);
        UNIT_ASSERT_VALUES_EQUAL(resident.UserDataCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(live, 1);
    }

    // The locals are gone, so if anything is still alive the cache kept it.
    UNIT_ASSERT_VALUES_EQUAL(live, 0);
}

Y_UNIT_TEST(EvictedUserStateReleasesOwnerAndHandsItsMemoryBack) {
    // The host cannot run the guest's deleter, so evicting user state has to
    // queue the guest's pointer for BridgeDrainReleasedUserData. Dropping it
    // instead would leak inside linear memory, where no host counter shows it.
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);
    i64 live = 0;

    auto handle = MakeCompartmentHandle(/*generation=*/8);
    TCompartmentResidentCache resident(handle->Compartment.get());

    // Past the cache's cap on remembered identities, so the oldest entries go.
    constexpr int kStates = 1200;
    TVector<TUnboxedValue> owners;
    for (int i = 0; i < kStates; ++i) {
        auto owner = mkql.MakeProbe(&live);
        resident.SetUserData(BridgeIdentityKey(owner), owner, 0x1000 + i);
        owners.push_back(std::move(owner));
    }

    UNIT_ASSERT_VALUES_EQUAL(live, kStates); // the test still holds every owner
    UNIT_ASSERT(resident.UserDataCount() < static_cast<size_t>(kStates));

    THashSet<ui64> released;
    ui64 value = 0;
    while (resident.PopReleasedUserData(value)) {
        UNIT_ASSERT_C(released.insert(value).second, "guest value handed back twice");
    }
    UNIT_ASSERT_VALUES_EQUAL(
        released.size() + resident.UserDataCount(),
        static_cast<size_t>(kStates));

    // Owners of evicted entries are held by the test alone now.
    owners.clear();
    UNIT_ASSERT_VALUES_EQUAL(live, static_cast<i64>(resident.UserDataCount()));
}

Y_UNIT_TEST(HandleTeardownOutsideBindAllocatorFreesEverything) {
    // The compute actors create TScopedAlloc unacquired and tear the query down
    // outside any BindAllocator scope. TQueryCompartmentScope therefore binds
    // the allocator in its own destructor; this is the state that has to
    // survive it -- MiniKQL values nothing but the handle still references.
    TDetachedMiniKqlEnv mkql;
    i64 live = 0;
    ui64 baseline = 0;
    constexpr size_t kBlob = 4ull << 20;

    auto handle = MakeCompartmentHandle(/*generation=*/9);
    {
        auto guard = Guard(*mkql.Alloc);
        baseline = mkql.Alloc->GetUsed();

        auto& table = *handle->BridgeNodes;
        auto& resident = *handle->Resident;

        // A handle the guest ref'd and never gave back.
        const ui64 kept = table.Register(
            EBridgeNodeKind::Resource,
            EBridgeValueKind::Resource,
            nullptr,
            mkql.MakeProbe(&live));
        table.Ref(kept);

        // A pinned column value plus guest state keyed by another identity.
        const TUnboxedValue column = mkql.MakeString(TString(kBlob, 'L'));
        UNIT_ASSERT(resident.Pin(BridgeIdentityKey(column), column, column.AsStringRef()) != 0);

        const TUnboxedValue stateOwner = mkql.MakeProbe(&live);
        resident.SetUserData(BridgeIdentityKey(stateOwner), stateOwner, 0xFEED);

        UNIT_ASSERT_VALUES_EQUAL(live, 2);
        UNIT_ASSERT(mkql.Alloc->GetUsed() >= baseline + kBlob);
    }

    // Outside the bind, with the locals gone: only the handle holds anything.
    UNIT_ASSERT_VALUES_EQUAL(live, 2);

    {
        // Exactly what TQueryCompartmentScope's destructor does.
        auto guard = Guard(*mkql.Alloc);
        handle.reset();
    }

    UNIT_ASSERT_VALUES_EQUAL(live, 0);
    AssertUsedBackToBaseline(*mkql.Alloc, baseline);
}

Y_UNIT_TEST(ScopeDestructorBindsAllocatorAndFrees) {
    // Same state as HandleTeardownOutsideBindAllocatorFreesEverything, but
    // through TQueryCompartmentScope itself: that is the type the compute
    // actors own, and a regression in its destructor would not show up in a
    // test that only copies the Guard(*alloc) / reset() pattern.
    TDetachedMiniKqlEnv mkql;
    i64 live = 0;
    ui64 baseline = 0;
    constexpr size_t kBlob = 4ull << 20;

    auto handle = MakeCompartmentHandle(/*generation=*/10);
    {
        auto guard = Guard(*mkql.Alloc);
        baseline = mkql.Alloc->GetUsed();

        const ui64 kept = handle->BridgeNodes->Register(
            EBridgeNodeKind::Resource,
            EBridgeValueKind::Resource,
            nullptr,
            mkql.MakeProbe(&live));
        handle->BridgeNodes->Ref(kept);

        const TUnboxedValue column = mkql.MakeString(TString(kBlob, 'S'));
        UNIT_ASSERT(handle->Resident->Pin(
            BridgeIdentityKey(column), column, column.AsStringRef()) != 0);

        const TUnboxedValue stateOwner = mkql.MakeProbe(&live);
        handle->Resident->SetUserData(BridgeIdentityKey(stateOwner), stateOwner, 0x51C0);
        UNIT_ASSERT_VALUES_EQUAL(live, 2);
    }

    UNIT_ASSERT_VALUES_EQUAL(live, 2);
    {
        TQueryCompartmentScope scope(std::move(handle), mkql.Alloc);
        UNIT_ASSERT(scope.HasHandle());
    }
    UNIT_ASSERT_VALUES_EQUAL(live, 0);
    AssertUsedBackToBaseline(*mkql.Alloc, baseline);
}

Y_UNIT_TEST(ResultSlotIsReusedAcrossRows) {
    // TWasmBridgeFunction::Run Allocs an 8-byte result slot and Frees it on
    // the way out. A missed Free would grow the arena by 8 bytes per row.
    auto handle = MakeCompartmentHandle(/*generation=*/11);
    auto& resident = *handle->Resident;

    resident.BeginRun();
    const ui64 first = resident.Alloc(sizeof(ui64));
    resident.Free(first);
    const ui64 arena = resident.ArenaBytes();

    for (int row = 0; row < 1024; ++row) {
        resident.BeginRun();
        const ui64 slot = resident.Alloc(sizeof(ui64));
        UNIT_ASSERT_VALUES_EQUAL(slot, first);
        resident.Free(slot);
    }

    UNIT_ASSERT_VALUES_EQUAL(resident.ArenaBytes(), arena);
}

Y_UNIT_TEST(GuestMakeStringLoopDoesNotAccumulateHostValues) {
    // Production Run: BeginRun, open a Run scope, invoke, copy the result
    // out, drop the scope. BridgeMakeString allocates a new MiniKQL string
    // every call; without the scope those would stay in the table for the
    // whole query, and without dropping the copied result they would stay
    // on the host heap.
    EnsureUdfHostIntrinsicsRegistered();
    TDetachedMiniKqlEnv mkql;
    auto guard = Guard(*mkql.Alloc);

    auto handle = MakeCompartmentHandle(/*generation=*/12);
    TCurrentQueryCompartmentGuard queryGuard(handle.get());
    TCurrentCompartmentGuard compartmentGuard(handle->Compartment.get());
    TWasmUdfInvocationContext context(handle->Compartment.get());
    TCurrentInvocationContextGuard invocationGuard(&context);
    TBridgeValueBuilderGuard vbGuard(*handle->BridgeNodes, &*mkql.ValueBuilder);

    const auto objectCode = CompileModuleObjectCode(
        BridgeMakeStringWast,
        EBytecodeFormat::HumanReadable);
    AddPrecompiledModule(
        handle->Compartment.get(),
        MakeModuleBytecode(BridgeMakeStringWast, objectCode, EBytecodeFormat::HumanReadable),
        "Leak");

    auto& table = *handle->BridgeNodes;
    auto& resident = *handle->Resident;
    const ui64 baseline = mkql.Alloc->GetUsed();
    ui64 arenaAfterFirst = 0;

    for (int row = 0; row < 256; ++row) {
        resident.BeginRun();
        TUnboxedValue result;
        {
            TBridgeRunScopeGuard scope(table);
            const ui64 resultOffset = resident.Alloc(sizeof(ui64));
            Y_DEFER {
                resident.Free(resultOffset);
            };
            *PtrFromVM(handle->Compartment.get(), std::bit_cast<ui64*>(resultOffset)) = NullBridgeHandle;

            InvokeUdfExport(
                handle->Compartment.get(),
                "make_str",
                std::bit_cast<uintptr_t>(&context),
                resultOffset,
                {});

            const ui64 resultHandle = *PtrFromVM(
                handle->Compartment.get(),
                std::bit_cast<ui64*>(resultOffset));
            UNIT_ASSERT(resultHandle != NullBridgeHandle);
            result = table.Resolve(resultHandle).Value;
        }
        UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
        UNIT_ASSERT(result.AsStringRef().Size() == 45u);
        if (row == 0) {
            arenaAfterFirst = resident.ArenaBytes();
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(resident.ArenaBytes(), arenaAfterFirst);
    AssertUsedBackToBaseline(*mkql.Alloc, baseline);
}

} // Y_UNIT_TEST_SUITE
