#include <ydb/services/udf_store/wasm/bridge_node_table.h>
#include <ydb/services/udf_store/wasm/bridge_types.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TWasmBridgeNodeTableTest) {

Y_UNIT_TEST(PackUnpackHandle) {
    const ui64 handle = PackBridgeHandle(/*generation*/ 7, /*index*/ 42);
    UNIT_ASSERT_VALUES_EQUAL(BridgeHandleGeneration(handle), 7u);
    UNIT_ASSERT_VALUES_EQUAL(BridgeHandleIndex(handle), 42u);
    UNIT_ASSERT_EXCEPTION(PackBridgeHandle(0, 1), yexception);
}

Y_UNIT_TEST(GenerationTicketStaysPackable) {
    // The generation field is 32 bits wide, so the process-wide counter has to
    // wrap into it rather than overflow out of PackBridgeHandle.
    UNIT_ASSERT_VALUES_EQUAL(BridgeGenerationFromTicket(0), 1u);
    UNIT_ASSERT_VALUES_EQUAL(BridgeGenerationFromTicket(1), 2u);
    UNIT_ASSERT_VALUES_EQUAL(
        BridgeGenerationFromTicket(MaxBridgeGeneration - 1),
        MaxBridgeGeneration);
    UNIT_ASSERT_VALUES_EQUAL(BridgeGenerationFromTicket(MaxBridgeGeneration), 1u);
    for (const ui64 ticket : {ui64{0}, MaxBridgeGeneration - 1, MaxBridgeGeneration, ui64{1} << 33}) {
        const ui64 generation = BridgeGenerationFromTicket(ticket);
        UNIT_ASSERT(generation != 0);
        UNIT_ASSERT_VALUES_EQUAL(
            BridgeHandleGeneration(PackBridgeHandle(generation, 1)),
            generation);
    }
}

Y_UNIT_TEST(RegisterResolveUnref) {
    TWasmBridgeNodeTable table(/*generation*/ 1);
    const ui64 h = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        /*type*/ nullptr,
        TUnboxedValuePod(i64{42}));
    UNIT_ASSERT(h != NullBridgeHandle);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(h).Value.Get<i64>(), 42);
    table.Unref(h);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(StaleGenerationThrows) {
    TWasmBridgeNodeTable table(/*generation*/ 2);
    const ui64 foreign = PackBridgeHandle(/*generation*/ 1, /*index*/ 1);
    UNIT_ASSERT_EXCEPTION(table.Resolve(foreign), yexception);
}

Y_UNIT_TEST(NullHandleThrows) {
    TWasmBridgeNodeTable table(/*generation*/ 1);
    UNIT_ASSERT_EXCEPTION(table.Resolve(NullBridgeHandle), yexception);
}

Y_UNIT_TEST(IdentityReuseSameBoxedPointer) {
    // Identity map keys on AsBoxed().Get() / AsRawStringValue(); scalars and
    // embedded short strings are not reusable.
    TWasmBridgeNodeTable table(/*generation*/ 9);
    const ui64 h1 = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{1}));
    UNIT_ASSERT_VALUES_EQUAL(table.TryReuse(TUnboxedValuePod(i64{1})), NullBridgeHandle);
    table.Unref(h1);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RefKeepsNodeAlive) {
    TWasmBridgeNodeTable table(/*generation*/ 3);
    const ui64 h = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Uint64,
        nullptr,
        TUnboxedValuePod(ui64{7}));
    table.Ref(h);
    table.Unref(h);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(h).Value.Get<ui64>(), 7u);
    table.Unref(h);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RefRejectsOverflowInsteadOfWrapping) {
    TWasmBridgeNodeTable table(/*generation*/ 5);
    const ui64 h = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Uint64,
        nullptr,
        TUnboxedValuePod(ui64{7}));

    // A guest that leaks BridgeRef in a loop gets here eventually; wrapping
    // would make the next Unref free a value the host still holds.
    table.Resolve(h).Refs = Max<ui32>();
    UNIT_ASSERT_EXCEPTION_CONTAINS(table.Ref(h), yexception, "refcount overflow");
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(h).Refs, Max<ui32>());

    table.Resolve(h).Refs = 1;
}

Y_UNIT_TEST(RunScopeReleasesForgottenNodes) {
    TWasmBridgeNodeTable table(/*generation*/ 11);
    {
        TBridgeRunScopeGuard scope(table);
        table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Int64,
            nullptr,
            TUnboxedValuePod(i64{1}));
        table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Int64,
            nullptr,
            TUnboxedValuePod(i64{2}));
        UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 2u);
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunScopeKeepsRefdNodes) {
    TWasmBridgeNodeTable table(/*generation*/ 12);
    ui64 kept = NullBridgeHandle;
    {
        TBridgeRunScopeGuard scope(table);
        kept = table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Int64,
            nullptr,
            TUnboxedValuePod(i64{5}));
        table.Ref(kept);
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(kept).Value.Get<i64>(), 5);
    table.Unref(kept);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(RunScopeToleratesExplicitUnref) {
    // A guest that releases its own temporaries is the normal case, not an
    // error: the scope must not trip over the node being gone already.
    TWasmBridgeNodeTable table(/*generation*/ 13);
    {
        TBridgeRunScopeGuard scope(table);
        const ui64 h = table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Int64,
            nullptr,
            TUnboxedValuePod(i64{3}));
        table.Unref(h);
        UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 0u);
}

Y_UNIT_TEST(NestedRunScopeLeavesOuterHandlesAlone) {
    TWasmBridgeNodeTable table(/*generation*/ 14);
    TBridgeRunScopeGuard outer(table);
    const ui64 outerHandle = table.Register(
        EBridgeNodeKind::Scalar,
        EBridgeValueKind::Int64,
        nullptr,
        TUnboxedValuePod(i64{1}));
    {
        TBridgeRunScopeGuard inner(table);
        table.Register(
            EBridgeNodeKind::Scalar,
            EBridgeValueKind::Int64,
            nullptr,
            TUnboxedValuePod(i64{2}));
        UNIT_ASSERT_VALUES_EQUAL(table.DebugRunScopeDepth(), 2u);
    }
    UNIT_ASSERT_VALUES_EQUAL(table.DebugRunScopeDepth(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(table.DebugSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(table.Resolve(outerHandle).Value.Get<i64>(), 1);
}

} // Y_UNIT_TEST_SUITE
