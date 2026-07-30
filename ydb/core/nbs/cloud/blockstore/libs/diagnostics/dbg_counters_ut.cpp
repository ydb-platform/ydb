#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/dbg_counters.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

namespace {

NMonitoring::TDynamicCounterPtr MakeRoot()
{
    return MakeIntrusive<NMonitoring::TDynamicCounters>();
}

ui64 GetValue(NMonitoring::TDynamicCounterPtr counters, const TString& name)
{
    UNIT_ASSERT(counters);
    auto counter = counters->GetCounter(name, false);
    UNIT_ASSERT(counter);
    return counter->Val();
}

ui64 GetSubgroupValue(
    NMonitoring::TDynamicCounterPtr root,
    const TString& connectionType,
    const TString& name)
{
    UNIT_ASSERT(root);
    auto subgroup = root->GetSubgroup("connectionType", connectionType);
    return GetValue(subgroup, name);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDBGCountersTest)
{
    Y_UNIT_TEST(ShouldIncrementEachConnectionCounterIndependently)
    {
        auto root = MakeRoot();
        TDBGConnectionCounters counters(root);

        // Initial state: all counters are zero.
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Reconnects"));

        counters.OnConnectAttempt();
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Reconnects"));

        counters.OnConnectOk();
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Reconnects"));

        counters.OnConnectErr();
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Reconnects"));

        counters.OnDisconnect();
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(0, GetValue(root, "Reconnects"));

        counters.OnReconnect();
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetValue(root, "Reconnects"));
    }

    Y_UNIT_TEST(ShouldAccumulateRepeatedCalls)
    {
        auto root = MakeRoot();
        TDBGConnectionCounters counters(root);

        for (int i = 0; i < 5; ++i) {
            counters.OnConnectAttempt();
        }
        for (int i = 0; i < 3; ++i) {
            counters.OnConnectOk();
        }
        for (int i = 0; i < 2; ++i) {
            counters.OnConnectErr();
        }

        UNIT_ASSERT_VALUES_EQUAL(5, GetValue(root, "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(3, GetValue(root, "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(2, GetValue(root, "ConnectErr"));
    }

    Y_UNIT_TEST(ShouldNotCrashWithNullParent)
    {
        TDBGConnectionCounters counters(nullptr);

        // None of the calls must dereference a null counter.
        counters.OnConnectAttempt();
        counters.OnConnectOk();
        counters.OnConnectErr();
        counters.OnDisconnect();
        counters.OnReconnect();
    }

    Y_UNIT_TEST(ShouldSplitByConnectionType)
    {
        auto root = MakeRoot();
        TDirectBlockGroupCounters counters(root);

        counters.OnConnectAttempt(EDBGConnectionType::DDisk);
        counters.OnConnectOk(EDBGConnectionType::DDisk);
        counters.OnConnectErr(EDBGConnectionType::DDisk);
        counters.OnReconnect(EDBGConnectionType::DDisk);

        counters.OnConnectAttempt(EDBGConnectionType::PBuffer);
        counters.OnConnectAttempt(EDBGConnectionType::PBuffer);

        // DDisk subgroup got its own events.
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetSubgroupValue(root, "DDisk", "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetSubgroupValue(root, "DDisk", "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetSubgroupValue(root, "DDisk", "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetSubgroupValue(root, "DDisk", "Reconnects"));

        // PBuffer subgroup is isolated from DDisk.
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetSubgroupValue(root, "PBuffer", "ConnectAttempts"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetSubgroupValue(root, "PBuffer", "ConnectOk"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetSubgroupValue(root, "PBuffer", "ConnectErr"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetSubgroupValue(root, "PBuffer", "Reconnects"));
    }

    Y_UNIT_TEST(ShouldRouteDisconnectToDDisk)
    {
        auto root = MakeRoot();
        TDirectBlockGroupCounters counters(root);

        // In production OnDisconnect is only ever called for DDisk.
        counters.OnDisconnect(EDBGConnectionType::DDisk);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetSubgroupValue(root, "DDisk", "Disconnects"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetSubgroupValue(root, "PBuffer", "Disconnects"));
    }

    Y_UNIT_TEST(ShouldNotCrashDirectBlockGroupCountersWithNullParent)
    {
        TDirectBlockGroupCounters counters(nullptr);

        counters.OnConnectAttempt(EDBGConnectionType::DDisk);
        counters.OnConnectOk(EDBGConnectionType::DDisk);
        counters.OnConnectErr(EDBGConnectionType::DDisk);
        counters.OnDisconnect(EDBGConnectionType::DDisk);
        counters.OnReconnect(EDBGConnectionType::DDisk);

        counters.OnConnectAttempt(EDBGConnectionType::PBuffer);
        counters.OnConnectOk(EDBGConnectionType::PBuffer);
        counters.OnConnectErr(EDBGConnectionType::PBuffer);
        counters.OnDisconnect(EDBGConnectionType::PBuffer);
        counters.OnReconnect(EDBGConnectionType::PBuffer);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
