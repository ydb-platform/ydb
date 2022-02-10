#include "tenant_slot_broker_impl.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/cms/console/config_index.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NKikimr {

using namespace NTenantSlotBroker;

namespace {

static const TString DATA_CENTER1 = ToString(1);
static const TString DATA_CENTER2 = ToString(2);
static const TString DATA_CENTER3 = ToString(3);

const TString SLOT1 = "slot-1";
const TString SLOT2 = "slot-2";
const TString SLOT3 = "slot-3";
const TString SLOT4 = "slot-4";
const TString SLOT5 = "slot-5";
const TString SLOT6 = "slot-6";

TTenantTestConfig::TTenantPoolConfig DefaultTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
        {{ {SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}},
           {SLOT2, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}},
           {SLOT3, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}} }},
        ""
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig DoubleTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
        {{ {SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}},
           {SLOT2, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}},
           {SLOT3, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}},
           {SLOT4, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}},
           {SLOT5, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}},
           {SLOT6, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}} }},
        ""
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig ShrinkTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
        {{ {SLOT3, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}} }},
        ""
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig ModifiedTypeTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
        {{ {SLOT1, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}},
           {SLOT2, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}},
           {SLOT3, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}} }},
        ""
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig AssignedTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
        {{ {SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}},
           {SLOT2, SLOT2_TYPE, DOMAIN1_NAME, TENANT1_2_NAME, {2, 2, 2}},
           {SLOT3, SLOT3_TYPE, DOMAIN1_NAME, TENANT1_3_NAME, {3, 3, 3}} }},
        ""
    };
    return res;
}

TTenantTestConfig TenantTestConfigSingleSlot()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME, TENANT1_3_NAME, TENANT1_4_NAME, TENANT1_5_NAME }}} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        false,
        // FakeSchemeShard
        true,
        // CreateConsole
        false,
        // Nodes {tenant_pool_config, data_center}
        {{
            // Node0
            {
                // TenantPoolConfig
                {
                    // Static slots {tenant, {cpu, memory, network}}
                    {},
                    // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                    {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                    ""
                }
            },
            // Node1
            {
                // TenantPoolConfig
                {
                    // Static slots {tenant, {cpu, memory, network}}
                    {},
                    // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                    {{ {DOMAIN1_SLOT2, SLOT2_TYPE, DOMAIN1_NAME, "", {2, 2, 2}} }},
                    ""
                }
            },
            // Node2
            {
                // TenantPoolConfig
                {
                    // Static slots {tenant, {cpu, memory, network}}
                    {},
                    // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                    {{ {DOMAIN1_SLOT3, SLOT3_TYPE, DOMAIN1_NAME, "", {3, 3, 3}} }},
                    ""
                }
            }
        }},
        // DataCenterCount
        1
    };
    return res;
}

TTenantTestConfig TenantTestConfigWithConsole()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{}}} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        false,
        // FakeSchemeShard
        true,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
            // Node0
            {
                // TenantPoolConfig
                {
                    // Static slots {tenant, {cpu, memory, network}}
                    {},
                    // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                    {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                    ""
                }
            }
        }},
        // DataCenterCount
        1
    };
    return res;
}

TTenantTestConfig TenantTestConfig3DC()
{
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME, TENANT1_3_NAME, TENANT1_4_NAME, TENANT1_5_NAME }}} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        false,
        // FakeSchemeShard
        true,
        // CreateConsole
        false,
        // Nodes {tenant_pool_config, data_center}
        {{
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()}
        }},
        // DataCenterCount
        3
    };
    return res;
}

struct TSlotCount {
    ui64 Required;
    ui64 Pending;
    ui64 Missing;
    ui64 Misplaced;
    ui64 Split;
    ui64 Pinned;

    TSlotCount(ui64 required = 0,
               ui64 pending = 0,
               ui64 missing = 0,
               ui64 misplaced = 0,
               ui64 split = 0,
               ui64 pinned = 0)
        : Required(required)
        , Pending(pending)
        , Missing(missing)
        , Misplaced(misplaced)
        , Split(split)
        , Pinned(pinned)
    {
    }

    TSlotCount &operator+=(const TSlotCount &other)
    {
        Required += other.Required;
        Pending += other.Pending;
        Missing += other.Missing;
        Misplaced += other.Misplaced;
        Split += other.Split;
        Pinned += other.Pinned;
        return *this;
    }
};

struct TSlotRequest {
    TSlotDescription Description;
    TSlotCount Count;

    TSlotRequest(const TString &type,
                 TString dc,
                 ui64 required,
                 ui64 pending,
                 ui64 missing,
                 ui64 misplaced = 0,
                 ui64 split = 0,
                 ui64 pinned = 0)
        : Description(type, dc)
        , Count(required, pending, missing, misplaced, split, pinned)
    {
    }

    TSlotRequest(const TString &type,
                 TString dc,
                 bool forceLocation,
                 ui32 group,
                 bool forceCollocation,
                 ui64 required,
                 ui64 pending,
                 ui64 missing,
                 ui64 misplaced,
                 ui64 split,
                 ui64 pinned = 0)
        : Description(type, dc, forceLocation, group, forceCollocation)
        , Count(required, pending, missing, misplaced, split, pinned)
    {
    }
};

void CollectRequests(TVector<TSlotRequest> &)
{
}

void CollectRequests(TVector<TSlotRequest> &requests, const TString &type, TString dataCenter, ui64 required, ui64 pending, ui64 missing)
{
    requests.push_back({type, dataCenter, required, pending, missing});
}

template <typename ...Ts>
void CollectRequests(TVector<TSlotRequest> &requests, const TString &type, TString dataCenter, ui64 required, ui64 pending, ui64 missing,
                     Ts... args)
{
    CollectRequests(requests, type, dataCenter, required, pending, missing);
    CollectRequests(requests, args...);
}

void CheckState(const NKikimrTenantSlotBroker::TTenantState &rec,
                const TString &name,
                THashMap<TSlotDescription, TSlotCount> expected)
{
    UNIT_ASSERT_VALUES_EQUAL(name, rec.GetTenantName());
    for (auto &slot : rec.GetRequiredSlots()) {
        TSlotDescription key(slot);
        UNIT_ASSERT(expected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(slot.GetCount(), expected[key].Required);
        expected[key].Required = 0;
    }
    for (auto &slot : rec.GetPendingSlots()) {
        TSlotDescription key(slot);
        UNIT_ASSERT(expected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(slot.GetCount(), expected[key].Pending);
        expected[key].Pending = 0;
    }
    for (auto &slot : rec.GetMissingSlots()) {
        TSlotDescription key(slot);
        UNIT_ASSERT(expected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(slot.GetCount(), expected[key].Missing);
        expected[key].Missing = 0;
    }
    for (auto &slot : rec.GetMisplacedSlots()) {
        TSlotDescription key(slot);
        UNIT_ASSERT(expected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(slot.GetCount(), expected[key].Misplaced);
        expected[key].Misplaced = 0;
    }
    for (auto &slot : rec.GetSplitSlots()) {
        TSlotDescription key(slot);
        UNIT_ASSERT(expected.contains(key));
        UNIT_ASSERT_VALUES_EQUAL(slot.GetCount(), expected[key].Split);
        expected[key].Split = 0;
    }
    for (auto &pr : expected) {
        UNIT_ASSERT_VALUES_EQUAL(pr.second.Required, 0);
        UNIT_ASSERT_VALUES_EQUAL(pr.second.Pending, 0);
        UNIT_ASSERT_VALUES_EQUAL(pr.second.Missing, 0);
        UNIT_ASSERT_VALUES_EQUAL(pr.second.Misplaced, 0);
        UNIT_ASSERT_VALUES_EQUAL(pr.second.Split, 0);
    }
}

bool CompareState(NKikimrTenantSlotBroker::TTenantState &rec, const TString &name,
                  THashMap<TSlotDescription, TSlotCount> expected)
{
    if (name != rec.GetTenantName())
        return false;
    for (auto &slot : rec.GetRequiredSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Required)
            return false;
        expected[key].Required = 0;
    }
    for (auto &slot : rec.GetPendingSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Pending)
            return false;
        expected[key].Pending = 0;
    }
    for (auto &slot : rec.GetMissingSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Missing)
            return false;
        expected[key].Missing = 0;
    }
    for (auto &slot : rec.GetMisplacedSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Misplaced)
            return false;
        expected[key].Misplaced = 0;
    }
    for (auto &slot : rec.GetSplitSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Split)
            return false;
        expected[key].Split = 0;
    }
    for (auto &slot : rec.GetPinnedSlots()) {
        TSlotDescription key(slot);
        if (!expected.contains(key))
            return false;
        if (slot.GetCount() != expected[key].Pinned)
            return false;
        expected[key].Pinned = 0;
    }
    for (auto &pr : expected) {
        if (pr.second.Required || pr.second.Pending || pr.second.Missing
            || pr.second.Misplaced || pr.second.Split || pr.second.Pinned)
            return false;
    }

    return true;
}

void AlterTenant(TTenantTestRuntime &runtime,
                 const TString &name,
                 const TVector<TSlotRequest> &requests)
{
    TAutoPtr<IEventHandle> handle;

    auto *event = new TEvTenantSlotBroker::TEvAlterTenant;
    event->Record.SetTenantName(name);
    for (auto &request : requests) {
        auto &slot = *event->Record.AddRequiredSlots();
        request.Description.Serialize(slot);
        slot.SetCount(request.Count.Required);
    }

    runtime.SendToBroker(event);
    runtime.GrabEdgeEventRethrow<TEvTenantSlotBroker::TEvTenantState>(handle);
}

void CheckAlterTenant(TTenantTestRuntime &runtime,
                      const TString &name,
                      const TVector<TSlotRequest> &requests)
{
    THashMap<TSlotDescription, TSlotCount> expected;
    TAutoPtr<IEventHandle> handle;

    auto *event = new TEvTenantSlotBroker::TEvAlterTenant;
    event->Record.SetTenantName(name);
    for (auto &request : requests) {
        auto &slot = *event->Record.AddRequiredSlots();
        request.Description.Serialize(slot);
        slot.SetCount(request.Count.Required);
        expected[request.Description] += request.Count;
    }

    runtime.SendToBroker(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvTenantSlotBroker::TEvTenantState>(handle);
    CheckState(reply->Record, name, expected);
}

template <typename ...Ts>
void AlterTenant(TTenantTestRuntime &runtime, const TString &name, Ts... args)
{
    TVector<TSlotRequest> requests;
    CollectRequests(requests, args...);
    AlterTenant(runtime, name, requests);
}

template <typename ...Ts>
void CheckAlterTenant(TTenantTestRuntime &runtime, const TString &name, Ts... args)
{
    TVector<TSlotRequest> requests;
    CollectRequests(requests, args...);
    CheckAlterTenant(runtime, name, requests);
}

template <typename ...Ts>
void CheckTenantState(TTenantTestRuntime &runtime,
                      const TString &name,
                      const TVector<TSlotRequest> requests)
{
    THashMap<TSlotDescription, TSlotCount> expected;
    TAutoPtr<IEventHandle> handle;

    for (auto &request : requests)
        expected[request.Description] += request.Count;

    bool ok =false;
    while (!ok) {
        auto *event = new TEvTenantSlotBroker::TEvGetTenantState;
        event->Record.SetTenantName(name);

        runtime.SendToBroker(event);
        auto reply = runtime.GrabEdgeEventRethrow<TEvTenantSlotBroker::TEvTenantState>(handle);
        ok = CompareState(reply->Record, name, expected);
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        if (!ok) {
            NKikimrTenantSlotBroker::TConfig config;
            TDuration delay = TDuration::MicroSeconds(config.GetPendingSlotTimeout()); 
            TDispatchOptions options;
            options.FinalEvents.emplace_back(TTenantSlotBroker::TEvPrivate::EvCheckSlotStatus, 1);
            runtime.DispatchEvents(options, delay);
        }
    }
}

template <typename ...Ts>
void CheckTenantState(TTenantTestRuntime &runtime, const TString &name, Ts... args)
{
    TVector<TSlotRequest> requests;
    CollectRequests(requests, args...);
    CheckTenantState(runtime, name, requests);
}

void CheckTenantsList(TTenantTestRuntime &runtime,
                      const TVector<std::pair<TString, TVector<TSlotRequest>>> &state)
{
    THashMap<TString, THashMap<TSlotDescription, TSlotCount>> expected;
    for (auto &tenant : state) {
        for (auto &slot : tenant.second) {
            expected[tenant.first][slot.Description] += slot.Count;
        }
    }

    auto *event = new TEvTenantSlotBroker::TEvListTenants;
    runtime.SendToBroker(event);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvTenantSlotBroker::TEvTenantsList>(handle);
    for (auto &tenant : reply->Record.GetTenants()) {
        UNIT_ASSERT(expected.contains(tenant.GetTenantName()));
        CheckState(tenant, tenant.GetTenantName(), expected[tenant.GetTenantName()]);
        expected.erase(tenant.GetTenantName());
    }
    UNIT_ASSERT(expected.empty());
}

void RestartTenantSlotBroker(TTenantTestRuntime &runtime)
{
    runtime.Register(CreateTabletKiller(MakeTenantSlotBrokerID(0)));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
    runtime.DispatchEvents(options);
}


} // anonymous namespace

Y_UNIT_TEST_SUITE(TTenantSlotBrokerTests) {
    Y_UNIT_TEST(TestAllocateExactSlots) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0,
                         SLOT2_TYPE, DATA_CENTER2, 2, 2, 0,
                         SLOT3_TYPE, DATA_CENTER3, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 10, 10, 10}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0,
                         SLOT2_TYPE, DATA_CENTER2, 2, 0, 0,
                         SLOT3_TYPE, DATA_CENTER3, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAllocateExactSlotsMissing) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 4, 3, 1,
                         SLOT2_TYPE, DATA_CENTER2, 5, 3, 2,
                         SLOT3_TYPE, DATA_CENTER3, 6, 3, 3);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 4, 0, 1,
                         SLOT2_TYPE, DATA_CENTER2, 5, 0, 2,
                         SLOT3_TYPE, DATA_CENTER3, 6, 0, 3);
    }

    Y_UNIT_TEST(TestAllocateTypedSlots) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 3, 0,
                         SLOT2_TYPE, ANY_DATA_CENTER, 2, 2, 0,
                         SLOT3_TYPE, ANY_DATA_CENTER, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 10, 10, 10}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 0, 0,
                         SLOT2_TYPE, ANY_DATA_CENTER, 2, 0, 0,
                         SLOT3_TYPE, ANY_DATA_CENTER, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAllocateTypedSlotsAll) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 9, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 0, 0);
    }

    Y_UNIT_TEST(TestAllocateTypedSlotsMissing) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 10, 9, 1);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 10, 0, 1);
    }

    Y_UNIT_TEST(TestAllocateLocatedSlots) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 3, 3, 0,
                         ANY_SLOT_TYPE, DATA_CENTER2, 3, 3, 0,
                         ANY_SLOT_TYPE, DATA_CENTER3, 3, 3, 0);
        // Expect small + medium + large from each data center.
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 3, 0, 0,
                         ANY_SLOT_TYPE, DATA_CENTER2, 3, 0, 0,
                         ANY_SLOT_TYPE, DATA_CENTER3, 3, 0, 0);
    }

    Y_UNIT_TEST(TestAllocateLocatedSlotsAll) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 9, 9, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 9, 0, 0);
    }

    Y_UNIT_TEST(TestAllocateLocatedSlotsMissing) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 10, 9, 1);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 10, 0, 1);
    }

    Y_UNIT_TEST(TestAlterMoreStrictAndKeep) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 1, 0, 0,
                         ANY_SLOT_TYPE, DATA_CENTER1, 1, 0, 0,
                         ANY_SLOT_TYPE, ANY_DATA_CENTER, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterLessStrictDataCenterAndKeep) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 0, 0);
        // Expect we got 1 slot from each data center.
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT1_TYPE, DATA_CENTER2, 1, 0, 0,
                         SLOT1_TYPE, DATA_CENTER3, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterLessStrictTypeAndKeep) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 3, 3, 0);
        // Expect we got 1 slot of each type.
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT3_TYPE, DATA_CENTER1, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterLessStrictTypeAndDataCenterAndKeep) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, ANY_DATA_CENTER, 9, 9, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         ANY_SLOT_TYPE, ANY_DATA_CENTER, 9, 0, 0);
        // Expect we got 1 slot of each type in each data center
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT3_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT1_TYPE, DATA_CENTER2, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER2, 1, 0, 0,
                         SLOT3_TYPE, DATA_CENTER2, 1, 0, 0,
                         SLOT1_TYPE, DATA_CENTER3, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER3, 1, 0, 0,
                         SLOT3_TYPE, DATA_CENTER3, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterAndDontKeep) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 1, 0,
                         SLOT1_TYPE, DATA_CENTER2, 1, 1, 0,
                         SLOT2_TYPE, DATA_CENTER2, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT1_TYPE, DATA_CENTER2, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER2, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterIncrease) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 1, 1, 1}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 1, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 4, 4, 4}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterDecrease) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 4, 4, 4}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0);
    }

    Y_UNIT_TEST(TestAlterRemove) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckAlterTenant(runtime, TENANT1_1_NAME);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_2_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 0, 0);
    }

    Y_UNIT_TEST(TestMoveSlotToAnotherTenant) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 2, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 1, 1);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 2, 2, 2},
                                   {TENANT1_2_NAME, 1, 1, 1}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 1);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 2, 2, 2}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 2, 0, 0);
    }

    Y_UNIT_TEST(TestMoveSlotWithFairShare) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 9, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        CheckAlterTenant(runtime, TENANT1_3_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        CheckAlterTenant(runtime, TENANT1_4_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        CheckTenantState(runtime, TENANT1_3_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        CheckTenantState(runtime, TENANT1_4_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 4);
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 0, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 2, 2, 2},
                                   {TENANT1_3_NAME, 2, 2, 2},
                                   {TENANT1_4_NAME, 2, 2, 2}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 2);
        CheckTenantState(runtime, TENANT1_3_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 2);
        CheckTenantState(runtime, TENANT1_4_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 4, 0, 2);
    }

    Y_UNIT_TEST(TestDisconnectAndReconnect) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 9, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 0, 0);

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(1), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(2), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 6, 6, 6},
                                   {TENANT1_1_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 0, 3);

        runtime.CreateTenantPool(0);
        runtime.CreateTenantPool(1);
        runtime.CreateTenantPool(2);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 9, 0, 0);

        // Check counters are correct.
        for (ui32 i = 0; i < runtime.GetNodeCount(); ++i)
        {
            auto tablets = GetServiceCounters(runtime.GetDynamicCounters(i), "tablets");
            auto tsb = tablets->FindSubgroup("type", "TENANT_SLOT_BROKER");

            if (tsb) {
                auto slot = tsb->GetSubgroup("SlotType", SLOT1_TYPE);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("FreeSlots")->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("AssignedSlots")->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("ConnectedSlots")->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("DisconnectedSlots")->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("RequiredSlots")->Val(), 9);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("MissingSlots")->Val(), 0);
                UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", ANY_DATA_CENTER)
                                             ->GetCounter("PendingSlots")->Val(), 0);
                for (auto &dc : TVector<TString>({DATA_CENTER1, DATA_CENTER2, DATA_CENTER3})) {
                    UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", dc)
                                                 ->GetCounter("FreeSlots")->Val(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", dc)
                                                 ->GetCounter("AssignedSlots")->Val(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", dc)
                                                 ->GetCounter("ConnectedSlots")->Val(), 3);
                    UNIT_ASSERT_VALUES_EQUAL(slot->GetSubgroup("SlotDataCenter", dc)
                                                 ->GetCounter("DisconnectedSlots")->Val(), 0);
                }
                break;
            }
        }
    }

    Y_UNIT_TEST(TestAddSlotToPool) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 7, 6, 1);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 7, 0, 1);

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 8, 8, 8},
                                   {TENANT1_1_NAME, 2, 2, 2},
                                   {TENANT1_2_NAME, 6, 6, 6}}});

        runtime.CreateTenantPool(0, DoubleTenantPoolConfig());

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 7, 7, 7}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, ANY_DATA_CENTER, 7, 0, 0);
    }

    Y_UNIT_TEST(TestRemoveSlotFromPool) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 8, 8, 8},
                                   {TENANT1_1_NAME, 2, 2, 2}}});
        runtime.CreateTenantPool(0, ShrinkTenantPoolConfig());

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 2, 2, 2}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 1);
    }

    Y_UNIT_TEST(TestChangeSlotType) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 4, 3, 1);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 4, 0, 1);

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 8, 8, 8},
                                   {TENANT1_1_NAME, 2, 2, 2},
                                   {TENANT1_2_NAME, 4, 4, 4}}});
        runtime.CreateTenantPool(0, ModifiedTypeTenantPoolConfig());

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 2, 2, 2},
                                   {TENANT1_2_NAME, 8, 8, 8}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 1);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 4, 0, 0);
    }

    Y_UNIT_TEST(TestChangeSlotOwner) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 6, 6, 6},
                                   {TENANT1_2_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 8, 8, 8},
                                   {TENANT1_1_NAME, 4, 4, 4},
                                   {TENANT1_2_NAME, 2, 2, 2}}});
        runtime.CreateTenantPool(0, AssignedTenantPoolConfig());

        TAutoPtr<IEventHandle> captured = nullptr;
        auto captureRegister = [&captured](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvTenantSlotBroker::EvRegisterPool
                && !captured) {
                captured = event;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };

        // Static assignemt with mixed slots should take place first.
        runtime.SetObserverFunc(captureRegister);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 5, 5, 5},
                                   {TENANT1_2_NAME, 4, 4, 4},
                                   {TENANT1_3_NAME, 3, 3, 3}}});
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
        if (captured)
            runtime.Send(captured.Release());
        // But should be fixed later by broker.
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 6, 6, 6},
                                   {TENANT1_2_NAME, 3, 3, 3}}});

        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
    }

    Y_UNIT_TEST(TestRestartTablet) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER2, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_3_NAME,
                         SLOT3_TYPE, DATA_CENTER3, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_4_NAME,
                         SLOT3_TYPE, DATA_CENTER1, 3, 3, 0,
                         SLOT1_TYPE, DATA_CENTER2, 3, 3, 0,
                         SLOT2_TYPE, DATA_CENTER3, 3, 3, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6},
                                   {TENANT1_3_NAME, 9, 9, 9},
                                   {TENANT1_4_NAME, 18, 18, 18}}});

        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(1), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(2), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 6, 6, 6},
                                   {TENANT1_1_NAME, 2, 2, 2},
                                   {TENANT1_2_NAME, 4, 4, 4},
                                   {TENANT1_3_NAME, 6, 6, 6},
                                   {TENANT1_4_NAME, 12, 12, 12}}});

        runtime.Register(CreateTabletKiller(MakeTenantSlotBrokerID(0)));
        runtime.CreateTenantPool(0);
        runtime.CreateTenantPool(1);
        runtime.CreateTenantPool(2);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6},
                                   {TENANT1_3_NAME, 9, 9, 9},
                                   {TENANT1_4_NAME, 18, 18, 18}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER2, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_3_NAME,
                         SLOT3_TYPE, DATA_CENTER3, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_4_NAME,
                         SLOT3_TYPE, DATA_CENTER1, 3, 0, 0,
                         SLOT1_TYPE, DATA_CENTER2, 3, 0, 0,
                         SLOT2_TYPE, DATA_CENTER3, 3, 0, 0);
    }

    Y_UNIT_TEST(TestListTenants) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER2, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_3_NAME,
                         SLOT3_TYPE, DATA_CENTER3, 3, 3, 0);
        CheckAlterTenant(runtime, TENANT1_4_NAME,
                         SLOT3_TYPE, DATA_CENTER1, 3, 3, 0,
                         SLOT1_TYPE, DATA_CENTER2, 3, 3, 0,
                         SLOT2_TYPE, DATA_CENTER3, 3, 3, 0);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3},
                                   {TENANT1_2_NAME, 6, 6, 6},
                                   {TENANT1_3_NAME, 9, 9, 9},
                                   {TENANT1_4_NAME, 18, 18, 18}}});

        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER2, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_3_NAME,
                         SLOT3_TYPE, DATA_CENTER3, 3, 0, 0);
        CheckTenantState(runtime, TENANT1_4_NAME,
                         SLOT3_TYPE, DATA_CENTER1, 3, 0, 0,
                         SLOT1_TYPE, DATA_CENTER2, 3, 0, 0,
                         SLOT2_TYPE, DATA_CENTER3, 3, 0, 0);

        CheckTenantsList(runtime,
                         {{ {TENANT1_1_NAME, {{ {SLOT1_TYPE, DATA_CENTER1, 3, 0, 0} }}},
                            {TENANT1_2_NAME, {{ {SLOT2_TYPE, DATA_CENTER2, 3, 0, 0} }}},
                            {TENANT1_3_NAME, {{ {SLOT3_TYPE, DATA_CENTER3, 3, 0, 0} }}},
                            {TENANT1_4_NAME, {{ {SLOT3_TYPE, DATA_CENTER1, 3, 0, 0},
                                                {SLOT1_TYPE, DATA_CENTER2, 3, 0, 0},
                                                {SLOT2_TYPE, DATA_CENTER3, 3, 0, 0} }}} }});
    }

    Y_UNIT_TEST(TestRandom) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());

        constexpr int NITERS = 10;

        TVector<TString> type = {SLOT1_TYPE, SLOT2_TYPE, SLOT3_TYPE};
        TVector<ui64> size = {1, 2, 3};
        TVector<TString> dc = {DATA_CENTER1, DATA_CENTER2, DATA_CENTER3};
        TVector<TString> tenants = {TENANT1_1_NAME, TENANT1_2_NAME, TENANT1_3_NAME, TENANT1_4_NAME};
        for (int i = 0; i < NITERS; ++i) {
            TVector<TVector<ui64>> freeSlots = { {{3, 3, 3}}, {{3, 3, 3}}, {{3, 3, 3}} };
            TVector<TVector<TVector<ui64>>> counts = {
                {{ {{0, 0, 0}}, {{0, 0, 0}}, {{0, 0, 0}} }},
                {{ {{0, 0, 0}}, {{0, 0, 0}}, {{0, 0, 0}} }},
                {{ {{0, 0, 0}}, {{0, 0, 0}}, {{0, 0, 0}} }},
                {{ {{0, 0, 0}}, {{0, 0, 0}}, {{0, 0, 0}} }} };
            TVector<ui64> res = { 0, 0, 0, 0 };
            TSet<ui32> poolsToRestart;

            for (int u = 0; u < 4; ++u) {
                for (int t = 0; t < 3; ++t)
                    for (int d = 0; d < 3; ++d) {
                        if (u < 3)
                            counts[u][t][d] = RandomNumber<ui64>(freeSlots[t][d] + 1);
                        else
                            counts[u][t][d] = freeSlots[t][d];;
                        freeSlots[t][d] -= counts[u][t][d];
                        res[u] += counts[u][t][d] * size[t];
                    }
                AlterTenant(runtime, tenants[u],
                            type[0], dc[0], counts[u][0][0], 0, 0,
                            type[0], dc[1], counts[u][0][1], 0, 0,
                            type[0], dc[2], counts[u][0][2], 0, 0,
                            type[1], dc[0], counts[u][1][0], 0, 0,
                            type[1], dc[1], counts[u][1][1], 0, 0,
                            type[1], dc[2], counts[u][1][2], 0, 0,
                            type[2], dc[0], counts[u][2][0], 0, 0,
                            type[2], dc[1], counts[u][2][1], 0, 0,
                            type[2], dc[2], counts[u][2][2], 0, 0);

                bool restartBroker = RandomNumber<ui64>(2) > 0;
                bool restartNode = RandomNumber<ui64>(2) > 0;
                if (restartBroker)
                    runtime.Register(CreateTabletKiller(MakeTenantSlotBrokerID(0)));

                if (restartNode && false) {
                    ui32 nodeId = RandomNumber<ui32>(runtime.GetNodeCount());
                    if (!poolsToRestart.contains(nodeId)) {
                        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(nodeId), 0),
                                                      runtime.Sender,
                                                      new TEvents::TEvPoisonPill));
                        poolsToRestart.insert(nodeId);
                    }
                }

                if (restartBroker) {
                    TDispatchOptions options;
                    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
                    runtime.DispatchEvents(options);
                }
            }

            bool restartBroker = RandomNumber<ui64>(2) > 0;
            if (restartBroker)
                RestartTenantSlotBroker(runtime);

            for (auto nodeId : poolsToRestart)
                runtime.CreateTenantPool(nodeId);
            poolsToRestart.clear();

            runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                       {tenants[0], res[0], res[0], res[0]},
                                       {tenants[1], res[1], res[1], res[1]},
                                       {tenants[2], res[2], res[2], res[2]},
                                       {tenants[3], res[3], res[3], res[3]}}});

            for (int u = 0; u < 4; ++u)
                CheckTenantState(runtime, tenants[u],
                                 type[0], dc[0], counts[u][0][0], 0, 0,
                                 type[0], dc[1], counts[u][0][1], 0, 0,
                                 type[0], dc[2], counts[u][0][2], 0, 0,
                                 type[1], dc[0], counts[u][1][0], 0, 0,
                                 type[1], dc[1], counts[u][1][1], 0, 0,
                                 type[1], dc[2], counts[u][1][2], 0, 0,
                                 type[2], dc[0], counts[u][2][0], 0, 0,
                                 type[2], dc[1], counts[u][2][1], 0, 0,
                                 type[2], dc[2], counts[u][2][2], 0, 0);
        }
    }

    Y_UNIT_TEST(TestSlotLabels) {
        TTenantTestRuntime runtime(TenantTestConfigSingleSlot());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 1, 1, 0);
        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 1, 1, 0);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 2, 2, 2}}});

        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT1,
                                               MakeSlotStatus(DOMAIN1_SLOT1, SLOT1_TYPE, TENANT1_1_NAME, 1, 1, 1, "slot-1"))}}, 0);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT2,
                                               MakeSlotStatus(DOMAIN1_SLOT2, SLOT2_TYPE, TENANT1_2_NAME, 2, 2, 2, "slot-1"))}}, 1);

        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT2_TYPE, DATA_CENTER1, 1, 0, 0,
                         SLOT3_TYPE, DATA_CENTER1, 1, 1, 0);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 5, 5, 5}}});

        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT3,
                                               MakeSlotStatus(DOMAIN1_SLOT3, SLOT3_TYPE, TENANT1_2_NAME, 3, 3, 3, "slot-2"))}}, 2);

        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         SLOT3_TYPE, DATA_CENTER1, 1, 0, 0);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 3, 3, 3}}});

        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT1,
                                               MakeSlotStatus(DOMAIN1_SLOT1, SLOT1_TYPE, TENANT1_1_NAME, 1, 1, 1, "slot-1"))}}, 0);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT2,
                                               MakeSlotStatus(DOMAIN1_SLOT2, SLOT2_TYPE, "", 2, 2, 2, ""))}}, 1);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT3,
                                               MakeSlotStatus(DOMAIN1_SLOT3, SLOT3_TYPE, TENANT1_2_NAME, 3, 3, 3, "slot-2"))}}, 2);

        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         ANY_SLOT_TYPE, ANY_DATA_CENTER, 1, 0, 0);

        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT1,
                                               MakeSlotStatus(DOMAIN1_SLOT1, SLOT1_TYPE, TENANT1_1_NAME, 1, 1, 1, "slot-1"))}}, 0);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT2,
                                               MakeSlotStatus(DOMAIN1_SLOT2, SLOT2_TYPE, "", 2, 2, 2, ""))}}, 1);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT3,
                                               MakeSlotStatus(DOMAIN1_SLOT3, SLOT3_TYPE, TENANT1_2_NAME, 3, 3, 3, "slot-2"))}}, 2);

        CheckAlterTenant(runtime, TENANT1_2_NAME,
                         ANY_SLOT_TYPE, ANY_DATA_CENTER, 1, 0, 0,
                         SLOT2_TYPE, DATA_CENTER1, 1, 1, 0);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 5, 5, 5}}});

        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT1,
                                               MakeSlotStatus(DOMAIN1_SLOT1, SLOT1_TYPE, TENANT1_1_NAME, 1, 1, 1, "slot-1"))}}, 0);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT2,
                                               MakeSlotStatus(DOMAIN1_SLOT2, SLOT2_TYPE, TENANT1_2_NAME, 2, 2, 2, "slot-1"))}}, 1);
        CheckTenantPoolStatus(runtime, 0,
                              {{std::make_pair(DOMAIN1_SLOT3,
                                               MakeSlotStatus(DOMAIN1_SLOT3, SLOT3_TYPE, TENANT1_2_NAME, 3, 3, 3, "slot-2"))}}, 2);
    }

    Y_UNIT_TEST(TestConfigSubscription) {
        TTenantTestRuntime runtime(TenantTestConfigWithConsole());

        // Add config for Tenant Slot Broker and wait for notification.
        auto *event = new TEvConsole::TEvConfigureRequest;
        event->Record.AddActions()->MutableAddConfigItem()->MutableConfigItem()
            ->MutableConfig()->MutableTenantSlotBrokerConfig()->SetPendingSlotTimeout(12345);
        runtime.SendToConsole(event);

        struct IsConfigNotificationProcessed {
            bool operator()(IEventHandle& ev)
            {
                if (ev.GetTypeRewrite() == NConsole::TEvConsole::EvConfigNotificationRequest) {
                    auto &rec = ev.Get<NConsole::TEvConsole::TEvConfigNotificationRequest>()->Record;
                    if (rec.GetConfig().GetTenantSlotBrokerConfig().GetPendingSlotTimeout() == 12345) {
                        SubscriptionId = rec.GetSubscriptionId();
                        ConfigId.Load(rec.GetConfigId());
                    }
                } else if (ev.GetTypeRewrite() == NConsole::TEvConsole::EvConfigNotificationResponse) {
                    auto &rec = ev.Get<NConsole::TEvConsole::TEvConfigNotificationResponse>()->Record;
                    if (SubscriptionId == rec.GetSubscriptionId()
                        && ConfigId == NConsole::TConfigId(rec.GetConfigId()))
                        return true;
                }

                return false;
            }

            ui64 SubscriptionId = 0;
            NConsole::TConfigId ConfigId;
        };

        TDispatchOptions options;
        options.FinalEvents.emplace_back(IsConfigNotificationProcessed(), 1);
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(TestPreferredDC) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 9, 9, 0, 6, 0}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 9, 0, 0, 6, 0}});
    }

    Y_UNIT_TEST(TestPreferredDCDetachMisplaced) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 9, 9, 0, 6, 0}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 9, 9, 9}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 9, 0, 0, 6, 0}});
        // Detach 6 slots and expect only misplaced slots to be detached.
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 3, 0, 0, 0, 0}});
    }

    Y_UNIT_TEST(TestPreferredDCMoveMisplaced) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 3, 3, 0, 0, 0}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 3, 0, 0, 0, 0}});

        // Not enough slots in preferred DC, should get one misplaced.
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 3, 0, 0, 1, 0}});

        // Slot in preferred DC is back and should be used.
        runtime.CreateTenantPool(0);
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 0, false, 3, 0, 0, 0, 0}});
    }

    Y_UNIT_TEST(TestGroupForced) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 3, 1, 0, 0}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 3, 3, 3}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0}});

        // Collocation is more important and we expect slots to be moved to another DC.
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 3, 0}});

        // Slot in preferred DC is back and slots should be moved again.
        runtime.CreateTenantPool(0);
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0}});
    }

    Y_UNIT_TEST(TestGroupRelaxed) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, false, 4, 4, 0, 1, 1}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 4, 4, 4}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 1, 1}});

        // Collocation is more important and we expect slots to be moved to another DC.
        // Split slot should be placed into preferred data center.
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 3, 1}});

        // Slot in preferred DC is back and slots should be moved again.
        runtime.CreateTenantPool(0);
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 1, 1}});
    }

    Y_UNIT_TEST(TestGroupMultipleAllocations) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 3, 1, 0, 0},
                          {SLOT2_TYPE, DATA_CENTER1, false, 1, true, 4, 3, 1, 0, 0},
                          {SLOT3_TYPE, DATA_CENTER1, false, 1, false, 4, 4, 0, 1, 1}});
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 21, 21, 21}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0},
                          {SLOT2_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0},
                          {SLOT3_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 1, 1}});

        // Collocation is more important and we expect slots to be moved to another DC.
        // Split slot should be placed into preferred data center.
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 3, 0},
                          {SLOT2_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 3, 0},
                          {SLOT3_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 3, 1}});

        // Slot in preferred DC is back and slots should be moved again.
        runtime.CreateTenantPool(0);
        CheckTenantState(runtime, TENANT1_1_NAME,
                         {{SLOT1_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0},
                          {SLOT2_TYPE, DATA_CENTER1, false, 1, true, 4, 0, 1, 0, 0},
                          {SLOT3_TYPE, DATA_CENTER1, false, 1, false, 4, 0, 0, 1, 1}});
    }

    Y_UNIT_TEST(TestSlotStats) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());
        CheckAlterTenant(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 3, 0,
                         SLOT2_TYPE, DATA_CENTER2, 2, 2, 0,
                         SLOT3_TYPE, DATA_CENTER3, 1, 1, 0);
        runtime.WaitForHiveState({{{DOMAIN1_NAME, 9, 9, 9},
                                   {TENANT1_1_NAME, 10, 10, 10}}});
        CheckTenantState(runtime, TENANT1_1_NAME,
                         SLOT1_TYPE, DATA_CENTER1, 3, 0, 0,
                         SLOT2_TYPE, DATA_CENTER2, 2, 0, 0,
                         SLOT3_TYPE, DATA_CENTER3, 1, 0, 0);

        TAutoPtr<IEventHandle> handle;
        runtime.SendToBroker(new TEvTenantSlotBroker::TEvGetSlotStats);
        auto reply = runtime.GrabEdgeEventRethrow<TEvTenantSlotBroker::TEvSlotStats>(handle);
        // <type, dc> -> <connected, free>
        THashMap<std::pair<TString, TString>, std::pair<ui64, ui64>> slots;
        for (auto &rec : reply->Record.GetSlotCounters())
            slots[std::make_pair(rec.GetType(), rec.GetDataCenter())]
                = std::make_pair(rec.GetConnected(), rec.GetFree());
        UNIT_ASSERT_VALUES_EQUAL(slots.size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER1)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER1)].second, 0);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER2)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER2)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER3)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT1_TYPE, DATA_CENTER3)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER1)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER1)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER2)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER2)].second, 1);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER3)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT2_TYPE, DATA_CENTER3)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER1)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER1)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER2)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER2)].second, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER3)].first, 3);
        UNIT_ASSERT_VALUES_EQUAL(slots[std::make_pair(SLOT3_TYPE, DATA_CENTER3)].second, 2);
    }

    Y_UNIT_TEST(TestRandomActions) {
        TTenantTestRuntime runtime(TenantTestConfig3DC());

        enum EAction {
            ALTER,
            CLEAR,
            RESTART,
            POOL_DOWN,
            POOL_UP
        };

        TVector<EAction> actions = { ALTER, ALTER, ALTER, CLEAR };
        TSet<std::pair<ui32, TString>> banned;
        TSet<std::pair<ui32, TString>> pinned;
        TSet<ui32> down;
        TVector<TString> slots = {SLOT1, SLOT2, SLOT3};
        TVector<TString> tenants = {TENANT1_1_NAME, TENANT1_2_NAME, TENANT1_3_NAME};
        TVector<TString> types = {SLOT1_TYPE, SLOT2_TYPE, SLOT3_TYPE, ANY_SLOT_TYPE};
        TVector<TString> dcs = {DATA_CENTER1, DATA_CENTER2, DATA_CENTER3, ANY_DATA_CENTER};

        for (int i = 0; i < 1000; ++i) {
            EAction action = actions[RandomNumber<ui64>(actions.size())];

            switch (action) {
            case ALTER:
                {
                    auto tenant = tenants[RandomNumber<ui64>(tenants.size())];
                    auto type = types[RandomNumber<ui64>(types.size())];
                    auto dc = dcs[RandomNumber<ui64>(dcs.size())];
                    AlterTenant(runtime, tenant, type, dc,
                                RandomNumber<ui64>(5), RandomNumber<ui64>(5), RandomNumber<ui64>(5));
                }
                break;
            case CLEAR:
                {
                    auto tenant = tenants[RandomNumber<ui64>(tenants.size())];
                    AlterTenant(runtime, tenant);
                }
                break;
            case RESTART:
                RestartTenantSlotBroker(runtime);
                break;
            case POOL_DOWN:
                {
                    auto node = RandomNumber<ui32>(runtime.GetNodeCount());
                    runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(node), 0),
                                                  runtime.Sender,
                                                  new TEvents::TEvPoisonPill));
                    down.insert(node);
                }
                break;
            case POOL_UP:
                if (!down.empty()) {
                    auto it = down.begin();
                    std::advance(it, RandomNumber<ui64>(down.size()));
                    runtime.CreateTenantPool(*it);
                    down.erase(it);
                }
                break;
            default:
                Y_FAIL("unknown action");
            }
        }
    }
}

} // namespace NKikimr
