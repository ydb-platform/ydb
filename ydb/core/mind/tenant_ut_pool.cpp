#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/testlib/tenant_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

struct TAction {
    TString SlotId;
    TString TenantName;
    NKikimrTenantPool::EStatus Status;
    ui64 Cookie;
    TString Label;
};

class TActionChecker : public TActorBootstrapped<TActionChecker> {
    TActorId EdgeActor;
    ui32 NodeId;
    ui32 Domain;
    TVector<TAction> Actions;
    THashMap<TString, TVector<TAction>> Answers;
    bool Finished;

public:
    TActionChecker(TActorId edge, ui32 nodeId, ui32 domain, TVector<TAction> actions)
        : EdgeActor(edge)
        , NodeId(nodeId)
        , Domain(domain)
        , Actions(std::move(actions))
        , Finished(false)
    {
    }

    void Bootstrap(const TActorContext &ctx)
    {
        ctx.Send(MakeTenantPoolID(NodeId, Domain), new TEvTenantPool::TEvTakeOwnership);
        for (auto &action : Actions) {
            auto *event = new TEvTenantPool::TEvConfigureSlot;
            event->Record.SetSlotId(action.SlotId);
            event->Record.SetAssignedTenant(action.TenantName);
            event->Record.SetLabel(action.Label);
            ctx.Send(MakeTenantPoolID(NodeId, Domain), event, 0, action.Cookie);

            Answers[action.SlotId].push_back(action);
        }
        Become(&TThis::StateWork);
    }

    void Handle(TEvTenantPool::TEvConfigureSlotResult::TPtr &ev, const TActorContext &ctx)
    {
        auto &rec = ev->Get()->Record;
        auto &id = rec.GetSlotStatus().GetId();
        UNIT_ASSERT(Answers.contains(id));
        auto &answer = Answers[id].front();
        UNIT_ASSERT_VALUES_EQUAL((int)rec.GetStatus(), (int)answer.Status);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, answer.Cookie);
        if (answer.Status == NKikimrTenantPool::SUCCESS) {
            UNIT_ASSERT_VALUES_EQUAL(rec.GetSlotStatus().GetAssignedTenant(), answer.TenantName);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetSlotStatus().GetLabel(), answer.Label);
        }
        Answers[id].erase(Answers[id].begin());
        if (Answers[id].empty())
            Answers.erase(id);

        if (Answers.empty()) {
            ctx.Send(EdgeActor, new TEvTest::TEvActionSuccess);
            Finished = true;
        }
    }

    void LostOwnership()
    {
        UNIT_ASSERT(Finished);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTenantPool::TEvConfigureSlotResult, Handle);
            IgnoreFunc(TEvTenantPool::TEvTenantPoolStatus);
            cFunc(TEvTenantPool::EvLostOwnership, LostOwnership);

        default:
            Y_FAIL("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->HasEvent() ? ev->GetBase()->ToString().data() : "serialized?");
            break;
        }
    }
};

void CollectActions(TVector<TAction> &actions, ui64 cookie, const TString &slotId,
                    const TString &tenantName, NKikimrTenantPool::EStatus status)
{
    actions.push_back({slotId, tenantName, status, cookie, ""});
}

void CollectActions(TVector<TAction> &actions, ui64 cookie, const TString &slotId,
                    const TString &tenantName, const TString &label,
                    NKikimrTenantPool::EStatus status)
{
    actions.push_back({slotId, tenantName, status, cookie, label});
}

template<typename ...Ts>
void CollectActions(TVector<TAction> &actions, ui64 cookie, const TString &slotId,
                    const TString &tenantName, NKikimrTenantPool::EStatus status,
                    Ts ...args)
{
    CollectActions(actions, cookie, slotId, tenantName, status);
    CollectActions(actions, cookie + 1, args...);
}

template<typename ...Ts>
void CollectActions(TVector<TAction> &actions, ui64 cookie, const TString &slotId,
                    const TString &tenantName, const TString &label,
                    NKikimrTenantPool::EStatus status, Ts ...args)
{
    CollectActions(actions, cookie, slotId, tenantName, status, label);
    CollectActions(actions, cookie + 1, args...);
}

template<typename ...Ts>
void CheckConfigureSlot(TTenantTestRuntime &runtime, ui32 domain,
                        Ts ...args)
{
    TVector<TAction> actions;
    CollectActions(actions, 1, args...);

    auto *actor = new TActionChecker(runtime.Sender, runtime.GetNodeId(0), domain, actions);
    runtime.Register(actor);

    TAutoPtr<IEventHandle> handle;
    runtime.GrabEdgeEventRethrow<TEvTest::TEvActionSuccess>(handle);
}

void CheckTenantPoolStatus(TTenantTestRuntime &runtime,
                           const TString &d1s1 = "", const TString &d1s2 = "", const TString &d1s3 = "")
{
    THashMap<TString, NKikimrTenantPool::TSlotStatus> domain1;
    domain1[DOMAIN1_SLOT1] = MakeSlotStatus(DOMAIN1_SLOT1, SLOT1_TYPE, d1s1, 1, 1, 1);
    domain1[DOMAIN1_SLOT2] = MakeSlotStatus(DOMAIN1_SLOT2, SLOT2_TYPE, d1s2, 2, 2, 2);
    domain1[DOMAIN1_SLOT3] = MakeSlotStatus(DOMAIN1_SLOT3, SLOT3_TYPE, d1s3, 3, 3, 3);
    CheckTenantPoolStatus(runtime, 0, domain1);
}

void CheckLabels(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                 const TString &database,
                 const TString &slot,
                 THashMap<TString, TString> attrs = {},
                 const NKikimrConfig::TMonitoringConfig &config = {})
{
    THashMap<TString, TString> dbLabels;
    THashSet<TString> dbServices;
    THashMap<TString, TString> attrLabels;
    THashSet<TString> attrServices;
    THashSet<TString> allServices;

    if (database)
        dbLabels[DATABASE_LABEL] = database;
    if (slot)
        dbLabels[SLOT_LABEL] = "static";

    if (slot.StartsWith("slot-"))
        dbLabels["host"] = slot;

    for (auto &attr : GetDatabaseAttributeLabels()) {
        if (attrs.contains(attr))
            attrLabels.insert(*attrs.find(attr));
    }

    for (auto &service : config.GetDatabaseLabels().GetServices())
        dbServices.insert(service);
    if (dbServices.empty())
        dbServices = GetDatabaseSensorServices();

    for (auto &group : config.GetDatabaseAttributeLabels().GetAttributeGroups()) {
        for (auto &service : group.GetServices())
            attrServices.insert(service);
    }
    if (attrServices.empty())
        attrServices = GetDatabaseAttributeSensorServices();

    allServices = dbServices;
    allServices.insert(attrServices.begin(), attrServices.end());

    for (auto &service : allServices) {
        THashSet<TString> allLabels = GetDatabaseAttributeLabels();
        allLabels.insert(DATABASE_LABEL);
        allLabels.insert(SLOT_LABEL);

        THashMap<TString, TString> labels;
        if (dbServices.contains(service)) {
            labels.insert(dbLabels.begin(), dbLabels.end());
            if (attrServices.contains(service)) {
                labels.erase(SLOT_LABEL); // no slot now
                allLabels.erase(SLOT_LABEL);
            }
        }
        if (attrServices.contains(service))
            labels.insert(attrLabels.begin(), attrLabels.end());
        auto serviceGroup = GetServiceCounters(counters, service, false);
        while (!labels.empty()) {
            TString name;
            TString value;
            serviceGroup->EnumerateSubgroups([&labels,&name,&value](const TString &n, const TString &v) {
                    UNIT_ASSERT(labels.contains(n));
                    UNIT_ASSERT_VALUES_EQUAL(v, labels[n]);
                    name = n;
                    value = v;
                });
            UNIT_ASSERT(name);
            UNIT_ASSERT(value);
            labels.erase(name);
            serviceGroup = serviceGroup->FindSubgroup(name, value);
        }
        serviceGroup->EnumerateSubgroups([&allLabels](const TString &name, const TString &) {
                UNIT_ASSERT(!allLabels.contains(name));
            });
    }
}

void ChangeMonitoringConfig(TTenantTestRuntime &runtime,
                            const NKikimrConfig::TMonitoringConfig &monCfg,
                            bool waitForPoolStatus = false)
{
    auto *event = new NConsole::TEvConsole::TEvConfigureRequest;
    event->Record.AddActions()->MutableRemoveConfigItems()
        ->MutableCookieFilter()->AddCookies("mon-tmp");
    auto &item = *event->Record.AddActions()
        ->MutableAddConfigItem()->MutableConfigItem();
    item.MutableConfig()->MutableMonitoringConfig()->CopyFrom(monCfg);
    item.SetCookie("mon-tmp");

    runtime.SendToConsole(event);

    struct TIsConfigNotificationProcessed {
        TIsConfigNotificationProcessed(ui32 count,
                                       ui32 waitForPoolStatus)
            : WaitForPoolStatus(waitForPoolStatus)
            , WaitForConfigNotification(count)
        {
        }

        bool operator()(IEventHandle& ev)
        {
            if (ev.GetTypeRewrite() == NConsole::TEvConsole::EvConfigNotificationResponse
                && WaitForConfigNotification) {
                auto &rec = ev.Get<NConsole::TEvConsole::TEvConfigNotificationResponse>()->Record;
                if (rec.GetConfigId().ItemIdsSize() != 1 || rec.GetConfigId().GetItemIds(0).GetId())
                    --WaitForConfigNotification;
            } else if (ev.GetTypeRewrite() == TEvTenantPool::EvTenantPoolStatus
                       && WaitForPoolStatus) {
                --WaitForPoolStatus;
            }

            return !WaitForPoolStatus && !WaitForConfigNotification;
        }

        ui32 WaitForPoolStatus;
        ui32 WaitForConfigNotification;
    };

    TDispatchOptions options;
    options.FinalEvents.emplace_back
        (TIsConfigNotificationProcessed(3 * runtime.GetNodeCount(),
                                        2 * waitForPoolStatus * runtime.GetNodeCount()));
    runtime.DispatchEvents(options);
}

} // anonymous namesapce

Y_UNIT_TEST_SUITE(TTenantPoolTests) {
    Y_UNIT_TEST(TestAssignTenantSimple) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_2_NAME, NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 2, 2, 2}}});

        CheckTenantPoolStatus(runtime, TENANT1_1_NAME, TENANT1_2_NAME, "");
    }

    Y_UNIT_TEST(TestAssignTenantStatic) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, DOMAIN1_NAME, NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 2, 2, 2}}});

        CheckTenantPoolStatus(runtime,
                              DOMAIN1_NAME, "", "");
    }

    Y_UNIT_TEST(TestAssignTenantMultiple) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 6, 6, 6}}});

        CheckTenantPoolStatus(runtime,
                              TENANT1_1_NAME, TENANT1_1_NAME, TENANT1_1_NAME);
    }

    Y_UNIT_TEST(TestAssignTenantWithReassigns) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_2_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, DOMAIN1_NAME, NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 4, 4, 4},
                                   {TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 2, 2, 2}}});

        CheckTenantPoolStatus(runtime,
                              TENANT1_1_NAME, TENANT1_2_NAME, DOMAIN1_NAME);
    }

    Y_UNIT_TEST(TestAssignTenantWithDetach) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, TENANT1_2_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, DOMAIN1_NAME, NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT2, "", NKikimrTenantPool::SUCCESS);
        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT3, "", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 1, 1, 1}}});

        CheckTenantPoolStatus(runtime,
                              TENANT1_1_NAME, "", "");
    }

    Y_UNIT_TEST(TestAssignTenantUnknownTenant) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_U_NAME, NKikimrTenantPool::UNKNOWN_TENANT);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1}}});

        CheckTenantPoolStatus(runtime);
    }

    Y_UNIT_TEST(TestAssignTenantPostponed) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT2, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT3, TENANT1_1_NAME, NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT2, TENANT1_2_NAME, NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT3, DOMAIN1_NAME, NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT2, "", NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT3, "", NKikimrTenantPool::SUCCESS,
                           DOMAIN1_SLOT2, TENANT1_U_NAME, NKikimrTenantPool::UNKNOWN_TENANT,
                           DOMAIN1_SLOT2, TENANT1_2_NAME, NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 2, 2, 2}}});

        CheckTenantPoolStatus(runtime,
                              TENANT1_1_NAME, TENANT1_2_NAME, "");
    }

    Y_UNIT_TEST(TestOwnership) {
        TTenantTestRuntime runtime(DefaultTenantTestConfig);
        TAutoPtr<IEventHandle> handle;

        TActorId sender2 = runtime.AllocateEdgeActor();

        // sender cannot configure slot because he is not owner
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvTenantPool::TEvConfigureSlot(DOMAIN1_SLOT1, TENANT1_1_NAME, "")));
        auto reply1 = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvConfigureSlotResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL((int)reply1->Record.GetStatus(), (int)NKikimrTenantPool::NOT_OWNER);

        // sender takes ownership
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvTenantPool::TEvTakeOwnership));
        runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvTenantPoolStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(runtime.Sender, handle->Recipient);

        // now sender successfully configures slot
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvTenantPool::TEvConfigureSlot(DOMAIN1_SLOT2, TENANT1_2_NAME, "")));
        auto reply2 = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvConfigureSlotResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL((int)reply2->Record.GetStatus(), (int)NKikimrTenantPool::SUCCESS);

        // sender2 takes ownership and sender 1 is informed
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      sender2,
                                      new TEvTenantPool::TEvTakeOwnership));
        runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvLostOwnership>(handle);
        UNIT_ASSERT_VALUES_EQUAL(runtime.Sender, handle->Recipient);
        runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvTenantPoolStatus>(handle);
        UNIT_ASSERT_VALUES_EQUAL(sender2, handle->Recipient);

        // sender now cannot configure slot again
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      runtime.Sender,
                                      new TEvTenantPool::TEvConfigureSlot(DOMAIN1_SLOT2, TENANT1_1_NAME, "")));
        auto reply3 = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvConfigureSlotResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL((int)reply3->Record.GetStatus(), (int)NKikimrTenantPool::NOT_OWNER);

        // sender2 successfully configures slot
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0), 0),
                                      sender2,
                                      new TEvTenantPool::TEvConfigureSlot(DOMAIN1_SLOT1, TENANT1_2_NAME, "")));
        auto reply4 = runtime.GrabEdgeEventRethrow<TEvTenantPool::TEvConfigureSlotResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL((int)reply4->Record.GetStatus(), (int)NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1},
                                   {TENANT1_2_NAME, 3, 3, 3}}});

        CheckTenantPoolStatus(runtime,
                              TENANT1_2_NAME, TENANT1_2_NAME, "");
    }

    Y_UNIT_TEST(TestSensorLabels) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            true,
            // CreateConsole
            false,
            // Nodes
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
        TTenantTestRuntime runtime(config);
        auto counters = runtime.GetDynamicCounters();
        auto &services = GetDatabaseSensorServices();
        for (auto &service : services) {
            auto group = GetServiceCounters(counters, service);
            auto counter = group->GetCounter("counter", true);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, "slot-1", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1}}});

        auto &attrServices = GetDatabaseAttributeSensorServices();
        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto tenantGroup = serviceGroup->FindSubgroup(DATABASE_LABEL, TENANT1_1_NAME);
            UNIT_ASSERT(tenantGroup);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> slotGroup;
            if (attrServices.contains(service)) {
                slotGroup = tenantGroup->FindSubgroup(HOST_LABEL, "slot-1");
            } else {
                slotGroup = tenantGroup->FindSubgroup(SLOT_LABEL, "static")->FindSubgroup(HOST_LABEL, "slot-1");
            }
            UNIT_ASSERT(slotGroup);
            auto counter = slotGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, DOMAIN1_NAME, "slot-2", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1}}});

        CheckLabels(counters, "", "");
        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto counter = serviceGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, "", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({});

        CheckLabels(counters, "", "");
        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto counter = serviceGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
        }

    }

    Y_UNIT_TEST(TestForcedSensorLabels) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            true,
            // CreateConsole
            false,
            // Nodes
            {{
                    // Node0
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            "",
                        }
                    }
            }},
            // DataCenterCount
            1
        };
        NKikimrConfig::TAppConfig ext;
        ext.MutableMonitoringConfig()->SetForceDatabaseLabels(true);
        TTenantTestRuntime runtime(config, ext);
        auto counters = runtime.GetDynamicCounters();
        auto &services = GetDatabaseSensorServices();
        for (auto &service : services) {
            auto group = GetServiceCounters(counters, service);
            auto counter = group->GetCounter("counter", true);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, TENANT1_1_NAME, "slot-1", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1}}});

        auto &attrServices = GetDatabaseAttributeSensorServices();

        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto tenantGroup = serviceGroup->FindSubgroup(DATABASE_LABEL, TENANT1_1_NAME);
            UNIT_ASSERT(tenantGroup);
            ::NMonitoring::TDynamicCounterPtr slotGroup;
            if (attrServices.contains(service)) {
                slotGroup = tenantGroup->FindSubgroup(HOST_LABEL, "slot-1");
            } else {
                slotGroup = tenantGroup->FindSubgroup(SLOT_LABEL, "static")->FindSubgroup(HOST_LABEL, "slot-1");
            }
            UNIT_ASSERT(slotGroup);
            auto counter = slotGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, DOMAIN1_NAME, "slot-2", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 1, 1, 1}}});

        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto tenantGroup = serviceGroup->FindSubgroup(DATABASE_LABEL, CanonizePath(DOMAIN1_NAME));
            UNIT_ASSERT(tenantGroup);
            ::NMonitoring::TDynamicCounterPtr slotGroup;
            if (attrServices.contains(service)) {
                slotGroup = tenantGroup;
            } else {
                slotGroup = tenantGroup->FindSubgroup(SLOT_LABEL, "static");
            }
            UNIT_ASSERT(slotGroup);
            auto counter = slotGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
            *counter = 1;
        }

        CheckConfigureSlot(runtime, 0,
                           DOMAIN1_SLOT1, "", NKikimrTenantPool::SUCCESS);

        runtime.WaitForHiveState({});

        for (auto &service : services) {
            auto serviceGroup = GetServiceCounters(counters, service, false);
            auto tenantGroup = serviceGroup->FindSubgroup(DATABASE_LABEL, "<none>");
            UNIT_ASSERT(tenantGroup);
            ::NMonitoring::TDynamicCounterPtr slotGroup;
            if (attrServices.contains(service)) {
                slotGroup = tenantGroup->FindSubgroup(HOST_LABEL, "unassigned");
            } else {
                slotGroup = tenantGroup->FindSubgroup(SLOT_LABEL, "static")->FindSubgroup(HOST_LABEL, "unassigned");
            }
            UNIT_ASSERT(slotGroup);
            auto counter = slotGroup->GetCounter("counter", true);
            UNIT_ASSERT(*counter == 0);
        }

    }

    Y_UNIT_TEST(TestSensorLabelsForStaticConfig) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            true,
            // CreateConsole
            false,
            // Nodes
            {{
                    // Node0
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {},
                            ""
                        }
                    },
                    // Node1
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {TENANT1_1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {},
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
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, DOMAIN1_NAME, {1, 1, 1}} }},
                            ""
                        }
                    },
                    // Node3
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            ""
                        }
                    },
                    // Node4
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            ""
                        }
                    },
                    // Node5
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, DOMAIN1_NAME, {1, 1, 1}} }},
                            ""
                        }
                    },
                    // Node6
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
                    // Node7
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}},
                               {DOMAIN1_SLOT2, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            ""
                        }
                    },
                    // Node8
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            ""
                        }
                    }

            }},
            // DataCenterCount
            1
        };

        TTenantTestRuntime runtime(config);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 6, 6, 6},
                                   {TENANT1_1_NAME, 5, 5, 5}}});

        TVector<std::pair<TString, TString>> labels = { { "", "" },
                                                        { TENANT1_1_NAME, "static" },
                                                        { "", "" },
                                                        { TENANT1_1_NAME, "dynamic" },
                                                        { "", "" },
                                                        { "", "" },
                                                        { "", "" },
                                                        { TENANT1_1_NAME, "<multiple>" },
                                                        { "<multiple>", "<multiple>"} };
        for (size_t i = 0; i < labels.size(); ++i) {
            auto counters = runtime.GetDynamicCounters(i);
            CheckLabels(counters, labels[i].first, labels[i].second);
        }
    }

    Y_UNIT_TEST(TestForcedSensorLabelsForStaticConfig) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME, TENANT1_2_NAME }}} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            true,
            // CreateConsole
            false,
            // Nodes
            {{
                    // Node0
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {},
                            "",
                        }
                    },
                    // Node1
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {TENANT1_1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {},
                            "",
                        }
                    },
                    // Node2
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, DOMAIN1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node3
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node4
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node5
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, DOMAIN1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node6
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node7
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}},
                               {DOMAIN1_SLOT2, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    },
                    // Node8
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    }

            }},
            // DataCenterCount
            1
        };

        NKikimrConfig::TAppConfig ext;
        ext.MutableMonitoringConfig()->SetForceDatabaseLabels(true);
        TTenantTestRuntime runtime(config, ext);

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 6, 6, 6},
                                   {TENANT1_1_NAME, 5, 5, 5}}});

        TVector<std::pair<TString, TString>> labels = { { CanonizePath(DOMAIN1_NAME), "static" },
                                                        { TENANT1_1_NAME, "static" },
                                                        { CanonizePath(DOMAIN1_NAME), "dynamic" },
                                                        { TENANT1_1_NAME, "dynamic" },
                                                        { CanonizePath(DOMAIN1_NAME), "static" },
                                                        { CanonizePath(DOMAIN1_NAME), "<multiple>" },
                                                        { "<none>", "dynamic" },
                                                        { TENANT1_1_NAME, "<multiple>" },
                                                        { "<multiple>", "<multiple>"} };
        for (size_t i = 0; i < labels.size(); ++i) {
            auto counters = runtime.GetDynamicCounters(i);
            CheckLabels(counters, labels[i].first, labels[i].second);
        }
    }

    Y_UNIT_TEST(TestDatabaseAttributeSensorLabels) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            false,
            // FakeSchemeShard
            false,
            // CreateConsole
            true,
            // Nodes {tenant_pool_config, data_center}
            {{
                    {
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {"slot", SLOT2_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            "node-type"
                        }
                    },
                }},
            // DataCenterCount
            1
        };
        TTenantTestRuntime runtime(config);
        auto counters = runtime.GetDynamicCounters();

        THashMap<TString, TString> attrs;
        for (auto &l : GetDatabaseAttributeLabels())
            attrs[l] = l + "_value";
        TVector<std::pair<TString, TString>> attrsv;
        for (auto &pr : attrs)
            attrsv.push_back(pr);

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          attrsv,
                          SLOT2_TYPE, ZONE_ANY, 1);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1}}});

        CheckLabels(counters, TENANT1_1_NAME, "slot-1", attrs);

        WaitTenantRunning(runtime, TENANT1_1_NAME); // workaround for scheme cache race
        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        runtime.WaitForHiveState({});

        CheckLabels(counters, "", "");
    }

    Y_UNIT_TEST(TestSensorsConfigForStaticSlot) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            false,
            // CreateConsole
            true,
            // Nodes {tenant_pool_config, data_center}
            {{
                    {
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {},
                            "node-type"
                        }
                    },
                }},
            // DataCenterCount
            1,
            // CreateConfigsDispatcher
            true
        };

        TTenantTestRuntime runtime(config);
        auto counters = runtime.GetDynamicCounters();

        CheckLabels(counters, "", "");

        NKikimrConfig::TAppConfig ext;
        auto &monCfg = *ext.MutableMonitoringConfig();
        monCfg.SetForceDatabaseLabels(true);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, CanonizePath(DOMAIN1_NAME),
                    monCfg.GetDatabaseLabels().GetStaticSlotLabelValue(),
                    {}, monCfg);

        monCfg.MutableDatabaseLabels()->SetStaticSlotLabelValue("very-static");
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, CanonizePath(DOMAIN1_NAME),
                    monCfg.GetDatabaseLabels().GetStaticSlotLabelValue(),
                    {}, monCfg);

        monCfg.MutableDatabaseLabels()->ClearStaticSlotLabelValue();
        monCfg.MutableDatabaseLabels()->AddServices("tablets");
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, CanonizePath(DOMAIN1_NAME),
                    monCfg.GetDatabaseLabels().GetStaticSlotLabelValue(),
                    {}, monCfg);

        monCfg.MutableDatabaseLabels()->SetStaticSlotLabelValue("static-again");
        monCfg.MutableDatabaseLabels()->ClearServices();
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, CanonizePath(DOMAIN1_NAME),
                    monCfg.GetDatabaseLabels().GetStaticSlotLabelValue(),
                    {}, monCfg);

        monCfg.MutableDatabaseLabels()->SetEnabled(false);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, "", "");

        monCfg.MutableDatabaseLabels()->SetEnabled(true);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, CanonizePath(DOMAIN1_NAME),
                    monCfg.GetDatabaseLabels().GetStaticSlotLabelValue(),
                    {}, monCfg);

        monCfg.SetForceDatabaseLabels(false);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, "", "");
    }

    Y_UNIT_TEST(TestSensorsConfigForDynamicSlotLabelValue) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME }}} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            true,
            // FakeSchemeShard
            true,
            // CreateConsole
            true,
            // Nodes {tenant_pool_config, data_center}
            {{
                    {
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}} }},
                            "node-type"
                        }
                    },
                }},
            // DataCenterCount
            1,
            // CreateConfigsDispatcher
            true
        };

        NKikimrConfig::TAppConfig ext;
        auto &monCfg = *ext.MutableMonitoringConfig();

        TTenantTestRuntime runtime(config, ext);
        auto counters = runtime.GetDynamicCounters();

        CheckLabels(counters, TENANT1_1_NAME,
                    monCfg.GetDatabaseLabels().GetDynamicSlotLabelValue(),
                    {}, monCfg);

        monCfg.MutableDatabaseLabels()->SetDynamicSlotLabelValue("very-dynamic");
        ChangeMonitoringConfig(runtime, monCfg, true);

        CheckLabels(counters, TENANT1_1_NAME,
                    monCfg.GetDatabaseLabels().GetDynamicSlotLabelValue(),
                    {}, monCfg);
    }

    Y_UNIT_TEST(TestSensorsConfigForDynamicSlot) {
        const TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID,  TVector<TString>()} }},
            // HiveId
            HIVE_ID,
            // FakeTenantSlotBroker
            false,
            // FakeSchemeShard
            false,
            // CreateConsole
            true,
            // Nodes {tenant_pool_config, data_center}
            {{
                    {
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {},
                            // Dynamic slots {id, type, domain, tenant, {cpu, memory, network}}
                            {{ {DOMAIN1_SLOT1, SLOT2_TYPE, DOMAIN1_NAME, "", {1, 1, 1}} }},
                            "node-type"
                        }
                    },
                }},
            // DataCenterCount
            1,
            // CreateConfigsDispatcher
            true
        };

        NKikimrConfig::TAppConfig ext;
        auto &monCfg = *ext.MutableMonitoringConfig();

        TTenantTestRuntime runtime(config, ext);
        auto counters = runtime.GetDynamicCounters();

        THashMap<TString, TString> attrs;
        for (auto &l : GetDatabaseAttributeLabels())
            attrs[l] = l + "_value";
        TVector<std::pair<TString, TString>> attrsv;
        for (auto &pr : attrs)
            attrsv.push_back(pr);

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          attrsv,
                          SLOT2_TYPE, ZONE_ANY, 1);

        runtime.WaitForHiveState({{{TENANT1_1_NAME, 1, 1, 1}}});

        CheckLabels(counters, TENANT1_1_NAME, "slot-1", attrs, monCfg);

        monCfg.MutableDatabaseLabels()->AddServices("ydb");
        monCfg.MutableDatabaseAttributeLabels()->AddAttributeGroups()
            ->AddServices("tablets");
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, TENANT1_1_NAME, "slot-1", attrs, monCfg);

        monCfg.MutableDatabaseLabels()->SetEnabled(false);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, "", "", attrs, monCfg);

        monCfg.MutableDatabaseAttributeLabels()->SetEnabled(false);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, "", "", {}, monCfg);

        monCfg.MutableDatabaseLabels()->SetEnabled(true);
        monCfg.MutableDatabaseAttributeLabels()->SetEnabled(true);
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, TENANT1_1_NAME, "slot-1", attrs, monCfg);

        monCfg.MutableDatabaseLabels()->ClearServices();
        monCfg.MutableDatabaseAttributeLabels()->ClearAttributeGroups();
        ChangeMonitoringConfig(runtime, monCfg);

        CheckLabels(counters, TENANT1_1_NAME, "slot-1", attrs, monCfg);
    }

    void TestState(
            const TTenantTestConfig::TStaticSlotConfig& staticSlot,
            const TTenantTestConfig::TDynamicSlotConfig& dynamicSlot,
            NKikimrTenantPool::EState expected) {

        TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME }}} }},
            HIVE_ID, // HiveId
            true, // FakeTenantSlotBroker
            true, // FakeSchemeShard
            false, // CreateConsole
            {{{ {}, {}, "node-type" }}}, // Nodes
            1 // DataCenterCount
        };

        if (staticSlot.Tenant) {
            config.Nodes.back().TenantPoolConfig.StaticSlots.push_back(staticSlot);
        } else if (dynamicSlot.Tenant) {
            config.Nodes.back().TenantPoolConfig.DynamicSlots.push_back(dynamicSlot);
        }

        TTenantTestRuntime runtime(config, {}, false);

        const TActorId& sender = runtime.Sender;
        const TActorId tenantPoolRoot = MakeTenantPoolRootID();
        const TActorId tenantPool = MakeTenantPoolID(runtime.GetNodeId(0), 0);

        using TEvStatus = TEvTenantPool::TEvTenantPoolStatus;
        using EState = NKikimrTenantPool::EState;

        auto checker = [](auto ev, EState expectedState) {
            UNIT_ASSERT(ev->Get());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetSlots(0).GetState(), expectedState);
        };

        runtime.CreateTenantPool(0);

        // Subscribe on root pool and wait until domain pool started
        runtime.Send(new IEventHandle(tenantPoolRoot, sender, new TEvents::TEvSubscribe()));
        checker(runtime.GrabEdgeEvent<TEvStatus>(sender), EState::TENANT_ASSIGNED);

        // Get status from domain pool
        runtime.Send(new IEventHandle(tenantPool, sender, new TEvTenantPool::TEvGetStatus(true)));
        checker(runtime.GrabEdgeEvent<TEvStatus>(sender), expected);
    }

    Y_UNIT_TEST(TestStateStatic) {
        TestState({TENANT1_1_NAME, {1, 1, 1}}, {}, NKikimrTenantPool::EState::TENANT_OK);
    }

    Y_UNIT_TEST(TestStateDynamic) {
        TestState({}, {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_1_NAME, {1, 1, 1}}, NKikimrTenantPool::EState::TENANT_OK);
    }

    Y_UNIT_TEST(TestStateDynamicTenantUnknown) {
        // After unsuccessful resolve on SS, tenant will be detached from slot, so state is unknown
        TestState({}, {DOMAIN1_SLOT1, SLOT1_TYPE, DOMAIN1_NAME, TENANT1_U_NAME, {1, 1, 1}}, NKikimrTenantPool::EState::STATE_UNKNOWN);
    }
}

} //namespace NKikimr

template <>
void Out<NKikimrTenantPool::EState>(IOutputStream& o, NKikimrTenantPool::EState x) {
    o << NKikimrTenantPool::EState_Name(x);
}
