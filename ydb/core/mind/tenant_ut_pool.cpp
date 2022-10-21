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
                            "",
                        }
                    },
                    // Node1
                    {
                        // TenantPoolConfig
                        {
                            // Static slots {tenant, {cpu, memory, network}}
                            {{ {TENANT1_1_NAME, {1, 1, 1}} }},
                            "",
                        }
                    },

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
                                                        { TENANT1_1_NAME, "static" }, };
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
                            {{ {DOMAIN1_NAME, {1, 1, 1}} }},
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

    void TestState(
            const TTenantTestConfig::TStaticSlotConfig& staticSlot,
            NKikimrTenantPool::EState expected) {

        TTenantTestConfig config = {
            // Domains {name, schemeshard {{ subdomain_names }}}
            {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, {{ TENANT1_1_NAME }}} }},
            HIVE_ID, // HiveId
            true, // FakeTenantSlotBroker
            true, // FakeSchemeShard
            false, // CreateConsole
            {{{ {}, "node-type" }}}, // Nodes
            1 // DataCenterCount
        };

        if (staticSlot.Tenant) {
            config.Nodes.back().TenantPoolConfig.StaticSlots.push_back(staticSlot);
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
        TestState({TENANT1_1_NAME, {1, 1, 1}}, NKikimrTenantPool::EState::TENANT_OK);
    }

}

} //namespace NKikimr

template <>
void Out<NKikimrTenantPool::EState>(IOutputStream& o, NKikimrTenantPool::EState x) {
    o << NKikimrTenantPool::EState_Name(x);
}
