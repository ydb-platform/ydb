#include "console_tenants_manager.h"
#include "ut_helpers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/path.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/mind/tenant_slot_broker_impl.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

namespace NKikimr {

using namespace NSchemeShard;
using namespace NTenantSlotBroker;
using namespace NConsole;
using namespace NConsole::NUT;

namespace {

const TString SLOT1 = "slot-1";
const TString SLOT2 = "slot-2";
const TString SLOT3 = "slot-3";

TTenantTestConfig::TTenantPoolConfig DefaultTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        "node-type"
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig FirstWithTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {TENANT1_1_NAME, {1, 1, 1}}}},
        "node-type"
    };
    return res;
}

TTenantTestConfig::TTenantPoolConfig FirstTenantPoolConfig()
{
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {},
        "node-type"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig()
{
    TTenantTestConfig res = {
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
                {FirstTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
                {DefaultTenantPoolConfig()},
        }},
        // DataCenterCount
        1
    };
    return res;
}

TString DefaultDatabaseQuotas() {
    return R"(
        data_size_hard_quota: 3000
        storage_quotas {
            unit_kind: "hdd"
            data_size_hard_quota: 2000
        }
        storage_quotas {
            unit_kind: "hdd-1"
            data_size_hard_quota: 1000
        }
    )";
}

void CheckAlterTenantSlots(TTenantTestRuntime &runtime, const TString &path,
                           ui64 generation, Ydb::StatusIds::StatusCode code,
                           TVector<TSlotRequest> add,
                           TVector<TSlotRequest> remove,
                           const TString &idempotencyKey = TString(),
                           const TVector<std::pair<TString, TString>> attrs = {})
{
    auto *event = new TEvConsole::TEvAlterTenantRequest;
    event->Record.MutableRequest()->set_path(path);
    event->Record.MutableRequest()->set_generation(generation);
    for (auto &slot : add) {
        auto &unit = *event->Record.MutableRequest()->add_computational_units_to_add();
        unit.set_unit_kind(slot.Type);
        unit.set_availability_zone(slot.Zone);
        unit.set_count(slot.Count);
    }
    for (auto &slot : remove) {
        auto &unit = *event->Record.MutableRequest()->add_computational_units_to_remove();
        unit.set_unit_kind(slot.Type);
        unit.set_availability_zone(slot.Zone);
        unit.set_count(slot.Count);
    }
    for (const auto& [key, value] : attrs) {
        (*event->Record.MutableRequest()->mutable_alter_attributes())[key] = value;
    }
    if (idempotencyKey) {
        event->Record.MutableRequest()->set_idempotency_key(idempotencyKey);
    }

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvAlterTenantResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetResponse().operation().status(), code);
}

void CheckAlterTenantSlots(TTenantTestRuntime &runtime, const TString &path,
                           Ydb::StatusIds::StatusCode code,
                           TVector<TSlotRequest> add,
                           TVector<TSlotRequest> remove)
{
    CheckAlterTenantSlots(runtime, path, 0, code, std::move(add), std::move(remove));
}

void CheckAlterRegisteredUnits(TTenantTestRuntime &runtime, const TString &path,
                               Ydb::StatusIds::StatusCode code,
                               TVector<TUnitRegistration> registerUnits,
                               TVector<TUnitRegistration> deregisterUnits)
{
    auto *event = new TEvConsole::TEvAlterTenantRequest;
    event->Record.MutableRequest()->set_path(path);
    for (auto &unit : registerUnits) {
        auto &rec = *event->Record.MutableRequest()->add_computational_units_to_register();
        rec.set_host(unit.Host);
        rec.set_port(unit.Port);
        rec.set_unit_kind(unit.Kind);
    }
    for (auto &unit : deregisterUnits) {
        auto &rec = *event->Record.MutableRequest()->add_computational_units_to_deregister();
        rec.set_host(unit.Host);
        rec.set_port(unit.Port);
        rec.set_unit_kind(unit.Kind);
    }

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvAlterTenantResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetResponse().operation().status(), code);
}

void CheckAlterTenantPools(TTenantTestRuntime &runtime,
                           const TString &path,
                           const TString &token,
                           Ydb::StatusIds::StatusCode code,
                           TVector<TPoolAllocation> add,
                           bool hasModifications = true)
{
    auto *event = new TEvConsole::TEvAlterTenantRequest;
    event->Record.MutableRequest()->set_path(path);
    if (token)
        event->Record.SetUserToken(token);
    for (auto &pool : add) {
        auto &unit = *event->Record.MutableRequest()->add_storage_units_to_add();
        unit.set_unit_kind(pool.PoolType);
        unit.set_count(pool.PoolSize);
    }

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);

    if (hasModifications && code == Ydb::StatusIds::SUCCESS) {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(NConsole::TTenantsManager::TEvPrivate::EvPoolAllocated);
        runtime.DispatchEvents(options);
    }

    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvAlterTenantResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetResponse().operation().status(), code);
}

void CheckAlterTenantPools(TTenantTestRuntime &runtime,
                           const TString &path,
                           Ydb::StatusIds::StatusCode code,
                           TVector<TPoolAllocation> add,
                           bool hasModifications = true)
{
    CheckAlterTenantPools(runtime, path, "", code, std::move(add), hasModifications);
}

void CheckListTenants(TTenantTestRuntime &runtime, TVector<TString> tenants)
{
    THashSet<TString> paths;
    for (auto &tenant : tenants)
        paths.insert(CanonizePath(tenant));

    auto *event = new TEvConsole::TEvListTenantsRequest;
    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvListTenantsResponse>(handle);
    Ydb::Cms::ListDatabasesResult result;
    reply->Record.GetResponse().operation().result().UnpackTo(&result);
    for (auto &tenant : result.paths()) {
        UNIT_ASSERT(paths.contains(tenant));
        paths.erase(tenant);
    }
    UNIT_ASSERT(paths.empty());
}

NKikimrConsole::TConfig GetCurrentConfig(TTenantTestRuntime &runtime)
{
    runtime.SendToConsole(new TEvConsole::TEvGetConfigRequest);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigResponse>(handle);
    return reply->Record.GetConfig();
}

void CheckGetConfig(TTenantTestRuntime &runtime,
                    const NKikimrConsole::TConfig &config)
{
    runtime.SendToConsole(new TEvConsole::TEvGetConfigRequest);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetConfigResponse>(handle);
    auto &rec = reply->Record.GetConfig();
    UNIT_ASSERT_VALUES_EQUAL(rec.DebugString(), config.DebugString());
}

void CheckSetConfig(TTenantTestRuntime &runtime,
                    const NKikimrConsole::TConfig &config,
                    Ydb::StatusIds::StatusCode code,
                    NKikimrConsole::TConfigItem::EMergeStrategy merge = NKikimrConsole::TConfigItem::OVERWRITE)
{
    runtime.SendToConsole(new TEvConsole::TEvSetConfigRequest(config, merge));
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvSetConfigResponse>(handle);
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus().GetCode(), code);
}

TString SendTenantCreationCommand(
        TTenantTestRuntime &runtime, const TString &name,
        const TString &idempotencyKey = TString(),
        TMaybe<Ydb::StatusIds::StatusCode> expectedStatus = Nothing())
{
    auto *request = new TEvConsole::TEvCreateTenantRequest;
    request->Record.MutableRequest()->set_path(name);
    auto &unit = *request->Record.MutableRequest()->mutable_resources()->add_storage_units();
    unit.set_unit_kind("hdd");
    unit.set_count(1);

    if (idempotencyKey) {
        request->Record.MutableRequest()->set_idempotency_key(idempotencyKey);
    }

    runtime.SendToConsole(request);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvCreateTenantResponse>(handle);

    if (expectedStatus) {
        UNIT_ASSERT(reply->Record.GetResponse().operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetResponse().operation().status(), *expectedStatus);
        return { };
    }

    UNIT_ASSERT(!reply->Record.GetResponse().operation().ready());
    return reply->Record.GetResponse().operation().id();
}

TString SendTenantRemovalCommand(TTenantTestRuntime &runtime, const TString &name)
{
    auto *request = new TEvConsole::TEvRemoveTenantRequest;
    request->Record.MutableRequest()->set_path(name);

    runtime.SendToConsole(request);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvRemoveTenantResponse>(handle);

    UNIT_ASSERT(!reply->Record.GetResponse().operation().ready());
    return reply->Record.GetResponse().operation().id();
}

void CheckNotificationRequest(TTenantTestRuntime &runtime, const TString &id)
{
    auto *request = new TEvConsole::TEvNotifyOperationCompletionRequest;
    request->Record.MutableRequest()->set_id(id);

    runtime.SendToConsole(request);
    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvNotifyOperationCompletionResponse>(handle);
    UNIT_ASSERT(!reply->Record.GetResponse().operation().ready());
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetResponse().operation().id(), id);
}

void CheckNotificationRequest(TTenantTestRuntime &runtime, const TString &id, Ydb::StatusIds::StatusCode code)
{
    auto *request = new TEvConsole::TEvNotifyOperationCompletionRequest;
    request->Record.MutableRequest()->set_id(id);

    runtime.SendToConsole(request);
    TAutoPtr<IEventHandle> handle;
    auto replies = runtime.GrabEdgeEventsRethrow<TEvConsole::TEvNotifyOperationCompletionResponse,
                                                         TEvConsole::TEvOperationCompletionNotification>(handle);
    UNIT_ASSERT(!std::get<0>(replies));
    UNIT_ASSERT(std::get<1>(replies)->Record.GetResponse().operation().ready());
    UNIT_ASSERT_VALUES_EQUAL(std::get<1>(replies)->Record.GetResponse().operation().id(), id);
    UNIT_ASSERT_VALUES_EQUAL(std::get<1>(replies)->Record.GetResponse().operation().status(), code);
}

void CheckCounter(TTenantTestRuntime &runtime,
                  TVector<std::pair<TString, TString>> groups,
                  TTenantsManager::ECounter counter,
                  ui64 value)
{
    ::NMonitoring::TDynamicCounterPtr counters;
    for (ui32 i = 0; i < runtime.GetNodeCount() && !counters; ++i) {
        auto tablets = GetServiceCounters(runtime.GetDynamicCounters(i), "tablets");
        counters = tablets->FindSubgroup("type", "CONSOLE");
    }

    UNIT_ASSERT(counters);
    for (auto &pr : groups) {
        counters = counters->FindSubgroup(pr.first, pr.second);
        UNIT_ASSERT(counters);
    }

    UNIT_ASSERT_VALUES_EQUAL(counters->GetCounter(TTenantsManager::TCounters::SensorName(counter), TTenantsManager::TCounters::IsDeriv(counter))->Val(), value);
}

void CheckTenantGeneration(TTenantTestRuntime &runtime,
                           const TString &path,
                           ui64 generation)
{
    auto *event = new TEvConsole::TEvGetTenantStatusRequest;
    event->Record.MutableRequest()->set_path(path);

    TAutoPtr<IEventHandle> handle;
    runtime.SendToConsole(event);
    auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvGetTenantStatusResponse>(handle);
    auto &operation = reply->Record.GetResponse().operation();
    UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Cms::GetDatabaseStatusResult status;
    UNIT_ASSERT(operation.result().UnpackTo(&status));

    UNIT_ASSERT_VALUES_EQUAL(status.generation(), generation);
}

NKikimrBlobStorage::TEvControllerConfigResponse ReadPoolState(TTenantTestRuntime &runtime, const TString &name)
{
    auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
    auto &read = *request->Record.MutableRequest()->AddCommand()->MutableReadStoragePool();
    read.SetBoxId(1);
    read.AddName(name);

    NTabletPipe::TClientConfig pipeConfig;
    pipeConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    runtime.SendToPipe(MakeBSControllerID(), runtime.Sender, request.Release(), 0, pipeConfig);

    auto ev = runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigResponse>(runtime.Sender);
    return ev->Get()->Record;
}

void CheckPoolScope(TTenantTestRuntime &runtime, const TString &name)
{
    auto response = ReadPoolState(runtime, name);
    const auto &scope = response.GetResponse().GetStatus(0).GetStoragePool(0).GetScopeId();
    UNIT_ASSERT(scope.GetX1() != 0);
    UNIT_ASSERT(scope.GetX2() != 0);
}

void RestartConsole(TTenantTestRuntime &runtime)
{
    runtime.Register(CreateTabletKiller(MakeConsoleID()));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(&IsTabletActiveEvent, 1);
    runtime.DispatchEvents(options);
}

struct CatchPoolEvent {
    CatchPoolEvent(TVector<TAutoPtr<IEventHandle>> &captured)
        : Captured(captured)
    {
    }

    TTenantTestRuntime::EEventAction operator()(TAutoPtr<IEventHandle>& ev)
    {
        if (ev->HasEvent()
            && (ev->Type == TTenantsManager::TEvPrivate::TEvPoolAllocated::EventType)
                || (ev->Type == TTenantsManager::TEvPrivate::TEvPoolFailed::EventType)
                || (ev->Type == TTenantsManager::TEvPrivate::TEvPoolDeleted::EventType)) {
            Captured.emplace_back(std::move(ev));
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    }

    TVector<TAutoPtr<IEventHandle>> &Captured;
};

void SendCaptured(TTenantTestRuntime &runtime, TVector<TAutoPtr<IEventHandle>> &events)
{
    for (auto &ev : events)
        runtime.Send(ev.Release());
    events.clear();
}

void LocalMiniKQL(TTenantTestRuntime& runtime, ui64 tabletId, const TString& query) {
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    request->Record.MutableProgram()->MutableProgram()->SetText(query);
    ForwardToTablet(runtime, tabletId, runtime.Sender, request.Release());

    auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(runtime.Sender);
    const auto& response = ev->Get()->Record;

    UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NKikimrProto::OK);
}

void MakePoolBorrowed(TTenantTestRuntime& runtime, const TString& tenant, const TString& pool) {
    LocalMiniKQL(runtime, MakeConsoleID(), Sprintf(R"(
        (
            (let key '('('Tenant (Utf8 '"%s")) '('PoolType (Utf8 '"%s"))))
            (let row '('('Borrowed (Bool '1))))
            (return (AsList (UpdateRow 'TenantPools key row)))
        )
    )", tenant.c_str(), pool.c_str()));
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TConsoleTxProcessorTests) {
    class TTestTransaction : public ITransaction {
    public:
        TTestTransaction(ui64 no, TTxProcessor::TPtr processor)
            : No(no)
            , Processor(processor)
        {
        }

        bool Execute(TTransactionContext &, const TActorContext &) override
        {
            return true;
        }

        bool Execute(const THashSet<ui64> &allowed)
        {
            return allowed.contains(No);
        }

        void Complete(const TActorContext &ctx) override
        {
            Processor->TxCompleted(this, ctx);
        }

        ui64 No;
        TTxProcessor::TPtr Processor;
    };

    class TTestExecutor : public ITxExecutor {
    public:
        TTestExecutor()
        {
        }

        void Execute(ITransaction *transaction, const TActorContext &ctx) override
        {

            THolder<TTestTransaction> tx{dynamic_cast<TTestTransaction*>(transaction)};
            if (tx->Execute(Allowed)) {
                Result.push_back(tx->No);
                tx->Complete(ctx);
            } else {
                Postponed.push_back(std::move(tx));
            }
        }

        void Run(const TActorContext &ctx)
        {
            for (auto it = Postponed.begin(); it != Postponed.end(); ) {
                auto cur = it;
                ++it;

                if ((*cur)->Execute(Allowed)) {
                    Result.push_back((*cur)->No);
                    (*cur)->Complete(ctx);
                    Postponed.erase(cur);
                }
            }
        }

        THashSet<ui64> Allowed;
        TList<THolder<TTestTransaction>> Postponed;
        TVector<ui64> Result;
    };

    class TTxTestActor : public TActorBootstrapped<TTxTestActor> {
    public:
        TTxTestActor(TActorId sink, std::function<void(const TActorContext &)> testFunction)
            : Sink(sink)
            , TestFunction(testFunction)
        {
        }

        void Bootstrap(const TActorContext &ctx)
        {
            TestFunction(ctx);
            ctx.Send(Sink, new TEvents::TEvWakeup);
        }
    private:
        TActorId Sink;
        std::function<void(const TActorContext &)> TestFunction;
    };

    void TestTxProcessorSingle(const TActorContext &ctx)
    {
        TTestExecutor executor;
        TTxProcessor::TPtr processor = new TTxProcessor(executor, "proc", NKikimrServices::CMS_CONFIGS);
        processor->ProcessTx(new TTestTransaction(1, processor), ctx);
        processor->ProcessTx(new TTestTransaction(2, processor), ctx);
        processor->ProcessTx(new TTestTransaction(3, processor), ctx);
        executor.Allowed.insert(1);
        executor.Allowed.insert(2);
        executor.Allowed.insert(3);
        executor.Run(ctx);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(", ", executor.Result), "1, 2, 3");
    }

    Y_UNIT_TEST(TestTxProcessorSingle) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        runtime.Register(new TTxTestActor(runtime.Sender, TestTxProcessorSingle));
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
    }

    void TestTxProcessorSubProcessor(const TActorContext &ctx)
    {
        TTestExecutor executor;
        TTxProcessor::TPtr processor = new TTxProcessor(executor, "proc", 0);
        auto sub1 = processor->GetSubProcessor("sub1", ctx, false);
        sub1->ProcessTx(new TTestTransaction(1, sub1), ctx);
        auto sub2 = processor->GetSubProcessor("sub2", ctx, false);
        sub2->ProcessTx(new TTestTransaction(2, sub2), ctx);
        processor->ProcessTx(new TTestTransaction(3, processor), ctx);
        sub1->ProcessTx(new TTestTransaction(4, sub1), ctx);
        sub2->ProcessTx(new TTestTransaction(5, sub2), ctx);
        executor.Allowed.insert(1);
        executor.Allowed.insert(2);
        executor.Allowed.insert(3);
        executor.Allowed.insert(4);
        executor.Allowed.insert(5);
        executor.Run(ctx);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(", ", executor.Result), "1, 2, 3, 4, 5");
        UNIT_ASSERT(processor->GetSubProcessor("sub1", ctx) == sub1);
        UNIT_ASSERT(processor->GetSubProcessor("sub2", ctx) == sub2);
    }

    Y_UNIT_TEST(TestTxProcessorSubProcessor) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        runtime.Register(new TTxTestActor(runtime.Sender, TestTxProcessorSubProcessor));
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
    }

    void TestTxProcessorTemporary(const TActorContext &ctx)
    {
        TTestExecutor executor;
        TTxProcessor::TPtr processor = new TTxProcessor(executor, "proc", 0);
        processor->ProcessTx(new TTestTransaction(1, processor), ctx);
        auto sub = processor->GetSubProcessor("sub", ctx);
        sub->ProcessTx(new TTestTransaction(2, sub), ctx);
        processor->ProcessTx(new TTestTransaction(3, processor), ctx);
        executor.Allowed.insert(1);
        executor.Allowed.insert(2);
        executor.Allowed.insert(3);
        executor.Run(ctx);
        UNIT_ASSERT_VALUES_EQUAL(JoinSeq(", ", executor.Result), "1, 3, 2");
        UNIT_ASSERT(processor->GetSubProcessor("sub", ctx) != sub);
    }

    Y_UNIT_TEST(TestTxProcessorTemporary) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        runtime.Register(new TTxTestActor(runtime.Sender, TestTxProcessorTemporary));
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
    }

    void TestTxProcessorRandom(const TActorContext &ctx)
    {
        TTestExecutor executor;
        TTxProcessor::TPtr processor = new TTxProcessor(executor, "proc", 0);
        TVector<ui64> postponed;

#ifndef NDEBUG
#  define COUNT 1000000
#else
#  define COUNT 10000000
#endif
        for (ui64 no = 1; no <= COUNT; ++no) {
            ui64 i = RandomNumber<ui64>(6);
            TTxProcessor::TPtr proc;
            if (!i)
                proc = processor;
            else
                proc = processor->GetSubProcessor(ToString(i), ctx, false);

            if (RandomNumber<ui64>(1))
                proc = proc->GetSubProcessor(ToString(RandomNumber<ui64>(5)), ctx);

            proc->ProcessTx(new TTestTransaction(no, proc), ctx);

            if (RandomNumber<ui64>(4) == 3)
                executor.Allowed.insert(no);
            else
                postponed.push_back(no);

            if (postponed.size() >= 2) {
                for (i = 0; i < RandomNumber<ui64>(postponed.size() / 2); ++i) {
                    auto it = postponed.begin() + RandomNumber<ui64>(postponed.size());
                    executor.Allowed.insert(*it);
                    postponed.erase(it);
                }
            }

            executor.Run(ctx);
        }
        for (auto no : postponed)
            executor.Allowed.insert(no);
        executor.Run(ctx);

        UNIT_ASSERT_VALUES_EQUAL(executor.Result.size(), COUNT);
    }

    Y_UNIT_TEST(TestTxProcessorRandom) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        runtime.Register(new TTxTestActor(runtime.Sender, TestTxProcessorRandom));
        TAutoPtr<IEventHandle> handle;
        runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(handle);
    }
}


Y_UNIT_TEST_SUITE(TConsoleTests) {
    void RestartTenantPool(TTenantTestRuntime& runtime) {
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(0)),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));

        runtime.CreateTenantPool(0, FirstWithTenantPoolConfig());
    }

    void RunTestCreateTenant(TTenantTestRuntime& runtime, bool shared = false) {
        using EType = TCreateTenantRequest::EType;

        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, shared ? EType::Shared : EType::Common)
                .WithPools({{"hdd", 1}, {"hdd-1", 2}}));

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, shared, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 2, 2}}, {});

        CheckPoolScope(runtime, TENANT1_1_NAME + ":hdd");
        CheckPoolScope(runtime, TENANT1_1_NAME + ":hdd-1");

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 1);
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_CREATE_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_CREATE_RESPONSES, 1);
    }

    Y_UNIT_TEST(TestCreateTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestCreateTenant(runtime);
    }

    Y_UNIT_TEST(TestCreateTenantExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateTenant(runtime);
    }

    Y_UNIT_TEST(TestCreateSharedTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateTenant(runtime, true);
    }

    Y_UNIT_TEST(TestCreateServerlessTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // create serverless tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {}, {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 2);
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_CREATE_REQUESTS, 2);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_CREATE_RESPONSES, 2);
    }

    Y_UNIT_TEST(TestCreateServerlessTenantWrongSharedDb) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // Empty shared db path
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(""));
        // Unknown shared db
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
    }

    void RunTestCreateTenantWrongName(TTenantTestRuntime& runtime) {
        // Empty path
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest(""));
        // Root path
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/"));
        // Root path
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("///"));
        // Wrong char.
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("tenant?"));
        // Wrong domain
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/wrong-domain/tenant"));
        // Tenant name starts with non-alpha
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/dc-1/users/1-tenant"));
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/dc-1/users/-tenant"));
        // Dir name starts with non-alpha
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/dc-1/1users/tenant"));
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest("/dc-1/_users/tenant"));

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 0);
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_CREATE_REQUESTS, 9);
        CheckCounter(runtime, {{ {"status", "BAD_REQUEST"} }}, TTenantsManager::COUNTER_CREATE_RESPONSES, 9);
    }

    Y_UNIT_TEST(TestCreateTenantWrongName) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestCreateTenantWrongName(runtime);
    }

    Y_UNIT_TEST(TestCreateTenantWrongNameExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateTenantWrongName(runtime);
    }

    void RunTestCreateTenantWrongPool(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest(TENANT1_1_NAME));
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest(TENANT1_1_NAME).WithPools({{"hdd", 0}}));
        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST, TCreateTenantRequest(TENANT1_1_NAME).WithPools({{"unknown", 1}}));
    }

    Y_UNIT_TEST(TestCreateTenantWrongPool) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestCreateTenantWrongPool(runtime);
    }

    Y_UNIT_TEST(TestCreateTenantWrongPoolExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateTenantWrongPool(runtime);
    }

    void RunTestCreateTenantAlreadyExists(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, "/dc-1/users/tenant-1", Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}});

        RestartTenantPool(runtime);

        CheckCreateTenant(runtime, "/dc-1/users/tenant-1", Ydb::StatusIds::ALREADY_EXISTS,
                          {{"hdd", 1}});

        CheckCreateTenant(runtime, "dc-1/users/tenant-1", Ydb::StatusIds::ALREADY_EXISTS,
                          {{"hdd", 1}});

        CheckCreateTenant(runtime, "//dc-1/users///tenant-1/", Ydb::StatusIds::ALREADY_EXISTS,
                          {{"hdd", 1}});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_CREATE_REQUESTS, 4);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_CREATE_RESPONSES, 1);
        CheckCounter(runtime, {{ {"status", "ALREADY_EXISTS"} }}, TTenantsManager::COUNTER_CREATE_RESPONSES, 3);
    }


    Y_UNIT_TEST(TestCreateTenantAlreadyExists) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestCreateTenantAlreadyExists(runtime);
    }

    Y_UNIT_TEST(TestCreateTenantAlreadyExistsExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateTenantAlreadyExists(runtime);
    }

    void RunTestGetUnknownTenantStatus(TTenantTestRuntime& runtime) {
        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_STATUS_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "NOT_FOUND"} }}, TTenantsManager::COUNTER_STATUS_RESPONSES, 1);
    }

    Y_UNIT_TEST(TestGetUnknownTenantStatus) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestGetUnknownTenantStatus(runtime);
    }

    Y_UNIT_TEST(TestGetUnknownTenantStatusExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestGetUnknownTenantStatus(runtime);
    }

    void RunTestRestartConsoleAndPools(TTenantTestRuntime& runtime) {
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(1)),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(2)),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));
        runtime.Send(new IEventHandle(MakeTenantPoolID(runtime.GetNodeId(3)),
                                      runtime.Sender,
                                      new TEvents::TEvPoisonPill));

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}});

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {{"hdd", 1, 1}}, {});

        RestartConsole(runtime);
        runtime.CreateTenantPool(1);
        runtime.CreateTenantPool(2);
        runtime.CreateTenantPool(3);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {{"hdd", 1, 1}}, {});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 1);
    }


    Y_UNIT_TEST(TestRestartConsoleAndPools) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestRestartConsoleAndPools(runtime);
    }

    Y_UNIT_TEST(TestRestartConsoleAndPoolsExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestRestartConsoleAndPools(runtime);
    }

    void RunTestAlterTenantModifyStorageResourcesForPending(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME).WithPools({{"hdd", 1}, {"hdd-1", 3}}));

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 3);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        TVector<TAutoPtr<IEventHandle>> captured;
        runtime.SetObserverFunc(CatchPoolEvent(captured));

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd", 2}, {"hdd-1", 3}}, {});

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 6);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 3);

        RestartConsole(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}}, {});

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 6);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 6);

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {}, false);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd-2", 1}});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}, {"hdd-2", 1, 1}}, {});

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 3);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 6);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 6);
        CheckCounter(runtime, {{ {"kind", "hdd-2"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd-2"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 1);

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd-2"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd-2"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 0);
    }

    Y_UNIT_TEST(TestAlterTenantModifyStorageResourcesForPending) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAlterTenantModifyStorageResourcesForPending(runtime);
    }

    Y_UNIT_TEST(TestAlterTenantModifyStorageResourcesForPendingExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAlterTenantModifyStorageResourcesForPending(runtime);
    }

    void RunTestAlterTenantModifyStorageResourcesForRunning(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 3}});

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd", 2}, {"hdd-1", 3}}, {});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {}, false);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd-2", 1}});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 3, 3}, {"hdd-1", 6, 6}, {"hdd-2", 1, 1}}, {});

        // Wrong unit kind.
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::BAD_REQUEST,
                              {{"unknown", 1}});
        // Zero pool size.
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::BAD_REQUEST,
                              {{"hdd-3", 0}});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_ALTER_REQUESTS, 5);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 3);
        CheckCounter(runtime, {{ {"status", "BAD_REQUEST"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 2);
    }

    Y_UNIT_TEST(TestAlterTenantModifyStorageResourcesForRunning) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAlterTenantModifyStorageResourcesForRunning(runtime);
    }

    Y_UNIT_TEST(TestAlterTenantModifyStorageResourcesForRunningExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAlterTenantModifyStorageResourcesForRunning(runtime);
    }

    void RunTestAlterUnknownTenant(TTenantTestRuntime& runtime) {
        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                              { }, { });

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_ALTER_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "NOT_FOUND"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 1);
    }


    Y_UNIT_TEST(TestAlterUnknownTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAlterUnknownTenant(runtime);
    }

    Y_UNIT_TEST(TestAlterUnknownTenantExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAlterUnknownTenant(runtime);
    }

    Y_UNIT_TEST(TestAlterBorrowedStorage) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);

        // create tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, TCreateTenantRequest::EType::Common)
                .WithPools({{"hdd", 1}}));

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}}, {});

        // mark pool as borrowed
        MakePoolBorrowed(runtime, TENANT1_1_NAME, "hdd");
        // restart to changes take effect
        RestartConsole(runtime);

        // trying to extend pool
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::BAD_REQUEST, {{"hdd", 1}}, {});
    }

    Y_UNIT_TEST(TestRemoveTenantWithBorrowedStorageUnits) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        runtime.SetLogPriority(NKikimrServices::CMS_TENANTS, NActors::NLog::PRI_DEBUG);

        // create tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, TCreateTenantRequest::EType::Common)
                .WithPools({{"hdd", 1}}));

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}}, {});

        // mark pool as borrowed
        MakePoolBorrowed(runtime, TENANT1_1_NAME, "hdd");
        // restart to changes take effect
        RestartConsole(runtime);

        // remove tenant
        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);
        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});

        // check pool (should be alive)
        auto poolState = ReadPoolState(runtime, TENANT1_1_NAME + ":hdd");
        UNIT_ASSERT_VALUES_EQUAL(poolState.GetResponse().GetStatus(0).StoragePoolSize(), 1);
    }

    Y_UNIT_TEST(TestAlterStorageUnitsOfSharedTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);

        // alter shared tenant (extend existent storage pool)
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd", 1}, {"hdd-1", 1}}, {});
        // check status
        CheckTenantStatus(runtime, TENANT1_1_NAME, true, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 2, 2}, {"hdd-1", 3, 3}}, {});

        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_ALTER_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 1);

        // alter shared tenant (add new storage pool)
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::UNSUPPORTED,
                              {{"hdd-2", 1}}, {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_ALTER_REQUESTS, 2);
        CheckCounter(runtime, {{ {"status", "UNSUPPORTED"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 1);

        // restart and try again
        RestartConsole(runtime);
        // alter shared tenant
        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::UNSUPPORTED,
                              {{"hdd-2", 1}}, {});
    }

    Y_UNIT_TEST(TestAlterServerlessTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // create serverless tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {}, {});
        // alter serverless tenant
        CheckAlterTenantSlots(runtime, TENANT1_2_NAME, Ydb::StatusIds::BAD_REQUEST,
                        {{ {SLOT1_TYPE, ZONE_ANY, 1} }},
                        {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_ALTER_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "BAD_REQUEST"} }}, TTenantsManager::COUNTER_ALTER_RESPONSES, 1);
    }

    void RunTestListTenants(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}});

        RestartTenantPool(runtime);

        CheckListTenants(runtime,
                         {{ TENANT1_1_NAME }});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_LIST_REQUESTS, 1);
    }

    Y_UNIT_TEST(TestListTenants) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestListTenants(runtime);
    }

    Y_UNIT_TEST(TestListTenantsExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestListTenants(runtime);
    }

    Y_UNIT_TEST(TestSetDefaultStorageUnitsQuota) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 3}});

        RestartTenantPool(runtime);

        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);
        config.MutableTenantsConfig()->SetDefaultStorageUnitsQuota(2);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);

        CheckCreateTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 2}});

        CheckCreateTenant(runtime, TENANT1_3_NAME, Ydb::StatusIds::BAD_REQUEST,
                          {{"hdd", 3}});
    }

    Y_UNIT_TEST(TestSetDefaultComputationalUnitsQuota) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 3);

        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);
        config.MutableTenantsConfig()->SetDefaultComputationalUnitsQuota(2);
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);

        CheckCreateTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 2);

        CheckCreateTenant(runtime, TENANT1_3_NAME, Ydb::StatusIds::BAD_REQUEST,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 3);
    }

    Y_UNIT_TEST(TestTenantConfigConsistency) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);

        // Repeated zone kind.
        NKikimrConsole::TConfig config1 = config;
        auto kind1 = config1.MutableTenantsConfig()->AddAvailabilityZoneKinds();
        kind1->CopyFrom(config1.GetTenantsConfig().GetAvailabilityZoneKinds(0));
        CheckSetConfig(runtime, config1, Ydb::StatusIds::BAD_REQUEST);
        // Repeated zone kind set.
        NKikimrConsole::TConfig config2 = config;
        auto set2 = config2.MutableTenantsConfig()->AddAvailabilityZoneSets();
        set2->CopyFrom(config2.GetTenantsConfig().GetAvailabilityZoneSets(0));
        CheckSetConfig(runtime, config2, Ydb::StatusIds::BAD_REQUEST);
        // Repeated computational unit kind.
        NKikimrConsole::TConfig config3 = config;
        auto kind3 = config3.MutableTenantsConfig()->AddComputationalUnitKinds();
        kind3->CopyFrom(config1.GetTenantsConfig().GetComputationalUnitKinds(0));
        CheckSetConfig(runtime, config3, Ydb::StatusIds::BAD_REQUEST);
        // Unknown zone kind used.
        NKikimrConsole::TConfig config4 = config;
        config4.MutableTenantsConfig()->MutableAvailabilityZoneSets(0)->AddZoneKinds("unknown");
        CheckSetConfig(runtime, config4, Ydb::StatusIds::BAD_REQUEST);
        // Unknown zone set.
        NKikimrConsole::TConfig config5 = config;
        config5.MutableTenantsConfig()->MutableComputationalUnitKinds(0)->SetAvailabilityZoneSet("unknown");
        CheckSetConfig(runtime, config5, Ydb::StatusIds::BAD_REQUEST);
        // Empty computational unit kind.
        NKikimrConsole::TConfig config6 = config;
        config6.MutableTenantsConfig()->MutableComputationalUnitKinds(0)->ClearTenantSlotType();
        CheckSetConfig(runtime, config6, Ydb::StatusIds::BAD_REQUEST);

        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        CheckSetConfig(runtime, {}, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestModifyUsedZoneKind) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 1);

        NKikimrConsole::TConfig config1 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config1.GetTenantsConfig().AvailabilityZoneKindsSize(); ++i) {
            if (config1.GetTenantsConfig().GetAvailabilityZoneKinds(i).GetKind() == ZONE1) {
                config1.MutableTenantsConfig()->MutableAvailabilityZoneKinds(i)->SetDataCenterName(ToString(2));
                break;
            }
        }
        // Cannot modify used zone1
        CheckSetConfig(runtime, config1, Ydb::StatusIds::BAD_REQUEST);

        NKikimrConsole::TConfig config2 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config2.GetTenantsConfig().AvailabilityZoneKindsSize(); ++i) {
            if (config2.GetTenantsConfig().GetAvailabilityZoneKinds(i).GetKind() == ZONE2) {
                config2.MutableTenantsConfig()->MutableAvailabilityZoneKinds(i)->SetDataCenterName(ToString(1));
                break;
            }
        }
        // OK to modify unused zone2
        CheckSetConfig(runtime, config2, Ydb::StatusIds::SUCCESS);

        CheckCreateTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT2_TYPE, ZONE2, 1);

        for (ui64 i = 0; i < config2.GetTenantsConfig().AvailabilityZoneKindsSize(); ++i) {
            if (config2.GetTenantsConfig().GetAvailabilityZoneKinds(i).GetKind() == ZONE2) {
                config2.MutableTenantsConfig()->MutableAvailabilityZoneKinds(i)->SetDataCenterName(ToString(2));
                break;
            }
        }
        // Now zone2 is also used.
        CheckSetConfig(runtime, config2, Ydb::StatusIds::BAD_REQUEST);

        CheckAlterTenantSlots(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                              {},
                              {{ {SLOT2_TYPE, ZONE2, 1} }});

        // Now zone2 is unused again.
        CheckSetConfig(runtime, config2, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestSetConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 1);

        NKikimrConsole::TConfig config1 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config1.GetTenantsConfig().ComputationalUnitKindsSize(); ++i) {
            if (config1.GetTenantsConfig().GetComputationalUnitKinds(i).GetKind() == SLOT1_TYPE) {
                config1.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetTenantSlotType("new_value");
                break;
            }
        }
        // Cannot modify tenant slot type for used SLOT1_TYPE
        CheckSetConfig(runtime, config1, Ydb::StatusIds::BAD_REQUEST);

        NKikimrConsole::TConfig config2 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config2.GetTenantsConfig().ComputationalUnitKindsSize(); ++i) {
            if (config2.GetTenantsConfig().GetComputationalUnitKinds(i).GetKind() == SLOT1_TYPE) {
                config2.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetAvailabilityZoneSet(ZONE2);
                break;
            }
        }
        // Cannot remove used zone1 from allowed availability zones of SLOT1_TYPE
        CheckSetConfig(runtime, config2, Ydb::StatusIds::BAD_REQUEST);

        // OK to add new allowed zones for SLOT1_TYPE
        NKikimrConsole::TConfig config3 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config3.GetTenantsConfig().ComputationalUnitKindsSize(); ++i) {
            if (config3.GetTenantsConfig().GetComputationalUnitKinds(i).GetKind() == SLOT1_TYPE) {
                config3.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetAvailabilityZoneSet("all");
                break;
            }
        }
        CheckSetConfig(runtime, config3, Ydb::StatusIds::SUCCESS);

        NKikimrConsole::TConfig config4 = GetCurrentConfig(runtime);
        for (ui64 i = 0; i < config4.GetTenantsConfig().ComputationalUnitKindsSize(); ++i) {
            if (config4.GetTenantsConfig().GetComputationalUnitKinds(i).GetKind() == SLOT2_TYPE) {
                config4.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetTenantSlotType(SLOT3_TYPE);
                config4.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetAvailabilityZoneSet(ZONE1);
                break;
            }
        }
        // OK to modify unused SLOT2_TYPE
        CheckSetConfig(runtime, config4, Ydb::StatusIds::SUCCESS);

        CheckCreateTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT2_TYPE, ZONE1, 1);

        for (ui64 i = 0; i < config4.GetTenantsConfig().ComputationalUnitKindsSize(); ++i) {
            if (config4.GetTenantsConfig().GetComputationalUnitKinds(i).GetKind() == SLOT2_TYPE) {
                config4.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetTenantSlotType("new_value");
                config4.MutableTenantsConfig()->MutableComputationalUnitKinds(i)->SetAvailabilityZoneSet(ZONE2);
                break;
            }
        }
        // Now SLOT2_TYPE is also used.
        CheckSetConfig(runtime, config4, Ydb::StatusIds::BAD_REQUEST);

        CheckAlterTenantSlots(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                              {},
                              {{ {SLOT2_TYPE, ZONE1, 1} }});

        // Now zone2 is unused again.
        CheckSetConfig(runtime, config4, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestMergeConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        NKikimrConsole::TConfig config = GetCurrentConfig(runtime);
        config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
            ->ClearAllowedHostUsageScopeKinds();
        CheckSetConfig(runtime, config, Ydb::StatusIds::SUCCESS);
        CheckGetConfig(runtime, config);

        {
            NKikimrConsole::TConfig delta;
            delta.MutableConfigsConfig()->MutableUsageScopeRestrictions()
                ->AddAllowedHostUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
            delta.MutableTenantsConfig()->MutableClusterQuota()
                ->SetTenantsQuota(10);
            CheckSetConfig(runtime, delta, Ydb::StatusIds::SUCCESS, NKikimrConsole::TConfigItem::MERGE);

            config.MutableConfigsConfig()->MutableUsageScopeRestrictions()
                ->AddAllowedHostUsageScopeKinds(NKikimrConsole::TConfigItem::LogConfigItem);
            config.MutableTenantsConfig()->MutableClusterQuota()
                ->SetTenantsQuota(10);
            CheckGetConfig(runtime, config);
        }

        {
            NKikimrConsole::TConfig delta;
            delta.MutableTenantsConfig()->MutableClusterQuota()
                ->SetTenantsQuota(0);
            CheckSetConfig(runtime, delta, Ydb::StatusIds::SUCCESS, NKikimrConsole::TConfigItem::MERGE);

            config.MutableTenantsConfig()->MutableClusterQuota()
                ->SetTenantsQuota(0);
            CheckGetConfig(runtime, config);
        }

        // Check merge doesn't disable checks.
        {
            NKikimrConsole::TConfig delta;
            auto zone = delta.MutableTenantsConfig()->AddAvailabilityZoneKinds();
            zone->SetKind(ZONE1);
            zone->SetDataCenterName(ToString(1));
            CheckSetConfig(runtime, delta, Ydb::StatusIds::BAD_REQUEST, NKikimrConsole::TConfigItem::MERGE);
        }

        // Overwrite zones only.
        {
            NKikimrConsole::TConfig delta;
            auto zone1 = delta.MutableTenantsConfig()->AddAvailabilityZoneKinds();
            zone1->SetKind(ZONE1);
            zone1->SetDataCenterName(ToString(1));
            auto set1 = delta.MutableTenantsConfig()->AddAvailabilityZoneSets();
            set1->SetName("all");
            set1->AddZoneKinds(ZONE1);
            auto set2 = delta.MutableTenantsConfig()->AddAvailabilityZoneSets();
            set2->SetName(ZONE1);
            set2->AddZoneKinds(ZONE1);
            CheckSetConfig(runtime, delta, Ydb::StatusIds::SUCCESS, NKikimrConsole::TConfigItem::MERGE_OVERWRITE_REPEATED);

            config.MutableTenantsConfig()->MutableAvailabilityZoneKinds()
                ->CopyFrom(delta.GetTenantsConfig().GetAvailabilityZoneKinds());
            config.MutableTenantsConfig()->MutableAvailabilityZoneSets()
                ->CopyFrom(delta.GetTenantsConfig().GetAvailabilityZoneSets());
            CheckGetConfig(runtime, config);
        }
    }

    void RunTestRemoveTenant(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 2}});

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 2, 2}}, {});

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND);

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_REMOVE_REQUESTS, 2);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 1);
        CheckCounter(runtime, {{ {"status", "NOT_FOUND"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 1);

        RestartConsole(runtime);

        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 2}});

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 2, 2}}, {});

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        // Restart to check we don't load any garbage for removed tenant.
        RestartConsole(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 0);
    }

    Y_UNIT_TEST(TestRemoveTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestRemoveTenant(runtime);
    }

    Y_UNIT_TEST(TestRemoveTenantExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestRemoveTenant(runtime);
    }

    Y_UNIT_TEST(TestRemoveSharedTenantWoServerlessTenants) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // remove shared tenant
        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);
        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_REMOVE_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 1);
    }

    Y_UNIT_TEST(TestRemoveSharedTenantWithServerlessTenants) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // create serverless tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {}, {});
        // remove shared tenant
        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::PRECONDITION_FAILED);
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_REMOVE_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "PRECONDITION_FAILED"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 1);
    }

    Y_UNIT_TEST(TestRemoveSharedTenantAfterRemoveServerlessTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // create serverless tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {}, {});
        // remove serverless tenant
        CheckRemoveTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS);
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});
        // remove shared tenant
        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);
        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_REMOVE_REQUESTS, 2);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 2);
    }

    Y_UNIT_TEST(TestRemoveServerlessTenant) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        // create shared tenant
        RunTestCreateTenant(runtime, true);
        // create serverless tenant
        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME)
                .WithSharedDbPath(TENANT1_1_NAME));
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING, {}, {});
        // remove serverless tenant
        CheckRemoveTenant(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS);
        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::NOT_FOUND,
                          Ydb::Cms::GetDatabaseStatusResult::STATE_UNSPECIFIED, {}, {});
        // check counters
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_REMOVE_REQUESTS, 1);
        CheckCounter(runtime, {{ {"status", "SUCCESS"} }}, TTenantsManager::COUNTER_REMOVE_RESPONSES, 1);
    }

    void RunTestCreateSubSubDomain(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 2}});

        RestartTenantPool(runtime);

        CheckCreateTenant(runtime, TENANT1_1_NAME + "/sub", Ydb::StatusIds::GENERIC_ERROR,
                          {{"hdd", 1}});
        WaitForTenantStatus(runtime, TENANT1_1_NAME + "/sub", Ydb::StatusIds::NOT_FOUND);

        // Check unsuccessful tenant creation doesn't break counters.
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 2);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 2);
        CheckCounter(runtime, {}, TTenantsManager::COUNTER_CONFIGURE_SUBDOMAIN_FAILED, 1);

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        CheckCreateTenant(runtime, TENANT1_1_NAME + "/sub", Ydb::StatusIds::SUCCESS,
                          {{"hdd", 2}});

        CheckCounter(runtime, {}, TTenantsManager::COUNTER_TENANTS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 2);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 2);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "hdd-1"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 0);
    }

    Y_UNIT_TEST(TestCreateSubSubDomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestCreateSubSubDomain(runtime);
    }

    Y_UNIT_TEST(TestCreateSubSubDomainExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestCreateSubSubDomain(runtime);
    }

    Y_UNIT_TEST(TestRegisterComputationalUnitsForPending) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME).WithPools({{"hdd", 1}, {"hdd-1", 1}}));

        runtime.WaitForHiveState({{{DOMAIN1_NAME, 8, 8, 8}}});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 1, 1}, {"hdd-1", 1, 1}}, {});

        CheckAlterRegisteredUnits(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                                  {{ {"host1", 1, "kind1"},
                                     {"host2", 2, "kind2"} }},
                                  {});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 1, 1}, {"hdd-1", 1, 1}},
                          {{"host1", 1, "kind1"}, {"host2", 2, "kind2"}});

        CheckCounter(runtime, {{ {"kind", "kind1"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "kind2"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);

        CheckAlterRegisteredUnits(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                                  {{ {"host3", 3, "kind2"} }},
                                  {{ {"host1", 1, ""} }});

        CheckCounter(runtime, {{ {"kind", "kind1"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 0);
        CheckCounter(runtime, {{ {"kind", "kind2"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 2);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 1, 1}, {"hdd-1", 1, 1}},
                          {{"host2", 2, "kind2"}, {"host3", 3, "kind2"}});

        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_2_NAME).WithPools({{"hdd", 1}, {"hdd-1", 1}}));

        CheckAlterRegisteredUnits(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                                  {{ {"host4", 4, "kind1"},
                                     {"host5", 5, "kind2"} }},
                                  {});

        CheckCounter(runtime, {{ {"kind", "kind1"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "kind2"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 3);

        CheckRemoveTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS);

        CheckCounter(runtime, {{ {"kind", "kind1"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "kind2"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);

        RestartConsole(runtime);

        CheckTenantStatus(runtime, TENANT1_2_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::PENDING_RESOURCES,
                          {{"hdd", 1, 1}, {"hdd-1", 1, 1}},
                          {{"host4", 4, "kind1"}, {"host5", 5, "kind2"}});

        CheckCounter(runtime, {{ {"kind", "kind1"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "kind2"} }}, TTenantsManager::COUNTER_REGISTERED_UNITS, 1);
    }

    void RunTestNotifyOperationCompletion(TTenantTestRuntime& runtime) {
        TAutoPtr<IEventHandle> handle;

        // This observer should intercept events about pool creation/removal
        // and thus pause tenant creation/removal.
        TVector<TAutoPtr<IEventHandle>> captured;
        runtime.SetObserverFunc(CatchPoolEvent(captured));

        // Send tenant creation command and store operation id.
        TString id = SendTenantCreationCommand(runtime, TENANT1_1_NAME, "create-1-key");
        // Send notification request. Tenant shouldn't be created by that time,
        // so we should get TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id);

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 0);

        // Now remove observer, get back intercepted event (if any) and wait for
        // tenant creation notification.
        {
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            SendCaptured(runtime, captured);
            auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvOperationCompletionNotification>(handle);
            auto &operation = reply->Record.GetResponse().operation();
            UNIT_ASSERT(operation.ready());
            UNIT_ASSERT_VALUES_EQUAL(operation.id(), id);
            UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        }

        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_REQUESTED_STORAGE_UNITS, 1);
        CheckCounter(runtime, {{ {"kind", "hdd"} }}, TTenantsManager::COUNTER_ALLOCATED_STORAGE_UNITS, 1);

        // Now notification request should cause instant TEvOperationCompletionNotification
        // with no TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id, Ydb::StatusIds::SUCCESS);

        // Create existing tenant using correct idempotency key, we should get the same operation id
        TString id2 = SendTenantCreationCommand(runtime, TENANT1_1_NAME, "create-1-key");
        UNIT_ASSERT_VALUES_EQUAL(id, id2);

        // Create existing tenant using different idempotency key, we should get an error
        SendTenantCreationCommand(runtime, TENANT1_1_NAME, "create-1-wrong-key", Ydb::StatusIds::ALREADY_EXISTS);

        // Send another tenant creation command and store operation id.
        runtime.SetObserverFunc(CatchPoolEvent(captured));
        id = SendTenantCreationCommand(runtime, TENANT1_1_NAME + "/sub");
        // Send notification request. Tenant shouldn't be created by that time,
        // so we should get TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id);

        // Not remove observer, get back intercepted event (if any) and wait for
        // tenant creation notification.
        {
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            SendCaptured(runtime, captured);
            auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvOperationCompletionNotification>(handle);
            auto &operation = reply->Record.GetResponse().operation();
            UNIT_ASSERT(operation.ready());
            UNIT_ASSERT_VALUES_EQUAL(operation.id(), id);
            UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::GENERIC_ERROR);
        }

        // Now notification request should cause instant TEvOperationCompletionNotification
        // with no TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id, Ydb::StatusIds::GENERIC_ERROR);

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}}, {});

        // Send tenant removal command and store operation id.
        runtime.SetObserverFunc(CatchPoolEvent(captured));
        id = SendTenantRemovalCommand(runtime, TENANT1_1_NAME);

        // Send notification request. Tenant shouldn't be removed by that time,
        // so we should get TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id);

        // Not remove observer, get back intercepted event (if any) and wait for
        // tenant removal notification.
        {
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            SendCaptured(runtime, captured);
            auto reply = runtime.GrabEdgeEventRethrow<TEvConsole::TEvOperationCompletionNotification>(handle);
            auto &operation = reply->Record.GetResponse().operation();
            UNIT_ASSERT(operation.ready());
            UNIT_ASSERT_VALUES_EQUAL(operation.id(), id);
            UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::SUCCESS);
        }

        // Now notification request should cause instant TEvOperationCompletionNotification
        // with no TEvNotifyOperationCompletionResponse.
        CheckNotificationRequest(runtime, id, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(TestNotifyOperationCompletion) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestNotifyOperationCompletion(runtime);
    }

    Y_UNIT_TEST(TestNotifyOperationCompletionExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestNotifyOperationCompletion(runtime);
    }

    void RunTestAuthorization(TTenantTestRuntime& runtime) {
        for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
            auto &appData = runtime.GetAppData(i);
            appData.AdministrationAllowedSIDs.push_back("root@builtin");
        }

        CheckCreateTenant(runtime, TENANT1_1_NAME,
                          NACLib::TUserToken("root@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          SLOT1_TYPE, ZONE1, 1);

        CheckCreateTenant(runtime, TENANT1_2_NAME,
                          NACLib::TUserToken("root@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          SLOT1_TYPE, ZONE1, 1);

        CheckCreateTenant(runtime, TENANT1_3_NAME,
                          NACLib::TUserToken("user1@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::UNAUTHORIZED,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          SLOT1_TYPE, ZONE1, 1);

        CheckAlterTenantPools(runtime, TENANT1_1_NAME,
                              NACLib::TUserToken("root@builtin", {}).SerializeAsString(),
                              Ydb::StatusIds::SUCCESS,
                              {{"hdd", 1}}, false);

        CheckRemoveTenant(runtime, TENANT1_1_NAME,
                          NACLib::TUserToken("root@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::SUCCESS);

        CheckAlterTenantPools(runtime, TENANT1_2_NAME,
                              NACLib::TUserToken("user1@builtin", {}).SerializeAsString(),
                              Ydb::StatusIds::UNAUTHORIZED,
                              {{"hdd", 1}}, false);

        CheckRemoveTenant(runtime, TENANT1_2_NAME,
                          NACLib::TUserToken("user1@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::UNAUTHORIZED);
    }

    Y_UNIT_TEST(TestAuthorization) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAuthorization(runtime);
    }

    Y_UNIT_TEST(TestAuthorizationExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAuthorization(runtime);
    }


    bool CheckAttrsPresent(TTenantTestRuntime& runtime, const TString& tenantName, THashMap<TString, TString> attrs, bool skipAbsent = false) {
        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(tenantName);
        ForwardToTablet(runtime, SCHEME_SHARD1_ID, runtime.Sender, request.Release());

        TAutoPtr<IEventHandle> handle;
        auto reply = runtime.GrabEdgeEvent<TEvSchemeShard::TEvDescribeSchemeResult>(handle);
        Cerr << "Reply: " << reply->GetRecord().DebugString() << "\n";
        for (auto &attr : reply->GetRecord().GetPathDescription().GetUserAttributes()) {
            if (!skipAbsent) {
                UNIT_ASSERT(attrs.contains(attr.GetKey()));
                UNIT_ASSERT_VALUES_EQUAL(attrs.at(attr.GetKey()), attr.GetValue());
            }
            attrs.erase(attr.GetKey());
        }
        return attrs.empty();
    }

    void RunTestAttributes(TTenantTestRuntime& runtime) {
        // Create tenant with attrs and check subdomain has them attached.
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
        CheckCreateTenant(runtime, TENANT1_1_NAME,
                          Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          TVector<std::pair<TString, TString>>({{"name1", "value1"}, {"name2", "value2"}}));


        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        UNIT_ASSERT(CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}, {"name2", "value2"}}));

        // Requests with wrong attributes.
        CheckCreateTenant(runtime, TENANT1_2_NAME,
                          Ydb::StatusIds::BAD_REQUEST,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          TVector<std::pair<TString, TString>>({{"", "value1"}}));
        CheckCreateTenant(runtime, TENANT1_2_NAME,
                          Ydb::StatusIds::BAD_REQUEST,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          TVector<std::pair<TString, TString>>({{"name1", ""}}));

        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 0,
                              Ydb::StatusIds::SUCCESS, {}, {},
                              TString(),
                              TVector<std::pair<TString, TString>>({{"name2", "value2_2"},{"name3", "value3"}}));

        int cnt = 1000;
        while (--cnt) {
            if (CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}, {"name3", "value3"}}, true))
                break;
            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_C(cnt, "attribute was has not been found");

        CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}, {"name2", "value2_2"}, {"name3", "value3"}});
    }

    void RunTestRemoveAttributes(TTenantTestRuntime& runtime) {
        // Create tenant with attrs and check subdomain has them attached.
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
        CheckCreateTenant(runtime, TENANT1_1_NAME,
                          Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}, {"hdd-1", 3}},
                          TVector<std::pair<TString, TString>>({{"name1", "value1"}, {"name2", "value2"}}));

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        UNIT_ASSERT(CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}, {"name2", "value2"}}));

        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 0,
                              Ydb::StatusIds::SUCCESS, {}, {},
                              TString(),
                              TVector<std::pair<TString, TString>>({{"name2", TString()}}));

        int cnt = 1000;
        while (--cnt) {
            if (!CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}, {"name2", "value2"}}, true))
                break;
            TDispatchOptions options;
            runtime.DispatchEvents(options, TDuration::MilliSeconds(100));
        }

        CheckAttrsPresent(runtime, TENANT1_1_NAME, THashMap<TString, TString> {{"name1", "value1"}});
    }

    Y_UNIT_TEST(TestAttributes) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAttributes(runtime);
    }

    Y_UNIT_TEST(TestAttributesExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAttributes(runtime);
    }

    Y_UNIT_TEST(TestRemoveAttributes) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestRemoveAttributes(runtime);
    }

    Y_UNIT_TEST(TestRemoveAttributesExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestRemoveAttributes(runtime);
    }

    void RunTestTenantGeneration(TTenantTestRuntime& runtime) {
        CheckCreateTenant(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 1);
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 1);

        RestartConsole(runtime);
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 1);

        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 2,
                              Ydb::StatusIds::BAD_REQUEST,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {});
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 1);

        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 1,
                              Ydb::StatusIds::SUCCESS,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {},
                              "alter-key-1");
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 2);

        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 1,
                              Ydb::StatusIds::BAD_REQUEST,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {});

        // Repeating the same idempotency key should result in a success
        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 1,
                              Ydb::StatusIds::SUCCESS,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {},
                              "alter-key-1");
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 2);

        RestartConsole(runtime);
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 2);

        // Repeating the same idempotency key should result in a success after restart
        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 1,
                              Ydb::StatusIds::SUCCESS,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {},
                              "alter-key-1");
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 2);

        // Alter tenant without setting an idempotency key
        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 2,
                              Ydb::StatusIds::SUCCESS,
                              {{ {SLOT1_TYPE, ZONE1, 2} }}, {});
        CheckTenantGeneration(runtime, TENANT1_1_NAME, 3);

        // As a side effect we forget old idempotency keys
        CheckAlterTenantSlots(runtime, TENANT1_1_NAME, 1,
                              Ydb::StatusIds::BAD_REQUEST,
                              {{ {SLOT1_TYPE, ZONE1, 1} }}, {},
                              "alter-key-1");
    }

    Y_UNIT_TEST(TestTenantGeneration) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestTenantGeneration(runtime);
    }

    Y_UNIT_TEST(TestTenantGenerationExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestTenantGeneration(runtime);
    }

    Y_UNIT_TEST(TestSchemeShardErrorForwarding) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        // Unauthorized create subdomain.
        CheckCreateTenant(runtime, "/dc-1/db-1",
                          NACLib::TUserToken("user1@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::UNAUTHORIZED,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 1);

        // Unauthorized create directory.
        CheckCreateTenant(runtime, "/dc-1/subdir/db-1",
                          NACLib::TUserToken("user1@builtin", {}).SerializeAsString(),
                          Ydb::StatusIds::UNAUTHORIZED,
                          {{"hdd", 1}},
                          SLOT1_TYPE, ZONE1, 1);
    }

    void RunTestAlterTenantTooManyStorageResourcesForRunning(TTenantTestRuntime& runtime) {
        using EType = TCreateTenantRequest::EType;

        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, EType::Common)
                .WithPools({{"hdd", 1}, {"hdd-1", 3}})
                .WithPlanResolution(500));

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd-1", 1000}}, {});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {}, false);

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}}, {});

        CheckAlterTenantPools(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                              {{"hdd-2", 1000}});

        CheckTenantStatus(runtime, TENANT1_1_NAME, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 3, 3}, {"hdd-2", 0, 0}}, {});
    }

    Y_UNIT_TEST(TestAlterTenantTooManyStorageResourcesForRunning) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestAlterTenantTooManyStorageResourcesForRunning(runtime);
    }

    Y_UNIT_TEST(TestAlterTenantTooManyStorageResourcesForRunningExtSubdomain) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig(), {}, true);
        RunTestAlterTenantTooManyStorageResourcesForRunning(runtime);
    }

    void RunTestDatabaseQuotas(TTenantTestRuntime& runtime, const TString& quotas, bool shared = false) {
        using EType = TCreateTenantRequest::EType;

        CheckCreateTenant(runtime, Ydb::StatusIds::SUCCESS,
            TCreateTenantRequest(TENANT1_1_NAME, shared ? EType::Shared : EType::Common)
                .WithPools({{"hdd", 1}, {"hdd-1", 1}})
                .WithDatabaseQuotas(quotas)
        );

        RestartTenantPool(runtime);

        CheckTenantStatus(runtime, TENANT1_1_NAME, shared, Ydb::StatusIds::SUCCESS,
                          Ydb::Cms::GetDatabaseStatusResult::RUNNING,
                          {{"hdd", 1, 1}, {"hdd-1", 1, 1}}, {});
    }

    Y_UNIT_TEST(TestDatabaseQuotas) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        RunTestDatabaseQuotas(runtime, DefaultDatabaseQuotas());
    }

    Y_UNIT_TEST(TestDatabaseQuotasBadOverallQuota) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST,
            TCreateTenantRequest(TENANT1_1_NAME, TCreateTenantRequest::EType::Common)
                .WithPools({{"hdd", 1}})
                .WithDatabaseQuotas(R"(
                        data_size_hard_quota: 1
                        data_size_soft_quota: 1000
                    )"
                )
        );
    }

    Y_UNIT_TEST(TestDatabaseQuotasBadStorageQuota) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());

        CheckCreateTenant(runtime, Ydb::StatusIds::BAD_REQUEST,
            TCreateTenantRequest(TENANT1_1_NAME, TCreateTenantRequest::EType::Common)
                .WithPools({{"hdd", 1}})
                .WithDatabaseQuotas(R"(
                        storage_quotas {
                            unit_kind: "hdd"
                            data_size_hard_quota: 1
                            data_size_soft_quota: 1000
                        }
                    )"
                )
        );
    }
}

} // namespace NKikimr
