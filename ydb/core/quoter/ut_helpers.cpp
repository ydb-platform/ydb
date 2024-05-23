#include "ut_helpers.h"

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/kesus/tablet/events.h>

namespace NKikimr {

const TString TKesusQuoterTestSetup::DEFAULT_KESUS_PARENT_PATH = Tests::TestDomainName;
const TString TKesusQuoterTestSetup::DEFAULT_KESUS_NAME = "KesusQuoter";
const TString TKesusQuoterTestSetup::DEFAULT_KESUS_PATH = TString::Join("/", DEFAULT_KESUS_PARENT_PATH, "/", DEFAULT_KESUS_NAME);
const TString TKesusQuoterTestSetup::DEFAULT_KESUS_RESOURCE = "Resource";

TKesusQuoterTestSetup::TKesusQuoterTestSetup(bool runServer)
    : MsgBusPort(PortManager.GetPort())
    , ServerSettings(MakeIntrusive<Tests::TServerSettings>(MsgBusPort))
{
    // Settings
    GetServerSettings()
        .SetNodeCount(2);

    if (runServer) {
        RunServer();
    }
}

void TKesusQuoterTestSetup::RunServer() {
    Server = MakeIntrusive<Tests::TServer>(ServerSettings, true);
    Client = MakeHolder<Tests::TClient>(*ServerSettings);

    SetupLogging();

    Client->InitRootScheme();

    RegisterQuoterService();
    CreateDefaultKesusAndResource();
}

void TKesusQuoterTestSetup::SetupLogging() {
    Server->GetRuntime()->SetLogPriority(NKikimrServices::KESUS_TABLET, NActors::NLog::PRI_TRACE);
    Server->GetRuntime()->SetLogPriority(NKikimrServices::QUOTER_SERVICE, NActors::NLog::PRI_TRACE);
    Server->GetRuntime()->SetLogPriority(NKikimrServices::QUOTER_PROXY, NActors::NLog::PRI_TRACE);
}

void TKesusQuoterTestSetup::RegisterQuoterService() {
    TTestActorRuntime* const runtime = GetServer().GetRuntime();
    const TActorId quoterServiceActorId = runtime->Register(CreateQuoterService());
    runtime->RegisterService(MakeQuoterServiceID(), quoterServiceActorId);
}

void TKesusQuoterTestSetup::CreateKesus(const TString& parent, const TString& name, NMsgBusProxy::EResponseStatus expectedStatus) {
    const NMsgBusProxy::EResponseStatus status = GetClient().CreateKesus(parent, name);
    UNIT_ASSERT_VALUES_EQUAL_C(status, expectedStatus, "Expected status: " << expectedStatus);
}

ui64 TKesusQuoterTestSetup::GetKesusTabletId(const TString& path) {
    TAutoPtr<NMsgBusProxy::TBusResponse> resp = Client->Ls(path);
    UNIT_ASSERT_EQUAL(resp->Record.GetStatusCode(), NKikimrIssues::TStatusIds::SUCCESS);
    const auto& pathDesc = resp->Record.GetPathDescription();
    UNIT_ASSERT(pathDesc.HasKesus());
    const ui64 tabletId = pathDesc.GetKesus().GetKesusTabletId();
    UNIT_ASSERT(tabletId);
    return tabletId;
}

NKikimrKesus::THierarchicalDRRResourceConfig TKesusQuoterTestSetup::MakeDefaultResourceProps() {
    NKikimrKesus::THierarchicalDRRResourceConfig ret;
    ret.SetMaxUnitsPerSecond(10);
    return ret;
}

void TKesusQuoterTestSetup::CreateKesusResource(const TString& kesusPath, const TString& resourcePath, const NKikimrKesus::THierarchicalDRRResourceConfig& cfg) {
    TTestActorRuntime* const runtime = Server->GetRuntime();

    TAutoPtr<NKesus::TEvKesus::TEvAddQuoterResource> request(new NKesus::TEvKesus::TEvAddQuoterResource());
    request->Record.MutableResource()->SetResourcePath(resourcePath);
    *request->Record.MutableResource()->MutableHierarchicalDRRResourceConfig() = cfg;

    TActorId sender = GetEdgeActor();
    Cerr << "AddQuoterResource: " << request->Record << Endl;
    ForwardToTablet(*runtime, GetKesusTabletId(kesusPath), sender, request.Release(), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvAddQuoterResourceResult>(handle);
    const NKikimrKesus::TEvAddQuoterResourceResult& record = handle->Get<NKesus::TEvKesus::TEvAddQuoterResourceResult>()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
}

void TKesusQuoterTestSetup::CreateDefaultKesusAndResource() {
    CreateKesus(DEFAULT_KESUS_PARENT_PATH, DEFAULT_KESUS_NAME);
    CreateKesusResource(DEFAULT_KESUS_PATH, DEFAULT_KESUS_RESOURCE);
}

void TKesusQuoterTestSetup::DeleteKesusResource(const TString& kesusPath, const TString& resourcePath) {
    TTestActorRuntime* const runtime = Server->GetRuntime();

    TAutoPtr<NKesus::TEvKesus::TEvDeleteQuoterResource> request(new NKesus::TEvKesus::TEvDeleteQuoterResource());
    request->Record.SetResourcePath(resourcePath);

    TActorId sender = GetEdgeActor();
    Cerr << "DeleteQuoterResource: " << request->Record << Endl;
    ForwardToTablet(*runtime, GetKesusTabletId(kesusPath), sender, request.Release(), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvDeleteQuoterResourceResult>(handle);
    const NKikimrKesus::TEvDeleteQuoterResourceResult& record = handle->Get<NKesus::TEvKesus::TEvDeleteQuoterResourceResult>()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
}

TActorId TKesusQuoterTestSetup::GetEdgeActor() {
    if (!EdgeActor) {
        EdgeActor = GetServer().GetRuntime()->AllocateEdgeActor(0);
    }
    return EdgeActor;
}

void TKesusQuoterTestSetup::GetQuota(const std::vector<std::tuple<TString, TString, ui64>>& resources, TEvQuota::EResourceOperator operation, TDuration deadline, TEvQuota::TEvClearance::EResult expectedResult) {
    SendGetQuotaRequest(resources, operation, deadline);
    auto answer = WaitGetQuotaAnswer();
    UNIT_ASSERT_VALUES_EQUAL(answer->Result, expectedResult);
}

void TKesusQuoterTestSetup::GetQuota(const TString& kesusPath, const TString& resourcePath, ui64 amount, TDuration deadline, TEvQuota::TEvClearance::EResult expectedResult) {
    GetQuota({{kesusPath, resourcePath, amount}}, TEvQuota::EResourceOperator::And, deadline, expectedResult);
}

void TKesusQuoterTestSetup::GetQuota(const TString& kesusPath, const TString& resourcePath, ui64 amount, TEvQuota::TEvClearance::EResult expectedResult) {
    GetQuota(kesusPath, resourcePath, amount, TDuration::Max(), expectedResult);
}

void TKesusQuoterTestSetup::SendGetQuotaRequest(const std::vector<std::tuple<TString, TString, ui64>>& resources, TEvQuota::EResourceOperator operation, TDuration deadline) {
    TVector<TEvQuota::TResourceLeaf> res;
    res.reserve(resources.size());
    for (auto&& [kesusPath, resourcePath, amount] : resources) {
        res.emplace_back(kesusPath, resourcePath, amount);
    }
    GetServer().GetRuntime()->Send(new IEventHandle(MakeQuoterServiceID(), GetEdgeActor(), new TEvQuota::TEvRequest(operation, std::move(res), deadline)));
}

void TKesusQuoterTestSetup::SendGetQuotaRequest(const TString& kesusPath, const TString& resourcePath, ui64 amount) {
    SendGetQuotaRequest(kesusPath, resourcePath, amount, TDuration::Max());
}

void TKesusQuoterTestSetup::SendGetQuotaRequest(const TString& kesusPath, const TString& resourcePath, ui64 amount, TDuration deadline) {
    SendGetQuotaRequest({{kesusPath, resourcePath, amount}}, TEvQuota::EResourceOperator::And, deadline);
}

THolder<TEvQuota::TEvClearance> TKesusQuoterTestSetup::WaitGetQuotaAnswer() {
    return GetServer().GetRuntime()->GrabEdgeEvent<TEvQuota::TEvClearance>();
}

void TKesusQuoterTestSetup::KillKesusTablet(const TString& kesusPath) {
    TTestActorRuntime* const runtime = Server->GetRuntime();

    TActorId sender = GetEdgeActor();
    Cerr << "Kill kesus tablet: " << kesusPath << Endl;
    ForwardToTablet(*runtime, GetKesusTabletId(kesusPath), sender, new TEvents::TEvPoisonPill(), 0);
}

NKikimrKesus::TEvGetQuoterResourceCountersResult TKesusQuoterTestSetup::GetQuoterCounters(const TString& kesusPath) {
    TTestActorRuntime* const runtime = Server->GetRuntime();

    ForwardToTablet(*runtime, GetKesusTabletId(kesusPath), GetEdgeActor(), new NKesus::TEvKesus::TEvGetQuoterResourceCounters(), 0);

    TAutoPtr<IEventHandle> handle;
    runtime->GrabEdgeEvent<NKesus::TEvKesus::TEvGetQuoterResourceCountersResult>(handle);
    NKikimrKesus::TEvGetQuoterResourceCountersResult record = handle->Get<NKesus::TEvKesus::TEvGetQuoterResourceCountersResult>()->Record;
    std::sort(record.MutableResourceCounters()->begin(), record.MutableResourceCounters()->end(),
        [] (const auto& r1, const auto& r2) {
            return r1.GetResourcePath() < r2.GetResourcePath();
        }
    );
    Cerr << (TStringBuilder() << "Kesus quoter counters: " << record << Endl);
    return record;
}

TKesusProxyTestSetup::TKesusProxyTestSetup() {
    Start();
}

TTestActorRuntime::TEgg MakeEgg() {
    return { new TAppData(0, 0, 0, 0, { }, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {} };
}

void TKesusProxyTestSetup::Start() {
    Runtime = MakeHolder<TTestActorRuntime>();
    Runtime->Initialize(MakeEgg());
    SetupLogging();

    Runtime->UpdateCurrentTime(TInstant::Now());

    StartKesusProxy();
}

void TKesusProxyTestSetup::StartKesusProxy() {
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path.push_back("Path");
    entry.Path.push_back("KesusName");
    auto kesusInfo = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TKesusInfo>();
    entry.KesusInfo = kesusInfo;
    kesusInfo->Kind = NSchemeCache::TSchemeCacheNavigate::KindKesus;
    kesusInfo->Description.SetName("KesusName");
    kesusInfo->Description.SetKesusTabletId(KESUS_TABLET_ID);

    auto pipeFactory = MakeHolder<TTestTabletPipeFactory>(this);
    PipeFactory = pipeFactory.Get();
    KesusProxyId = Runtime->Register(CreateKesusQuoterProxy(QUOTER_ID, entry, GetEdgeActor(), std::move(pipeFactory)));
    Runtime->EnableScheduleForActor(KesusProxyId);
}

void TKesusProxyTestSetup::SetupLogging() {
    Runtime->SetLogPriority(NKikimrServices::KESUS_TABLET, NActors::NLog::PRI_TRACE);
    Runtime->SetLogPriority(NKikimrServices::QUOTER_SERVICE, NActors::NLog::PRI_TRACE);
    Runtime->SetLogPriority(NKikimrServices::QUOTER_PROXY, NActors::NLog::PRI_TRACE);
}

TActorId TKesusProxyTestSetup::GetEdgeActor() {
    if (!EdgeActor) {
        EdgeActor = Runtime->AllocateEdgeActor(0);
    }
    return EdgeActor;
}

TActorId TKesusProxyTestSetup::GetPipeEdgeActor() {
    if (!PipeEdgeActor) {
        PipeEdgeActor = Runtime->AllocateEdgeActor(0);
    }
    return PipeEdgeActor;
}

void TKesusProxyTestSetup::WaitProxyStart() {
    if (PipeFactory->GetPipesCreatedCount() == 0) {
        WaitPipesCreated(1);
    }
}

void TKesusProxyTestSetup::SendNotConnected(TTestTabletPipeFactory::TTestTabletPipe* pipe) {
    WaitProxyStart();
    Runtime->Send(
        new IEventHandle(
            KesusProxyId, pipe->GetSelfID(),
            new TEvTabletPipe::TEvClientConnected(KESUS_TABLET_ID, NKikimrProto::ERROR, pipe->GetSelfID(), TActorId(), true, false, 0)
        ),
        0, true
    );
}

void TKesusProxyTestSetup::SendConnected(TTestTabletPipeFactory::TTestTabletPipe* pipe) {
    WaitProxyStart();
    Runtime->Send(
        new IEventHandle(
            KesusProxyId, pipe->GetSelfID(),
            new TEvTabletPipe::TEvClientConnected(KESUS_TABLET_ID, NKikimrProto::OK, pipe->GetSelfID(), pipe->GetSelfID(), true, false, 0)
        ), 0, true
    );
}

void TKesusProxyTestSetup::SendDestroyed(TTestTabletPipeFactory::TTestTabletPipe* pipe) {
    WaitProxyStart();
    Runtime->Send(new IEventHandle(KesusProxyId, pipe->GetSelfID(), new TEvTabletPipe::TEvClientDestroyed(KESUS_TABLET_ID, pipe->GetSelfID(), pipe->GetSelfID())), 0, true);
}

void TKesusProxyTestSetup::WaitPipesCreated(size_t count) {
    UNIT_ASSERT(PipeFactory);
    TDispatchOptions pipesCreated;
    pipesCreated.CustomFinalCondition = [this, count] {
        return PipeFactory->GetPipesCreatedCount() >= count;
    };
    Runtime->DispatchEvents(pipesCreated);
}

void TKesusProxyTestSetup::WaitEvent(ui32 eventType, ui32 requiredCount) {
    TDispatchOptions waitOpt;
    waitOpt.FinalEvents.push_back(
        TDispatchOptions::TFinalEventCondition(
            [eventType](IEventHandle& ev) {
                return
                    ev.GetTypeRewrite() == eventType
                    || ev.Type == eventType; // Events forwarded to pipe
            },
            requiredCount));
    GetRuntime().DispatchEvents(waitOpt);
}

ui32 TKesusProxyTestSetup::WaitEvent(const THashSet<ui32>& eventTypes, ui32 requiredCount) {
    TDispatchOptions waitOpt;
    ui32 firedEvent = 0;
    waitOpt.FinalEvents.push_back(
        TDispatchOptions::TFinalEventCondition(
            [&eventTypes, &firedEvent](IEventHandle& ev) {
                if (IsIn(eventTypes, ev.GetTypeRewrite())) {
                    firedEvent = ev.GetTypeRewrite();
                    return true;
                }
                if (ev.Type != ev.GetTypeRewrite() && IsIn(eventTypes, ev.Type)) { // Events forwarded to pipe
                    firedEvent = ev.Type;
                    return true;
                }
                return false;
            },
            requiredCount));
    GetRuntime().DispatchEvents(waitOpt);
    return firedEvent;
}

void TKesusProxyTestSetup::WaitConnected() {
    WaitEvent<TEvTabletPipe::TEvClientConnected>();
}

void TKesusProxyTestSetup::SendProxyRequest(const TString& resourceName) {
    WaitProxyStart();
    Runtime->Send(new IEventHandle(KesusProxyId, GetEdgeActor(), new TEvQuota::TEvProxyRequest(resourceName)), 0, true);
}

THolder<TEventHandle<TEvQuota::TEvProxySession>> TKesusProxyTestSetup::ProxyRequest(const TString& resourceName, TEvQuota::TEvProxySession::EResult expectedResult) {
    SendProxyRequest(resourceName);

    TAutoPtr<IEventHandle> handle;
    TEvQuota::TEvProxySession* ret = Runtime->GrabEdgeEvent<TEvQuota::TEvProxySession>(handle);
    UNIT_ASSERT_EQUAL_C(ret->Result, expectedResult, "Actual result: " << static_cast<int>(ret->Result) << ", but expected: " << static_cast<int>(expectedResult));
    return THolder<TEventHandle<TEvQuota::TEvProxySession>>{static_cast<TEventHandle<TEvQuota::TEvProxySession>*>(handle.Release())};
}

void TKesusProxyTestSetup::SendProxyStats(TDeque<TEvQuota::TProxyStat> stats) {
    WaitProxyStart();
    Runtime->Send(new IEventHandle(KesusProxyId, GetEdgeActor(), new TEvQuota::TEvProxyStats(std::move(stats))), 0, true);
}

THolder<TEventHandle<TEvQuota::TEvProxyUpdate>> TKesusProxyTestSetup::GetProxyUpdate() {
    TAutoPtr<IEventHandle> handle;
    Runtime->GrabEdgeEvent<TEvQuota::TEvProxyUpdate>(handle);
    return THolder<TEventHandle<TEvQuota::TEvProxyUpdate>>{static_cast<TEventHandle<TEvQuota::TEvProxyUpdate>*>(handle.Release())};
}

void TKesusProxyTestSetup::SendCloseSession(const TString& resource, ui64 resourceId) {
    WaitProxyStart();
    Runtime->Send(new IEventHandle(KesusProxyId, GetEdgeActor(), new TEvQuota::TEvProxyCloseSession(resource, resourceId)), 0, true);
}

void TKesusProxyTestSetup::SendResourcesAllocated(TTestTabletPipeFactory::TTestTabletPipe* pipe, ui64 resId, double amount, Ydb::StatusIds::StatusCode status) {
    auto ev = std::make_unique<NKesus::TEvKesus::TEvResourcesAllocated>();
    auto* resInfo = ev->Record.AddResourcesInfo();
    resInfo->SetResourceId(resId);
    resInfo->SetAmount(amount);
    resInfo->MutableStateNotification()->SetStatus(status);

    Runtime->Send(new IEventHandle(KesusProxyId, pipe->GetSelfID(), ev.release(), 0, true));
}

bool TKesusProxyTestSetup::ConsumeResource(ui64 resId, double amount, TDuration tickSize, std::function<void()> afterStat, const size_t maxUpdates) {
    UNIT_ASSERT(maxUpdates > 0);
    UNIT_ASSERT(resId > 0);
    size_t updatesGot = 0;
    const TInstant start = Runtime->GetCurrentTime();
    while (updatesGot < maxUpdates) {
        ++updatesGot;
        SendProxyStats({TEvQuota::TProxyStat(resId, 1, 0, {}, 1, amount, 0, 0)});
        WaitEvent<TEvQuota::TEvProxyStats>(); // wait event to be processed

        afterStat();

        for (size_t i = 0; i < 2; ++i) { // The first is for TEvProxyStats answer, the second is for real answer
            auto update = GetProxyUpdate();
            UNIT_ASSERT_VALUES_EQUAL(update->Get()->QuoterId, resId);
            UNIT_ASSERT_GT_C(update->Get()->Resources.size(), 0, "Resources count: " << update->Get()->Resources.size());
            bool found = false;
            for (const auto& res : update->Get()->Resources) {
                UNIT_ASSERT(res.ResourceId);
                if (res.ResourceId == resId) {
                    found = true;
                    UNIT_ASSERT_VALUES_EQUAL(res.ResourceState, TEvQuota::EUpdateState::Normal);

                    UNIT_ASSERT_VALUES_EQUAL(res.Update.size(), 1);
                    const auto& updateTick = res.Update.front();
                    UNIT_ASSERT_VALUES_EQUAL(updateTick.Channel, 0);
                    UNIT_ASSERT_VALUES_EQUAL(updateTick.Policy, TEvQuota::ETickPolicy::Front);
                    const bool noAmount = updateTick.Rate == 0 && updateTick.Ticks == 0;
                    if (!noAmount) {
                        UNIT_ASSERT(res.SustainedRate > 0);
                        UNIT_ASSERT_VALUES_EQUAL(updateTick.Ticks, 2);
                        UNIT_ASSERT(updateTick.Rate > 0);
                    }

                    const TDuration timeToCharge = amount / updateTick.Rate * tickSize;
                    if (Runtime->GetCurrentTime() - start >= timeToCharge || amount <= updateTick.Rate) {
                        // spend and exit
                        SendProxyStats({TEvQuota::TProxyStat(resId, 1, amount, {}, 0, 0, 0, 0)});
                        return true;
                    }
                }
            }
            UNIT_ASSERT(found);
        }
    }
    return false;
}

TKesusProxyTestSetup::TTestTabletPipeFactory::~TTestTabletPipeFactory() {
    if (NextPipe < PipesExpectedToCreate.size()) {
        UNIT_FAIL_NONFATAL("Expected " << PipesExpectedToCreate.size() << " kesus tablet pipe creations, but actually were only " << NextPipe << " ones");
    }
    for (size_t i = NextPipe; i < PipesExpectedToCreate.size(); ++i) {
        delete PipesExpectedToCreate[i];
    }
    PipesExpectedToCreate.clear();
}

IActor* TKesusProxyTestSetup::TTestTabletPipeFactory::CreateTabletPipe(const NActors::TActorId& owner, ui64 tabletId, const NKikimr::NTabletPipe::TClientConfig&) {
    UNIT_ASSERT(owner);
    UNIT_ASSERT_VALUES_EQUAL(owner, Parent->KesusProxyId);
    UNIT_ASSERT(tabletId);
    UNIT_ASSERT_VALUES_EQUAL(tabletId, KESUS_TABLET_ID);
    if (NextPipe >= PipesExpectedToCreate.size()) {
        ExpectTabletPipeConnection();
        UNIT_ASSERT(NextPipe < PipesExpectedToCreate.size());
    }
    return PipesExpectedToCreate[NextPipe++];
}

TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe* TKesusProxyTestSetup::TTestTabletPipeFactory::ExpectTabletPipeCreation(bool wait) {
    THolder<TTestTabletPipe> pipe = MakeHolder<TTestTabletPipe>(this);
    TTestTabletPipe* ret = pipe.Get();
    PipesExpectedToCreate.push_back(pipe.Release());
    if (wait) {
        Parent->WaitPipesCreated(PipesExpectedToCreate.size());
    }
    return ret;
}

TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe* TKesusProxyTestSetup::TTestTabletPipeFactory::ExpectTabletPipeConnection() {
    TTestTabletPipe* pipe = ExpectTabletPipeCreation();
    EXPECT_CALL(*pipe, OnStart())
        .WillOnce(Invoke(pipe, &TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendConnected));
    return pipe;
}

TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::TTestTabletPipe(TTestTabletPipeFactory* parent)
    : Parent(parent)
{
}

STFUNC(TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        FFunc(TEvTabletPipe::EvSend, HandleSend);
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoisonPill);
    default:
        UNIT_ASSERT_C(false, "Unexpected event got in tablet pipe: " << ev->GetTypeRewrite());
    }
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::Bootstrap(const TActorContext& ctx) {
    SelfID = ctx.SelfID;
    Become(&TTestTabletPipe::StateFunc);
    OnStart();
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::HandlePoisonPill() {
    IsDead = true;
    OnPoisonPill();
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::HandleSend(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    Y_UNUSED(ctx);
    ev->DropRewrite();
    switch (ev->GetTypeRewrite()) {
        hFunc(NKesus::TEvKesus::TEvSubscribeOnResources,   HandleSubscribeOnResources);
        hFunc(NKesus::TEvKesus::TEvUpdateConsumptionState, HandleUpdateConsumptionState);
        hFunc(NKesus::TEvKesus::TEvResourcesAllocatedAck,  HandleResourcesAllocatedAck);
    default:
        UNIT_ASSERT_C(false, "Unexpected send event got in tablet pipe: " << ev->GetTypeRewrite());
    }
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::HandleSubscribeOnResources(NKesus::TEvKesus::TEvSubscribeOnResources::TPtr& ev) {
    const TActorId proxy = ActorIdFromProto(ev->Get()->Record.GetActorID());
    UNIT_ASSERT_VALUES_EQUAL(proxy, Parent->Parent->KesusProxyId);

    OnSubscribeOnResources(ev->Get()->Record, ev->Cookie);
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::HandleUpdateConsumptionState(NKesus::TEvKesus::TEvUpdateConsumptionState::TPtr& ev) {
    OnUpdateConsumptionState(ev->Get()->Record, ev->Cookie);
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::HandleResourcesAllocatedAck(NKesus::TEvKesus::TEvResourcesAllocatedAck::TPtr& ev) {
    OnResourcesAllocatedAck(ev->Get()->Record, ev->Cookie);
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendNotConnected() {
    Send(
        Parent->Parent->KesusProxyId,
        new TEvTabletPipe::TEvClientConnected(KESUS_TABLET_ID, NKikimrProto::ERROR, SelfID, TActorId(), true, false, 0)
    );
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendConnected() {
    Send(
        Parent->Parent->KesusProxyId,
        new TEvTabletPipe::TEvClientConnected(KESUS_TABLET_ID, NKikimrProto::OK, SelfID, SelfID, true, false, 0)
    );
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendDestroyed() {
    Send(Parent->Parent->KesusProxyId, new TEvTabletPipe::TEvClientDestroyed(KESUS_TABLET_ID, SelfID, SelfID));
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendSubscribeOnResourceResult(const NKikimrKesus::TEvSubscribeOnResourcesResult& record, ui64 cookie) {
    auto ev = std::make_unique<NKesus::TEvKesus::TEvSubscribeOnResourcesResult>();
    ev->Record.CopyFrom(record);
    Send(Parent->Parent->KesusProxyId, ev.release(), 0, cookie);
}

void TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::SendUpdateConsumptionStateAck() {
    auto ev = std::make_unique<NKesus::TEvKesus::TEvUpdateConsumptionStateAck>();
    Send(Parent->Parent->KesusProxyId, ev.release());
}

THolder<IEventHandle> TKesusProxyTestSetup::TTestTabletPipeFactory::TTestTabletPipe::GetDestroyedEventHandle() {
    return MakeHolder<IEventHandle>(Parent->Parent->KesusProxyId, SelfID, new TEvTabletPipe::TEvClientDestroyed(KESUS_TABLET_ID, SelfID, SelfID));
}

} // namespace NKikimr
