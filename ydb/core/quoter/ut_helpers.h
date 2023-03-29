#pragma once
#include "quoter_service.h"
#include "quoter_service_impl.h"
#include "kesus_quoter_proxy.h"

#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/kesus.pb.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>

namespace NKikimr {

using namespace testing; // gtest

class TKesusQuoterTestSetup {
public:
    static const TString DEFAULT_KESUS_PARENT_PATH;
    static const TString DEFAULT_KESUS_NAME;

    static const TString DEFAULT_KESUS_PATH;
    static const TString DEFAULT_KESUS_RESOURCE;

    TKesusQuoterTestSetup(bool runServer = true);

    void RunServer();

    Tests::TServerSettings& GetServerSettings() {
        return *ServerSettings;
    }

    Tests::TServer& GetServer() {
        UNIT_ASSERT(Server);
        return *Server;
    }

    Tests::TClient& GetClient() {
        UNIT_ASSERT(Client);
        return *Client;
    }

    static NKikimrKesus::THierarchicalDRRResourceConfig MakeDefaultResourceProps();

    void CreateKesus(const TString& parent, const TString& name, NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK);
    void KillKesusTablet(const TString& kesusPath = DEFAULT_KESUS_PATH);

    void CreateKesusResource(const TString& kesusPath = DEFAULT_KESUS_PATH, const TString& resourcePath = DEFAULT_KESUS_RESOURCE, const NKikimrKesus::THierarchicalDRRResourceConfig& cfg = MakeDefaultResourceProps());
    void CreateDefaultKesusAndResource();

    void DeleteKesusResource(const TString& kesusPath = DEFAULT_KESUS_PATH, const TString& resourcePath = DEFAULT_KESUS_RESOURCE);

    NKikimrKesus::TEvGetQuoterResourceCountersResult GetQuoterCounters(const TString& kesusPath);

    void GetQuota(const std::vector<std::tuple<TString, TString, ui64>>& resources, TEvQuota::EResourceOperator operation = TEvQuota::EResourceOperator::And, TDuration deadline = TDuration::Max(), TEvQuota::TEvClearance::EResult expectedResult = TEvQuota::TEvClearance::EResult::Success);
    void GetQuota(const TString& kesusPath, const TString& resourcePath, ui64 amount = 1, TEvQuota::TEvClearance::EResult expectedResult = TEvQuota::TEvClearance::EResult::Success);
    void GetQuota(const TString& kesusPath, const TString& resourcePath, ui64 amount, TDuration deadline, TEvQuota::TEvClearance::EResult expectedResult = TEvQuota::TEvClearance::EResult::Success);

    void SendGetQuotaRequest(const std::vector<std::tuple<TString, TString, ui64>>& resources, TEvQuota::EResourceOperator operation = TEvQuota::EResourceOperator::And, TDuration deadline = TDuration::Max());
    void SendGetQuotaRequest(const TString& kesusPath, const TString& resourcePath, ui64 amount = 1);
    void SendGetQuotaRequest(const TString& kesusPath, const TString& resourcePath, ui64 amount, TDuration deadline);

    THolder<TEvQuota::TEvClearance> WaitGetQuotaAnswer();

    TActorId GetEdgeActor();

private:
    void SetupLogging();
    void RegisterQuoterService();

    ui64 GetKesusTabletId(const TString& path);

private:
    TPortManager PortManager;
    const ui16 MsgBusPort;
    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    TActorId EdgeActor;
};

class TKesusProxyTestSetup {
public:
    static constexpr ui64 QUOTER_ID = 42;
    static constexpr ui64 KESUS_TABLET_ID = 100500;

    class TTestTabletPipeFactory : public NQuoter::ITabletPipeFactory {
    public:
        class TTestTabletPipe;

    public:
        explicit TTestTabletPipeFactory(TKesusProxyTestSetup* parent)
            : Parent(parent)
        {
        }

        ~TTestTabletPipeFactory();

        IActor* CreateTabletPipe(const NActors::TActorId& owner, ui64 tabletId, const NKikimr::NTabletPipe::TClientConfig& config) override;

        TTestTabletPipe* ExpectTabletPipeCreation(bool wait = false); // Set expectation to creation of a new pipe.
        TTestTabletPipe* ExpectTabletPipeConnection(); // Set expectation of creation and connecting to new pipe. If no expectation for a pipe is set this is set by default.

        const std::vector<TTestTabletPipe*>& GetPipes() const {
            return PipesExpectedToCreate;
        }

        size_t GetPipesCreatedCount() const {
            return NextPipe;
        }

        class TTestTabletPipe : public TActorBootstrapped<TTestTabletPipe> {
        public:
            TTestTabletPipe(TTestTabletPipeFactory* parent);

            MOCK_METHOD(void, OnStart, (), ());
            MOCK_METHOD(void, OnPoisonPill, (), ());
            MOCK_METHOD(void, OnSubscribeOnResources, (const NKikimrKesus::TEvSubscribeOnResources&, ui64 cookie), ());
            MOCK_METHOD(void, OnUpdateConsumptionState, (const NKikimrKesus::TEvUpdateConsumptionState&, ui64 cookie), ());
            MOCK_METHOD(void, OnResourcesAllocatedAck, (const NKikimrKesus::TEvResourcesAllocatedAck&, ui64 cookie), ());

            void SendNotConnected();
            void SendConnected();
            void SendDestroyed();

            void SendSubscribeOnResourceResult(const NKikimrKesus::TEvSubscribeOnResourcesResult& record, ui64 cookie);
            void SendUpdateConsumptionStateAck();

            THolder<IEventHandle> GetDestroyedEventHandle();

            const TActorId& GetSelfID() const {
                return SelfID;
            }

        public:
            void Bootstrap(const TActorContext& ctx);

        private:
            void HandlePoisonPill();
            void HandleSend(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

            void HandleSubscribeOnResources(NKesus::TEvKesus::TEvSubscribeOnResources::TPtr& ev);
            void HandleUpdateConsumptionState(NKesus::TEvKesus::TEvUpdateConsumptionState::TPtr& ev);
            void HandleResourcesAllocatedAck(NKesus::TEvKesus::TEvResourcesAllocatedAck::TPtr& ev);

            STFUNC(StateFunc);

        private:
            TTestTabletPipeFactory* Parent;
            TActorId SelfID;
            bool IsDead = false;
        };

    private:
        TKesusProxyTestSetup* Parent;
        size_t NextPipe = 0;
        std::vector<TTestTabletPipe*> PipesExpectedToCreate;
    };

public:
    TKesusProxyTestSetup();

    TTestTabletPipeFactory& GetPipeFactory() {
        UNIT_ASSERT(PipeFactory);
        return *PipeFactory;
    }

    TTestActorRuntime& GetRuntime() {
        return *Runtime;
    }

    const TActorId& GetKesusProxyId() const {
        return KesusProxyId;
    }

    void WaitProxyStart();

    TActorId GetEdgeActor();
    TActorId GetPipeEdgeActor();

    void SendProxyRequest(const TString& resourceName);
    THolder<TEventHandle<TEvQuota::TEvProxySession>> ProxyRequest(const TString& resourceName, TEvQuota::TEvProxySession::EResult = TEvQuota::TEvProxySession::Success);

    void SendProxyStats(TDeque<TEvQuota::TProxyStat> stats);
    THolder<TEventHandle<TEvQuota::TEvProxyUpdate>> GetProxyUpdate();

    void SendCloseSession(const TString& resource, ui64 resourceId);

    void WaitEvent(ui32 eventType, ui32 requiredCount = 1);

    ui32 WaitEvent(const THashSet<ui32>& eventTypes, ui32 requiredCount = 1); // returns fired event type

    template <class TEvent>
    void WaitEvent(ui32 requiredCount = 1) {
        WaitEvent(TEvent::EventType, requiredCount);
    }

    void WaitPipesCreated(size_t count);
    void WaitConnected();
    void SendNotConnected(TTestTabletPipeFactory::TTestTabletPipe* pipe);
    void SendConnected(TTestTabletPipeFactory::TTestTabletPipe* pipe);
    void SendDestroyed(TTestTabletPipeFactory::TTestTabletPipe* pipe);
    void SendResourcesAllocated(TTestTabletPipeFactory::TTestTabletPipe* pipe, ui64 resId, double amount, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Update/Stat exchange
    Y_WARN_UNUSED_RESULT bool ConsumeResource(ui64 resId, double amount, TDuration tickSize, std::function<void()> afterStat, size_t maxUpdates = 15);

    // Consume with Kesus allocation
    Y_WARN_UNUSED_RESULT bool ConsumeResourceAllocateByKesus(TTestTabletPipeFactory::TTestTabletPipe* pipe, ui64 resId, double amount, TDuration tickSize, size_t maxUpdates = 15) {
        return ConsumeResource(resId, amount, tickSize, [=] {
            Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
            SendResourcesAllocated(pipe, resId, 10);
        }, maxUpdates);
    }

    // Consume with offline allocation
    Y_WARN_UNUSED_RESULT bool ConsumeResourceAdvanceTime(ui64 resId, double amount, TDuration tickSize, size_t maxUpdates = 15) {
        return ConsumeResource(resId, amount, tickSize, [this] {
            Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
        }, maxUpdates);
    }

private:
    void Start();

    void StartKesusProxy();
    void SetupLogging();

private:
    THolder<TTestActorRuntime> Runtime;
    TActorId KesusProxyId;
    TTestTabletPipeFactory* PipeFactory = nullptr;
    TActorId EdgeActor;
    TActorId PipeEdgeActor;
};

} // namespace NKikimr
