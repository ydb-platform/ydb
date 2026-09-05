#include "dq_channel_service.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/threading/mux_event/mux_event.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/library/yql/dq/actors/dq.h>
#include <util/random/random.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_CHANNELS

using namespace NKikimr::NKqp;
using namespace NYql::NDq;

using namespace NYdb;
using namespace NYdb::NTable;

template<>
void Out<NYql::NDq::EDqFillLevel>(IOutputStream& os, const NYql::NDq::EDqFillLevel l) {
    os << static_cast<ui32>(l);
}

struct TEvTestPrivate {
    enum ERole {
        Producer,
        Consumer,
    };

    enum EEv {
        EvStart = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvStart : public NActors::TEventLocal<TEvStart, EvStart> {
        TEvStart(NActors::TActorId peerId) : PeerId(peerId) {}
        NActors::TActorId PeerId;
    };

    struct TEvFinished : public NActors::TEventLocal<TEvFinished, EvFinished> {
        TEvFinished(ERole role, bool error) : Role(role), Error(error) {}
        ERole Role;
        bool Error;
    };
};

// Tracks quota strictly - it is an error to free more bytes than were allocated (like the real
// TChannelQuotaManager does with VERIFY) and to leave anything allocated at the end of the test.
struct TTestQuotaManager : public IMemoryQuotaManager {

    bool AllocateQuota(ui64 memorySize, bool /* isOptional */) override {
        if (Quota.load() + static_cast<i64>(memorySize) > static_cast<i64>(Limit)) {
            return false;
        }
        Quota += memorySize;
        Allocated += memorySize;
        return true;
    }

    void FreeQuota(ui64 memorySize) override {
        if (Quota.fetch_sub(memorySize) < static_cast<i64>(memorySize)) {
            Underflows++;
        }
        Freed += memorySize;
    }

    ui64 GetCurrentQuota() const override {
        return Quota.load();
    }

    ui64 GetMaxMemorySize() const override {
        return Limit;
    }

    // stands for the node level memory availability, see NRm::TTxState::GetMemoryAvailability:
    // negative under memory pressure, otherwise what is left of the limit
    i64 GetMemoryAvailability() const override {
        return MemoryPressure.load() ? -1 : static_cast<i64>(Limit) - Quota.load();
    }

    TString MemoryConsumptionDetails() const override {
        return TStringBuilder() << "Quota=" << Quota.load() << ", Limit=" << Limit;
    }

    static constexpr ui64 Limit = 1ull << 30; // large enough to never be exceeded by the tests
    std::atomic<bool> MemoryPressure = false;
    std::atomic<i64> Quota = 0;
    std::atomic<ui64> Allocated = 0;
    std::atomic<ui64> Freed = 0;
    std::atomic<ui64> Underflows = 0;
};

struct TWorkerSettings {
    int StartDelayMs = 10;
    int MessageCount = 0;
    int MinMessageSize = 10;
    int MaxMessageSize = 10000;
    bool EarlyFinish = false;
    int PauseMessageIndex = -1;
    int PauseDelayMs = 0;
};

struct TFailureSettings {
    int Data = 0;
    int Ack = 0;
    int Update = 0;
    int Discovery = 0;
};

template <typename TDerived>
class TWorkerActor : public NActors::TActor<TDerived> {
public:
    TWorkerActor(const TString& logPrefix, std::shared_ptr<IDqChannelService> service, ui32 channelId, const TWorkerSettings& settings,
        IMemoryQuotaManager::TPtr quotaManager)
        : NActors::TActor<TDerived>(&TWorkerActor::StateFunc)
        , LogPrefix(logPrefix)
        , Service(service)
        , ChannelId(channelId)
        , Settings(settings)
        , QuotaManager(std::move(quotaManager))
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            hFunc(TEvTestPrivate::TEvStart, HandleStart);
            hFunc(TEvDqCompute::TEvResumeExecution, HandleResume);
            hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleAbort);
        }
    }

    virtual void Run() = 0;

    virtual void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        Run();
    }

    virtual void HandleResume(TEvDqCompute::TEvResumeExecution::TPtr&) {
        Run();
    }

    virtual void HandleStart(TEvTestPrivate::TEvStart::TPtr& ev) {
        RunnerId = ev->Sender;
        PeerId = ev->Get()->PeerId;
        YDB_LOG_DEBUG("TEST START",
            {"selfId", this->SelfId()},
            {"channelId", ChannelId},
            {"peerId", PeerId});
        if (Settings.StartDelayMs) {
            this->Schedule(TDuration::MilliSeconds(RandomNumber<ui64>(Settings.StartDelayMs) + 1), new NActors::TEvents::TEvWakeup());
        } else {
            Run();
        }
    }

    virtual void HandleAbort(NYql::NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
        YDB_LOG_DEBUG("Test worker received abort execution",
            {"selfId", this->SelfId()},
            {"channelId", ChannelId},
            {"issues", ev->Get()->GetIssues().ToOneLineString()});
        this->Send(RunnerId, new TEvTestPrivate::TEvFinished(TEvTestPrivate::ERole::Producer, true));
        this->PassAway();
    }

    TString LogPrefix;
    std::shared_ptr<IDqChannelService> Service;
    std::shared_ptr<IChannelBuffer> Buffer;
    ui32 ChannelId;
    NActors::TActorId PeerId;
    NActors::TActorId RunnerId;
    TWorkerSettings Settings;
    IMemoryQuotaManager::TPtr QuotaManager;
    int MessageIndex = 0;
    bool Started = false;
};

class TProducerActor : public TWorkerActor<TProducerActor> {
public:
    TProducerActor(std::shared_ptr<IDqChannelService> service, ui32 channelId, const TWorkerSettings& settings,
        IMemoryQuotaManager::TPtr quotaManager)
        : TWorkerActor("PROD ", service, channelId, settings, std::move(quotaManager))
    {}

    void Run() override {
        if (!Started) {
            TChannelFullInfo info(ChannelId, SelfId(), PeerId, 0, 1, TCollectStatsLevel::None);
            Buffer = Service->GetOutputBuffer(info, QuotaManager, nullptr);
            Started = true;
        }
        if (Buffer->IsFinished()) {
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST FINISHED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
            Send(RunnerId, new TEvTestPrivate::TEvFinished(TEvTestPrivate::ERole::Producer, false));
            PassAway();
            return;
        }
        while (Buffer->GetFillLevel() == EDqFillLevel::NoLimit && MessageIndex < Settings.MessageCount) {
            if (Settings.PauseMessageIndex == MessageIndex) {
                if (!ResumeTime) {
                    ResumeTime = TInstant::Now() + TDuration::MilliSeconds(Settings.PauseDelayMs);
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST PAUSED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                }
                if (TInstant::Now() < ResumeTime) {
                    Schedule(ResumeTime, new NActors::TEvents::TEvWakeup());
                    return;
                } else {
                    ResumeTime = TInstant::Zero();
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST RESUMED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                }
            }
            auto bytes = Settings.MinMessageSize + RandomNumber<ui64>(Settings.MaxMessageSize - Settings.MinMessageSize);
            Buffer->Push(TDataChunk(NYql::TChunkedBuffer(TString(bytes, 'a')), 1, false));
            MessageIndex++;
        }
        if (MessageIndex == Settings.MessageCount) {
            Buffer->SendFinish();
        }
    }

    TInstant ResumeTime;
};

class TConsumerActor : public TWorkerActor<TConsumerActor> {
public:
    TConsumerActor(std::shared_ptr<IDqChannelService> service, ui32 channelId, const TWorkerSettings& settings,
        IMemoryQuotaManager::TPtr quotaManager)
        : TWorkerActor("CONS ", service, channelId, settings, std::move(quotaManager))
    {}

    void Run() override {
        if (!Started) {
            TChannelFullInfo info(ChannelId, PeerId, SelfId(), 0, 1, TCollectStatsLevel::None);
            Buffer = Service->GetInputBuffer(info, QuotaManager);
            Started = true;
        }
        TDataChunk data;
        while (MessageIndex < Settings.MessageCount) {
            if (Settings.PauseMessageIndex == MessageIndex) {
                if (!ResumeTime) {
                    ResumeTime = TInstant::Now() + TDuration::MilliSeconds(Settings.PauseDelayMs);
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST PAUSED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                }
                if (TInstant::Now() < ResumeTime) {
                    Schedule(ResumeTime, new NActors::TEvents::TEvWakeup());
                    return;
                } else {
                    ResumeTime = TInstant::Zero();
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST RESUMED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                }
            }
            if (!Buffer->Pop(data)) {
                break;
            }
            MessageIndex++;
        }
        if (Settings.EarlyFinish && MessageIndex == Settings.MessageCount) {
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST EARLY FINISH SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
            Buffer->EarlyFinish();
            MessageIndex++;
        }
        if (MessageIndex <= Settings.MessageCount && Buffer->Pop(data)) {
            MessageIndex++;
        }
        if (Buffer->IsFinished()) {
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST FINISHED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
            Send(RunnerId, new TEvTestPrivate::TEvFinished(TEvTestPrivate::ERole::Consumer, false));
            PassAway();
        }
    }

    TInstant ResumeTime;
};

struct TLoadTest {

    virtual void Prepare() {
        settings.NodeCount = Local ? 1 : 2;
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::EPriority::PRI_TRACE);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
        if (Local) {
            NodeIndex1 = NodeIndex0;
        }
    }

    virtual void Init() {
        Runner = std::make_unique<TKikimrRunner>(settings);
        Runtime = Runner->GetTestServer().GetRuntime();
        Runtime->SetUseRealInterconnect();

        Control0 = Runtime->AllocateEdgeActor(0);
        Control1 = Local ? Control0 : Runtime->AllocateEdgeActor(1);

        Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(0)), Control0, new TEvPrivate::TEvServiceLookup(), NodeIndex0);
        auto serviceReply = Runtime->GrabEdgeEvent<TEvPrivate::TEvServiceReply>(Control0)->Release();
        Service0 = serviceReply->Service;

        if (Local) {
            Service1 = Service0;
        } else {
            Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(1)), Control1, new TEvPrivate::TEvServiceLookup(), NodeIndex1);
            auto serviceReply = Runtime->GrabEdgeEvent<TEvPrivate::TEvServiceReply>(Control1)->Release();
            Service1 = serviceReply->Service;
        }
    }

    virtual void Start() {
        for (auto i = 0; i < Count; i ++) {
            auto channelId = i + 1;
            if ((i & 1) == 0) {
                auto producer = Runtime->Register(new TProducerActor(Service0, channelId, ProducerSettings, OutputQuotaManager), NodeIndex0);
                auto consumer = Runtime->Register(new TConsumerActor(Service1, channelId, ConsumerSettings, InputQuotaManager), NodeIndex1);
                Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
                Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            } else {
                auto producer = Runtime->Register(new TProducerActor(Service1, channelId, ProducerSettings, OutputQuotaManager), NodeIndex1);
                auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
                Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
                Runtime->Send(producer, Control1, new TEvTestPrivate::TEvStart(consumer), NodeIndex1, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            }
        }
    }

    virtual void Wait() {
        try {
            for (auto i = 0; i < Count; i++) {
                auto msg0 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, TDuration::Seconds(10));
                Actors.erase(msg0->Sender);
                FinishCount[NodeIndex0][msg0->Get()->Role]++;
                ErrorCount += msg0->Get()->Error;
                auto msg1 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, TDuration::Seconds(10));
                Actors.erase(msg1->Sender);
                FinishCount[NodeIndex1][msg1->Get()->Role]++;
                ErrorCount += msg1->Get()->Error;
            }
        } catch (NActors::TEmptyEventQueueException&) {
            if (!Actors.empty()) {
                TStringBuilder builder;
                builder << "NOT FINISHED ACTORS ";
                for (auto actorId : Actors) {
                    builder << ' ' << actorId;
                }
                UNIT_ASSERT_C(false, builder);
            }
        }
    }

    virtual void Check() {
        if (Local) {
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Producer], Count);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Consumer], Count);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Producer], (Count + 1) / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Consumer], Count / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[1][TEvTestPrivate::ERole::Producer], Count / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[1][TEvTestPrivate::ERole::Consumer], (Count + 1) / 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(ErrorCount, 0);
    }

    // stops the actor system, all channel descriptors are destroyed and all quota is released here
    virtual void Destroy() {
        Service0.reset();
        Service1.reset();
        Runtime = nullptr;
        Runner.reset();
    }

    virtual void CheckQuota() {
        for (const auto& [name, quotaManager] : {
            std::make_pair(TStringBuf("Output"), OutputQuotaManager),
            std::make_pair(TStringBuf("Input"), InputQuotaManager)}) {
            TStringBuilder details;
            details << name << " quota: Quota=" << quotaManager->Quota.load()
                << ", Allocated=" << quotaManager->Allocated.load()
                << ", Freed=" << quotaManager->Freed.load()
                << ", Underflows=" << quotaManager->Underflows.load();
            UNIT_ASSERT_VALUES_EQUAL_C(quotaManager->Underflows.load(), 0, details);
            UNIT_ASSERT_VALUES_EQUAL_C(quotaManager->Quota.load(), 0, details);
        }
    }

    virtual void Run() {
        Prepare();
        Init();
        Start();
        Wait();
        Check();
        Destroy();
        CheckQuota();
    }

    int Count = 1;
    bool Local = true;
    ui32 NodeIndex0 = 0;
    ui32 NodeIndex1 = 1;
    TKikimrSettings settings;
    std::unique_ptr<TKikimrRunner> Runner;
    NActors::TTestActorRuntime* Runtime;
    std::shared_ptr<TDqChannelService> Service0;
    std::shared_ptr<TDqChannelService> Service1;
    NActors::TActorId Control0;
    NActors::TActorId Control1;
    TWorkerSettings ProducerSettings;
    TWorkerSettings ConsumerSettings;
    std::shared_ptr<TTestQuotaManager> OutputQuotaManager = std::make_shared<TTestQuotaManager>();
    std::shared_ptr<TTestQuotaManager> InputQuotaManager = std::make_shared<TTestQuotaManager>();
    THashSet<NActors::TActorId> Actors;
    int ErrorCount = 0;
    int FinishCount[2][2] = {{0, 0}, {0, 0}};
};

// Keeps the consumer node under permanent memory pressure, so every channel is throttled down to the
// cold inflight window. The transfer must still complete - cold inflight is never zero.
struct TMemoryPressureTest : public TLoadTest {

    void Prepare() override {
        TLoadTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingChannelBackpressure(true);
    }

    void Run() override {
        Prepare();
        Init();
        // a local buffer keeps whichever quota manager reached GetOrCreateLocalBuffer first, and both
        // roles run on real threads - set the pressure on both so the 1n case is not racy
        InputQuotaManager->MemoryPressure = true;
        OutputQuotaManager->MemoryPressure = true;
        Start();
        Wait();
        Check();
        Destroy();
        CheckQuota();
    }
};

// Single channel, the consumer pops a few messages (so the channel gets warm) and then stalls.
// While it stalls the producer must be blocked at the cold inflight window rather than at the
// full RemoteChannelInflightBytes one - but only when EnableSpillingChannelBackpressure is set.
struct TThrottleTest : public TLoadTest {

    void Prepare() override {
        TLoadTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableSpillingChannelBackpressure(Enabled);
    }

    void Run() override {
        Prepare();
        Init();
        InputQuotaManager->MemoryPressure = true;
        Start();
        CheckThrottled();
        InputQuotaManager->MemoryPressure = false;
        Wait();
        Check();
        Destroy();
        CheckQuota();
    }

    // The only output descriptor of this single channel test, on the producer (node 0) side.
    // Locks in the same order as the channel service itself: service Mutex, then session Mutex.
    std::shared_ptr<TOutputDescriptor> FindOutputDescriptor() {
        std::lock_guard lock(Service0->Mutex);
        for (auto& [nodeId, state] : Service0->NodeStates) {
            std::lock_guard stateLock(state->Mutex);
            for (auto& [info, descriptor] : state->OutputDescriptors) {
                return descriptor;
            }
        }
        return {};
    }

    // Polls until the predicate holds, so that nothing depends on how fast the actors got scheduled.
    // The consumer sleeps for PauseDelayMs after PauseMessageIndex pops, that is the budget here.
    bool WaitFor(const std::function<bool(const std::shared_ptr<TOutputDescriptor>&)>& predicate,
        std::shared_ptr<TOutputDescriptor>& descriptor) {
        auto deadline = TInstant::Now() + TDuration::MilliSeconds(ConsumerSettings.PauseDelayMs / 4);
        do {
            if (auto current = FindOutputDescriptor()) {
                descriptor = current;
                if (predicate(current)) {
                    return true;
                }
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        return false;
    }

    void CheckThrottled() {
        std::shared_ptr<TOutputDescriptor> descriptor;

        // the producer is only warm once the consumer has popped something, wait for that first
        auto warm = WaitFor([](const auto& d) { return d->RemotePopBytes.load() > 0; }, descriptor);

        UNIT_ASSERT_C(descriptor, "output descriptor of the channel not found");

        // one chunk may always overshoot the window, it is pushed before the level is recomputed
        auto coldBytes = descriptor->ColdInflightBytes + ProducerSettings.MaxMessageSize;

        auto details = [&]() {
            return TStringBuilder() << "Enabled=" << Enabled << ", Warm=" << warm
                << ", PushBytes=" << descriptor->PushBytes.load()
                << ", RemotePopBytes=" << descriptor->RemotePopBytes.load()
                << ", PeerMemoryPressure=" << descriptor->PeerMemoryPressure.load()
                << ", MaxInflightBytes=" << descriptor->MaxInflightBytes
                << ", ColdInflightBytes=" << descriptor->ColdInflightBytes;
        };

        UNIT_ASSERT_C(warm, details());

        if (Enabled) {
            // the bound holds at every moment, but the pressure needs one update to reach the sender
            UNIT_ASSERT_C(WaitFor([](const auto& d) { return d->PeerMemoryPressure.load(); }, descriptor), details());
            UNIT_ASSERT_LE_C(descriptor->PushBytes.load() - descriptor->RemotePopBytes.load(), coldBytes, details());
            UNIT_ASSERT_LT_C(descriptor->PushBytes.load() - descriptor->RemotePopBytes.load(),
                descriptor->MaxInflightBytes, details());
        } else {
            // nothing throttles the producer, it pushes far past the cold window while the consumer sleeps
            UNIT_ASSERT_C(WaitFor([&](const auto& d) {
                return d->PushBytes.load() - d->RemotePopBytes.load() > coldBytes; }, descriptor), details());
            UNIT_ASSERT_C(!descriptor->PeerMemoryPressure.load(), details());
        }
    }

    bool Enabled = true;
};

struct TReconTest : public TLoadTest {

    void Prepare() override {
        TLoadTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(20);
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(10);
    }

    void Start() override {
        for (auto i = 0; i < Count; i ++) {
            auto channelId = i + 1;
            if ((i & 1) == 0) {
                auto producerSettings = ProducerSettings;
                producerSettings.PauseMessageIndex = (channelId + producerSettings.MessageCount / 2) % producerSettings.MessageCount;
                producerSettings.PauseDelayMs = 50;
                auto producer = Runtime->Register(new TProducerActor(Service0, channelId, producerSettings, OutputQuotaManager), NodeIndex0);
                auto consumer = Runtime->Register(new TConsumerActor(Service1, channelId, ConsumerSettings, InputQuotaManager), NodeIndex1);
                Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
                Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            } else {
                auto producerSettings = ProducerSettings;
                producerSettings.PauseMessageIndex = channelId;
                producerSettings.PauseDelayMs = 50;
                auto producer = Runtime->Register(new TProducerActor(Service1, channelId, producerSettings, OutputQuotaManager), NodeIndex1);
                auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
                Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
                Runtime->Send(producer, Control1, new TEvTestPrivate::TEvStart(consumer), NodeIndex1, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            }
        }
    }

};

Y_UNIT_TEST_SUITE(Channels20) {

    void LoadTest(int count, bool local, const TWorkerSettings& producerSettings, const TWorkerSettings& consumerSettings, const TFailureSettings& = TFailureSettings{}) {
        TLoadTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = producerSettings;
        test.ConsumerSettings = consumerSettings;

        test.Run();
    }

    void LoadTest(int count, bool local, const TWorkerSettings& settings = TWorkerSettings{}, const TFailureSettings& failureSettings = TFailureSettings{}) {
        LoadTest(count, local, settings, settings, failureSettings);
    }

    Y_UNIT_TEST(EmptyFinish2n) {
        LoadTest(100, false);
    }

    Y_UNIT_TEST(SimpleFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    Y_UNIT_TEST(ConsumerPauseThenResume2n) {
        LoadTest(50, false,
            TWorkerSettings{ .MessageCount = 100 },
            TWorkerSettings{ .MessageCount = 100, .PauseMessageIndex = 20, .PauseDelayMs = 200 });
    }

    Y_UNIT_TEST(EmptyFinish1n) {
        LoadTest(100, true);
    }

    Y_UNIT_TEST(SimpleFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    Y_UNIT_TEST(MissedData) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 100 }, TFailureSettings{ .Data = 10 });
    }

    void MemoryPressureTest(int count, bool local) {
        TMemoryPressureTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = TWorkerSettings{ .MessageCount = 100 };
        test.ConsumerSettings = TWorkerSettings{ .MessageCount = 100 };

        test.Run();
    }

    Y_UNIT_TEST(MemoryPressure2n) {
        MemoryPressureTest(50, false);
    }

    Y_UNIT_TEST(MemoryPressure1n) {
        MemoryPressureTest(50, true);
    }

    void ThrottleTest(bool enabled) {
        TThrottleTest test;

        test.Enabled = enabled;
        test.Count = 1;
        test.Local = false;
        // 200 * ~64KB is way above the cold inflight window and still below RemoteChannelInflightBytes,
        // so an unthrottled producer would push all of it while the consumer sleeps
        test.ProducerSettings = TWorkerSettings{
            .MessageCount = 200, .MinMessageSize = 60000, .MaxMessageSize = 70000 };
        test.ConsumerSettings = TWorkerSettings{
            .MessageCount = 200, .MinMessageSize = 60000, .MaxMessageSize = 70000,
            .PauseMessageIndex = 5, .PauseDelayMs = 6000 };

        test.Run();
    }

    Y_UNIT_TEST(MemoryPressureThrottles2n) {
        ThrottleTest(true);
    }

    // the very same scenario must not throttle anything while the feature flag is off
    Y_UNIT_TEST(MemoryPressureDisabled2n) {
        ThrottleTest(false);
    }

    Y_UNIT_TEST(Reconciliation) {
        TReconTest test;

        test.Count = 1;
        test.Local = false;
        test.ProducerSettings.MessageCount = 100;
        test.ConsumerSettings.MessageCount = 100;

        test.Run();
    }
}
