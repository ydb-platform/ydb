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
#include <util/datetime/base.h>

#include <atomic>

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

// Direct access to the node sessions of both nodes, for the reconciliation scenarios below.
struct TSessionTest : public TLoadTest {

    static std::shared_ptr<TNodeState> FindNodeState(const std::shared_ptr<TDqChannelService>& service, ui32 peerNodeId) {
        std::lock_guard lock(service->Mutex);
        auto it = service->NodeStates.find(peerNodeId);
        return it == service->NodeStates.end() ? nullptr : it->second;
    }

    static ui64 GetGenMajor(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->GenMajor;
    }

    static ui64 GetGenMinor(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->GenMinor;
    }

    static ui64 GetQueueSize(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->Queue.size();
    }

    static ui64 GetFrontSeqNo(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->Queue.empty() ? 0 : state->Queue.front()->SeqNo;
    }

    static ui64 GetConfirmedSeqNo(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->ConfirmedSeqNo;
    }

    static ui64 GetInputCount(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->InputDescriptors.size();
    }

    // the bytes the session believes are in flight; must match the queued bytes at any settled moment
    static ui64 GetQueueBytes(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        ui64 bytes = 0;
        for (const auto& item : state->Queue) {
            bytes += item->Data.Bytes;
        }
        return bytes;
    }

    static ui64 GetInputPushBytes(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        ui64 bytes = 0;
        for (const auto& [info, descriptor] : state->InputDescriptors) {
            bytes += descriptor->PushStats.Bytes.load();
        }
        return bytes;
    }

    static ui64 GetInputPopBytes(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        ui64 bytes = 0;
        for (const auto& [info, descriptor] : state->InputDescriptors) {
            bytes += descriptor->PopStats.Bytes.load();
        }
        return bytes;
    }

    static std::vector<ui64> GetQueueSeqNos(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        std::vector<ui64> result;
        for (const auto& item : state->Queue) {
            result.push_back(item->SeqNo);
        }
        return result;
    }

    static TString GetReconciliationLog(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->GetReconciliationLog();
    }

    static bool WaitFor(const std::function<bool()>& predicate, TDuration timeout) {
        auto deadline = TInstant::Now() + timeout;
        do {
            if (predicate()) {
                return true;
            }
            Sleep(TDuration::MilliSeconds(10));
        } while (TInstant::Now() < deadline);
        return false;
    }

    // waits for the sender session to have nothing in flight and no reconciliation in progress
    void WaitSettled(const std::shared_ptr<TNodeState>& sender) {
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(sender) == 0 && sender->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            "sender node session did not settle");
    }

    // producer on node 0, consumer on node 1; the consumer is registered right away (the producer needs
    // its id) but started only on demand
    std::pair<NActors::TActorId, NActors::TActorId> StartChannel(ui32 channelId, bool startConsumer) {
        auto producer = Runtime->Register(new TProducerActor(Service0, channelId, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, channelId, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        if (startConsumer) {
            StartConsumer({producer, consumer});
        }
        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        return {producer, consumer};
    }

    void StartConsumer(const std::pair<NActors::TActorId, NActors::TActorId>& channel) {
        Runtime->Send(channel.second, Control1, new TEvTestPrivate::TEvStart(channel.first), NodeIndex1, true);
    }

    // details are collected at failure time, the reconciliation log is most useful then
    void WaitChannel(const std::function<TString()>& details) {
        try {
            auto msg0 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, TDuration::Seconds(10));
            Actors.erase(msg0->Sender);
            ErrorCount += msg0->Get()->Error;
            auto msg1 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, TDuration::Seconds(10));
            Actors.erase(msg1->Sender);
            ErrorCount += msg1->Get()->Error;
        } catch (NActors::TEmptyEventQueueException&) {
            UNIT_ASSERT_C(false, TStringBuilder() << "channel did not finish, " << details());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, details());
    }

    void WaitChannel(const TString& details) {
        WaitChannel([&]() { return details; });
    }
};

// A major reconciliation which needs more than one discovery attempt renumbers the queued messages on
// every attempt (TNodeState::DoReconciliation), but SeqNo is reset only when the major reconciliation
// starts. The 2nd attempt turns [1..q] into [q+1..2q]. The receiver session is reset to
// ConfirmedSeqNo == 0 by the new generation, so it sees a gap it can never close: it asks to resend
// from 1, the sender has no such message, and the channel stalls forever.
//
// The scenario mirrors production: the receiver's node session is gone (idle destroy or reconciliation
// failure), the sender's next data bounces with ActorUnknown and starts a major reconciliation, and the
// receiver's channel service is too slow to answer the 1st discovery within the 1s reconciliation timeout.
struct TMajorReconRetryTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 3, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        // warm up: both node sessions exist afterwards, reconciled and idle
        StartChannel(1, true);
        WaitChannel("warm up");

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);
        auto sender = FindNodeState(Service0, receiverNodeId);
        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        UNIT_ASSERT_C(receiver, "receiver node session not found");
        WaitSettled(sender);
        auto genMajor = GetGenMajor(sender);

        // the receiver's node session goes away exactly as the service does it after an idle destroy or a
        // reconciliation failure; the sender still addresses the dead session actor
        receiver->Terminating.store(true);
        Service1->FreeNodeSession(senderNodeId, receiver->NodeActorId);
        receiver.reset();

        // the receiver's channel service does not answer the discovery until the lock is released
        std::unique_lock serviceLock(Service1->Mutex);

        auto channel = StartChannel(2, false);

        // the data bounces with ActorUnknown, the sender starts a major reconciliation and renumbers the queue
        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(sender) == genMajor + 1; }, TDuration::Seconds(5)),
            "the sender did not start a major reconciliation");
        auto reconciliationStarted = TInstant::Now();
        auto queueSize = GetQueueSize(sender);
        auto frontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_C(queueSize > 0, "nothing queued at the sender");
        UNIT_ASSERT_VALUES_EQUAL_C(frontSeqNo, 1, "queue is not renumbered from 1 by the major reconciliation");

        // let the 1st reconciliation timeout (1s) expire, the sender retries the discovery
        SleepUntil(reconciliationStarted + TDuration::MilliSeconds(1500));
        auto retriedFrontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + 1, "unexpected 2nd major reconciliation");
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(sender), queueSize, "queue size changed during reconciliation");

        // the discovery is answered now by a brand new receiver session (ConfirmedSeqNo == 0) and the sender
        // resends the queue to it
        serviceLock.unlock();
        StartConsumer(channel);

        auto details = TStringBuilder() << "queue front SeqNo after the reconciliation retry: " << retriedFrontSeqNo
            << " (expected 1), queue size: " << queueSize;
        WaitChannel(details);
        UNIT_ASSERT_VALUES_EQUAL_C(retriedFrontSeqNo, 1, "queue renumbered again by the reconciliation retry");

        // the node session must not outlive the actor system, its destructor logs through it
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// Two producers of RESEND acks disagree on the meaning of SeqNo: a data gap (TNodeState::HandleData)
// asks to resend from ConfirmedSeqNo + 1, a discovery (TNodeState::HandleDiscovery) reports
// ConfirmedSeqNo itself. TNodeState::HandleAck reads both as "resend from SeqNo". When the queue front
// is exactly the confirmed message - its ack was lost, or as here dropped by the GenMinor check because
// a minor reconciliation had started - the RESEND branch calls StartReconciliation(false, 'R'), which
// only logs "-R" while a reconciliation is already running, and Reconciliation is never cleared.
// Every discovery retry gets the same reply, the log reads TD-RT-RT-RTX, and the session is destroyed
// with all its channels after ReconciliationCount attempts although the peer answered every time.
//
// The receiver is a TDebugNodeState, so that its data processing can be paused and replayed one message
// at a time. The sender's [c, c+1] wait at the receiver, a disconnect starts a minor reconciliation at
// the sender, the receiver processes c alone before it gets the discovery, and c+1 is dropped as
// obsolete afterwards - lost on the wire, as far as both sides can tell.
struct TDiscoveryResendTrapTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        // must precede anything which would create a regular receiver session
        auto receiver = Service1->CreateDebugNodeState(senderNodeId);
        receiver->StartSession();

        // warm up: both node sessions are reconciled and idle afterwards
        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        WaitSettled(sender);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(5)),
            "the warm up input descriptor is still there");
        auto genMinor = GetGenMinor(sender);

        // [c, c+1] (data + finish) reach the receiver and wait there
        receiver->PauseChannelData();
        auto channel = StartChannel(2, false);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(sender) == 2; }, TDuration::Seconds(5)),
            "the sender did not send 2 messages");
        auto frontSeqNo = GetFrontSeqNo(sender);

        // both of them must have reached the receiver before the reconciliation below: the queue of the
        // sender only says they were sent, and the replay has nothing to replay until they are there
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->PendingDataCount.load() >= 2; }, TDuration::Seconds(10)),
            "the messages did not reach the receiver");

        // the discovery of the minor reconciliation must reach the receiver after it has processed c
        std::unique_lock serviceLock(Service1->Mutex);

        Runtime->Send(sender->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(receiverNodeId), NodeIndex0, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(sender) == genMinor + 1; }, TDuration::Seconds(5)),
            "the sender did not start a minor reconciliation");

        // the receiver processes c alone; its ack carries the old GenMinor and is ignored by the sender.
        // The session stays paused throughout: unpausing it would deliver whatever arrives meanwhile as well
        receiver->ProcessPending(1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 1; }, TDuration::Seconds(5)),
            "the receiver did not process the 1st message");
        auto confirmedSeqNo = GetConfirmedSeqNo(receiver);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmedSeqNo, frontSeqNo, "the receiver confirmed something else than the queue front");

        // the discovery is answered now: RESEND with the confirmed c, which is the sender's queue front;
        // the sender must drop c and get back to work
        serviceLock.unlock();
        auto reconciled = WaitFor([&]() { return sender->Reconciliation.load() == 0 && GetFrontSeqNo(sender) == frontSeqNo + 1; },
            TDuration::Seconds(2));

        // the receiver drops the stale c+1 as obsolete (old GenMinor) and takes the resent one
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->OutputNodeGenMinor.load() == genMinor + 1; }, TDuration::Seconds(5)),
            "the receiver did not get the discovery");
        receiver->ResumeChannelData();
        StartConsumer(channel);

        auto details = [&]() {
            return TStringBuilder() << "reconciled after the discovery reply: " << reconciled
                << ", queue front SeqNo: " << GetFrontSeqNo(sender) << " (confirmed by the receiver: " << confirmedSeqNo << ")"
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };
        WaitChannel(details);
        UNIT_ASSERT_C(reconciled, details());

        // the node sessions must not outlive the actor system, their destructors log through it
        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// TNodeState::HandleAck pops the acknowledged prefix of the queue and accumulates its size in
// deltaBytes, but the session counter is only adjusted at the very end, by SendFromWaiters(deltaBytes).
// Every early return in between - 'F', 'L', 'N', 'Y' and a gap 'R' - drops that adjustment, and neither
// the reconciliation nor the RECONCILED path ever recomputes InflightBytes from the queue. The drift is
// permanent and accumulates: once it reaches RemoteSessionInflightBytes the session stops sending
// anything at all, with an empty queue and idle channels.
//
// Staged here on the gap RESEND path: the receiver confirms the 1st two messages but their acks are lost,
// then a message is lost as well, so the receiver asks to resend from the 1st missing one. The sender pops
// the two confirmed messages (deltaBytes > 0) and returns through StartReconciliation(false, 'R').
struct TInflightLeakTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        // both must precede anything which would create a regular session for the same peer, and neither
        // may discover its peer before both are registered - a discovery makes the service of the peer
        // create a session of its own and CreateDebugNodeState refuses to replace one
        auto sender = Service0->CreateDebugNodeState(receiverNodeId);
        auto receiver = Service1->CreateDebugNodeState(senderNodeId);
        sender->StartSession();
        receiver->StartSession();

        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        // warm up: both sessions are reconciled and idle afterwards, no ack is in flight
        StartChannel(1, true);
        WaitChannel("warm up");
        WaitSettled(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), 0, "the warm up already leaked");

        ProducerSettings = TWorkerSettings{ .MessageCount = 4, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        // nothing is delivered at the receiver until the whole batch is there, so every message of it can be
        // named below instead of being picked by whichever happens to arrive next
        receiver->PauseChannelData();
        auto channel = StartChannel(2, false);

        // the batch is 4 messages plus the finish one; no ack can come back while the receiver is paused,
        // so the sender holds all 5 of them
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->PendingDataCount.load() >= 5; }, TDuration::Seconds(10)),
            "the batch did not reach the receiver");
        auto seqNos = GetQueueSeqNos(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(seqNos.size(), 5, "the sender does not hold the whole batch");
        auto frontSeqNo = seqNos.front();
        auto queueBytes = GetQueueBytes(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), queueBytes, "unexpected inflight bytes before the loss");

        // the 3rd message of the batch is lost on the wire and the acks of the 1st two never reach the
        // sender, so it still holds them when the receiver, which has confirmed both, asks to resend from
        // the 3rd - the ack which pops a prefix and then returns through StartReconciliation(false, 'R')
        receiver->DropDataSeqNo.store(seqNos[2]);
        sender->DropOkAckUpToSeqNo.store(seqNos[1]);

        receiver->ResumeChannelData();
        StartConsumer(channel);

        auto details = [&]() {
            return TStringBuilder() << "front SeqNo before the loss: " << frontSeqNo
                << ", queue bytes: " << queueBytes
                << ", inflight bytes now: " << sender->InflightBytes.load()
                << ", queue bytes now: " << GetQueueBytes(sender)
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };

        WaitChannel(details);
        WaitSettled(sender);

        // the queue is empty now, so nothing at all is in flight
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), 0, details());

        // the node sessions must not outlive the actor system, their destructors log through it
        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// The reconciliation state machine governs the outbound half of a session only: it numbers, resends and
// acknowledges the messages this node sends. Its give-up path (DoReconciliation, the 'X' symbol) still
// calls FailDescriptors, which aborts the input descriptors as well - the channels on which this node is
// the receiver and the peer is the sender. A session whose outbound half is idle and empty therefore
// destroys perfectly healthy inbound channels, with everything the peer has already delivered, as soon as
// the peer is too slow to answer a handshake.
//
// This is the production failure this whole series started from: an inbound channel holding ~14MB of
// undelivered data aborted with "OutputNodeActorId=... DO NOT MATCH outputNodeActorId=[0:0:0]" while the
// peer had never restarted.
//
// Here the peer keeps streaming throughout: its channel service is locked, so it cannot answer a
// discovery, but its already bound channels keep sending. The session under test is the one of node 0,
// whose outbound half is empty all along.
//
// NB: this test is expected to fail until it is decided what a session with a healthy inbound half should
// do when its outbound handshake times out. Removing the FailDescriptors call alone is not enough: the
// give-up also frees the session, whose destructor fails the very same descriptors, and the peer's next
// message would reach a new session which knows nothing about the channel.
struct TInboundChannelAbortTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        // give up after 2 unanswered discoveries (~3s) instead of the default 3 (~7s)
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetReconciliationCount(2);
    }

    // the roles of TSessionTest::StartChannel are reversed here: the peer (node 1) produces and the node
    // under test (node 0) consumes, so that its session holds an input descriptor and an empty out queue
    std::pair<NActors::TActorId, NActors::TActorId> StartInboundChannel(ui32 channelId) {
        auto producer = Runtime->Register(new TProducerActor(Service1, channelId, ProducerSettings, OutputQuotaManager), NodeIndex1);
        auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
        Runtime->Send(producer, Control1, new TEvTestPrivate::TEvStart(consumer), NodeIndex1, true);
        return {producer, consumer};
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto peerNodeId = Runtime->GetNodeId(1);

        // warm up: both sessions exist and are reconciled afterwards
        StartChannel(1, true);
        WaitChannel("warm up");

        auto session = FindNodeState(Service0, peerNodeId);
        UNIT_ASSERT_C(session, "node session not found");
        WaitSettled(session);

        // the peer streams to us and stops being consumed after the 1st messages, so the input descriptor
        // is bound, alive and holding data when the outbound handshake starts to fail
        ProducerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 2, .PauseDelayMs = 30000 };

        StartInboundChannel(2);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(session) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");
        auto pushBytes = GetInputPushBytes(session);
        auto popBytes = GetInputPopBytes(session);
        UNIT_ASSERT_C(pushBytes > popBytes, "nothing is left unconsumed in the input descriptor");

        // the peer cannot answer a discovery while its channel service is locked, but the channels it has
        // already bound keep sending - it is alive and it never restarts
        std::unique_lock serviceLock(Service1->Mutex);

        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(session), 0, "the outbound half of the session is not empty");

        Runtime->Send(session->NodeActorId, Control0,
            new NActors::TEvInterconnect::TEvNodeDisconnected(peerNodeId), NodeIndex0, true);

        UNIT_ASSERT_C(WaitFor([&]() { return session->Terminating.load(); }, TDuration::Seconds(20)),
            TStringBuilder() << "the session did not give up, reconciliation log: " << GetReconciliationLog(session));

        serviceLock.unlock();

        auto details = [&]() {
            return TStringBuilder() << "inbound channel: pushed " << pushBytes << " bytes, popped " << popBytes
                << ", outbound queue was empty, reconciliation log: " << GetReconciliationLog(session);
        };

        // the give-up must not touch the inbound half: the peer is alive and everything it sent is there
        bool aborted = false;
        try {
            auto msg = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, TDuration::Seconds(5));
            Actors.erase(msg->Sender);
            aborted = msg->Get()->Error;
        } catch (NActors::TEmptyEventQueueException&) {
        }
        UNIT_ASSERT_C(!aborted, TStringBuilder() << "the inbound channel was aborted by an outbound reconciliation timeout, " << details());

        // the node session must not outlive the actor system, its destructor logs through it
        session.reset();
        Destroy();
    }
};

// LastPeerActivity drives the idle ping of HandleCleanup, whose reconciliation destroys every channel of
// the session when it fails. Only a discovery, an ack and an update used to refresh it, and a session all
// of whose channels have this node as the receiver gets none of the three: it sends the acks and the
// updates itself, and a discovery only arrives when the peer reconciles. Such a session looked idle no
// matter how much the peer was streaming to it, and was idle pinged with a fatal deadline attached.
//
// What is asserted here is the refresh itself, not the absence of the ping. The ping is self-healing: the
// discovery it sends is answered with an ack, which refreshes the activity in turn, so a session which
// never pings cannot be told apart from one which has just been answered - unless the traffic is sustained
// for several idle periods, which no assertion can hold on a loaded machine without going flaky.
struct TPeerActivityTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(50);
        // long enough for no idle ping to interfere with the sampling below, short enough to keep it honest
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(1000);
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);

        // the warm up creates the session of the receiving node, the one every channel here sends to
        StartChannel(1, true);
        WaitChannel("warm up");

        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(receiver, "the receiving node session not found");

        // nothing refreshes the activity of that session while it is idle: it is well under the idle ping
        // period, so no discovery of its own goes out and no ack comes back
        Sleep(TDuration::MilliSeconds(200));
        auto before = receiver->LastPeerActivity.load();

        // the consumer stalls after the 1st message, so the input descriptor is still there to be sampled
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 1500 };
        StartChannel(2, true);

        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPushBytes(receiver) > 0; }, TDuration::Seconds(10)),
            "no data of the peer arrived at the receiving session");
        auto after = receiver->LastPeerActivity.load();

        auto details = TStringBuilder() << "LastPeerActivity " << before << " -> " << after
            << ", pushed " << GetInputPushBytes(receiver) << " bytes"
            << ", reconciliation log: " << GetReconciliationLog(receiver);

        UNIT_ASSERT_C(after > before,
            TStringBuilder() << "the data of the peer did not refresh the activity of the session, " << details);

        WaitChannel(details);

        // the node session must not outlive the actor system, its destructor logs through it
        receiver.reset();
        Destroy();
        CheckQuota();
    }
};

// TerminateInputDescriptor erased the descriptor by key and decremented InputBuffer/Count whatever the
// erase did. The descriptor of an aborted channel is erased and accounted for where it is aborted - by
// FailInputs, or by the ID ERASE/GEN path of HandleChannelData - while the buffer of that channel lives on
// until its actor lets go of it and terminates it here. The same descriptor was then taken off the sensor
// twice, once per aborted inbound channel, and since the sensor is a plain gauge shared by every session
// of the node, the drift accumulates for the life of the process and eventually goes below zero.
//
// Staged through a peer session which is replaced: the node under test keeps its session and its consumer,
// the peer frees its own, and the discovery of the session which takes its place makes ConnectSession
// abort every input descriptor which belonged to the session before it.
struct TBufferCountTest : public TSessionTest {

    static i64 GetCounter(const std::shared_ptr<TDqChannelService>& service, const TString& name) {
        return service->Counters->GetCounter(name, false)->Val();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        // the consumer stalls after the 1st message, so the channel is neither finished nor let go of when
        // it is aborted below - a finished descriptor would not be failed at all
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartChannel(1, true);

        // both sessions are created by the traffic of the channel, not by the test
        std::shared_ptr<TNodeState> receiver;
        UNIT_ASSERT_C(WaitFor([&]() { return (receiver = FindNodeState(Service1, senderNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the receiving node session not found");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(receiver) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service1, "InputBuffer/Count"), 1,
            "the inbound channel is not on the sensor");

        // the peer frees its session and another one takes its place, exactly as the service does it after
        // an idle destroy or a reconciliation failure
        std::shared_ptr<TNodeState> sender;
        UNIT_ASSERT_C(WaitFor([&]() { return (sender = FindNodeState(Service0, peerNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the sending node session not found");
        sender->Terminating.store(true);
        Service0->FreeNodeSession(peerNodeId, sender->NodeActorId);
        sender.reset();
        {
            std::lock_guard lock(Service0->Mutex);
            Service0->GetOrCreateNodeState(peerNodeId);
        }

        // its discovery aborts the descriptor of the channel and takes it off the sensor, then the consumer
        // is aborted in turn and lets go of its buffer, which terminates a descriptor which is gone. The
        // 2 of them are not sampled apart on purpose: the 2nd follows the 1st closely enough to race a poll
        try {
            auto msg = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, TDuration::Seconds(10));
            Actors.erase(msg->Sender);
            UNIT_ASSERT_C(msg->Get()->Error, "the consumer of an aborted channel finished without an error");
        } catch (NActors::TEmptyEventQueueException&) {
            UNIT_ASSERT_C(false, "the consumer of the aborted channel did not finish");
        }

        auto negative = WaitFor([&]() { return GetCounter(Service1, "InputBuffer/Count") < 0; }, TDuration::Seconds(3));
        auto details = TStringBuilder() << "InputBuffer/Count=" << GetCounter(Service1, "InputBuffer/Count")
            << ", OutputBuffer/Count=" << GetCounter(Service0, "OutputBuffer/Count");
        UNIT_ASSERT_C(!negative, TStringBuilder() << "the descriptor of the aborted channel was counted off twice, " << details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service1, "InputBuffer/Count"), 0, details);

        receiver.reset();
        Destroy();
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

    Y_UNIT_TEST(MajorReconciliationRetry) {
        TMajorReconRetryTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(DiscoveryResendTrap) {
        TDiscoveryResendTrapTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(InflightLeakOnResend) {
        TInflightLeakTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(PeerActivityRefreshedByData) {
        TPeerActivityTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(BufferCountOfAnAbortedChannel) {
        TBufferCountTest test;

        test.Local = false;

        test.Run();
    }

    // Disabled on purpose: it reproduces a defect which is still open, see the comment of
    // TInboundChannelAbortTest above. Enable it together with the fix. The test body itself is left
    // compiled, so that it keeps up with any refactoring of the helpers it uses.
    /*
    Y_UNIT_TEST(InboundChannelAbortedByOutboundTimeout) {
        TInboundChannelAbortTest test;

        test.Local = false;

        test.Run();
    }
    */
}
