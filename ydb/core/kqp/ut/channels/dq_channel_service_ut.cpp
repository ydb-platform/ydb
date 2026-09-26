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
#include <util/stream/str.h>
#include <util/system/unaligned_mem.h>

#include <atomic>
#include <optional>

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
        EvStep,
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

    // the test lets a worker take its next step, see TWorkerSettings::FinishOnStep
    struct TEvStep : public NActors::TEventLocal<TEvStep, EvStep> {
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
    // the producer writes the index of each message into its 1st bytes and the consumer fails on any
    // message out of order, missing or cut short by the finish; needs MinMessageSize >= 4
    bool CheckOrder = false;
    // the producer sends the finish only once the test has sent it TEvStep
    bool FinishOnStep = false;
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
            hFunc(TEvTestPrivate::TEvStep, HandleStep);
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

    virtual void HandleStep(TEvTestPrivate::TEvStep::TPtr&) {
        Stepped = true;
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
    bool Stepped = false;
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
            TString payload(bytes, 'a');
            if (Settings.CheckOrder) {
                Y_ENSURE(bytes >= sizeof(ui32));
                WriteUnaligned<ui32>(payload.begin(), MessageIndex);
            }
            Buffer->Push(TDataChunk(NYql::TChunkedBuffer(std::move(payload)), 1, false));
            MessageIndex++;
        }
        if (MessageIndex == Settings.MessageCount && (!Settings.FinishOnStep || Stepped)) {
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
            if (!CheckOrder(data)) {
                return;
            }
            MessageIndex++;
        }
        if (Settings.EarlyFinish && MessageIndex == Settings.MessageCount) {
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST EARLY FINISH SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
            Buffer->EarlyFinish();
            MessageIndex++;
        }
        if (MessageIndex <= Settings.MessageCount && Buffer->Pop(data)) {
            if (MessageIndex < Settings.MessageCount && !CheckOrder(data)) {
                return;
            }
            MessageIndex++;
        }
        if (Buffer->IsFinished()) {
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST FINISHED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
            // the finish itself is popped as one more message
            auto error = Settings.CheckOrder && MessageIndex != Settings.MessageCount + 1;
            if (error) {
                LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST ORDER SelfId=" << SelfId()
                    << ", ChannelId=" << ChannelId << ", finished after " << MessageIndex << " message(s) of " << Settings.MessageCount);
            }
            Send(RunnerId, new TEvTestPrivate::TEvFinished(TEvTestPrivate::ERole::Consumer, error));
            PassAway();
        }
    }

    // a data message must carry the index of the next one, the finish comes after all of them
    bool CheckOrder(const TDataChunk& data) {
        if (!Settings.CheckOrder) {
            return true;
        }
        std::optional<ui32> index;
        if (data.Buffer.Size() >= sizeof(ui32)) {
            TString head;
            TStringOutput output(head);
            data.Buffer.CopyTo(output, sizeof(ui32));
            index = ReadUnaligned<ui32>(head.data());
        }
        if (index == static_cast<ui32>(MessageIndex)) {
            return true;
        }
        LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST ORDER SelfId=" << SelfId()
            << ", ChannelId=" << ChannelId << ", expected message " << MessageIndex << ", got "
            << (index ? ToString(*index) : TString("the finish")));
        Send(RunnerId, new TEvTestPrivate::TEvFinished(TEvTestPrivate::ERole::Consumer, true));
        PassAway();
        return false;
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

    // the actor the session sends to, learned from the 1st ack of the generation
    static NActors::TActorId GetInputNodeActorId(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->InputNodeActorId;
    }

    static ui64 GetReconciliationCount(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->ReconciliationCount;
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

    static std::shared_ptr<TOutputDescriptor> FindOutputDescriptor(const std::shared_ptr<TNodeState>& state, ui64 channelId) {
        std::lock_guard lock(state->Mutex);
        for (const auto& [info, descriptor] : state->OutputDescriptors) {
            if (info.ChannelId == channelId) {
                return descriptor;
            }
        }
        return {};
    }

    static std::vector<ui64> GetQueueSeqNos(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        std::vector<ui64> result;
        for (const auto& item : state->Queue) {
            result.push_back(item->SeqNo);
        }
        return result;
    }

    static ui64 CountPings(const std::shared_ptr<TNodeState>& state) {
        ui64 count = 0;
        for (auto symbol : GetReconciliationLog(state)) {
            count += (symbol == 'I');
        }
        return count;
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

    // the peer (node 1) produces and the node under test (node 0) consumes, the other way round from
    // StartChannel, so that the session of node 0 holds an input descriptor
    std::pair<NActors::TActorId, NActors::TActorId> StartInboundChannel(ui32 channelId, bool startConsumer) {
        auto producer = Runtime->Register(new TProducerActor(Service1, channelId, ProducerSettings, OutputQuotaManager), NodeIndex1);
        auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
        Actors.insert(producer);
        Actors.insert(consumer);
        if (startConsumer) {
            Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
        }
        Runtime->Send(producer, Control1, new TEvTestPrivate::TEvStart(consumer), NodeIndex1, true);
        return {producer, consumer};
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

// A 2nd attempt of a major reconciliation renumbered the queue from q+1 while the receiver was back at
// ConfirmedSeqNo 0, so it asked to resend a message the sender no longer had and the channel stalled.
struct TMajorReconRetryTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 3, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

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

        receiver->Terminating.store(true);
        Service1->FreeNodeSession(senderNodeId, receiver->NodeActorId);
        receiver.reset();

        // the receiver's channel service does not answer the discovery until the lock is released
        std::unique_lock serviceLock(Service1->Mutex);

        auto channel = StartChannel(2, false);

        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(sender) == genMajor + 1; }, TDuration::Seconds(5)),
            "the sender did not start a major reconciliation");
        auto queueSize = GetQueueSize(sender);
        auto frontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_C(queueSize > 0, "nothing queued at the sender");
        UNIT_ASSERT_VALUES_EQUAL_C(frontSeqNo, 1, "queue is not renumbered from 1 by the major reconciliation");

        // the retry is what is under test, so it is waited for rather than assumed to have happened by
        // some time: a late timer would leave the sample looking at the 1st attempt on any version of the
        // code. DoReconciliation rebuilds the queue under the same lock it counts the attempt under, so a
        // count of 2 means the rebuild of the retry is complete
        UNIT_ASSERT_C(WaitFor([&]() { return GetReconciliationCount(sender) >= 2; }, TDuration::Seconds(5)),
            "the sender did not retry the discovery");
        auto retriedFrontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + 1, "unexpected 2nd major reconciliation");
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(sender), queueSize, "queue size changed during reconciliation");

        serviceLock.unlock();
        StartConsumer(channel);

        auto details = TStringBuilder() << "queue front SeqNo after the reconciliation retry: " << retriedFrontSeqNo
            << " (expected 1), queue size: " << queueSize;
        WaitChannel(details);
        UNIT_ASSERT_VALUES_EQUAL_C(retriedFrontSeqNo, 1, "queue renumbered again by the reconciliation retry");

        // a node session logs through the actor system from its destructor, so it may not outlive it
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// A gap RESEND asks to resend from ConfirmedSeqNo + 1, a discovery reply reports ConfirmedSeqNo itself,
// and HandleAck read both as the former: with the queue front at the confirmed message it restarted a
// reconciliation already running, which cleared nothing, so every retry got the same reply (TD-RT-RT-RTX)
// and the session died with its channels although the peer answered each time.
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

        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        WaitSettled(sender);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(5)),
            "the warm up input descriptor is still there");
        auto genMinor = GetGenMinor(sender);

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

        // c alone, its ack carrying the old GenMinor for the sender to ignore. The session stays paused:
        // unpausing it would deliver whatever else arrived meanwhile
        receiver->ProcessPending(1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 1; }, TDuration::Seconds(5)),
            "the receiver did not process the 1st message");
        auto confirmedSeqNo = GetConfirmedSeqNo(receiver);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmedSeqNo, frontSeqNo, "the receiver confirmed something else than the queue front");

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

        // a node session logs through the actor system from its destructor, so it may not outlive it
        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// HandleAck popped the acknowledged prefix but adjusted InflightBytes only at its end, so every early
// return dropped the adjustment and the drift stood until the session stopped sending altogether. Staged
// on the gap RESEND path: the acks of the 1st two messages are lost and the 3rd message with them.
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

        StartChannel(1, true);
        WaitChannel("warm up");
        WaitSettled(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), 0, "the warm up already leaked");

        ProducerSettings = TWorkerSettings{ .MessageCount = 4, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        // nothing is delivered until the whole batch is there, so the messages to lose can be named below
        // instead of being whichever happens to arrive next
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

        // the 3rd message of the batch is lost and the acks of the 1st two with it, so the sender still
        // holds both when the receiver, which confirmed them, asks to resend from the 3rd - the ack which
        // pops a prefix and then leaves through StartReconciliation(false, 'R')
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

        // a node session logs through the actor system from its destructor, so it may not outlive it
        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// The give-up path of the reconciliation calls FailDescriptors, which aborts the inbound channels too, so
// a session with an idle and empty outbound half destroys healthy ones as soon as the peer is slow to
// answer a handshake. Here that peer keeps streaming: its service is locked, its bound channels are not.
//
// Expected to fail until it is settled what such a session should do. Dropping the FailDescriptors call is
// not enough: the give-up frees the session, whose destructor fails the same descriptors, and the next
// message of the peer would reach a new session which knows nothing of the channel.
struct TInboundChannelAbortTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        // give up after 2 unanswered discoveries (~3s) instead of the default 3 (~7s)
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetReconciliationCount(2);
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto peerNodeId = Runtime->GetNodeId(1);

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

        StartInboundChannel(2, true);
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

        // a node session logs through the actor system from its destructor, so it may not outlive it
        session.reset();
        Destroy();
    }
};

// Only a discovery, an ack and an update used to refresh LastPeerActivity, and a session whose channels
// all have this node as the receiver gets none of them, so it looked idle however much the peer streamed.
//
// The refresh is asserted rather than the absence of a ping: a ping is answered with an ack which
// refreshes the activity in turn, so a session which never pings looks like one just answered.
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

        StartChannel(1, true);
        WaitChannel("warm up");

        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(receiver, "the receiving node session not found");

        // The pushed bytes waited for below are summed over every input descriptor of the session, and the
        // warm up channel leaves one carrying some until its consumer lets go of the buffer, which happens
        // after its TEvFinished has been grabbed. Waiting for it to go makes that wait mean the 2nd channel.
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(10)),
            "the input descriptor of the warm up channel is still there");

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

        // a node session logs through the actor system from its destructor, so it may not outlive it
        receiver.reset();
        Destroy();
        CheckQuota();
    }
};

// TerminateInputDescriptor decremented InputBuffer/Count whatever its erase did, so an aborted channel,
// erased and accounted for where it was aborted, came off the shared gauge twice and drove it below zero.
// Staged through a peer session which is replaced while this node keeps its session and its consumer.
struct TBufferCountTest : public TSessionTest {

    static i64 GetCounter(const std::shared_ptr<TDqChannelService>& service, const TString& name) {
        return service->Counters->GetCounter(name, false)->Val();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        // the consumer stalls after the 1st message: a finished channel would be let go of, and a finished
        // descriptor is not failed at all
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartChannel(1, true);

        std::shared_ptr<TNodeState> receiver;
        UNIT_ASSERT_C(WaitFor([&]() { return (receiver = FindNodeState(Service1, senderNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the receiving node session not found");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(receiver) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service1, "InputBuffer/Count"), 1,
            "the inbound channel is not on the sensor");

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

        // The discovery aborts the descriptor and takes it off the sensor, then the consumer is aborted in
        // turn and lets go of its buffer, terminating a descriptor which is gone. The 2 are not sampled
        // apart: the 2nd follows the 1st closely enough to race any poll.
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

// The watchdog must not fire on a queue which is moving. On a slow link every message is older than the
// idle period by the time it is confirmed, so the age of the front says nothing; only the absence of
// progress does. Staged with a peer which confirms 1 message every 50ms against a 1s idle period - the
// slack a loaded machine needs to never leave a gap of a whole period between 2 confirmations.
struct TSlowQueueTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(50);
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(1000);
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        // the peer delivers, and so confirms, only what the test replays
        peer->PauseChannelData();

        ProducerSettings = TWorkerSettings{ .MessageCount = 150, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return peer->PendingDataCount.load() >= 100; }, TDuration::Seconds(10)),
            "the messages did not reach the peer");

        // the 1st confirmation proves the replay confirms at the current generation before anything is
        // sampled: a reconciliation which slipped in during the setup would make the sender resend at a
        // new GenMinor and the copies replayed here obsolete
        auto frontAtStart = GetFrontSeqNo(session);
        peer->ProcessPending(1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetFrontSeqNo(session) > frontAtStart; }, TDuration::Seconds(5)),
            "the 1st replayed message was not confirmed");
        auto frontBefore = GetFrontSeqNo(session);
        auto pingsBefore = CountPings(session);
        auto genMinor = GetGenMinor(session);

        // 4 idle periods, the queue moving all along and its front older than the period after the 1st 20
        // confirmations
        auto deadline = TInstant::Now() + TDuration::Seconds(4);
        while (TInstant::Now() < deadline) {
            peer->ProcessPending(1);
            Sleep(TDuration::MilliSeconds(50));
        }

        auto details = TStringBuilder() << "the queue front moved from SeqNo " << frontBefore << " to "
            << GetFrontSeqNo(session) << ", " << GetQueueSize(session) << " message(s) still queued"
            << ", reconciliation log: " << GetReconciliationLog(session);
        UNIT_ASSERT_C(GetQueueSize(session) > 0, TStringBuilder() << "the queue ran dry, " << details);
        UNIT_ASSERT_C(GetGenMinor(session) == genMinor, TStringBuilder() << "the session reconciled, " << details);
        UNIT_ASSERT_C(GetFrontSeqNo(session) > frontBefore + 10, TStringBuilder() << "the queue did not move, " << details);
        UNIT_ASSERT_C(CountPings(session) == pingsBefore, TStringBuilder() << "a moving queue was pinged, " << details);

        peer->ResumeChannelData();
        WaitChannel(details);

        session.reset();
        peer.reset();
        Destroy();
        CheckQuota();
    }
};

// A push into an empty queue starts the clock of the watchdog. Without that a session idle for longer than
// the period is pinged at the next cleanup tick for a message it has only just sent, and every restart
// from idle begins with a reconciliation and a resend.
struct TIdleRestartTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(50);
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(1000);
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        // the traffic of the peer, replayed by the test, keeps the liveness probe quiet throughout; the
        // queue of the session stays empty until the push under test
        session->PauseChannelData();
        ProducerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartInboundChannel(2, true);
        UNIT_ASSERT_C(WaitFor([&]() { return session->PendingDataCount.load() >= 100; }, TDuration::Seconds(10)),
            "the traffic of the peer did not arrive");

        // idle for longer than the period, with the peer heard from every 200ms
        for (int i = 0; i < 8; ++i) {
            session->ProcessPending(1);
            Sleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(session), 0, "the session has something queued before the push");
        auto pingsBefore = CountPings(session);

        // nothing the session sends is confirmed, so the message pushed now stays queued
        peer->PauseChannelData();
        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) > 0; }, TDuration::Seconds(5)),
            "nothing was queued by the push");
        auto pushedAt = TInstant::Now();

        // No ping inside the period which starts with the push, then one once it is over - the queue is
        // stuck for real - and the next one a full period after that, not at the next tick: the resend of
        // a reconciliation restarts the clock as well. The peer keeps being heard from throughout, so none
        // of it can be the liveness probe.
        TInstant firstPing;
        TInstant secondPing;
        auto deadline = pushedAt + TDuration::Seconds(6);
        while (TInstant::Now() < deadline && !secondPing) {
            session->ProcessPending(1);
            auto pings = CountPings(session);
            if (!firstPing && pings > pingsBefore) {
                firstPing = TInstant::Now();
            } else if (firstPing && pings > pingsBefore + 1) {
                secondPing = TInstant::Now();
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        auto details = TStringBuilder() << "pinged " << (firstPing ? firstPing - pushedAt : TDuration::Zero())
            << " after the push and again " << (secondPing ? secondPing - firstPing : TDuration::Zero()) << " later"
            << ", inbound traffic left: " << session->PendingDataCount.load()
            << ", reconciliation log: " << GetReconciliationLog(session);
        UNIT_ASSERT_C(firstPing, TStringBuilder() << "the stuck queue was never pinged, " << details);
        UNIT_ASSERT_C(firstPing - pushedAt >= TDuration::MilliSeconds(700),
            TStringBuilder() << "pinged right after a push into an idle queue, " << details);
        UNIT_ASSERT_C(secondPing, TStringBuilder() << "the stuck queue was not pinged again, " << details);
        UNIT_ASSERT_C(secondPing - firstPing >= TDuration::MilliSeconds(700),
            TStringBuilder() << "pinged again right after the resend, " << details);

        peer->ResumeChannelData();
        session->ResumeChannelData();
        session.reset();
        peer.reset();
        Destroy();
    }
};

// The watchdog of the outbound half: every other trigger needs an ack, a bounce or a dropped link, and one
// TNodeState covers both directions, so the traffic of the peer cannot answer for the half it is not
// sending on. Staged with an outbound channel whose messages never reach the peer while that peer streams
// on an inbound one, replayed here inside the idle period so that the liveness probe cannot be what fires.
struct TOutboundStallTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(50);
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(200);
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        // sending on one channel and receiving on another, as a session between nodes which run stages of
        // the same query does; both sides are registered before either discovers the other
        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        session->PauseChannelData();
        // Nothing the session sends reaches the peer, so nothing can confirm it and the queue cannot drain
        // until this test lets it. Losing the acks instead would leave the reply to a discovery able to
        // recover the queue, as it carries the SeqNo the peer confirmed, and a machine which delayed this
        // test past an idle period would find the stall already gone.
        peer->PauseChannelData();

        ProducerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartInboundChannel(2, true);
        UNIT_ASSERT_C(WaitFor([&]() { return session->PendingDataCount.load() >= 60; }, TDuration::Seconds(10)),
            "the peer did not fill the inbound channel");

        auto pingsBefore = CountPings(session);

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) > 0; }, TDuration::Seconds(10)),
            "the session never had anything queued");

        // 1 message of the peer every 50ms against the 200ms idle period: the session never looks idle, so
        // the liveness probe cannot be what fires below
        bool pinged = false;
        auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (TInstant::Now() < deadline) {
            session->ProcessPending(1);
            if (CountPings(session) > pingsBefore) {
                pinged = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        auto queueWhenPinged = GetQueueSize(session);
        auto details = [&]() {
            return TStringBuilder() << "the queue holds " << GetQueueSize(session) << " message(s), "
                << queueWhenPinged << " of them when it was pinged"
                << ", inbound traffic left: " << session->PendingDataCount.load()
                << ", reconciliation log: " << GetReconciliationLog(session);
        };

        UNIT_ASSERT_C(pinged, TStringBuilder() << "the stalled outbound queue was never pinged, " << details());
        UNIT_ASSERT_C(queueWhenPinged > 0, TStringBuilder() << "the queue was not stalled, " << details());

        peer->ResumeChannelData();
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) == 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the ping did not recover the queue, " << details());

        session->ResumeChannelData();
        session.reset();
        peer.reset();
        Destroy();
    }
};

// The other reason HandleCleanup pings: a session with channels but nothing queued has no stall to watch,
// and a peer which freed its session with the link up sends no disconnect, so its inbound channels would
// hang. The discovery makes that peer announce itself for ConnectSession to fail them instead.
struct TLivenessProbeTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetCleanupPeriodMs(50);
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetIdlePingPeriodMs(200);
    }

    void Run() override {
        Prepare();
        Init();

        auto peerNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartInboundChannel(1, true);

        std::shared_ptr<TNodeState> session;
        UNIT_ASSERT_C(WaitFor([&]() { return (session = FindNodeState(Service0, peerNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the node session not found");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(session) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");

        // acks and updates do not go through the queue, so the watchdog of it has nothing to watch here
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(session), 0, "the session has something queued");

        // The ping of either session counts: both go idle within microseconds of each other, both run the
        // same timers, and whichever asks first refreshes the other and can keep it from asking at all. The
        // session of the peer is in the same state, channels and an empty queue, so its ping is the same
        // branch. This side goes stale first, by the ack round trip, but a cleanup tick is far longer.
        auto peer = FindNodeState(Service1, Runtime->GetNodeId(0));
        auto pingsBefore = CountPings(session) + (peer ? CountPings(peer) : 0);
        auto pinged = WaitFor([&]() {
            return CountPings(session) + (peer ? CountPings(peer) : 0) > pingsBefore;
        }, TDuration::Seconds(5));

        auto details = TStringBuilder() << "the session holds " << GetInputCount(session)
            << " inbound channel(s) and an empty queue, its log: " << GetReconciliationLog(session)
            << ", the log of the peer: " << (peer ? GetReconciliationLog(peer) : "no session");
        UNIT_ASSERT_C(pinged, TStringBuilder() << "the peer went quiet and the session never asked, " << details);
        UNIT_ASSERT_C(GetInputCount(session) > 0, TStringBuilder() << "the channel is gone, " << details);

        session.reset();
        peer.reset();
        Destroy();
    }
};

// A bounce naming an actor the session does not address is the echo of a copy sent to an actor already
// superseded, see HandleUndelivered: taking it for the death of the live peer used to start a major
// reconciliation, and the peer then failed every unfinished channel bound to the generation left behind.
// Both halves are pinned: the stale bounce changes nothing, the genuine one still reconciles.
struct TStaleBounceTest : public TSessionTest {

    void SendBounce(const std::shared_ptr<TNodeState>& session, NActors::TActorId bouncedFrom) {
        Runtime->Send(session->NodeActorId, bouncedFrom,
            new NActors::TEvents::TEvUndelivered(TEvDqCompute::TEvChannelDataV2::EventType,
                NActors::TEvents::TEvUndelivered::ReasonActorUnknown),
            NodeIndex0, true);
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        UNIT_ASSERT_C(receiver, "receiver node session not found");
        WaitSettled(sender);

        // the consumer lets go of the descriptor only after its TEvFinished has been grabbed, so the
        // bounce below starts from a receiver with nothing left of the warm up
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(10)),
            "the input descriptor of the warm up channel is still there");

        auto peerActorId = GetInputNodeActorId(sender);
        UNIT_ASSERT_C(peerActorId == receiver->NodeActorId, TStringBuilder() << "InputNodeActorId " << peerActorId
            << ", the session actor of the peer " << receiver->NodeActorId);

        auto genMajor = GetGenMajor(sender);
        auto details = [&]() {
            return TStringBuilder() << "GenMajor " << genMajor << " -> " << GetGenMajor(sender)
                << ", Reconciliation=" << sender->Reconciliation.load()
                << ", InputNodeActorId " << GetInputNodeActorId(sender) << " (the peer session actor " << peerActorId << ")"
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };

        // the session actor of the peer is alive all along; only the actor the bounce names differs. The
        // stale one lives on the peer node, as the superseded session actor did, so a check on the node
        // alone would not tell the two apart
        SendBounce(sender, Genuine ? peerActorId : Control1);

        if (Genuine) {
            UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(sender) == genMajor + 1 && sender->Reconciliation.load() == 0; },
                TDuration::Seconds(5)), TStringBuilder() << "the genuine bounce did not reconcile the session, " << details());
            UNIT_ASSERT_C(GetReconciliationLog(sender).Contains("U"),
                TStringBuilder() << "the major was started by something else than the bounce, " << details());
        } else {
            // nothing is expected to happen, so the wait has to time out to mean anything
            UNIT_ASSERT_C(!WaitFor([&]() { return GetGenMajor(sender) != genMajor || GetReconciliationLog(sender).Contains("U"); },
                TDuration::Seconds(2)), TStringBuilder() << "the stale bounce started a major reconciliation, " << details());
            UNIT_ASSERT_VALUES_EQUAL_C(sender->Reconciliation.load(), 0, details());
            UNIT_ASSERT_C(GetInputNodeActorId(sender) == peerActorId,
                TStringBuilder() << "the stale bounce forgot the peer, " << details());
        }

        // a channel started after the bounce still goes through, and at the generation it was left at:
        // an untouched session in the one case, a recovered one in the other
        StartChannel(2, true);
        WaitChannel(details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + (Genuine ? 1 : 0), details());

        // a node session logs through the actor system from its destructor, so it may not outlive it
        sender.reset();
        receiver.reset();
        Destroy();
        CheckQuota();
    }

    // the bounce names the session actor of the peer, as one from a peer which really died
    bool Genuine = false;
};

// SendFromWaiters published that the last waiting message of a channel was gone from its WaitQueue before
// the message had its SeqNo, so a push of the channel in between skipped the WaitQueue and overtook it. With
// the finish overtaking, the receiver dropped the message as late and the channel finished short of it.
// Staged by parking the session right there and pushing the finish meanwhile.
struct TWaiterOvertakeTest : public TSessionTest {

    void Prepare() override {
        TSessionTest::Prepare();
        // a single message fills the window, so the 2nd one has to wait for the ack of the 1st
        settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig()->SetRemoteSessionInflightBytes(100);
    }

    void Run() override {
        Prepare();
        Init();

        auto receiverNodeId = Runtime->GetNodeId(1);

        auto sender = Service0->CreateDebugNodeState(receiverNodeId);
        sender->StartSession();
        WaitSettled(sender);

        // the ack of the 1st message is held back until the 2nd one waits for the window
        sender->PauseChannelAck();

        ProducerSettings = TWorkerSettings{ .StartDelayMs = 0, .MessageCount = 2, .MinMessageSize = 150, .MaxMessageSize = 151,
            .CheckOrder = true, .FinishOnStep = true };
        ConsumerSettings = ProducerSettings;
        auto channel = StartChannel(1, true);

        std::shared_ptr<TOutputDescriptor> descriptor;
        UNIT_ASSERT_C(WaitFor([&]() {
            descriptor = FindOutputDescriptor(sender, 1);
            return descriptor && descriptor->WaitQueueSize.load() == 1 && GetQueueSize(sender) == 1;
        }, TDuration::Seconds(10)), "the 2nd message does not wait for the window");

        sender->HoldWaiterDequeue.store(true);
        sender->ResumeChannelAck();
        UNIT_ASSERT_C(WaitFor([&]() { return sender->WaiterDequeueHeld.load(); }, TDuration::Seconds(5)),
            "the waiting message was not taken off its WaitQueue");

        // the finish is pushed while the waiting message has no SeqNo yet; it must queue up behind it
        Runtime->Send(channel.first, Control0, new TEvTestPrivate::TEvStep(), NodeIndex0, true);
        Sleep(TDuration::MilliSeconds(200));
        sender->HoldWaiterDequeue.store(false);

        WaitChannel([&]() { return TStringBuilder() << "reconciliation log: " << GetReconciliationLog(sender); });

        // a node session logs through the actor system from its destructor, so it may not outlive it
        descriptor.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// Many channels behind a session window a few messages wide, and channel windows so narrow that a producer
// pushes a message or two at a time: the WaitQueue of every channel keeps emptying and filling, which is
// where a push could overtake a waiting message. Every consumer checks the order of what it gets.
struct TOrderTest : public TLoadTest {

    void Prepare() override {
        TLoadTest::Prepare();
        auto* config = settings.AppConfig.MutableTableServiceConfig()->MutableDqChannelConfig();
        config->SetRemoteSessionInflightBytes(4096);
        config->SetRemoteChannelInflightBytes(2048);
        config->SetRemoteChannelColdInflightBytes(1024);
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

    Y_UNIT_TEST(OutboundStallPingedWhilePeerStreams) {
        TOutboundStallTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(SlowQueueNotPinged) {
        TSlowQueueTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(IdleRestartNotPingedEarly) {
        TIdleRestartTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(QuietPeerProbed) {
        TLivenessProbeTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(WaiterNotOvertakenByFastPath) {
        TWaiterOvertakeTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(OrderUnderWaiterPressure) {
        TOrderTest test;

        test.Count = 20;
        test.Local = false;
        test.ProducerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 4, .MaxMessageSize = 1000, .CheckOrder = true };
        test.ConsumerSettings = test.ProducerSettings;

        test.Run();
    }

    Y_UNIT_TEST(StaleBounceIgnored) {
        TStaleBounceTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(GenuineBounceReconciles) {
        TStaleBounceTest test;

        test.Local = false;
        test.Genuine = true;

        test.Run();
    }

    // Disabled while the defect it reproduces is open, see TInboundChannelAbortTest above; enable it with
    // the fix. The body stays compiled so that it keeps up with the helpers it uses.
    /*
    Y_UNIT_TEST(InboundChannelAbortedByOutboundTimeout) {
        TInboundChannelAbortTest test;

        test.Local = false;

        test.Run();
    }
    */
}
