#pragma once

// Shared harness of the DQ Channels 2.0 tests: producer / consumer worker actors driving IChannelBuffer
// directly, a two node TKikimrRunner and direct access to the node sessions of both channel services.

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>
#include <ydb/library/yql/dq/runtime/ut/ut_helper.h>

#include <ydb/library/yql/dq/actors/dq.h>
#include <util/random/random.h>
#include <util/datetime/base.h>
#include <util/system/unaligned_mem.h>

#include <array>
#include <atomic>
#include <functional>
#include <optional>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_CHANNELS

using namespace NKikimr::NKqp;
using namespace NYql::NDq;

using namespace NYdb;
using namespace NYdb::NTable;

template<>
inline void Out<NYql::NDq::EDqFillLevel>(IOutputStream& os, const NYql::NDq::EDqFillLevel l) {
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
        EvCallback,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvStart : public NActors::TEventLocal<TEvStart, EvStart> {
        TEvStart(NActors::TActorId peerId) : PeerId(peerId) {}
        NActors::TActorId PeerId;
    };

    // Error is what the test counts as a failure: a verification failure, or an abort the settings did
    // not expect. Aborted tells an abort from a verification failure, Reason says what happened.
    struct TFinishInfo {
        ERole Role;
        ui32 ChannelId;
        bool Error;
        bool Aborted;
        TString Reason;
        TDqAsyncStats PushStats;
        TDqAsyncStats PopStats;
    };

    struct TEvFinished : public NActors::TEventLocal<TEvFinished, EvFinished>, public TFinishInfo {
        TEvFinished(ERole role, ui32 channelId, bool error, bool aborted = false, const TString& reason = {})
            : TFinishInfo{role, channelId, error, aborted, reason, {}, {}} {}
    };
};

// runs callbacks inside an actor, for what the service expects to be called from one - the wake-up
// callback of a channel storage notifies through TActivationContext
class TCallbackActor : public NActors::TActor<TCallbackActor> {
public:
    struct TEvCallback : public NActors::TEventLocal<TEvCallback, TEvTestPrivate::EvCallback> {
        TEvCallback(std::function<void()> callback) : Callback(std::move(callback)) {}
        std::function<void()> Callback;
    };

    TCallbackActor() : NActors::TActor<TCallbackActor>(&TCallbackActor::StateFunc) {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCallback, Handle);
        }
    }

    void Handle(TEvCallback::TPtr& ev) {
        ev->Get()->Callback();
    }
};

inline TString RoleName(TEvTestPrivate::ERole role) {
    return role == TEvTestPrivate::ERole::Producer ? "producer" : "consumer";
}

// Tracks quota strictly - it is an error to free more bytes than were allocated (like the real
// TChannelQuotaManager does with VERIFY) and to leave anything allocated at the end of the test.
struct TTestQuotaManager : public IMemoryQuotaManager {

    TTestQuotaManager(ui64 limit = DefaultLimit) : Limit(limit) {}

    bool AllocateQuota(ui64 memorySize, bool /* isOptional */) override {
        if (Quota.load() + static_cast<i64>(memorySize) > static_cast<i64>(Limit.load())) {
            Rejected++;
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
        return Limit.load();
    }

    // stands for the node level memory availability, see NRm::TTxState::GetMemoryAvailability:
    // negative under memory pressure, otherwise what is left of the limit
    i64 GetMemoryAvailability() const override {
        return MemoryPressure.load() ? -1 : static_cast<i64>(Limit.load()) - Quota.load();
    }

    TString MemoryConsumptionDetails() const override {
        return TStringBuilder() << "Quota=" << Quota.load() << ", Limit=" << Limit.load();
    }

    static constexpr ui64 DefaultLimit = 1ull << 30; // large enough to never be exceeded by the tests
    std::atomic<ui64> Limit;
    std::atomic<bool> MemoryPressure = false;
    std::atomic<i64> Quota = 0;
    std::atomic<ui64> Allocated = 0;
    std::atomic<ui64> Freed = 0;
    std::atomic<ui64> Underflows = 0;
    std::atomic<ui64> Rejected = 0;
};

struct TWorkerSettings {
    int StartDelayMs = 10;
    int MessageCount = 0;
    int MinMessageSize = 10;
    int MaxMessageSize = 10000;
    bool EarlyFinish = false;
    int PauseMessageIndex = -1;
    int PauseDelayMs = 0;
    // producer: keep pushing at SoftLimit (a spilling channel reports it while anything is spilled)
    bool PushOnSoftLimit = false;
    // producer: a checkpoint (Id = index) / watermark (TimestampUs = index) precedes every Nth data
    // message, 0 for none; consumer: what to expect, at those exact positions
    int CheckpointEvery = 0;
    int WatermarkEvery = 0;
    // producer: one more checkpoint (Id = MessageCount) after the finish; consumer: expect it
    bool CheckpointAfterFinish = false;
    // producer: this many data messages after the finish, which the service must drop
    int DataAfterFinish = 0;
    // the worker is expected to be aborted by the service; finishing normally is the error then
    bool ExpectAbort = false;
    // producer: IsEarlyFinished() must hold once finished
    bool ExpectEarlyFinished = false;
    // consumer: let go of the buffer right after the finish chunk, without waiting for IsFinished()
    bool LeaveAfterFinishChunk = false;
    // pause once at a random message for a random delay up to this many ms, 0 for the fixed pause above
    int RandomPauseMaxMs = 0;
    TCollectStatsLevel StatsLevel = TCollectStatsLevel::None;
    // producer: a storage for the output buffer, by channel id
    std::function<IDqChannelStorage::TPtr(ui32)> StorageFactory;
};

// Applied to TDebugNodeState sessions on both nodes, see TLoadTest::Init. Percentages of messages lost
// on arrival; the Count fields limit the number of messages the probability applies to, 0 for all.
struct TFailureSettings {
    int Data = 0;
    ui64 DataCount = 0;
    int Ack = 0;
    ui64 AckCount = 0;
    ui64 Update = 0;    // the next N updates are lost
    ui64 Discovery = 0; // the next N discoveries are lost

    bool Any() const {
        return Data || Ack || Update || Discovery;
    }
};

// Every data payload of at least this size starts with the index of the message and its size, so the
// consumer can tell a reordered, duplicated or corrupted chunk from a good one
constexpr ui32 PayloadHeaderSize = 2 * sizeof(ui32);

template <typename TDerived>
class TWorkerActor : public NActors::TActor<TDerived> {
public:
    TWorkerActor(TEvTestPrivate::ERole role, const TString& logPrefix, std::shared_ptr<IDqChannelService> service, ui32 channelId,
        const TWorkerSettings& settings, IMemoryQuotaManager::TPtr quotaManager)
        : NActors::TActor<TDerived>(&TWorkerActor::StateFunc)
        , Role(role)
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

    // the peer may bind first and its buffer notifies this side before the runner started it
    virtual void HandleResume(TEvDqCompute::TEvResumeExecution::TPtr&) {
        if (RunnerId) {
            Run();
        }
    }

    virtual void HandleStart(TEvTestPrivate::TEvStart::TPtr& ev) {
        RunnerId = ev->Sender;
        PeerId = ev->Get()->PeerId;
        YDB_LOG_DEBUG("TEST START",
            {"selfId", this->SelfId()},
            {"channelId", ChannelId},
            {"peerId", PeerId});
        if (PendingAbort) {
            Finish(!Settings.ExpectAbort, true, *PendingAbort);
            return;
        }
        if (Settings.RandomPauseMaxMs && Settings.MessageCount) {
            Settings.PauseMessageIndex = RandomNumber<ui64>(Settings.MessageCount);
            Settings.PauseDelayMs = RandomNumber<ui64>(Settings.RandomPauseMaxMs) + 1;
        }
        if (Settings.StartDelayMs) {
            this->Schedule(TDuration::MilliSeconds(RandomNumber<ui64>(Settings.StartDelayMs) + 1), new NActors::TEvents::TEvWakeup());
        } else {
            Run();
        }
    }

    virtual void HandleAbort(NYql::NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
        auto reason = TStringBuilder() << "aborted with " << NYql::NDqProto::StatusIds::StatusCode_Name(ev->Get()->Record.GetStatusCode())
            << ": " << ev->Get()->GetIssues().ToOneLineString();
        YDB_LOG_DEBUG("Test worker received abort execution",
            {"selfId", this->SelfId()},
            {"channelId", ChannelId},
            {"reason", reason});
        if (!RunnerId) {
            PendingAbort = reason; // reported once there is a runner to report to
            return;
        }
        Finish(!Settings.ExpectAbort, true, reason);
    }

    // The buffer is released with the actor, which is what the service sees as the channel let go of
    void Finish(bool error, bool aborted, const TString& reason) {
        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST FINISHED SelfId=" << this->SelfId()
            << ", ChannelId=" << ChannelId << ", Error=" << error << ", Aborted=" << aborted << ", Reason=" << reason);
        auto ev = new TEvTestPrivate::TEvFinished(Role, ChannelId, error, aborted, reason);
        if (Buffer) {
            Buffer->ExportPushStats(ev->PushStats);
            Buffer->ExportPopStats(ev->PopStats);
        }
        this->Send(RunnerId, ev);
        this->PassAway();
    }

    void Fail(const TString& reason) {
        Finish(true, false, TStringBuilder() << RoleName(Role) << " of channel " << ChannelId << ": " << reason);
    }

    // true while the worker has to wait for the pause to end
    bool Paused() {
        if (Settings.PauseMessageIndex != MessageIndex) {
            return false;
        }
        if (!ResumeTime) {
            ResumeTime = TInstant::Now() + TDuration::MilliSeconds(Settings.PauseDelayMs);
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST PAUSED SelfId=" << this->SelfId() << ", ChannelId=" << ChannelId);
        }
        if (TInstant::Now() < ResumeTime) {
            this->Schedule(ResumeTime, new NActors::TEvents::TEvWakeup());
            return true;
        }
        if (!Resumed) {
            Resumed = true;
            LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST RESUMED SelfId=" << this->SelfId() << ", ChannelId=" << ChannelId);
        }
        return false;
    }

    TEvTestPrivate::ERole Role;
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
    TInstant ResumeTime;
    bool Resumed = false;
    std::optional<TString> PendingAbort;
};

class TProducerActor : public TWorkerActor<TProducerActor> {
public:
    TProducerActor(std::shared_ptr<IDqChannelService> service, ui32 channelId, const TWorkerSettings& settings,
        IMemoryQuotaManager::TPtr quotaManager)
        : TWorkerActor(TEvTestPrivate::ERole::Producer, "PROD ", service, channelId, settings, std::move(quotaManager))
    {}

    static TDataChunk BuildData(ui32 index, ui32 bytes) {
        TString payload(bytes, 'a');
        if (bytes >= PayloadHeaderSize) {
            WriteUnaligned<ui32>(payload.Detach(), index);
            WriteUnaligned<ui32>(payload.Detach() + sizeof(ui32), bytes);
        }
        return TDataChunk(NYql::TChunkedBuffer(std::move(payload)), 1, false);
    }

    void PushCheckpoint(ui64 id) {
        NYql::NDqProto::TCheckpoint checkpoint;
        checkpoint.SetId(id);
        checkpoint.SetGeneration(1);
        Buffer->Push(TDataChunk(std::move(checkpoint)));
    }

    void PushWatermark(ui64 timestampUs) {
        NYql::NDqProto::TWatermark watermark;
        watermark.SetTimestampUs(timestampUs);
        Buffer->Push(TDataChunk(std::move(watermark)));
    }

    bool CanPush() {
        auto level = Buffer->GetFillLevel();
        return level == EDqFillLevel::NoLimit || (Settings.PushOnSoftLimit && level == EDqFillLevel::SoftLimit);
    }

    void Run() override {
        if (!Started) {
            TChannelFullInfo info(ChannelId, SelfId(), PeerId, 0, 1, Settings.StatsLevel);
            IDqChannelStorage::TPtr storage = Settings.StorageFactory ? Settings.StorageFactory(ChannelId) : nullptr;
            Buffer = Service->GetOutputBuffer(info, QuotaManager, storage);
            Started = true;
        }
        if (Buffer->IsFinished()) {
            if (Settings.ExpectAbort) {
                Fail("finished without the expected abort");
            } else if (Settings.ExpectEarlyFinished && !Buffer->IsEarlyFinished()) {
                Fail("finished without IsEarlyFinished");
            } else {
                Finish(false, false, {});
            }
            return;
        }
        while (CanPush() && MessageIndex < Settings.MessageCount) {
            if (Paused()) {
                return;
            }
            if (MessageIndex && Settings.CheckpointEvery && MessageIndex % Settings.CheckpointEvery == 0) {
                PushCheckpoint(MessageIndex);
            }
            if (MessageIndex && Settings.WatermarkEvery && MessageIndex % Settings.WatermarkEvery == 0) {
                PushWatermark(MessageIndex);
            }
            auto bytes = Settings.MinMessageSize + RandomNumber<ui64>(Settings.MaxMessageSize - Settings.MinMessageSize + 1);
            Buffer->Push(BuildData(MessageIndex, bytes));
            MessageIndex++;
        }
        if (MessageIndex == Settings.MessageCount && !FinishSent) {
            FinishSent = true;
            Buffer->SendFinish();
            if (Settings.CheckpointAfterFinish) {
                PushCheckpoint(Settings.MessageCount);
            }
            for (int i = 0; i < Settings.DataAfterFinish; i++) {
                Buffer->Push(BuildData(Settings.MessageCount + i, Settings.MinMessageSize));
            }
        }
    }

    bool FinishSent = false;
};

class TConsumerActor : public TWorkerActor<TConsumerActor> {
public:
    TConsumerActor(std::shared_ptr<IDqChannelService> service, ui32 channelId, const TWorkerSettings& settings,
        IMemoryQuotaManager::TPtr quotaManager)
        : TWorkerActor(TEvTestPrivate::ERole::Consumer, "CONS ", service, channelId, settings, std::move(quotaManager))
    {}

    // a chunk in the order the producer pushed them: control chunks before the data message of their
    // index, data messages in sequence, the finish last; empty reason if so
    TString Verify(const TDataChunk& data) {
        if (data.Checkpoint) {
            auto id = data.Checkpoint->GetId();
            auto expected = FinishSeen ? Settings.MessageCount : MessageIndex;
            bool allowed = FinishSeen ? Settings.CheckpointAfterFinish
                : (Settings.CheckpointEvery && MessageIndex && MessageIndex % Settings.CheckpointEvery == 0);
            if (!allowed || id != static_cast<ui64>(expected) || (LastCheckpointId && id <= *LastCheckpointId)) {
                return TStringBuilder() << "unexpected checkpoint " << id << " at message " << MessageIndex
                    << ", finish seen: " << FinishSeen << ", last checkpoint: " << (LastCheckpointId ? ToString(*LastCheckpointId) : "none");
            }
            LastCheckpointId = id;
            Checkpoints++;
            return {};
        }
        if (data.Watermark) {
            auto ts = data.Watermark->GetTimestampUs();
            bool allowed = !FinishSeen && Settings.WatermarkEvery && MessageIndex && MessageIndex % Settings.WatermarkEvery == 0;
            if (!allowed || ts != static_cast<ui64>(MessageIndex) || (LastWatermark && ts <= *LastWatermark)) {
                return TStringBuilder() << "unexpected watermark " << ts << " at message " << MessageIndex
                    << ", last watermark: " << (LastWatermark ? ToString(*LastWatermark) : "none");
            }
            LastWatermark = ts;
            Watermarks++;
            return {};
        }
        if (FinishSeen) {
            return TStringBuilder() << "a chunk after the finish, bytes=" << data.Bytes << ", finished=" << data.Finished;
        }
        // a data message may have an empty payload, it is told from the finish chunk by its rows
        if (!data.Buffer.Empty() || data.Rows) {
            if (MessageIndex >= Settings.MessageCount) {
                return TStringBuilder() << "data message beyond MessageCount=" << Settings.MessageCount;
            }
            auto size = data.Buffer.Size();
            if (data.Bytes != size + 1) {
                return TStringBuilder() << "Bytes=" << data.Bytes << " vs payload size " << size;
            }
            if (size >= PayloadHeaderSize) {
                TString payload;
                for (auto rest = data.Buffer; !rest.Empty(); rest.Erase(rest.Front().Buf.size())) {
                    payload += rest.Front().Buf;
                    if (payload.size() >= PayloadHeaderSize) {
                        break;
                    }
                }
                auto index = ReadUnaligned<ui32>(payload.data());
                auto bytes = ReadUnaligned<ui32>(payload.data() + sizeof(ui32));
                if (index != static_cast<ui32>(MessageIndex)) {
                    return TStringBuilder() << "message " << index << " where " << MessageIndex << " was expected";
                }
                if (bytes != size) {
                    return TStringBuilder() << "message " << index << " with " << size << " bytes instead of " << bytes;
                }
            }
            if (data.Rows != 1) {
                return TStringBuilder() << "message " << MessageIndex << " with Rows=" << data.Rows;
            }
            MessageIndex++;
        }
        if (data.Finished) {
            FinishSeen = true;
        }
        return {};
    }

    void Run() override {
        if (!Started) {
            TChannelFullInfo info(ChannelId, PeerId, SelfId(), 0, 1, Settings.StatsLevel);
            Buffer = Service->GetInputBuffer(info, QuotaManager);
            Started = true;
        }
        TDataChunk data;
        while (!EarlyFinished) {
            if (MessageIndex < Settings.MessageCount && Paused()) {
                return;
            }
            if (Settings.EarlyFinish && MessageIndex == Settings.MessageCount) {
                LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, LogPrefix << "TEST EARLY FINISH SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                Buffer->EarlyFinish();
                EarlyFinished = true;
                break;
            }
            if (!Buffer->Pop(data)) {
                break;
            }
            if (auto reason = Verify(data)) {
                Fail(reason);
                return;
            }
            if (FinishSeen && Settings.LeaveAfterFinishChunk) {
                Finish(false, false, {});
                return;
            }
        }
        if (Buffer->IsFinished()) {
            if (Settings.ExpectAbort) {
                Fail("finished without the expected abort");
            } else if (auto reason = VerifyFinished()) {
                Fail(reason);
            } else {
                Finish(false, false, {});
            }
        }
    }

    TString VerifyFinished() {
        if (EarlyFinished) {
            return {};
        }
        if (MessageIndex != Settings.MessageCount) {
            return TStringBuilder() << "finished with " << MessageIndex << " of " << Settings.MessageCount << " messages";
        }
        if (!FinishSeen) {
            return "finished without the finish chunk";
        }
        int checkpoints = 0;
        int watermarks = 0;
        for (int i = 1; i < Settings.MessageCount; i++) {
            checkpoints += Settings.CheckpointEvery && i % Settings.CheckpointEvery == 0;
            watermarks += Settings.WatermarkEvery && i % Settings.WatermarkEvery == 0;
        }
        checkpoints += Settings.CheckpointAfterFinish;
        if (Checkpoints != checkpoints || Watermarks != watermarks) {
            return TStringBuilder() << "popped " << Checkpoints << " checkpoints and " << Watermarks << " watermarks, expected "
                << checkpoints << " and " << watermarks;
        }
        return {};
    }

    bool EarlyFinished = false;
    bool FinishSeen = false;
    int Checkpoints = 0;
    int Watermarks = 0;
    std::optional<ui64> LastCheckpointId;
    std::optional<ui64> LastWatermark;
};

struct TLoadTest {

    virtual ~TLoadTest() = default;

    virtual void Prepare() {
        settings.NodeCount = Local ? 1 : 2;
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::EPriority::PRI_TRACE);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
        if (Local) {
            NodeIndex1 = NodeIndex0;
        }
        if (Failures.Any()) {
            // a lost last ack of a session is recovered by the idle ping, within the wait for the sensors
            Limits.CleanupPeriod = Min(Limits.CleanupPeriod, TDuration::MilliSeconds(50));
            Limits.IdlePingPeriod = Min(Limits.IdlePingPeriod, TDuration::MilliSeconds(200));
        }
        auto& tableService = *settings.AppConfig.MutableTableServiceConfig();
        tableService.SetEnableSpillingChannelBackpressure(Limits.EnableSpillingChannelBackpressure);
        auto& config = *tableService.MutableDqChannelConfig();
        config.SetLocalChannelInflightBytes(Limits.LocalChannelInflightBytes);
        config.SetLocalChannelColdInflightBytes(Limits.LocalChannelColdInflightBytes);
        config.SetRemoteChannelInflightBytes(Limits.RemoteChannelInflightBytes);
        config.SetRemoteChannelColdInflightBytes(Limits.RemoteChannelColdInflightBytes);
        config.SetRemoteSessionInflightBytes(Limits.RemoteSessionInflightBytes);
        config.SetReconciliationCount(Limits.ReconciliationCount);
        config.SetCleanupPeriodMs(Limits.CleanupPeriod.MilliSeconds());
        config.SetIdlePingPeriodMs(Limits.IdlePingPeriod.MilliSeconds());
        config.SetIdleDestroyPeriodMs(Limits.IdleDestroyPeriod.MilliSeconds());
        config.SetUnboundWaitPeriodMs(Limits.UnboundWaitPeriod.MilliSeconds());
    }

    virtual void Init() {
        Runner = std::make_unique<TKikimrRunner>(settings);
        Runtime = Runner->GetTestServer().GetRuntime();
        Runtime->SetUseRealInterconnect();

        // the real bound of every GrabEdgeEvent: its own timeout is in simulated time
        Runtime->SetDispatchTimeout(WaitTimeout);

        Control0 = Runtime->AllocateEdgeActor(0);
        Control1 = Local ? Control0 : Runtime->AllocateEdgeActor(1);
        Callback0 = Runtime->Register(new TCallbackActor(), NodeIndex0);
        Callback1 = Local ? Callback0 : Runtime->Register(new TCallbackActor(), NodeIndex1);

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

        if (!Local && (Failures.Any() || UseDebugSessions)) {
            InitDebugSessions();
        }
    }

    void RunInActor(ui32 nodeIndex, std::function<void()> callback) {
        auto actor = nodeIndex == NodeIndex0 ? Callback0 : Callback1;
        auto control = nodeIndex == NodeIndex0 ? Control0 : Control1;
        Runtime->Send(actor, control, new TCallbackActor::TEvCallback(std::move(callback)), nodeIndex, true);
    }

    // Both sessions are debug ones, registered before either discovers its peer (a discovery makes the
    // service of the peer create a regular session and CreateDebugNodeState refuses to replace one).
    void InitDebugSessions() {
        Debug0 = Service0->CreateDebugNodeState(Runtime->GetNodeId(1));
        Debug1 = Service1->CreateDebugNodeState(Runtime->GetNodeId(0));
        for (auto& debug : {Debug0, Debug1}) {
            debug->SetLossProbability(Failures.Data / 100.0, Failures.DataCount, Failures.Ack / 100.0, Failures.AckCount);
            debug->DropUpdateCount.store(Failures.Update);
            debug->DropDiscoveryCount.store(Failures.Discovery);
        }
        Debug0->StartSession();
        Debug1->StartSession();
    }

    void StartPair(NActors::TActorId producer, NActors::TActorId consumer, ui32 producerNodeIndex, ui32 consumerNodeIndex) {
        auto producerControl = producerNodeIndex == NodeIndex0 ? Control0 : Control1;
        auto consumerControl = consumerNodeIndex == NodeIndex0 ? Control0 : Control1;
        Runtime->Send(consumer, consumerControl, new TEvTestPrivate::TEvStart(producer), consumerNodeIndex, true);
        Runtime->Send(producer, producerControl, new TEvTestPrivate::TEvStart(consumer), producerNodeIndex, true);
        Actors.insert(producer);
        Actors.insert(consumer);
    }

    virtual void Start() {
        for (auto i = 0; i < Count; i ++) {
            auto channelId = i + 1;
            if ((i & 1) == 0) {
                auto producer = Runtime->Register(new TProducerActor(Service0, channelId, ProducerSettings, OutputQuotaManager), NodeIndex0);
                auto consumer = Runtime->Register(new TConsumerActor(Service1, channelId, ConsumerSettings, InputQuotaManager), NodeIndex1);
                StartPair(producer, consumer, NodeIndex0, NodeIndex1);
            } else {
                auto producer = Runtime->Register(new TProducerActor(Service1, channelId, ProducerSettings, OutputQuotaManager), NodeIndex1);
                auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
                StartPair(producer, consumer, NodeIndex1, NodeIndex0);
            }
        }
    }

    void Account(const TEvTestPrivate::TEvFinished::TPtr& msg, ui32 nodeIndex) {
        Actors.erase(msg->Sender);
        FinishCount[nodeIndex][msg->Get()->Role]++;
        ErrorCount += msg->Get()->Error;
        AbortCount += msg->Get()->Aborted;
        if (msg->Get()->Error && Errors.size() < 10) {
            Errors.push_back(msg->Get()->Reason);
        }
        Finished.push_back(static_cast<const TEvTestPrivate::TFinishInfo&>(*msg->Get()));
    }

    // the finish of the given role of the given channel among those collected so far
    const TEvTestPrivate::TFinishInfo* FindFinished(ui32 channelId, TEvTestPrivate::ERole role) const {
        for (const auto& info : Finished) {
            if (info.ChannelId == channelId && info.Role == role) {
                return &info;
            }
        }
        return nullptr;
    }

    TString ErrorDetails() const {
        TStringBuilder builder;
        builder << "ErrorCount=" << ErrorCount << ", AbortCount=" << AbortCount;
        for (const auto& error : Errors) {
            builder << "\n  " << error;
        }
        return builder;
    }

    virtual void Wait() {
        try {
            for (auto i = 0; i < Count; i++) {
                auto msg0 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, WaitTimeout);
                Account(msg0, NodeIndex0);
                auto msg1 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, WaitTimeout);
                Account(msg1, NodeIndex1);
            }
        } catch (NActors::TEmptyEventQueueException&) {
            if (!Actors.empty()) {
                TStringBuilder builder;
                builder << "NOT FINISHED ACTORS ";
                for (auto actorId : Actors) {
                    builder << ' ' << actorId;
                }
                builder << ", " << ErrorDetails();
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
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
    }

    static i64 GetCounter(const std::shared_ptr<TDqChannelService>& service, const TString& name, bool derivative = false) {
        return service->Counters->GetCounter(name, derivative)->Val();
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

    static TString SensorDetails(const std::shared_ptr<TDqChannelService>& service) {
        TStringBuilder builder;
        for (auto name : Gauges) {
            builder << name << '=' << GetCounter(service, name) << ' ';
        }
        std::lock_guard lock(service->Mutex);
        for (auto& [nodeId, state] : service->NodeStates) {
            std::lock_guard stateLock(state->Mutex);
            builder << "; session " << state->LogPrefix << "queue=" << state->Queue.size()
                << ", outputs=" << state->OutputDescriptors.size() << ", inputs=" << state->InputDescriptors.size()
                << ", log=" << state->GetReconciliationLog();
        }
        return builder;
    }

    // Every gauge is back to 0 once every worker has let go of its buffer, which happens right after its
    // TEvFinished, so this is polled. The derived counters say whether the run was clean.
    virtual void CheckSensors() {
        for (auto& service : {Service0, Service1}) {
            auto settled = WaitFor([&]() {
                for (auto name : Gauges) {
                    if (GetCounter(service, name) != 0) {
                        return false;
                    }
                }
                return true;
            }, TDuration::Seconds(5));
            UNIT_ASSERT_C(settled, TStringBuilder() << "sensors of node " << service->NodeId << " not back to 0: " << SensorDetails(service));
            if (!ExpectReconciliation) {
                UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(service, "Session/Reconciliations", true), 0, "a reconciliation in a clean run");
                UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(service, "Session/MessagesResent", true), 0, "a resend in a clean run");
            }
        }
    }

    // stops the actor system, all channel descriptors are destroyed and all quota is released here
    virtual void Destroy() {
        // a node session logs through the actor system from its destructor, so it may not outlive it
        Debug0.reset();
        Debug1.reset();
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
                << ", Underflows=" << quotaManager->Underflows.load()
                << ", Rejected=" << quotaManager->Rejected.load();
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
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    static constexpr std::array<const char*, 11> Gauges = {
        "OutputBuffer/Count", "OutputBuffer/InflightBytes", "OutputBuffer/InflightMessages",
        "OutputBuffer/WaiterCount", "OutputBuffer/WaiterBytes", "OutputBuffer/WaiterMessages", "OutputBuffer/ThrottledCount",
        "InputBuffer/Count", "InputBuffer/InflightBytes",
        "LocalBuffer/Count", "LocalBuffer/InflightBytes",
    };

    int Count = 1;
    bool Local = true;
    ui32 NodeIndex0 = 0;
    ui32 NodeIndex1 = 1;
    TKikimrSettings settings;
    TDqChannelLimits Limits;
    TFailureSettings Failures;
    bool UseDebugSessions = false;
    bool ExpectReconciliation = false;
    TDuration WaitTimeout = TDuration::Seconds(10);
    std::unique_ptr<TKikimrRunner> Runner;
    NActors::TTestActorRuntime* Runtime;
    std::shared_ptr<TDqChannelService> Service0;
    std::shared_ptr<TDqChannelService> Service1;
    std::shared_ptr<TDebugNodeState> Debug0;
    std::shared_ptr<TDebugNodeState> Debug1;
    NActors::TActorId Control0;
    NActors::TActorId Control1;
    NActors::TActorId Callback0;
    NActors::TActorId Callback1;
    TWorkerSettings ProducerSettings;
    TWorkerSettings ConsumerSettings;
    std::shared_ptr<TTestQuotaManager> OutputQuotaManager = std::make_shared<TTestQuotaManager>();
    std::shared_ptr<TTestQuotaManager> InputQuotaManager = std::make_shared<TTestQuotaManager>();
    THashSet<NActors::TActorId> Actors;
    int ErrorCount = 0;
    int AbortCount = 0;
    TVector<TString> Errors;
    TVector<TEvTestPrivate::TFinishInfo> Finished;
    int FinishCount[2][2] = {{0, 0}, {0, 0}};
};

// Keeps the consumer node under permanent memory pressure, so every channel is throttled down to the
// cold inflight window. The transfer must still complete - cold inflight is never zero.
struct TMemoryPressureTest : public TLoadTest {

    void Prepare() override {
        Limits.EnableSpillingChannelBackpressure = true;
        TLoadTest::Prepare();
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
        // the descriptors die under pressure, the sensor is decremented by their destructors
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// Single channel, the consumer pops a few messages (so the channel gets warm) and then stalls.
// While it stalls the producer must be blocked at the cold inflight window rather than at the
// full RemoteChannelInflightBytes one - but only when EnableSpillingChannelBackpressure is set.
struct TThrottleTest : public TLoadTest {

    void Prepare() override {
        Limits.EnableSpillingChannelBackpressure = Enabled;
        TLoadTest::Prepare();
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
        CheckSensors();
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
                << ", ColdInflightBytes=" << descriptor->ColdInflightBytes
                << ", ThrottledCount=" << GetCounter(Service0, "OutputBuffer/ThrottledCount");
        };

        UNIT_ASSERT_C(warm, details());

        if (Enabled) {
            // the bound holds at every moment, but the pressure needs one update to reach the sender
            UNIT_ASSERT_C(WaitFor([](const auto& d) { return d->PeerMemoryPressure.load(); }, descriptor), details());
            UNIT_ASSERT_LE_C(descriptor->PushBytes.load() - descriptor->RemotePopBytes.load(), coldBytes, details());
            UNIT_ASSERT_LT_C(descriptor->PushBytes.load() - descriptor->RemotePopBytes.load(),
                descriptor->MaxInflightBytes, details());
            UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "OutputBuffer/ThrottledCount"), 1, details());
        } else {
            // nothing throttles the producer, it pushes far past the cold window while the consumer sleeps
            UNIT_ASSERT_C(WaitFor([&](const auto& d) {
                return d->PushBytes.load() - d->RemotePopBytes.load() > coldBytes; }, descriptor), details());
            UNIT_ASSERT_C(!descriptor->PeerMemoryPressure.load(), details());
            UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "OutputBuffer/ThrottledCount"), 0, details());
        }
    }

    bool Enabled = true;
};

struct TReconTest : public TLoadTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(20);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(10);
        ExpectReconciliation = true;
        TLoadTest::Prepare();
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
                StartPair(producer, consumer, NodeIndex0, NodeIndex1);
            } else {
                auto producerSettings = ProducerSettings;
                producerSettings.PauseMessageIndex = channelId;
                producerSettings.PauseDelayMs = 50;
                auto producer = Runtime->Register(new TProducerActor(Service1, channelId, producerSettings, OutputQuotaManager), NodeIndex1);
                auto consumer = Runtime->Register(new TConsumerActor(Service0, channelId, ConsumerSettings, InputQuotaManager), NodeIndex0);
                StartPair(producer, consumer, NodeIndex1, NodeIndex0);
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

    static ui64 GetOutputCount(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->OutputDescriptors.size();
    }

    static ui64 GetWaitersQueueSize(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        return state->WaitersQueue.size();
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

    static std::vector<std::shared_ptr<TOutputDescriptor>> GetOutputDescriptors(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        std::vector<std::shared_ptr<TOutputDescriptor>> result;
        for (const auto& [info, descriptor] : state->OutputDescriptors) {
            result.push_back(descriptor);
        }
        return result;
    }

    static std::vector<std::shared_ptr<TInputDescriptor>> GetInputDescriptors(const std::shared_ptr<TNodeState>& state) {
        std::lock_guard lock(state->Mutex);
        std::vector<std::shared_ptr<TInputDescriptor>> result;
        for (const auto& [info, descriptor] : state->InputDescriptors) {
            result.push_back(descriptor);
        }
        return result;
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

    using TLoadTest::WaitFor;

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

    // one finish from each side of a channel started by StartChannel / StartInboundChannel; the details
    // are collected at failure time, the reconciliation log is most useful then
    void WaitChannel(const std::function<TString()>& details) {
        try {
            auto msg0 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, WaitTimeout);
            Account(msg0, NodeIndex0);
            auto msg1 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, WaitTimeout);
            Account(msg1, NodeIndex1);
        } catch (NActors::TEmptyEventQueueException&) {
            UNIT_ASSERT_C(false, TStringBuilder() << "channel did not finish, " << details() << ", " << ErrorDetails());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, TStringBuilder() << details() << ", " << ErrorDetails());
    }

    void WaitChannel(const TString& details) {
        WaitChannel([&]() { return details; });
    }

    // the given number of finishes from the given control in whatever order, see FindFinished
    void WaitFinishes(NActors::TActorId control, ui32 nodeIndex, ui32 count, const TString& what) {
        for (ui32 i = 0; i < count; i++) {
            WaitFinished(control, nodeIndex, what);
        }
    }

    // one finish from the given control, whatever the outcome
    TEvTestPrivate::TFinishInfo WaitFinished(NActors::TActorId control, ui32 nodeIndex, const TString& what) {
        try {
            auto msg = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(control, WaitTimeout);
            Account(msg, nodeIndex);
            return static_cast<const TEvTestPrivate::TFinishInfo&>(*msg->Get());
        } catch (NActors::TEmptyEventQueueException&) {
            UNIT_ASSERT_C(false, TStringBuilder() << what << " did not finish, " << ErrorDetails());
            throw;
        }
    }
};
