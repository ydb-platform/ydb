#include "yql_pq_composite_read_session.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_info_aggregation_actor.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/guid.h>

#define SRC_LOG_T(s) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql {

namespace {

using namespace NActors;
using namespace NYdb::NTopic;

class ICompositeTopicReadSessionControlImpl : public ICompositeTopicReadSessionControl {
public:
    using TPtr = std::weak_ptr<ICompositeTopicReadSessionControlImpl>;

    // Minimal time for external partitions
    virtual void AdvanceTime(TInstant readTime) = 0;

    // Minimal time for local partition, should be called periodically to update partitions idleness
    virtual TInstant GetReadTime() = 0;
};

class TDqPqReadBalancerActor final : public TActorBootstrapped<TDqPqReadBalancerActor>, public IActorExceptionHandler {
    static constexpr TDuration WAKEUP_PERIOD = TDuration::Seconds(1);
    static constexpr TDuration REPORT_MIN_DELTA = TDuration::Seconds(1);
    static constexpr char READ_TIME_COUNTER[] = "read_time";
    static constexpr char PARTITION_COUNTER[] = "partition_counter";

    struct TMetrics {
        TMetrics(const TCompositeTopicReadSessionSettings& settings)
            : Counters(settings.Counters)
            , PendingPartitionsCount(Counters->GetCounter("distributed_read_session/pending_partitions_count"))
            , AmountPartitionsCount(settings.AmountPartitionsCount)
            , PendingPartitionsCountValue(AmountPartitionsCount)
            , ReadTimeDelta(Counters->GetCounter("distributed_read_session/read_time_delta_ms"))
        {
            PendingPartitionsCount->Add(PendingPartitionsCountValue);
        }

        void UpdatePendingPartitionsCount(ui64 value) {
            value = AmountPartitionsCount - value;
            PendingPartitionsCount->Sub( PendingPartitionsCountValue);
            PendingPartitionsCount->Add(value);
            PendingPartitionsCountValue = value;
        }

        ~TMetrics() {
            if (PendingPartitionsCount) {
                PendingPartitionsCount->Sub(PendingPartitionsCountValue);
            }
        }

    private:
        ::NMonitoring::TDynamicCounterPtr Counters;
        ::NMonitoring::TDynamicCounters::TCounterPtr PendingPartitionsCount;
        const ui64 AmountPartitionsCount = 0;
        ui64 PendingPartitionsCountValue = 0;

    public:
        ::NMonitoring::TDynamicCounters::TCounterPtr ReadTimeDelta;
    };

public:
    TDqPqReadBalancerActor(ICompositeTopicReadSessionControlImpl::TPtr controller, const TCompositeTopicReadSessionSettings& settings)
        : TxId(settings.TxId)
        , TaskId(settings.TaskId)
        , AggregatorActor(settings.AggregatorActor)
        , Controller(std::move(controller))
        , AmountPartitionsCount(settings.AmountPartitionsCount)
        , Metrics(settings)
    {
        Y_VALIDATE(AggregatorActor, "Missing aggregator actor");
        Y_VALIDATE(Controller.lock(), "Missing controller");
        Y_VALIDATE(settings.BaseSettings.Topics_.size() == 1, "Expected exactly one topic");

        const auto& topic = settings.BaseSettings.Topics_[0];
        const auto partitionsCount = topic.PartitionIds_.size();
        Y_VALIDATE(settings.AmountPartitionsCount >= partitionsCount, "Invalid amount of partitions");

        TopicPath = topic.Path_;

        {
            ReadTimeValue.SetCounterId(BuildCounterName(READ_TIME_COUNTER));
            auto& aggSettings = *ReadTimeValue.MutableSettings();
            aggSettings.SetScalarAggDeltaThreshold(REPORT_MIN_DELTA.MilliSeconds());
            *aggSettings.MutableReportPeriod() = NProtoInterop::CastToProto(WAKEUP_PERIOD);
        }

        {
            PartitionsCountValue.SetCounterId(BuildCounterName(PARTITION_COUNTER));
            PartitionsCountValue.SetAggSum(partitionsCount);
            auto& aggSettings = *PartitionsCountValue.MutableSettings();
            aggSettings.SetScalarAggDeltaThreshold(0);
            *aggSettings.MutableReportPeriod() = NProtoInterop::CastToProto(WAKEUP_PERIOD);
        }
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);

        LogPrefix = TStringBuilder() << "[TDqPqReadBalancerActor] TxId: " << TxId << ", AggregatorActor: " << AggregatorActor << ", TopicPath: " << TopicPath << ", TaskId: " << TaskId << ", SelfId: " << SelfId() << ". ";
        SRC_LOG_D("Start, AmountPartitionsCount: " << AmountPartitionsCount);

        ScheduleWakeup();
        SendValues();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NDq::TInfoAggregationActorEvents::TEvOnAggregateUpdated, Handle);
        hFunc(TEvents::TEvPoison, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
    );

    bool OnUnhandledException(const std::exception& e) final {
        SRC_LOG_E("Unhandled exception: " << e.what());
        return true;
    }

private:
    void Handle(NDq::TInfoAggregationActorEvents::TEvOnAggregateUpdated::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& counterId = record.GetCounterId();
        const auto value = record.GetScalar();
        SRC_LOG_D("Received TEvOnAggregateUpdated, from " << ev->Sender << ", counter id: " << counterId << ", value: " << value);

        if (counterId == PartitionsCountValue.GetCounterId()) {
            AllPartitionsStarted = static_cast<ui64>(value) == AmountPartitionsCount;
            Metrics.UpdatePendingPartitionsCount(value);
            SRC_LOG_D("Partitions started: " << value << ", AmountPartitionsCount: " << AmountPartitionsCount);
        } else if (counterId == ReadTimeValue.GetCounterId()) {
            ExternalReadTime = TInstant::MilliSeconds(value);
            SRC_LOG_D("ExternalReadTime: " << *ExternalReadTime);

            if (!PartitionCountReported) {
                PartitionCountReported = true;
                Send(AggregatorActor, new NDq::TInfoAggregationActorEvents::TEvUpdateCounter(PartitionsCountValue));
            }
        } else {
            Y_VALIDATE(false, "Unknown counter id: " << counterId);
        }

        if (const auto sharedController = Controller.lock(); sharedController && AllPartitionsStarted) {
            Y_VALIDATE(ExternalReadTime, "ExternalReadTime is not set after all partitions started");
            sharedController->AdvanceTime(*ExternalReadTime);
        }
    }

    void Handle(TEvents::TEvPoison::TPtr&) {
        SRC_LOG_I("Received TEvPoison");
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        const auto tag = ev->Get()->Tag;
        SRC_LOG_T("Received TEvWakeup, tag: " << tag);

        if (tag) {
            ScheduleWakeup();
        }

        SendValues();
    }

    TString BuildCounterName(const TString& counter) const {
        return TStringBuilder() << "groupp=distributed_topic_read_session;topic=" << TopicPath << ";counter=" << counter;
    }

    void ScheduleWakeup() const {
        Schedule(WAKEUP_PERIOD, new TEvents::TEvWakeup(1));
    }

    void SendValues() {
        const auto sharedController = Controller.lock();
        if (!sharedController) {
            return;
        }

        const auto readTime = sharedController->GetReadTime();
        SRC_LOG_D("Send values"
            << ", LastSendAt: " << LastSendAt
            << ", SendReadTime: " << readTime
            << ", PartitionCountReported: " << PartitionCountReported
            << ", AllPartitionsStarted: " << AllPartitionsStarted
            << ", ExternalReadTime: " << ExternalReadTime.value_or(TInstant::Zero()));

        const i64 newValue = readTime.MilliSeconds();
        const auto now = TInstant::Now();
        if (now - LastSendAt < WAKEUP_PERIOD && std::abs(newValue - ReadTimeValue.GetAggMin()) < static_cast<i64>(REPORT_MIN_DELTA.MilliSeconds())) {
            return;
        }

        if (ExternalReadTime) {
            Metrics.ReadTimeDelta->Set(newValue - ExternalReadTime->MilliSeconds());

            if (now - LastSendAt >= WAKEUP_PERIOD) {
                // Send partitions count only after report current time
                Send(AggregatorActor, new NDq::TInfoAggregationActorEvents::TEvUpdateCounter(PartitionsCountValue));
            }
        }

        LastSendAt = now;
        ReadTimeValue.SetAggMin(newValue);
        Send(AggregatorActor, new NDq::TInfoAggregationActorEvents::TEvUpdateCounter(ReadTimeValue));
    }

    const NDq::TTxId TxId;
    const ui64 TaskId = 0;
    const TActorId AggregatorActor;
    const ICompositeTopicReadSessionControlImpl::TPtr Controller;
    const ui64 AmountPartitionsCount = 0;
    TMetrics Metrics;
    TString TopicPath;
    TString LogPrefix;

    bool PartitionCountReported = false;
    bool AllPartitionsStarted = false;
    std::optional<TInstant> ExternalReadTime;
    NDqProto::TEvUpdateCounterValue PartitionsCountValue;
    NDqProto::TEvUpdateCounterValue ReadTimeValue;
    TInstant LastSendAt;
};

// All blocking methods are not supported (supposed to be used from actor system)
class TCompositeTopicReadSession final : public IReadSession, public ICompositeTopicReadSessionControlImpl {
    struct TTopicEventSizeVisitor {
        template <typename TEv>
        void operator()(const TEv& ev) {
            Size = sizeof(ev);
        }

        void operator()(const TReadSessionEvent::TDataReceivedEvent& ev) {
            Size = sizeof(ev);

            auto messagesCount = ev.GetMessagesCount();
            if (ev.HasCompressedMessages()) {
                const auto& compressedMessages = ev.GetCompressedMessages();
                messagesCount -= compressedMessages.size();

                for (const auto& compressedMessage : compressedMessages) {
                    Size += sizeof(compressedMessage);
                    Size += compressedMessage.GetProducerId().size() + compressedMessage.GetMessageGroupId().size();
                    Size += compressedMessage.GetData().size();
                }
            }

            if (messagesCount) {
                for (const auto& message : ev.GetMessages()) {
                    Size += sizeof(message);
                    Size += message.GetProducerId().size() + message.GetMessageGroupId().size();
                    if (message.HasException()) {
                        Size += message.GetBrokenData().size();
                    } else {
                        Size += message.GetData().size();
                    }
                }
            }
        }

        static ui64 GetEventSize(const TReadSessionEvent::TEvent& event) {
            TTopicEventSizeVisitor visitor;
            std::visit(visitor, event);
            return visitor.Size;
        }

    private:
        ui64 Size = 0;
    };

    class TReadSessionWrapper {
    public:
        TReadSessionWrapper(TCompositeTopicReadSession* self, ui64 idx, const std::shared_ptr<IReadSession>& readSession)
            : Self(self)
            , Idx(idx)
            , ReadSession(readSession)
        {}

        NThreading::TFuture<void> WaitEvent() const {
            return ReadSession->WaitEvent();
        }

        bool Close(TDuration timeout) const {
            return ReadSession->Close(timeout);
        }

        bool IsIdle() const {
            return !LastEvent && !WaitEvent().IsReady() && LastReadTime + Self->IdleTimeout >= TInstant::Now();
        }

        bool IsSuspended() const {
            return LastEvent && !Self->CanProcessEvent(Idx, *LastEvent);
        }

        bool ReadEvent(const TReadSessionGetEventSettings& settings) {
            if (!LastEvent) {
                LastEvent = ReadSession->GetEvent(settings);

                if (LastEvent) {
                    LastReadTime = TInstant::Now();
                } else {
                    return false;
                }
            }

            return !IsSuspended();
        }

        TReadSessionEvent::TEvent ExtractEvent() {
            Y_VALIDATE(LastEvent, "Unexpected extract event call");
            auto event = std::move(*LastEvent);
            LastEvent = std::nullopt;
            HasProcessedEvents = HasProcessedEvents || std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(event);
            return event;
        }

    private:
        TCompositeTopicReadSession* Self;
        ui64 Idx = 0;
        std::shared_ptr<IReadSession> ReadSession;
        std::optional<TReadSessionEvent::TEvent> LastEvent;
        TInstant LastReadTime;
        YDB_READONLY(bool, HasProcessedEvents, false);
    };

public:
    TCompositeTopicReadSession(const TActorSystem* actorSystem, ITopicClient& topicClient, const TCompositeTopicReadSessionSettings& settings)
        : SessionId(CreateGuidAsString())
        , MaxPartitionReadSkew(settings.MaxPartitionReadSkew)
        , IdleTimeout(settings.IdleTimeout)
        , ActorSystem(actorSystem)
    {
        Y_VALIDATE(MaxPartitionReadSkew, "MaxPartitionReadSkew must be positive");
        Y_VALIDATE(settings.AggregatorActor, "AggregatorActor must be set");
        Y_VALIDATE(settings.BaseSettings.Topics_.size() == 1, "Supported only single topic reading");

        const auto& partitions = settings.BaseSettings.Topics_[0].PartitionIds_;
        Y_VALIDATE(partitions.size() > 0, "Can not start read session without partitions");
        PartitionsReadTime.resize(partitions.size());

        auto sessionSettings = settings.BaseSettings;
        PartitionIdToSessionIdx.reserve(partitions.size());
        ReadSessions.reserve(partitions.size());
        for (ui64 i = 0; i < partitions.size(); ++i) {
            const auto partitionId = partitions[i];
            Y_VALIDATE(PartitionIdToSessionIdx.emplace(partitionId, i).second, "Duplicate partition id: " << partitionId);
            Y_VALIDATE(PartitionsReadTimeSet.emplace(TInstant::Zero(), i).second, "Unexpected PartitionsReadTimeSet");

            sessionSettings.Topics_[0].PartitionIds_.clear();
            sessionSettings.Topics_[0].AppendPartitionIds(partitionId);
            ReadSessions.emplace_back(this, i, topicClient.CreateReadSession(sessionSettings));
        }
    }

    ~TCompositeTopicReadSession() {
        if (ActorSystem && BalancerActor) {
            ActorSystem->Send(BalancerActor, new TEvents::TEvPoison());
        }
    }

    void SetBalancerActor(const TActorId& balancerActor) {
        BalancerActor = balancerActor;
    }

    // IReadSession

    NThreading::TFuture<void> WaitEvent() final {
        if (ReadyReadSessionIdx) {
            return NThreading::MakeFuture();
        }

        std::vector<NThreading::TFuture<void>> futures;
        futures.reserve(ReadSessions.size());
        for (ui64 i = 0; i < ReadSessions.size(); ++i) {
            auto& readSession = ReadSessions[i];
            if (readSession.IsSuspended()) {
                continue;
            }

            futures.emplace_back(readSession.WaitEvent());
        }

        if (!AdvanceTimePromise) {
            AdvanceTimePromise = NThreading::NewPromise<void>();
        }

        futures.emplace_back(AdvanceTimePromise->GetFuture());
        return NThreading::WaitAny(futures);
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        Y_VALIDATE(!block, "Block methods are not supported");
        return GetEvents(TReadSessionGetEventSettings()
            .MaxByteSize(maxByteSize)
            .MaxEventsCount(maxEventsCount)
        );
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(!settings.Block_, "Block methods are not supported");
        auto getEventSettings = TReadSessionGetEventSettings(settings)
            .MaxEventsCount(1);

        ui64 usedSize = 0;
        std::vector<TReadSessionEvent::TEvent> result;
        while (auto event = GetEvent(getEventSettings)) {
            result.emplace_back(std::move(*event));
            usedSize += TTopicEventSizeVisitor::GetEventSize(result.back());

            if ((settings.MaxEventsCount_ && result.size() >= *settings.MaxEventsCount_) || usedSize >= settings.MaxByteSize_) {
                break;
            }

            getEventSettings.MaxByteSize(settings.MaxByteSize_ - usedSize);
        }

        return result;
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) final {
        Y_VALIDATE(!block, "Block methods are not supported");
        return GetEvent(TReadSessionGetEventSettings()
            .MaxByteSize(maxByteSize)
            .MaxEventsCount(1)
        );
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(!settings.Block_, "Block methods are not supported");
        RefreshReadyReadSessionsIdx(settings);

        if (!ReadyReadSessionIdx) {
            return std::nullopt;
        }

        const auto i = *ReadyReadSessionIdx;
        ReadyReadSessionIdx.reset();

        auto& readSession = ReadSessions[i];
        auto event = readSession.ExtractEvent();

        if (readSession.ReadEvent(settings)) {
            ReadyReadSessionIdx = i;
            SequentialEventsRead++;
        } else {
            SequentialEventsRead = 0;
        }

        return event;
    }

    bool Close(TDuration timeout) final {
        bool success = true;
        TStringBuilder errors;
        for (const auto& readSession : ReadSessions) {
            try {
                success = readSession.Close(timeout) && success;
            } catch (const std::exception& e) {
                errors << e.what() << "; " << Endl;
            }
        }

        if (errors) {
            throw yexception() << "Failed to close composite read session: " << errors;
        }

        return success;
    }

    TReaderCounters::TPtr GetCounters() const final {
        return nullptr;
    }

    std::string GetSessionId() const final {
        return SessionId;
    }

    // ICompositeTopicReadSessionControl

    void AdvancePartitionTime(ui64 partitionId, TInstant lastEventTime) final {
        const auto it = PartitionIdToSessionIdx.find(partitionId);
        Y_VALIDATE(it != PartitionIdToSessionIdx.end(), "Partition " << partitionId << " not found");
        const auto idx = it->second;

        // Refresh minimal read time for local partition
        if (const auto prevTime = PartitionsReadTime[idx]; lastEventTime > prevTime) {
            const auto lastReadTime = GetReadTime();
            PartitionsReadTimeSet.erase({prevTime, idx});
            PartitionsReadTimeSet.emplace(lastEventTime, idx);
            PartitionsReadTime[idx] = lastEventTime;

            const auto newTime = GetReadTime();
            if (lastReadTime != newTime) {
                ActorSystem->Send(BalancerActor, new TEvents::TEvWakeup());
            }
        }
    }

    void AdvanceTime(TInstant readTime) final {
        const auto prevTime = ExternalReadTime;
        ExternalReadTime = readTime;

        if (AdvanceTimePromise && prevTime < readTime) {
            const auto it = PartitionsReadTimeSet.upper_bound({prevTime + MaxPartitionReadSkew, Max<ui64>()});
            if (it != PartitionsReadTimeSet.end() && it->first <= readTime + MaxPartitionReadSkew) {
                // There is new not suspended partition, we should refresh WaitEvent feature
                AdvanceTimePromise->SetValue();
                AdvanceTimePromise.reset();
            }
        }

        if (ReadyReadSessionIdx && ReadSessions[*ReadyReadSessionIdx].IsSuspended()) {
            ReadyReadSessionIdx.reset();
        }
    }

    TInstant GetReadTime() final {
        if (IdleTimeout) {
            while (!PartitionsReadTimeSet.empty()) {
                const auto it = PartitionsReadTimeSet.begin();
                const auto idx = it->second;
                if (!ReadSessions[idx].IsIdle()) {
                    break;
                }

                PartitionsReadTime[idx] = TInstant::Max(); // Guaranty that next event will be processed
                PartitionsReadTimeSet.erase(it);
            }
        }

        if (PartitionsReadTimeSet.empty()) {
            // All partitions are idle
            return TInstant::Max();
        }

        return PartitionsReadTimeSet.begin()->first;
    }

private:
    void RefreshReadyReadSessionsIdx(const TReadSessionGetEventSettings& settings) {
        if (SequentialEventsRead >= ReadSessions.size()) {
            // Switch to another partition
            SequentialEventsRead = 0;
            ReadyReadSessionIdx.reset();
        }

        for (ui64 i = 0; !ReadyReadSessionIdx && i < ReadSessions.size(); ++i) {
            if (++LastReadyReadSessionIdx >= ReadSessions.size()) {
                LastReadyReadSessionIdx = 0;
            }

            if (ReadSessions[LastReadyReadSessionIdx].ReadEvent(settings)) {
                ReadyReadSessionIdx = LastReadyReadSessionIdx;
            }
        }
    }

    bool CanProcessEvent(ui64 idx, const TReadSessionEvent::TEvent& event) {
        if (!std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(event)) {
            return true;
        }

        if (const auto readTime = PartitionsReadTime[idx]) {
            return readTime <= ExternalReadTime + MaxPartitionReadSkew;
        }

        // If session is not initialized yet and we already sent one event, we should wait AdvancePartitionTime call
        return !ReadSessions[idx].GetHasProcessedEvents();
    }

    const TString SessionId;
    const TDuration MaxPartitionReadSkew;
    const TDuration IdleTimeout;
    const TActorSystem* const ActorSystem = nullptr;
    TActorId BalancerActor;
    std::optional<NThreading::TPromise<void>> AdvanceTimePromise;

    // Sessions time state
    TInstant ExternalReadTime; // Minimal read time from external partitions
    std::vector<TInstant> PartitionsReadTime;
    std::set<std::pair<TInstant, ui64>> PartitionsReadTimeSet;
    std::unordered_map<ui64, ui64> PartitionIdToSessionIdx;

    // Sessions reading state
    std::vector<TReadSessionWrapper> ReadSessions;
    std::optional<ui64> ReadyReadSessionIdx; // Session with at least one ready event
    ui64 LastReadyReadSessionIdx = 0;
    ui64 SequentialEventsRead = 0;
};

} // anonymous namespace

std::pair<std::shared_ptr<NYdb::NTopic::IReadSession>, ICompositeTopicReadSessionControl::TPtr> CreateCompositeTopicReadSession(
    const TActorContext& ctx,
    ITopicClient& topicClient,
    const TCompositeTopicReadSessionSettings& settings
) {
    const auto compositeReadSession = std::make_shared<TCompositeTopicReadSession>(ctx.ActorSystem(), topicClient, settings);
    compositeReadSession->SetBalancerActor(ctx.RegisterWithSameMailbox(new TDqPqReadBalancerActor(compositeReadSession, settings)));
    return {compositeReadSession, compositeReadSession};
}

} // namespace NYql
