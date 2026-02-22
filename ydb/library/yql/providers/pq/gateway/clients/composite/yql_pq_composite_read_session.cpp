#include "yql_pq_composite_read_session.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/signals/signal_utils.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/guid.h>

#include <ranges>

#define SRC_LOG_T(s) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)

#define SRC_LOG_AS_T(s) LOG_TRACE_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_D(s) LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_I(s) LOG_INFO_S(*ActorSystem,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_N(s) LOG_NOTICE_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_W(s) LOG_WARN_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_E(s) LOG_ERROR_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_AS_C(s) LOG_CRIT_S(*ActorSystem,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql {

namespace {

using namespace NActors;
using namespace NKikimr::NOlap::NCounters;
using namespace NYdb::NTopic;

class ICompositeTopicReadSessionControlImpl : public ICompositeTopicReadSessionControl {
public:
    using TPtr = std::weak_ptr<ICompositeTopicReadSessionControlImpl>;

    virtual void AdvanceExternalTime(TInstant readTime) = 0;

    virtual TInstant GetReadTime() = 0;

    virtual NThreading::TFuture<void> SubscribeOnUpdate() = 0;
};

class TDqPqReadBalancerActor final : public TActorBootstrapped<TDqPqReadBalancerActor>, public IActorExceptionHandler {
    static constexpr TDuration WAKEUP_PERIOD = TDuration::Seconds(1);
    static constexpr TDuration REPORT_MIN_DELTA = TDuration::Seconds(1);
    static constexpr TDuration HANGING_CHECK_PERIOD = TDuration::Minutes(1);

    enum class EWakeup {
        Periodic,
        SubscribeOnUpdate,
    };

    class TMetrics {
    public:
        TMetrics(const TCompositeTopicReadSessionSettings& settings)
            : Counters(settings.Counters)
            , ReadTimeDelta(Counters->GetCounter("DistributedReadSession/ReadTimeDeltaMs"))
            , InternalWakeupRate(Counters->GetCounter("DistributedReadSession/InternalWakeupRate", /* derivative */ true))
            , ExternalWakeupRate(Counters->GetCounter("DistributedReadSession/ExternalWakeupRate", /* derivative */ true))
            , UnregisteredPartitionsCount(Counters->GetCounter("DistributedReadSession/ExternalCounter/UnregisteredPartitions"), settings.AmountPartitionsCount)
            , UnregisteredPartitionsCountUpdateRate(Counters->GetCounter("DistributedReadSession/ExternalCounter/UnregisteredPartitionsUpdateRate", /* derivative */ true))
            , ExternalReadTime(Counters->GetCounter("DistributedReadSession/ExternalCounter/ReadTimeUtc"))
            , ExternalReadTimeUpdateRate(Counters->GetCounter("DistributedReadSession/ExternalCounter/ReadTimeUpdateRate", /* derivative */ true))
        {}

        void UpdatePendingPartitionsCount(i64 value) {
            UnregisteredPartitionsCount.Set(value);
            UnregisteredPartitionsCountUpdateRate->Inc();
        }

        void UpdateExternalReadTime(TInstant value) {
            ExternalReadTime.Set(value.MilliSeconds());
            ExternalReadTimeUpdateRate->Inc();
        }

        const NMonitoring::TDynamicCounterPtr Counters;
        TIntCounter ReadTimeDelta;
        const NMonitoring::TDynamicCounters::TCounterPtr InternalWakeupRate;
        const NMonitoring::TDynamicCounters::TCounterPtr ExternalWakeupRate;

    private:
        TIntCounter UnregisteredPartitionsCount;
        const NMonitoring::TDynamicCounters::TCounterPtr UnregisteredPartitionsCountUpdateRate;
        TIntCounter ExternalReadTime;
        const NMonitoring::TDynamicCounters::TCounterPtr ExternalReadTimeUpdateRate;
    };

    class TValueReporter {
        class TMetrics {
        public:
            TMetrics(const TString& name, const TDqPqReadBalancerActor::TMetrics& metrics)
                : ReportValue(metrics.Counters->GetCounter(TStringBuilder() << "DistributedReadSession/LocalCounter/" << name))
                , ReportRate(metrics.Counters->GetCounter(TStringBuilder() << "DistributedReadSession/LocalCounter/" << name << "UpdateRate", /* derivative */ true))
            {}

            void SetValue(i64 value) {
                ReportValue.Set(value);
                ReportRate->Inc();
            }

        private:
            TIntCounter ReportValue;
            const NMonitoring::TDynamicCounters::TCounterPtr ReportRate;
        };

    public:
        ~TValueReporter() {
            if (ActorSystem) {
                LastValue.ClearAction();
                ActorSystem->Send(AggregatorActor, new NDq::TPqInfoAggregationActorEvents::TEvUpdateCounter(LastValue));
            }
        }

        TString GetId() const {
            return LastValue.GetCounterId();
        }

        void SetActorSystem(const TActorContext& ctx) {
            ActorSystem = ctx.ActorSystem();
            SelfId = ctx.SelfID;
        }

        void MaybeFlush(bool force = false) {
            MaybeUpdate(GetValue(), force);
        }

        void MaybeUpdate(i64 newValue, bool force = false) {
            const auto now = TInstant::Now();
            if (!force && now - LastSendAt < UpdatePeriod && std::abs(newValue - GetValue()) < UpdateDeltaThreshold) {
                return;
            }

            Metrics.SetValue(newValue);
            LastSendAt = now;
            SetValue(newValue);
            LastValue.SetSeqNo(LastValue.GetSeqNo() + 1);

            Y_VALIDATE(ActorSystem, "ActorSystem is null");
            ActorSystem->Send(std::make_unique<IEventHandle>(AggregatorActor, SelfId, new NDq::TPqInfoAggregationActorEvents::TEvUpdateCounter(LastValue)));
        }

        static TValueReporter ReadTimeValue(const TDqPqReadBalancerActor::TMetrics& metrics, const TCompositeTopicReadSessionSettings& settings) {
            NPq::NProto::TEvDqPqUpdateCounterValue value;

            value.SetCounterId(BuildCounterName(settings.InputIndex, "read_time"));
            value.SetAggMin(0);

            auto& aggSettings = *value.MutableSettings();
            aggSettings.SetScalarAggDeltaThreshold(REPORT_MIN_DELTA.MilliSeconds());
            *aggSettings.MutableReportPeriod() = NProtoInterop::CastToProto(WAKEUP_PERIOD);

            return TValueReporter("ReadTimeUtc", metrics, settings.AggregatorActor, std::move(value));
        }

        static TValueReporter PendingPartitionsCountValue(const TDqPqReadBalancerActor::TMetrics& metrics, const TCompositeTopicReadSessionSettings& settings) {
            NPq::NProto::TEvDqPqUpdateCounterValue value;

            value.SetCounterId(BuildCounterName(settings.InputIndex, "partition_counter"));
            value.SetAggSum(settings.BaseSettings.Topics_[0].PartitionIds_.size());

            auto& aggSettings = *value.MutableSettings();
            aggSettings.SetScalarAggDeltaThreshold(0);
            *aggSettings.MutableReportPeriod() = NProtoInterop::CastToProto(WAKEUP_PERIOD);

            return TValueReporter("UnregisteredPartitions", metrics, settings.AggregatorActor, std::move(value));
        }

    private:
        static TString BuildCounterName(ui64 inputIndex, const TString& counter) {
            return TStringBuilder() << "group=distributed_topic_read_session;input_index=" << inputIndex << ";counter=" << counter;
        }

        TValueReporter(const TString& name, const TDqPqReadBalancerActor::TMetrics& metrics, const TActorId& aggregatorActor, NPq::NProto::TEvDqPqUpdateCounterValue&& value)
            : LastValue(std::move(value))
            , UpdatePeriod(NProtoInterop::CastFromProto(LastValue.GetSettings().GetReportPeriod()))
            , UpdateDeltaThreshold(LastValue.GetSettings().GetScalarAggDeltaThreshold())
            , AggregatorActor(aggregatorActor)
            , Metrics(name, metrics)
        {
            Y_VALIDATE(AggregatorActor, "Missing aggregator actor");
        }

        i64 GetValue() {
            switch (LastValue.GetActionCase()) {
                case NPq::NProto::TEvDqPqUpdateCounterValue::ActionCase::kAggMin:
                    return LastValue.GetAggMin();
                case NPq::NProto::TEvDqPqUpdateCounterValue::ActionCase::kAggSum:
                    return LastValue.GetAggSum();
                default:
                    Y_VALIDATE(false, "Unexpected action case: " << static_cast<ui64>(LastValue.GetActionCase()));
            }
        }

        void SetValue(i64 value) {
            switch (LastValue.GetActionCase()) {
                case NPq::NProto::TEvDqPqUpdateCounterValue::ActionCase::kAggMin:
                    LastValue.SetAggMin(value);
                    break;
                case NPq::NProto::TEvDqPqUpdateCounterValue::ActionCase::kAggSum:
                    LastValue.SetAggSum(value);
                    break;
                default:
                    Y_VALIDATE(false, "Unexpected action case: " << static_cast<ui64>(LastValue.GetActionCase()));
            }
        }

        NPq::NProto::TEvDqPqUpdateCounterValue LastValue;
        const TDuration UpdatePeriod;
        const i64 UpdateDeltaThreshold = 0;
        const TActorId AggregatorActor;
        const TActorSystem* ActorSystem = nullptr;
        TActorId SelfId;
        TInstant LastSendAt;
        TMetrics Metrics;
    };

public:
    TDqPqReadBalancerActor(ICompositeTopicReadSessionControlImpl::TPtr controller, const TCompositeTopicReadSessionSettings& settings)
        : TxId(settings.TxId)
        , TaskId(settings.TaskId)
        , Cluster(settings.Cluster)
        , TopicPath(settings.BaseSettings.Topics_[0].Path_)
        , Controller(std::move(controller))
        , AmountPartitionsCount(settings.AmountPartitionsCount)
        , Metrics(settings)
        , PartitionsCountValue(TValueReporter::PendingPartitionsCountValue(Metrics, settings))
        , ReadTimeValue(TValueReporter::ReadTimeValue(Metrics, settings))
    {
        Y_VALIDATE(Controller.lock(), "Missing controller");
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);

        PartitionsCountValue.SetActorSystem(ActorContext());
        ReadTimeValue.SetActorSystem(ActorContext());
        LogPrefix = TStringBuilder() << "[" << ActorName << "] TxId: " << TxId << ", TaskId: " << TaskId << ", Cluster: " << Cluster << ", TopicPath: " << TopicPath << ", SelfId: " << SelfId() << ". ";
        SRC_LOG_D("Start, AmountPartitionsCount: " << AmountPartitionsCount);

        ScheduleWakeup();
        SendValues();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NDq::TPqInfoAggregationActorEvents::TEvOnAggregateUpdated, Handle);
        hFunc(TEvents::TEvPoison, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
    );

    bool OnUnhandledException(const std::exception& e) final {
        SRC_LOG_E("Unhandled exception: " << e.what());
        return true;
    }

    static constexpr char ActorName[] = "DQ_PQ_READ_BALANCER_ACTOR";

private:
    void Handle(NDq::TPqInfoAggregationActorEvents::TEvOnAggregateUpdated::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const auto& counterId = record.GetCounterId();
        const auto value = record.GetScalar();
        const ui64 seqNo = record.GetSeqNo();
        SRC_LOG_D("Received TEvOnAggregateUpdated, from " << ev->Sender << ", counter id: " << counterId << ", seq no: " << seqNo << ", value: " << value);

        if (const auto [it, inserted] = ExternalSeqNo.emplace(counterId, seqNo); !inserted) {
            if (it->second > seqNo) {
                SRC_LOG_N("Ignoring TEvOnAggregateUpdated, seq no is less than current seq no " << it->second << ", counter id: " << counterId << ", seq no: " << seqNo << ", value: " << value);
                return;
            }

            it->second = seqNo;
        }

        if (counterId == PartitionsCountValue.GetId()) {
            AllPartitionsStarted = value == AmountPartitionsCount;
            Metrics.UpdatePendingPartitionsCount(AmountPartitionsCount - value);
            SRC_LOG_D("Partitions started: " << value << ", AmountPartitionsCount: " << AmountPartitionsCount);
        } else if (counterId == ReadTimeValue.GetId()) {
            ExternalReadTime = TInstant::MilliSeconds(value);
            Metrics.UpdateExternalReadTime(*ExternalReadTime);
            SRC_LOG_D("ExternalReadTime: " << *ExternalReadTime);

            PartitionsCountValue.MaybeFlush(!PartitionCountReported);
            PartitionCountReported = true;
        } else {
            Y_VALIDATE(false, "Unknown counter id: " << counterId);
        }

        if (const auto sharedController = Controller.lock(); sharedController && AllPartitionsStarted) {
            Y_VALIDATE(ExternalReadTime, "ExternalReadTime is not set after all partitions started");
            sharedController->AdvanceExternalTime(*ExternalReadTime);
        }
    }

    void Handle(TEvents::TEvPoison::TPtr&) {
        SRC_LOG_I("Received TEvPoison");
        PassAway();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        const auto tag = ev->Get()->Tag;
        SRC_LOG_T("Received TEvWakeup, tag: " << tag);

        switch (static_cast<EWakeup>(tag)) {
            case EWakeup::Periodic:
                Metrics.InternalWakeupRate->Inc();
                WakeupScheduled = false;
                break;
            case EWakeup::SubscribeOnUpdate:
                Metrics.ExternalWakeupRate->Inc();
                SubscribedOnUpdate = false;
                break;
        }

        ScheduleWakeup();
        SendValues();
    }

    void ScheduleWakeup() {
        if (!WakeupScheduled) {
            WakeupScheduled = true;
            Schedule(WAKEUP_PERIOD, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::Periodic)));
        }

        if (const auto sharedController = Controller.lock(); sharedController && !SubscribedOnUpdate) {
            SubscribedOnUpdate = true;
            sharedController->SubscribeOnUpdate().Subscribe([self = SelfId(), actorSystem = ActorContext().ActorSystem()](const NThreading::TFuture<void>&) {
                actorSystem->Send(self, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::SubscribeOnUpdate)));
            });
        }
    }

    void SendValues() {
        const auto sharedController = Controller.lock();
        if (!sharedController) {
            return;
        }

        const auto readTime = sharedController->GetReadTime();
        SRC_LOG_D("Send values"
            << ", local ReadTime: " << readTime
            << ", PartitionCountReported: " << PartitionCountReported
            << ", AllPartitionsStarted: " << AllPartitionsStarted
            << ", ExternalReadTime: " << ExternalReadTime.value_or(TInstant::Zero()));

        CheckHanging(readTime);
        ReadTimeValue.MaybeUpdate(readTime.MilliSeconds());
        Metrics.ReadTimeDelta.Set(static_cast<i64>(readTime.MilliSeconds()) - static_cast<i64>(ExternalReadTime.value_or(TInstant::Zero()).MilliSeconds()));

        if (ExternalReadTime) {
            PartitionsCountValue.MaybeFlush();
        }
    }

    void CheckHanging(TInstant readTime) {
        const auto now = TInstant::Now();
        if (ExternalReadTime && readTime > *ExternalReadTime) {
            LastHangingCheckTime = now;
            return;
        }

        if (const auto delta = now - LastHangingCheckTime; delta > HANGING_CHECK_PERIOD) {
            const auto sharedController = Controller.lock();
            SRC_LOG_E("Hanging check failed, there is no progress during last " << delta
                << ", LocalReadTime: " << readTime
                << ", ExternalReadTime: " << (ExternalReadTime ? ToString(*ExternalReadTime) : "<null>")
                << ", PartitionCountReported: " << PartitionCountReported
                << ", AllPartitionsStarted: " << AllPartitionsStarted
                << ", Controller: " << (sharedController ? sharedController->GetInternalState() : "<null>"));
            LastHangingCheckTime = now;
        }
    }

    // Settings
    const NDq::TTxId TxId;
    const ui64 TaskId = 0;
    const TString Cluster;
    const TString TopicPath;
    const ICompositeTopicReadSessionControlImpl::TPtr Controller;
    const i64 AmountPartitionsCount = 0;
    TMetrics Metrics;
    TString LogPrefix;

    // Runtime info
    bool PartitionCountReported = false;
    bool AllPartitionsStarted = false;
    bool WakeupScheduled = false;
    bool SubscribedOnUpdate = false;
    std::optional<TInstant> ExternalReadTime;
    TValueReporter PartitionsCountValue;
    TValueReporter ReadTimeValue;
    std::unordered_map<TString, ui64> ExternalSeqNo;

    // Hanging tracking
    TInstant LastHangingCheckTime = TInstant::Now();
};

// All blocking methods are not supported (supposed to be used from actor system).
// Partition reading strategy:
// - Read by round-robin events from all not suspended partitions
// - First read most old partitions
class TCompositeTopicReadSession final : public IReadSession, public ICompositeTopicReadSessionControlImpl {
    class TMetrics {
    public:
        explicit TMetrics(NMonitoring::TDynamicCounterPtr counters)
            : WaitSuspendedPartitionsCount(counters->GetCounter("DistributedReadSession/Wait/SuspendedPartitionsCount"))
            , WaitPendingPartitionsCount(counters->GetCounter("DistributedReadSession/Wait/PendingPartitionsCount"))
            , WaitIdlePartitionsCount(counters->GetCounter("DistributedReadSession/Wait/IdlePartitionsCount"))
            , SuspendedPartitionsCount(counters->GetCounter("DistributedReadSession/PartitionsCount/Suspended"))
            , PendingPartitionsCount(counters->GetCounter("DistributedReadSession/PartitionsCount/Pending"))
            , ReadyPartitionsCount(counters->GetCounter("DistributedReadSession/PartitionsCount/Ready"))
            , IdlePartitionsCount(counters->GetCounter("DistributedReadSession/PartitionsCount/Idle"))
        {}

        TIntCounter WaitSuspendedPartitionsCount;
        TIntCounter WaitPendingPartitionsCount;
        TIntCounter WaitIdlePartitionsCount;
        TIntCounter SuspendedPartitionsCount;
        TIntCounter PendingPartitionsCount;
        TIntCounter ReadyPartitionsCount;
        TIntCounter IdlePartitionsCount;
    };

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

    class TPartitionSession {
    public:
        using TPtr = std::shared_ptr<TPartitionSession>;

        TPartitionSession(ui64 partitionId, std::shared_ptr<IReadSession> readSession, const TCompositeTopicReadSessionSettings& settings)
            : IdleTimeout(settings.IdleTimeout)
            , MaxPartitionReadSkew(settings.MaxPartitionReadSkew)
            , ReadSession(std::move(readSession))
            , PartitionId(partitionId)
        {
            Y_VALIDATE(ReadSession, "ReadSession must be set");
        }

        // Read session control

        NThreading::TFuture<void> WaitEvent() const {
            return ReadSession->WaitEvent();
        }

        bool Close() const {
            return ReadSession->Close(TDuration::Zero());
        }

        std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) {
            if (auto event = ReadSession->GetEvent(settings)) {
                LastEventReadTime = TInstant::Now();
                return event;
            }
            return std::nullopt;
        }

        // Read session info

        bool IsSuspended(TInstant timeLowerBound) {
            return GetReadTime() > timeLowerBound + (2 * MaxPartitionReadSkew) / 3;
        }

        bool IsIdle() const {
            return IdleTimeout && !WaitEvent().IsReady() && LastEventReadTime + IdleTimeout < TInstant::Now();
        }

        TString GetInternalState() const {
            return TStringBuilder() << "{"
                << "PartitionId: " << PartitionId
                << ", ReadTime: "<< ReadTime
                << ", LastEventReadTime: " << LastEventReadTime
                << ", IsReady: " << WaitEvent().IsReady() << "}";
        }

    private:
        // Settings
        const TDuration IdleTimeout;
        const TDuration MaxPartitionReadSkew;
        const std::shared_ptr<IReadSession> ReadSession;
        YDB_READONLY_CONST(ui64, PartitionId);

        // Read info
        YDB_ACCESSOR_DEF(TInstant, ReadTime); // Reported by external system by call AdvancePartitionTime
        TInstant LastEventReadTime;
    };

    class TPartitionKey {
    public:
        explicit TPartitionKey(TPartitionSession::TPtr session)
            : Session(std::move(session))
        {}

        bool operator<(const TPartitionKey& other) const {
            const auto lReadTime = Session->GetReadTime();
            const auto rReadTime = other.Session->GetReadTime();
            if (lReadTime != rReadTime) {
                return lReadTime < rReadTime;
            }

            return (uintptr_t)Session.get() < (uintptr_t)other.Session.get();
        }

        TPartitionSession* operator->() const {
            return Session.get();
        }

        const TPartitionSession::TPtr Session;
    };

    using TPartitionSet = std::set<TPartitionKey>;

public:
    TCompositeTopicReadSession(const TActorSystem* actorSystem, ITopicClient& topicClient, const TCompositeTopicReadSessionSettings& settings)
        : MaxPartitionReadSkew(settings.MaxPartitionReadSkew)
        , ActorSystem(actorSystem)
        , Metrics(settings.Counters)
        , AdvanceTimeSignal(settings.Counters, "DistributedReadSession/Future/AdvanceTime")
        , MinReadTimeChangedSignal(settings.Counters, "DistributedReadSession/Future/MinReadTimeChanged")
    {
        Y_VALIDATE(MaxPartitionReadSkew, "MaxPartitionReadSkew must be positive");
        Y_VALIDATE(ActorSystem, "ActorSystem must be set");
        Y_VALIDATE(settings.BaseSettings.Topics_.size() == 1, "Supported only single topic reading");

        const auto& topic = settings.BaseSettings.Topics_[0];
        LogPrefix = TStringBuilder() << "[" << __func__ << "] TxId: " << settings.TxId << ", TaskId: " << settings.TaskId << ", Cluster: " << settings.Cluster << ", TopicPath: " << topic.Path_ << ". ";

        const auto& partitions = topic.PartitionIds_;
        Y_VALIDATE(partitions.size() > 0, "Can not start read session without partitions");
        SRC_LOG_AS_I("Created"
            << ", MaxPartitionReadSkew: " << MaxPartitionReadSkew
            << ", IdleTimeout: " << settings.IdleTimeout
            << ", LocalPartitionsCount: " << partitions.size()
            << ", AmountPartitionsCount: " << settings.AmountPartitionsCount
            << ", AggregatorActor: " << settings.AggregatorActor);

        TStringBuilder sessionIdBuilder;
        auto sessionSettings = settings.BaseSettings;
        PartitionSessions.reserve(partitions.size());
        for (auto partitionId : partitions) {
            sessionSettings.Topics_[0].PartitionIds_.clear();
            sessionSettings.Topics_[0].AppendPartitionIds(partitionId);
            auto readSession = topicClient.CreateReadSession(sessionSettings);

            sessionIdBuilder << partitionId << "=" << readSession->GetSessionId() << ";";
            Y_VALIDATE(PartitionSessions.emplace(
                partitionId,
                std::make_shared<TPartitionSession>(partitionId, std::move(readSession), settings)
            ).second, "Duplicate partition id: " << partitionId);
        }

        SessionId = sessionIdBuilder;

        // From start all partitions are ready or pending
        for (const auto& [_, session] : PartitionSessions) {
            if (!session->WaitEvent().IsReady()) {
                Y_VALIDATE(PendingPartitions.emplace(session).second, "Unexpected PendingPartitions");
            } else {
                Y_VALIDATE(ReadyPartitions.emplace(session).second, "Unexpected ReadyPartitions");
            }
        }
        Metrics.PendingPartitionsCount.Set(PendingPartitions.size());
        Metrics.ReadyPartitionsCount.Set(ReadyPartitions.size());
        NextReadyPartition = ReadyPartitions.end();
    }

    ~TCompositeTopicReadSession() {
        ClearBalancerActor();
    }

    void SetBalancerActor(const TActorId& balancerActor) {
        BalancerActor = balancerActor;
    }

    // IReadSession

    NThreading::TFuture<void> WaitEvent() final {
        ui64 waitPendingPartitions = 0;
        ui64 waitIdlePartitions = 0;
        ui64 waitSuspendedPartitions = 0;
        Y_DEFER {
            Metrics.WaitSuspendedPartitionsCount.Set(waitSuspendedPartitions);
            Metrics.WaitPendingPartitionsCount.Set(waitPendingPartitions);
            Metrics.WaitIdlePartitionsCount.Set(waitIdlePartitions);
        };

        RefreshReadyPartitions();
        if (!ReadyPartitions.empty()) {
            // There are already ready events
            return NThreading::MakeFuture();
        }

        // Wait for all not suspended sessions
        std::vector<NThreading::TFuture<void>> futures;
        futures.reserve(PendingPartitions.size() + IdlePartitions.size());
        for (const auto& session : std::views::join(std::array{
            std::views::all(PendingPartitions),
            std::views::all(IdlePartitions)
        })) {
            futures.emplace_back(session->WaitEvent());
        }

        waitPendingPartitions = PendingPartitions.size();
        waitIdlePartitions = IdlePartitions.size();

        if (!SuspendedPartitions.empty()) {
            // Wait for advance time, when some partition will be unsuspended
            futures.emplace_back(AdvanceTimeSignal.GetFuture());
            waitSuspendedPartitions = 1;
        }

        Y_VALIDATE(!futures.empty(), "Unexpected empty futures");
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

        if (!settings.MaxByteSize_) {
            return {};
        }

        auto getEventSettings = TReadSessionGetEventSettings(settings)
            .MaxEventsCount(1)
            .MaxByteSize(settings.MaxByteSize_);

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

        if (!settings.MaxByteSize_) {
            return std::nullopt;
        }

        auto maybeEvent = ReadEventFromReadyPartitions(settings);

        RefreshReadyPartitions();

        if (!maybeEvent) {
            maybeEvent = ReadEventFromReadyPartitions(settings);
        }

        return maybeEvent;
    }

    bool Close(TDuration timeout) final {
        Y_VALIDATE(!timeout, "Timeout is not supported");
        SRC_LOG_AS_I("Closing session");
        ClearBalancerActor();

        bool success = true;
        TStringBuilder errors;
        for (const auto& [_, session] : PartitionSessions) {
            try {
                success = session->Close() && success;
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
        const auto it = PartitionSessions.find(partitionId);
        Y_VALIDATE(it != PartitionSessions.end(), "Partition " << partitionId << " not found");
        const TPartitionKey key(it->second);
        if (key->GetReadTime() >= lastEventTime) {
            SRC_LOG_AS_D("Partition " << partitionId << " already advanced to " << key->GetReadTime() << ", but got time: " << lastEventTime);
            return;
        }

        NextReadyPartition = ReadyPartitions.end();
        const auto minReadTimeBefore = GetMinimalLocalReadTime();

        const auto update = [&](TPartitionSet& p) -> ui64 {
            if (const auto it = p.find(key); it != p.end()) {
                p.erase(it);
                key->SetReadTime(lastEventTime);
                Y_VALIDATE(p.emplace(key).second, "Unexpected partitions set");
                return 1;
            }
            return 0;
        };
        Y_VALIDATE(update(SuspendedPartitions) + update(PendingPartitions) + update(ReadyPartitions) + update(IdlePartitions) == 1, "Unexpected partition state");

        RefreshPartitionsState(minReadTimeBefore);
    }

    void AdvanceExternalTime(TInstant readTime) final {
        SRC_LOG_AS_T("AdvanceExternalTime: " << readTime << ", previous ExternalReadTime: " << ExternalReadTime);
        ExternalReadTime = readTime;
        RefreshPartitionsState();
    }

    TInstant GetReadTime() final {
        RefreshPartitionsState();
        const auto result = GetMinimalLocalReadTime();
        SRC_LOG_AS_T("GetReadTime result: " << result);
        return result;
    }

    NThreading::TFuture<void> SubscribeOnUpdate() final {
        return MinReadTimeChangedSignal.GetFuture();
    }

    TString GetInternalState() final {
        TStringBuilder state;

        const auto update = [&](const TString& name, const TPartitionSet& p) {
            state << name << ": ";
            for (const auto& partition : p) {
                state << partition->GetInternalState() << " ";
            }
            state << ", ";
        };
        update("SuspendedPartitions", SuspendedPartitions);
        update("PendingPartitions", PendingPartitions);
        update("ReadyPartitions", ReadyPartitions);
        update("IdlePartitions", IdlePartitions);

        state
            << "ExternalReadTime: " << ExternalReadTime << ", "
            << "AdvanceTimeSignal ready: " << AdvanceTimeSignal.GetFuture().IsReady() << ", "
            << "MinReadTimeChangedSignal ready: " << MinReadTimeChangedSignal.GetFuture().IsReady();

        return state;
    }

private:
    void ClearBalancerActor() {
        if (!ActorSystem || !BalancerActor) {
            return;
        }

        SRC_LOG_AS_I("Clear balancer actor: " << BalancerActor);
        ActorSystem->Send(BalancerActor, new TEvents::TEvPoison());
        BalancerActor = {};
    }

    void RefreshPartitionsState(std::optional<TInstant> minReadTimeBefore = std::nullopt) {
        NextReadyPartition = ReadyPartitions.end(); // Reset iterator before modification
        if (!minReadTimeBefore) {
            minReadTimeBefore = GetMinimalLocalReadTime();
        }
        ui64 unsuspendedPartitionsCount = 0;

        // Refresh partitions idleness
        {
            const auto update = [&](TPartitionSet& p) {
                while (!p.empty() && (*p.begin())->IsIdle()) {
                    Y_VALIDATE(IdlePartitions.emplace(*p.begin()).second, "Unexpected IdlePartitions");
                    p.erase(p.begin());
                }
            };
            update(SuspendedPartitions);
            update(PendingPartitions);
        }

        // Unsuspend some partitions
        const auto timeLowerBound = std::min(ExternalReadTime, GetMinimalLocalReadTime());
        while (!SuspendedPartitions.empty() && (*SuspendedPartitions.begin())->GetReadTime() <= timeLowerBound + MaxPartitionReadSkew / 3) {
            unsuspendedPartitionsCount++;
            DistributePartitionSession(*SuspendedPartitions.begin());
            SuspendedPartitions.erase(SuspendedPartitions.begin());
        }

        SRC_LOG_AS_T("Unsuspended partitions count: " << unsuspendedPartitionsCount);

        // Suspend some partitions
        {
            const auto update = [&](TPartitionSet& p) {
                while (!p.empty() && (*p.rbegin())->IsSuspended(timeLowerBound)) {
                    const auto key = *p.rbegin();
                    p.erase(std::prev(p.end()));

                    if (key->IsIdle()) {
                        Y_VALIDATE(IdlePartitions.emplace(key).second, "Unexpected IdlePartitions");
                    } else {
                        Y_VALIDATE(SuspendedPartitions.emplace(key).second, "Unexpected SuspendedPartitions");
                    }
                }
            };
            update(PendingPartitions);
            update(ReadyPartitions);
        }

        // Report update
        if (unsuspendedPartitionsCount) {
            AdvanceTimeSignal.Signal();
        }

        if (*minReadTimeBefore != GetMinimalLocalReadTime()) {
            MinReadTimeChangedSignal.Signal();
        }

        UpdateMetrics();
    }

    void DistributePartitionSession(const TPartitionKey& key) {
        if (key->IsIdle()) {
            Y_VALIDATE(IdlePartitions.emplace(key).second, "Unexpected IdlePartitions");
        } else if (key->WaitEvent().IsReady()) {
            Y_VALIDATE(ReadyPartitions.emplace(key).second, "Unexpected ReadyPartitions");
        } else {
            Y_VALIDATE(PendingPartitions.emplace(key).second, "Unexpected PendingPartitions");
        }
    }

    TInstant GetMinimalLocalReadTime() const {
        auto result = TInstant::Max();

        const auto update = [&](const TPartitionSet& p) {
            if (!p.empty()) {
                result = std::min(result, (*p.begin())->GetReadTime());
            }
        };
        update(SuspendedPartitions);
        update(PendingPartitions);
        update(ReadyPartitions);

        return result;
    }

    std::optional<TReadSessionEvent::TEvent> ReadEventFromReadyPartitions(const TReadSessionGetEventSettings& settings) {
        if (ReadyPartitions.empty()) {
            return std::nullopt;
        }

        if (NextReadyPartition == ReadyPartitions.end()) {
            NextReadyPartition = ReadyPartitions.begin();
        }

        const auto key = *NextReadyPartition;
        auto event = key->GetEvent(settings);
        Y_VALIDATE(event, "Unexpected empty event for ready partition");

        if (!key->WaitEvent().IsReady()) {
            // There are no ready events in this partition, so move it to pending / idle
            DistributePartitionSession(key);
            NextReadyPartition = ReadyPartitions.erase(NextReadyPartition);
        } else {
            // Move to next partition
            NextReadyPartition++;
        }

        if (!key->GetReadTime()) {
            // It was first event in this session, we should wait for time reports, so move partition to suspended
            AdvancePartitionTime(key->GetPartitionId(), TInstant::Zero() + MaxPartitionReadSkew);
        }

        UpdateMetrics();
        return event;
    }

    void RefreshReadyPartitions() {
        if (!ReadyPartitions.empty() && ++RefreshReadyPartitionsSkipped < PartitionSessions.size()) {
            return;
        }
        RefreshReadyPartitionsSkipped = 0;

        const auto timeLowerBound = std::min(ExternalReadTime, GetMinimalLocalReadTime());
        const auto update = [&](TPartitionSet& p) {
            for (auto it = p.begin(); it != p.end();) {
                if (!(*it)->WaitEvent().IsReady()) {
                    ++it;
                    continue;
                }

                if ((*it)->IsSuspended(timeLowerBound)) {
                    Y_VALIDATE(SuspendedPartitions.emplace(*it).second, "Unexpected SuspendedPartitions");
                } else {
                    Y_VALIDATE(ReadyPartitions.emplace(*it).second, "Unexpected ReadyPartitions");
                }

                it = p.erase(it);
            }
        };
        update(PendingPartitions);
        update(IdlePartitions);
        UpdateMetrics();
    }

    void UpdateMetrics() {
        Metrics.SuspendedPartitionsCount.Set(SuspendedPartitions.size());
        Metrics.PendingPartitionsCount.Set(PendingPartitions.size());
        Metrics.ReadyPartitionsCount.Set(ReadyPartitions.size());
        Metrics.IdlePartitionsCount.Set(IdlePartitions.size());
    }

    // Settings
    TString SessionId;
    const TDuration MaxPartitionReadSkew;
    const TActorSystem* const ActorSystem = nullptr;
    TMetrics Metrics;
    TString LogPrefix;
    TActorId BalancerActor;
    std::unordered_map<ui64, TPartitionSession::TPtr> PartitionSessions; // PartitionId -> Session

    // Partitions reading state
    TPartitionSet SuspendedPartitions; // Partitions for which ReadTime > ExternalReadTime + MaxPartitionReadSkew * (2 / 3)
    TPartitionSet PendingPartitions; // Partitions without ready events and not suspended
    TPartitionSet ReadyPartitions; // Partitions with ready events, which is not suspended <=> WaitEvent() future is ready
    TPartitionSet IdlePartitions; // Partitions removed from min local read time calculation by IdleTimeout
    TPartitionSet::iterator NextReadyPartition; // Next partition from which will be sent event
    ui64 RefreshReadyPartitionsSkipped = 0;

    // Signal when some partitions become not suspended
    TSignalWrapper AdvanceTimeSignal;

    // Signal when min across local partition read time is changed
    TSignalWrapper MinReadTimeChangedSignal;

    // Runtime info
    TInstant ExternalReadTime; // Minimal read time across all topic partitions
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
