#include "dq_pq_info_aggregation_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <library/cpp/protobuf/interop/cast.h>

#define LOG_T(s) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_I(s) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_N(s) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_W(s) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)
#define LOG_C(s) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, GetLogPrefix() << s)

namespace NYql::NDq {

namespace {

using namespace NActors;

class TDqPqInfoAggregationActor : public TActor<TDqPqInfoAggregationActor>, public IActorExceptionHandler {
    using TBase = TActor<TDqPqInfoAggregationActor>;

    struct TPrivateEvents {
        enum EEv : ui32 {
            EvSendStatistics = TPqInfoAggregationActorEvents::EvEnd,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvSendStatistics : TEventLocal<TEvSendStatistics, EvSendStatistics> {
            explicit TEvSendStatistics(const TString& counterId)
                : CounterId(counterId)
            {}

            const TString CounterId;
        };
    };

    class TAggregatorBase {
        struct TSenderInfo {
            i64 Value = 0;
            ui64 SeqNo = 0;
        };

    public:
        using TPtr = std::shared_ptr<TAggregatorBase>;

        TAggregatorBase(const TString& name, TDqPqInfoAggregationActor* self, const TString& counterId, const NPq::NProto::TEvDqPqUpdateCounterValue::TSettings& settings)
            : Self(self)
            , Name(name)
            , CounterId(counterId)
            , SendPeriod(NProtoInterop::CastFromProto(settings.GetReportPeriod()))
            , DeltaThreshold(settings.GetScalarAggDeltaThreshold())
        {
            Y_VALIDATE(Self, "Self is null");
            Y_VALIDATE(SendPeriod, "SendPeriod should be non zero");

            LastSentValue.SetCounterId(CounterId);
            ScheduleSend();

            LOG_D("Created new counter, SendPeriod: " << SendPeriod << ", DeltaThreshold: " << DeltaThreshold);
        }

        virtual ~TAggregatorBase() = default;

        bool Remove(const TActorId& sender) {
            const auto it = SenderValues.find(sender);
            Y_VALIDATE(it != SenderValues.end(), "Sender not found");

            DoRemove(it->second.Value, sender);
            SenderValues.erase(it);
            Y_VALIDATE(Self->Senders[sender].erase(CounterId), "Counter " << CounterId << " not found in sender: " << sender);

            TrySendValue();

            return SenderValues.empty();
        }

        void Update(const NPq::NProto::TEvDqPqUpdateCounterValue& value, const TActorId& sender) {
            const auto newValue = GetUpdateValue(value);
            const ui64 newSeqNo = value.GetSeqNo();
            const auto [it, inserted] = SenderValues.emplace(sender, TSenderInfo{.Value = newValue, .SeqNo = newSeqNo});
            LOG_D("Update counter, sender: " << sender << ", seq no: " << newSeqNo << ", new value: " << newValue << ", old value: " << it->second.Value << ", is new: " << inserted);

            std::optional<i64> oldValue;
            if (inserted) {
                Y_VALIDATE(Self->Senders[sender].emplace(CounterId).second, "Duplicated CounterId: " << CounterId);
            } else if (it->second.SeqNo > newSeqNo) {
                LOG_N("Skip update, seq no: " << newSeqNo << " < " << it->second.SeqNo);
                return;
            } else {
                oldValue = it->second.Value;
                it->second = TSenderInfo{.Value = newValue, .SeqNo = newSeqNo};
            }

            DoUpdate(newValue, oldValue, sender);

            if (inserted) {
                Send();
            } else {
                TrySendValue();
            }
        }

        void Send() {
            if (const auto aggValue = GetAggValue()) {
                LastSentValue.SetScalar(*aggValue);
                LastSentValue.SetSeqNo(LastSentValue.GetSeqNo() + 1);
                LOG_D("Send value: " << LastSentValue.GetScalar() << ", seq no: " << LastSentValue.GetSeqNo());

                for (const auto& [sender, _] : SenderValues) {
                    Self->Send(sender, new TPqInfoAggregationActorEvents::TEvOnAggregateUpdated(LastSentValue), IEventHandle::FlagTrackDelivery);
                }
            }
        }

        void ScheduleSend() {
            Self->Schedule(SendPeriod, new TPrivateEvents::TEvSendStatistics(CounterId));
        }

    protected:
        virtual i64 GetUpdateValue(const NPq::NProto::TEvDqPqUpdateCounterValue& value) const = 0;

        virtual void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) = 0;

        virtual void DoRemove(i64 oldValue, const TActorId& sender) = 0;

        virtual std::optional<i64> GetAggValue() const = 0;

        TString GetLogPrefix() const {
            return TStringBuilder() << Self->GetLogPrefix() << " [" << Name << "] CounterId: " << CounterId << ". ";
        }

    private:
        bool TrySendValue() {
            if (const auto aggValue = GetAggValue()) {
                LOG_D("Agg value: " << *aggValue << ", LastSentValue: " << LastSentValue.GetScalar());

                if (!LastSentValue.HasScalar() || std::abs(*aggValue - LastSentValue.GetScalar()) > DeltaThreshold) {
                    Send();
                    return true;
                }
            }
            return false;
        }

        TDqPqInfoAggregationActor* Self;
        const TString Name;
        const TString CounterId;
        const TDuration SendPeriod;
        const i64 DeltaThreshold = 0;
        NPq::NProto::TEvDqPqOnAggregatedValueUpdated LastSentValue;
        std::unordered_map<TActorId, TSenderInfo> SenderValues;
    };

    class TMinAggregator final : public TAggregatorBase {
    public:
        TMinAggregator(TDqPqInfoAggregationActor* self, const TString& counterId, const NPq::NProto::TEvDqPqUpdateCounterValue::TSettings& settings)
            : TAggregatorBase(__func__, self, counterId, settings)
        {}

    protected:
        i64 GetUpdateValue(const NPq::NProto::TEvDqPqUpdateCounterValue& value) const final {
            Y_VALIDATE(value.GetActionCase() == NPq::NProto::TEvDqPqUpdateCounterValue::kAggMin, "Unexpected action case");
            return value.GetAggMin();
        }

        void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) final {
            if (oldValue) {
                Y_VALIDATE(OrderedValues.erase({*oldValue, sender}), "Unexpected OrderedValues");
            }
            Y_VALIDATE(OrderedValues.emplace(newValue, sender).second, "Unexpected OrderedValues");
        }

        void DoRemove(i64 oldValue, const TActorId& sender) final {
            Y_VALIDATE(OrderedValues.erase({oldValue, sender}), "Unexpected OrderedValues");
        }

        std::optional<i64> GetAggValue() const final {
            if (OrderedValues.empty()) {
                return std::nullopt;
            }
            return OrderedValues.begin()->first;
        }

    private:
        std::set<std::pair<i64, TActorId>> OrderedValues;
    };

    class TSumAggregator final : public TAggregatorBase {
    public:
        TSumAggregator(TDqPqInfoAggregationActor* self, const TString& counterId, const NPq::NProto::TEvDqPqUpdateCounterValue::TSettings& settings)
            : TAggregatorBase(__func__, self, counterId, settings)
        {}

        i64 GetUpdateValue(const NPq::NProto::TEvDqPqUpdateCounterValue& value) const final {
            Y_VALIDATE(value.GetActionCase() == NPq::NProto::TEvDqPqUpdateCounterValue::kAggSum, "Unexpected action case");
            return value.GetAggSum();
        }

        void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) final {
            Y_UNUSED(sender);
            Sum += newValue - oldValue.value_or(0);
        }

        void DoRemove(i64 oldValue, const TActorId& sender) final {
            Y_UNUSED(sender);
            Sum -= oldValue;
        }

        std::optional<i64> GetAggValue() const final {
            return Sum;
        }

    private:
        i64 Sum = 0;
    };

    static constexpr char ActorName[] = "DQ_PQ_INFO_AGGREGATION_ACTOR";

public:
    explicit TDqPqInfoAggregationActor(const TTxId& txId)
        : TBase(&TThis::StateFunc)
        , TxId(txId)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TPqInfoAggregationActorEvents::TEvUpdateCounter, Handle);
        hFunc(TPrivateEvents::TEvSendStatistics, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvents::TEvPoison, Handle);
    );

    bool OnUnhandledException(const std::exception& e) final {
        LOG_E("Unhandled exception: " << e.what());
        return true;
    }

private:
    void Handle(TPqInfoAggregationActorEvents::TEvUpdateCounter::TPtr& ev) {
        const auto& sender = ev->Sender;
        const auto& record = ev->Get()->Record;
        const auto& counterId = record.GetCounterId();
        LOG_D("Received TEvUpdateCounter from: " << sender << ", counter: " << counterId);
        LOG_T("Received TEvUpdateCounter from: " << sender << ", data: " << record.ShortDebugString());

        if (record.GetActionCase() == NPq::NProto::TEvDqPqUpdateCounterValue::ACTION_NOT_SET) {
            RemoveSender(sender);
            return;
        }

        const auto [it, inserted] = AggregateValues.emplace(counterId, nullptr);
        if (inserted) {
            switch (record.GetActionCase()) {
                case NPq::NProto::TEvDqPqUpdateCounterValue::kAggMin:
                    it->second = std::make_shared<TMinAggregator>(this, counterId, record.GetSettings());
                    break;
                case NPq::NProto::TEvDqPqUpdateCounterValue::kAggSum:
                    it->second = std::make_shared<TSumAggregator>(this, counterId, record.GetSettings());
                    break;
                case NPq::NProto::TEvDqPqUpdateCounterValue::ACTION_NOT_SET:
                    Y_VALIDATE(false, "Unexpected action case");
            }

            Y_VALIDATE(it->second, "Aggregator is null, action: " << static_cast<ui64>(record.GetActionCase()));
        }

        it->second->Update(record, sender);
    }

    void Handle(TPrivateEvents::TEvSendStatistics::TPtr& ev) {
        const auto& counterId = ev->Get()->CounterId;
        LOG_T("Received TEvSendStatistics, counterId: " << counterId);

        const auto it = AggregateValues.find(counterId);
        if (it == AggregateValues.end()) {
            LOG_I("Aggregate value not found for periodic send: " << counterId);
            return;
        }

        it->second->Send();
        it->second->ScheduleSend();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto& sender = ev->Sender;
        if (const auto reason = ev->Get()->Reason; reason != TEvents::TEvUndelivered::ReasonActorUnknown) {
            LOG_W("Skip TEvUndelivered from: " << sender << ", reason: " << reason);
            return;
        }

        LOG_I("Received TEvUndelivered from: " << sender << ", actor unknown");
        RemoveSender(sender);
    }

    void Handle(TEvents::TEvPoison::TPtr& ev) {
        LOG_I("Received TEvPoison from: " << ev->Sender);
        PassAway();
    }

    void RemoveSender(const TActorId& sender) {
        LOG_I("Received removing counters for sender: " << sender);
        const auto it = Senders.find(sender);
        if (it == Senders.end()) {
            LOG_I("Sender not found for removing: " << sender);
            return;
        }

        for (const auto& counterId : std::vector(it->second.begin(), it->second.end())) {
            const auto counterIt = AggregateValues.find(counterId);
            Y_VALIDATE(counterIt != AggregateValues.end(), "Aggregate value not found: " << counterId);

            if (counterIt->second->Remove(sender)) {
                LOG_I("Aggregate value removed: " << counterId);
                AggregateValues.erase(counterIt);
            }
        }

        Y_VALIDATE(it->second.empty(), "Sender not empty: " << sender);
        Senders.erase(it);
    }

    TString GetLogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] TxId: " << TxId << ", SelfId: " << SelfId() << ". ";
    }

    const TTxId TxId;
    std::unordered_map<TString, TAggregatorBase::TPtr> AggregateValues;
    std::unordered_map<TActorId, std::unordered_set<TString>> Senders;
};

} // anonymous namespace

IActor* CreateDqPqInfoAggregationActor(const TTxId& txId) {
    return new TDqPqInfoAggregationActor(txId);
}

void RegisterDqPqInfoAggregationActorFactory(TDqAsyncIoFactory& factory) {
    factory.RegisterControlPlane("PqInfoAggregator", [](IDqAsyncIoFactory::TControlPlaneArguments&& args) {
        return CreateDqPqInfoAggregationActor(args.TxId);
    });
}

} // namespace NYql::NDq
