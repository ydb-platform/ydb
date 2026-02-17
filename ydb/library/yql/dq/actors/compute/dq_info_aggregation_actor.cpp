#include "dq_info_aggregation_actor.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
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

class TDqInfoAggregationActor : public TActor<TDqInfoAggregationActor>, public IActorExceptionHandler {
    using TBase = TActor<TDqInfoAggregationActor>;

    struct TPrivateEvents {
        enum EEv : ui32 {
            EvSendStatistics = EventSpaceBegin(TEvents::ES_PRIVATE),
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
    public:
        using TPtr = std::shared_ptr<TAggregatorBase>;

        TAggregatorBase(const TString& name, TDqInfoAggregationActor* self, const TString& counterId, const NDqProto::TEvUpdateCounterValue::TSettings& settings)
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

            LOG_D("Created new counter, SendPeriod: " << SendPeriod);
        }

        virtual ~TAggregatorBase() = default;

        void Update(const NDqProto::TEvUpdateCounterValue& value, const TActorId& sender) {
            const auto newValue = GetUpdateValue(value);
            const auto [it, inserted] = SenderValues.emplace(sender, newValue);
            LOG_D("Update counter, sender: " << sender << ", new value: " << newValue << ", old value: " << it->second << ", is new: " << inserted);

            std::optional<i64> oldValue;
            if (!inserted) {
                oldValue = it->second;
                it->second = newValue;
            }

            DoUpdate(newValue, oldValue, sender);

            if (const auto aggValue = GetAggValue()) {
                LOG_D("Agg value: " << *aggValue);

                if (!LastSentValue.HasScalar() || std::abs(*aggValue - LastSentValue.GetScalar()) > DeltaThreshold) {
                    Send();
                }
            }
        }

        void Send() {
            if (const auto aggValue = GetAggValue()) {
                LastSentValue.SetScalar(*aggValue);
                LOG_D("Send value: " << LastSentValue.GetScalar());

                for (const auto& [sender, _] : SenderValues) {
                    Self->Send(sender, new TInfoAggregationActorEvents::TEvOnAggregateUpdated(LastSentValue));
                }
            }
        }

        void ScheduleSend() {
            Self->Schedule(SendPeriod, new TPrivateEvents::TEvSendStatistics(CounterId));
        }

    protected:
        virtual i64 GetUpdateValue(const NDqProto::TEvUpdateCounterValue& value) const = 0;

        virtual void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) = 0;

        virtual std::optional<i64> GetAggValue() const = 0;

        TString GetLogPrefix() const {
            return TStringBuilder() << Self->GetLogPrefix() << " [" << Name << "] CounterId: " << CounterId << ". ";
        }

    private:
        TDqInfoAggregationActor* const Self;
        const TString Name;
        const TString CounterId;
        const TDuration SendPeriod;
        const i64 DeltaThreshold = 0;
        NDqProto::TEvOnAggregatedValueUpdated LastSentValue;
        std::unordered_map<TActorId, i64> SenderValues;
    };

    class TMinAggregator final : public TAggregatorBase {
    public:
        TMinAggregator(TDqInfoAggregationActor* self, const TString& counterId, const NDqProto::TEvUpdateCounterValue::TSettings& settings)
            : TAggregatorBase(__func__, self, counterId, settings)
        {}

    protected:
        i64 GetUpdateValue(const NDqProto::TEvUpdateCounterValue& value) const final {
            Y_VALIDATE(value.GetActionCase() == NDqProto::TEvUpdateCounterValue::kAggMin, "Unexpected action case");
            return value.GetAggMin();
        }

        void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) final {
            if (oldValue) {
                OrderedValues.erase({*oldValue, sender});
            }
            OrderedValues.emplace(newValue, sender);
        }

        std::optional<i64> GetAggValue() const {
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
        TSumAggregator(TDqInfoAggregationActor* self, const TString& counterId, const NDqProto::TEvUpdateCounterValue::TSettings& settings)
            : TAggregatorBase(__func__, self, counterId, settings)
        {}

        i64 GetUpdateValue(const NDqProto::TEvUpdateCounterValue& value) const final {
            Y_VALIDATE(value.GetActionCase() == NDqProto::TEvUpdateCounterValue::kAggSum, "Unexpected action case");
            return value.GetAggSum();
        }

        void DoUpdate(i64 newValue, std::optional<i64> oldValue, const TActorId& sender) final {
            Y_UNUSED(sender);
            Sum += newValue - oldValue.value_or(0);
        }

        std::optional<i64> GetAggValue() const {
            return Sum;
        }

    private:
        i64 Sum = 0;
    };

public:
    explicit TDqInfoAggregationActor(const TTxId& txId)
        : TBase(&TThis::StateFunc)
        , TxId(txId)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TInfoAggregationActorEvents::TEvUpdateCounter, Handle);
        hFunc(TPrivateEvents::TEvSendStatistics, Handle);
        hFunc(TEvents::TEvPoison, Handle);
    );

    bool OnUnhandledException(const std::exception& e) final {
        LOG_E("Unhandled exception: " << e.what());
        return true;
    }

private:
    void Handle(TInfoAggregationActorEvents::TEvUpdateCounter::TPtr& ev) {
        const auto& sender = ev->Sender;
        const auto& record = ev->Get()->Record;
        const auto& counterId = record.GetCounterId();
        LOG_D("Received TEvUpdateCounter from: " << sender << ", counter: " << counterId);
        LOG_T("Received TEvUpdateCounter from: " << sender << ", data: " << record.ShortDebugString());

        auto it = AggregateValues.find(counterId);
        if (it == AggregateValues.end()) {
            TAggregatorBase::TPtr aggregator;
            switch (record.GetActionCase()) {
                case NDqProto::TEvUpdateCounterValue::kAggMin:
                    aggregator = std::make_shared<TMinAggregator>(this, counterId, record.GetSettings());
                    break;
                case NDqProto::TEvUpdateCounterValue::kAggSum:
                    aggregator = std::make_shared<TSumAggregator>(this, counterId, record.GetSettings());
                    break;
                case NDqProto::TEvUpdateCounterValue::ACTION_NOT_SET:
                    LOG_E("Action not set");
                    return;
            }

            it = AggregateValues.emplace(counterId, std::move(aggregator)).first;
        }

        it->second->Update(record, sender);
    }

    void Handle(TPrivateEvents::TEvSendStatistics::TPtr& ev) {
        const auto& counterId = ev->Get()->CounterId;
        LOG_T("Received TEvSendStatistics, counterId: " << counterId);

        const auto it = AggregateValues.find(counterId);
        if (it == AggregateValues.end()) {
            LOG_E("Aggregate value not found: " << counterId);
            return;
        }

        it->second->Send();
        it->second->ScheduleSend();
    }

    void Handle(TEvents::TEvPoison::TPtr& ev) {
        LOG_I("Received TEvPoison from: " << ev->Sender);
        PassAway();
    }

    TString GetLogPrefix() const {
        return TStringBuilder() << "[TDqInfoAggregationActor] TxId: " << TxId << ", SelfId: " << SelfId() << ". ";
    }

    const TTxId TxId;
    std::unordered_map<TString, TAggregatorBase::TPtr> AggregateValues;
};

} // anonymous namespace

IActor* CreateDqInfoAggregationActor(const TTxId& txId) {
    return new TDqInfoAggregationActor(txId);
}

} // namespace NYql::NDq
