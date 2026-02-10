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

        TAggregatorBase(TDqInfoAggregationActor* self, const TString& counterId, TDuration sendPeriod)
            : Self(self)
            , CounterId(counterId)
            , SendPeriod(sendPeriod)
        {
            Y_VALIDATE(Self, "Self is null");
            Y_VALIDATE(SendPeriod, "SendPeriod should be non zero");
            ScheduleSend();
        }

        virtual ~TAggregatorBase() = default;

        virtual void Update(const NDqProto::TEvUpdateCounterValue& value, const TActorId& sender) = 0;

        virtual void Send() = 0;

        void ScheduleSend() {
            Self->Schedule(SendPeriod, new TPrivateEvents::TEvSendStatistics(CounterId));
        }

    protected:
        TDqInfoAggregationActor* const Self;
        const TString CounterId;

    private:
        const TDuration SendPeriod;
    };

    class TMinAggregator final : public TAggregatorBase {
        using TBase = TAggregatorBase;

    public:
        TMinAggregator(TDqInfoAggregationActor* self, const TString& counterId, const NDqProto::TEvUpdateCounterValue::TSettings& settings)
            : TBase(self, counterId, NProtoInterop::CastFromProto(settings.GetReportPeriod()))
            , DeltaThreshold(settings.GetScalarAggDeltaThreshold())
        {
            LastSentValue.SetCounterId(CounterId);
        }

        void Update(const NDqProto::TEvUpdateCounterValue& value, const TActorId& sender) final {
            Y_VALIDATE(value.GetActionCase() == NDqProto::TEvUpdateCounterValue::kAggMin, "Unexpected action case");
            const auto newValue = value.GetAggMin();
   
            if (const auto [it, inserted] = SenderValues.emplace(sender, newValue); !inserted) {
                OrderedValues.erase({it->second, sender});
                it->second = newValue;
            }

            OrderedValues.emplace(newValue, sender);

            if (!LastSentValue.HasScalar() || std::abs(OrderedValues.begin()->first - LastSentValue.GetScalar()) > DeltaThreshold) {
                Send();
            }
        }

        void Send() final {
            if (OrderedValues.empty()) {
                return;
            }

            LastSentValue.SetScalar(OrderedValues.begin()->first);

            for (const auto& [sender, _] : SenderValues) {
                Self->Send(sender, new TInfoAggregationActorEvents::TEvOnAggregateUpdated(LastSentValue));
            }
        }

    private:
        const i64 DeltaThreshold = 0;
        NDqProto::TEvOnAggregatedValueUpdated LastSentValue;
        std::unordered_map<TActorId, i64> SenderValues;
        std::set<std::pair<i64, TActorId>> OrderedValues;
    };

public:
    TDqInfoAggregationActor()
        : TBase(&TThis::StateFunc)
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
        return TStringBuilder() << "[TDqInfoAggregationActor] SelfId: " << SelfId() << ". ";
    }

    std::unordered_map<TString, TAggregatorBase::TPtr> AggregateValues;
};

} // anonymous namespace

IActor* CreateDqInfoAggregationActor() {
    return new TDqInfoAggregationActor();
}

} // namespace NYql::NDq
