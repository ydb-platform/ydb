#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/utils/actors/rich_actor.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

enum class EActorFutureCallbackFailureReason {
    None,
    Undelivered,
    NodeDisconnected,
    Timeout,
};

enum EExecutorPoolType {
    Main,
    FullResultWriter,

    TotalCount,
};

template <typename EventType>
struct TRichActorFutureCallback : public TRichActor<TRichActorFutureCallback<EventType>> {
    using TCallback = std::function<void(TAutoPtr<NActors::TEventHandle<EventType>>&)>;
    using TFailure = std::function<void(void)>;
    using TBase = TRichActor<TRichActorFutureCallback<EventType>>;

    static constexpr char ActorName[] = "YQL_DQ_ACTOR_FUTURE_CALLBACK";

    TRichActorFutureCallback(
        TCallback&& callback,
        TFailure&& failure,
        TDuration timeout,
        TString context = {},
        NActors::TActorId targetActorId = {})
        : TBase(&TRichActorFutureCallback::StateWaitForEvent)
        , Callback(std::move(callback))
        , Failure(std::move(failure))
        , Timeout(timeout)
        , Context(std::move(context))
        , TargetActorId(targetActorId)
    { }

private:
    const TCallback Callback;
    const TFailure Failure;
    const TDuration Timeout;
    const TString Context;
    const NActors::TActorId TargetActorId;
    EActorFutureCallbackFailureReason FailureReason = EActorFutureCallbackFailureReason::None;
    bool TimerStarted = false;
    NActors::TSchedulerCookieHolder TimerCookieHolder;

    STRICT_STFUNC(StateWaitForEvent,
        HFunc(EventType, Handle)
        cFunc(NActors::TEvents::TEvBootstrap::EventType, OnFailure)
        hFunc(NActors::TEvInterconnect::TEvNodeConnected, [this] (NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) mutable {
            this->Subscribe(ev->Get()->NodeId);
        })
        hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, [this] (NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) mutable {
            this->Unsubscribe(ev->Get()->NodeId);
            FailureReason = EActorFutureCallbackFailureReason::NodeDisconnected;
            YQL_CLOG(DEBUG, ProviderDq) << "ActorFutureCallback failed: interconnect node disconnected"
                << " context=" << Context
                << " disconnectedNodeId=" << ev->Get()->NodeId
                << " targetActorId=" << TargetActorId
                << " callbackActorId=" << this->SelfId();
            TimerStarted = true;
            OnFailure();
        })
        hFunc(NActors::TEvents::TEvUndelivered, [this] (NActors::TEvents::TEvUndelivered::TPtr& ev) mutable {
            const auto* undelivered = ev->Get();
            this->Unsubscribe(ev->Sender.NodeId());
            FailureReason = EActorFutureCallbackFailureReason::Undelivered;
            YQL_CLOG(DEBUG, ProviderDq) << "ActorFutureCallback failed: event undelivered (fast failure, not a timeout)"
                << " context=" << Context
                << " undeliveredReason=" << static_cast<int>(undelivered->Reason)
                << " sourceEventType=" << undelivered->SourceType
                << " unsure=" << undelivered->Unsure
                << " targetActorId=" << TargetActorId
                << " callbackActorId=" << this->SelfId();
            TimerStarted = true;
            OnFailure();
        })
    )

    void Handle(typename EventType::TPtr ev, const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        Callback(ev);
        this->PassAway();
    }

    TAutoPtr<NActors::IEventHandle> AfterRegister(const NActors::TActorId& self, const NActors::TActorId& parentId) override {
        return new NActors::IEventHandle(self, parentId, new NActors::TEvents::TEvBootstrap, 0);
    }

    void OnFailure() {
        if (TimerStarted) {
            if (FailureReason == EActorFutureCallbackFailureReason::None) {
                FailureReason = EActorFutureCallbackFailureReason::Timeout;
                YQL_CLOG(DEBUG, ProviderDq) << "ActorFutureCallback failed: response timeout"
                    << " context=" << Context
                    << " timeout=" << Timeout
                    << " targetActorId=" << TargetActorId
                    << " callbackActorId=" << this->SelfId();
            }
            Failure();
            this->PassAway();
        } else {
            TimerStarted = true;
            TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
            this->Schedule(Timeout, new NActors::TEvents::TEvBootstrap, TimerCookieHolder.Get());
        }
    }
};

template <class TDerived>
class TSynchronizableRichActor : public TRichActor<TDerived> {
public:
    using TBase = TRichActor<TDerived>;
    template <class TEvType>
    using TCallback = std::function<void(typename TEvType::TPtr&)>;
    using TAbstractCallback = std::function<void(TAutoPtr<NActors::IEventHandle>&)>;
    using THandler = typename TBase::TReceiveFunc;

    enum ESyncState {
        E_IDLE,
        E_SYNC_REQUESTED,
        E_SYNC_RECEIVED,
    };

    template <class... Args>
    explicit TSynchronizableRichActor(Args&&... args)
        : TRichActor<TDerived>(std::forward<Args>(args)...) {}

    template <class TEvType>
    void Synchronize(TCallback<TEvType> callback) {
        switch (SyncState_) {
            case E_SYNC_REQUESTED:
                throw yexception() << "Synchronization was already requested";
                break;
            case E_IDLE:
                [[fallthrough]];
            case E_SYNC_RECEIVED:
                InterruptedHandler_ = TBase::CurrentStateFunc();
                SyncCallback_ = [callback](TAutoPtr<NActors::IEventHandle>& ev) {
                    auto* x = reinterpret_cast<typename TEvType::TPtr*>(&ev);
                    callback(*x);
                };
                TBase::Become(&TSynchronizableRichActor::SyncHandler);
                ExpectedEventType_ = TEvType::EventType;
                SyncState_ = E_SYNC_REQUESTED;
                break;
        }
    }

protected:
    void AddCriticalEventType(ui32 type) {
        CriticalEventTypes_.insert(type);
    }

private:
    THandler InterruptedHandler_{nullptr};
    TDeque<TAutoPtr<NActors::IEventHandle>> DelayedEvents_{};
    TAbstractCallback SyncCallback_{nullptr};
    ESyncState SyncState_{E_IDLE};
    ui32 ExpectedEventType_{0};
    THashSet<ui32> CriticalEventTypes_{};

    void SyncHandler(TAutoPtr<NActors::IEventHandle>& ev) {
        const ui32 etype = ev->GetTypeRewrite();
        if (etype == ExpectedEventType_) {
            OnSync(ev);
        } else if (CriticalEventTypes_.contains(etype)) {
            (this->*InterruptedHandler_)(ev);
        } else {
            EnqueueEvent(ev);
        }
    }

    void OnSync(TAutoPtr<NActors::IEventHandle>& ev) {
        YQL_CLOG(DEBUG, ProviderDq) << "OnSync(): delayed messages " << DelayedEvents_.size();
        SyncState_ = E_SYNC_RECEIVED;
        TBase::Become(InterruptedHandler_);
        SyncCallback_(ev);
        if (SyncState_ == E_SYNC_REQUESTED) {
            return;
        }
        SyncCallback_ = nullptr;

        while (!DelayedEvents_.empty() && !TBase::Killed) {
            auto event = std::move(DelayedEvents_.front());
            DelayedEvents_.pop_front();
            InterruptedHandler_ = TBase::CurrentStateFunc();
            (this->*InterruptedHandler_)(event);
            if (SyncState_ == E_SYNC_REQUESTED) {
                return;
            }
        }

        InterruptedHandler_ = nullptr;
        ExpectedEventType_ = 0;
        SyncState_ = E_IDLE;
    }

    void EnqueueEvent(TAutoPtr<NActors::IEventHandle>& ev) {
        DelayedEvents_.emplace_back(ev.Release());
    }
};

} // namespace NYql
