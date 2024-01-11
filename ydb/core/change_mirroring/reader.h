#pragma once

#include <concepts>

#include <util/system/types.h>

#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NChangeMirroring {

template <class TDerived>
class TDerivedMixin {
public:
    TDerived& Derived() { return static_cast<TDerived&>(*this); }
    const TDerived& Derived() const { return static_cast<const TDerived&>(*this); }
    TDerived* DerivedPtr() { return static_cast<TDerived*>(this); }
    const TDerived* DerivedPtr() const { return static_cast<const TDerived*>(this); }
};

#define DEFINE_DERIVED_STATEFN() \
    template <class TStateBase> \
    using TFn = void (TStateBase::*)(TAutoPtr<NActors::IEventHandle>& ev); \
    template <class T, TFn<T> fn> \
    void DerivedStateFn(TAutoPtr<NActors::IEventHandle>& ev) { \
        (static_cast<T*>(this)->*fn)(ev); \
    } \

/* AI stands for actor interface */
class AIReader {
public:
    struct TEvReader {
        enum {
            EvNeedPoll,
            EvNeedPollResult,
            EvPoll,
            EvPollResult,
            EvRemaining,
            EvRemainingResult,
            EvReadNext,
            EvReadNextResult,
        };

        struct TEvNeedPoll: public NActors::TEventLocal<TEvNeedPoll, EvNeedPoll> {
        };

        struct TEvNeedPollResult : public NActors::TEventLocal<TEvNeedPollResult, EvNeedPollResult> {
            TEvNeedPollResult(bool need) : Need(need) {}

            bool Need = false;
        };

        struct TEvPoll: public NActors::TEventLocal<TEvPoll, EvPoll> {
        };

        struct TEvPollResult : public NActors::TEventLocal<TEvPollResult, EvPollResult> {
        };

        struct TEvRemaining: public NActors::TEventLocal<TEvRemaining, EvRemaining> {
        };

        struct TEvRemainingResult : public NActors::TEventLocal<TEvRemainingResult, EvRemainingResult> {
            TEvRemainingResult(ui64 remaining) : Remaining(remaining) {}

            ui64 Remaining = 0;
        };

        struct TEvReadNext: public NActors::TEventLocal<TEvReadNext, EvReadNext> {
        };

        struct TEvReadNextResult : public NActors::TEventLocal<TEvReadNextResult, EvReadNextResult> {
            TEvReadNextResult(TRcBuf&& data) : Data(std::move(data)) {}

            TRcBuf Data;
        };

    };

    using TEvents = TEvReader;
    struct Tag {};
    struct TReaderActorHandle {
        const NActors::TActorId ActorId;
    };
    struct TReaderClientActorHandle {
        const NActors::TActorId ActorId;
    };

    class IClient {
    public:
        virtual ~IClient() {};
        virtual void NeedPollResult(Tag, const TEvReader::TEvNeedPollResult& result) { Y_UNUSED(result); };
        virtual void PollResult(Tag) {};
        virtual void ReadNextResult(Tag, const TEvReader::TEvReadNextResult& result) { Y_UNUSED(result); };
        virtual void RemainingResult(Tag, const TEvReader::TEvRemainingResult& result) { Y_UNUSED(result); };
    };

    class IServer {
    public:
        virtual ~IServer() {};
        virtual void NeedPoll(Tag) {};
        virtual void Poll(Tag) {};
        virtual void ReadNext(Tag) {};
        virtual void Remaining(Tag) {};
    };

    template <class TDerived>
    class TClientDerivedCaller
        : public TDerivedMixin<TDerived>
    {
    public:
        void OnNeedPollResult(TEvReader::TEvNeedPollResult::TPtr &ev) {
            this->Derived().NeedPollResult(Tag{}, *ev->Get());
        }

        void OnNeedPollResult(TEvReader::TEvRemainingResult::TPtr &ev) {
            this->Derived().RemainingResult(Tag{}, *ev->Get());
        }

        void OnPollResult(TEvReader::TEvPollResult::TPtr &) {
            this->Derived().PollResult(Tag{});
        }

        void OnReadNextResult(TEvReader::TEvReadNextResult::TPtr &ev) {
            this->Derived().ReadNextResult(Tag{}, *ev->Get());
        }
    public:
        ~TClientDerivedCaller() {
            static_assert(std::derived_from<TDerived, IClient>, "TDerived should be derived from IClient");
        }
    };

    class TClientEventDispatcherBase {};

    template <class TImpl>
    class TClientEventDispatcher
        : public TClientEventDispatcherBase
        , public TImpl
    {
    public:
        using TClientEventDispatcherType = TClientEventDispatcher<TImpl>;
        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvReader::TEvNeedPollResult, TImpl::OnNeedPollResult);
                hFunc(TEvReader::TEvPollResult, TImpl::OnPollResult);
                hFunc(TEvReader::TEvReadNextResult, TImpl::OnReadNextResult);
            }
        }
    };

    template <class TDerived>
    using TClientEventDerivedDispatcher = TClientEventDispatcher<TClientDerivedCaller<TDerived>>;

    class TClientEventSender {
        template <class TEv>
        void Send(std::unique_ptr<TEv> ev) {
            Sender->Send(Impl, ev.release());
        }
    public:
        template <class TSender>
        TClientEventSender(const TReaderActorHandle impl, TSender& sender)
            : Impl(impl.ActorId)
            , Sender(sender.ActorOps())
        {
            static_assert(std::derived_from<TSender, TClientEventDispatcherBase>, "TSender should be derived from TClientEventDispatcher");
        }
        void NeedPoll(Tag) { Send(std::make_unique<TEvReader::TEvNeedPoll>()); }
        void Poll(Tag) { Send(std::make_unique<TEvReader::TEvPoll>()); }
        void Remaining(Tag) { Send(std::make_unique<TEvReader::TEvRemaining>()); }
        void ReadNext(Tag) { Send(std::make_unique<TEvReader::TEvReadNext>()); }
    private:
        const NActors::TActorId Impl;
        NActors::IActorOps* Sender;
    };

    template <class TDerived>
    class TDefaultClientBase
        : public IClient
        , public TClientEventDerivedDispatcher<TDerived>
        , public TClientEventSender
    {
    public:
        using TBase = TDefaultClientBase<TDerived>;

        TDefaultClientBase(const TReaderActorHandle impl)
            : TClientEventSender::TClientEventSender(impl, *this)
        {}

        NActors::IActorOps* ActorOps() {
            return static_cast<NActors::IActorOps*>(this->DerivedPtr());
        }

        void BecomeStateWork() {
            auto ptr = &TDerived::template DerivedStateFn<
                typename TDefaultClientBase<TDerived>::template TClientEventDispatcher<TClientDerivedCaller<TDerived>>,
                &TDefaultClientBase<TDerived>::template TClientEventDispatcher<TClientDerivedCaller<TDerived>>::StateWork>;
            this->Derived().Become(ptr);
        }
    };

    template <class TDerived>
    class TServerDerivedCaller
        : public TDerivedMixin<TDerived>
    {
    public:
        void OnNeedPoll(TEvReader::TEvNeedPoll::TPtr &) {
            this->Derived().NeedPoll(Tag{});
        }

        void OnPoll(TEvReader::TEvPoll::TPtr &) {
            this->Derived().Poll(Tag{});
        }

        void OnReadNext(TEvReader::TEvReadNext::TPtr &) {
            this->Derived().ReadNext(Tag{});
        }

        void OnRemaining(TEvReader::TEvRemaining::TPtr &) {
            this->Derived().Remaining(Tag{});
        }
    public:
        ~TServerDerivedCaller() {
            static_assert(std::derived_from<TDerived, IServer>, "TDerived should be derived from IServer");
        }
    };

    class TServerEventDispatcherBase {};

    template <class TImpl>
    class TServerEventDispatcher
        : public TServerEventDispatcherBase
        , public TImpl
    {
    public:
        using TServerEventDispatcherType = TServerEventDispatcher<TImpl>;
        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvReader::TEvNeedPoll, TImpl::OnNeedPoll);
                hFunc(TEvReader::TEvPoll, TImpl::OnPoll);
                hFunc(TEvReader::TEvReadNext, TImpl::OnReadNext);
                hFunc(TEvReader::TEvRemaining, TImpl::OnRemaining);
            }
        }
    };

    template <class TDerived>
    using TServerEventDerivedDispatcher = TServerEventDispatcher<TServerDerivedCaller<TDerived>>;

    class TServerEventSender {
        template <class TEv>
        void Send(std::unique_ptr<TEv> ev) {
            Sender->Send(Impl, ev.release());
        }
    public:
        template <class TSender>
        TServerEventSender(const TReaderClientActorHandle impl, TSender& sender)
            : Impl(impl.ActorId)
            , Sender(sender.ActorOps())
        {
            static_assert(std::derived_from<TSender, TServerEventDispatcherBase>, "TSender should be derived from TServerEventDispatcher");
        }
        void AnswerNeedPoll(Tag, bool need) { Send(std::make_unique<TEvReader::TEvNeedPollResult>(need)); }
        void AnswerPoll(Tag) { Send(std::make_unique<TEvReader::TEvPollResult>()); }
        void AnswerRemaining(Tag, ui64 remaining) { Send(std::make_unique<TEvReader::TEvRemainingResult>(remaining)); }
        void AnswerReadNext(Tag, TRcBuf&& data) { Send(std::make_unique<TEvReader::TEvReadNextResult>(std::move(data))); }
    private:
        const NActors::TActorId Impl;
        NActors::IActorOps* Sender;
    };

    template <class TDerived>
    class TDefaultServerBase
        : public IServer
        , public TServerEventDerivedDispatcher<TDerived>
        , public TServerEventSender
    {
    public:
        using TBase = TDefaultServerBase<TDerived>;

        TDefaultServerBase(const TReaderClientActorHandle impl)
            : TServerEventSender::TServerEventSender(impl, *this)
        {}

        NActors::IActorOps* ActorOps() {
            return static_cast<NActors::IActorOps*>(this->DerivedPtr());
        }

        void BecomeStateWork() {
            auto ptr = &TDerived::template DerivedStateFn<
                typename TDefaultServerBase<TDerived>::template TServerEventDispatcher<TServerDerivedCaller<TDerived>>,
                &TDefaultServerBase<TDerived>::template TServerEventDispatcher<TServerDerivedCaller<TDerived>>::StateWork>;
            this->Derived().Become(ptr);
        }
    };
};

} // namespace NKikimr::NChangeMirroring
