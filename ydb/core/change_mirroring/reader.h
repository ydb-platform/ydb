#pragma once

#include <concepts>

#include <util/system/types.h>

#include <ydb/library/actors/util/rc_buf.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/events.h>

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
            EvPoll = EventSpaceBegin(TKikimrEvents::ES_CHANGE_EXCHANGE_READER),
            EvPollResult,
            EvEnd,
        };

        struct TEvPoll: public NActors::TEventLocal<TEvPoll, EvPoll> {
        };

        struct TEvPollResult : public NActors::TEventLocal<TEvPollResult, EvPollResult> {
            TEvPollResult(TVector<TRcBuf> data)
                : Data(data)
            {}
            TVector<TRcBuf> Data;
        };
    };

    struct Tag {};
    struct TReaderActorHandle {
        const NActors::TActorId ActorId;
    };

    class IClient {
    public:
        virtual ~IClient() {};
        virtual void PollResult(Tag, const TEvReader::TEvPollResult& result) { Y_UNUSED(result); };
    };

    class IServer {
    public:
        virtual ~IServer() {};
        virtual void Poll(Tag, const AIReader::TEvReader::TEvPoll::TPtr& result) { Y_UNUSED(result); };
    };

    template <class TDerived>
    class TClientDerivedCaller
        : public TDerivedMixin<TDerived>
    {
    public:
        void OnPollResult(const TEvReader::TEvPollResult::TPtr &ev) {
            this->DerivedPtr()->PollResult(Tag{}, *ev->Get());
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
                hFunc(TEvReader::TEvPollResult, TImpl::OnPollResult);
            }
        }
    };

    template <class TDerived>
    using TClientEventDerivedDispatcher = TClientEventDispatcher<TClientDerivedCaller<TDerived>>;

    template <class TDerived>
    class TDefaultClientBase
        : public IClient
        , public TClientEventDerivedDispatcher<TDerived>
    {
    public:
        using TBase = TDefaultClientBase<TDerived>;

        TDefaultClientBase(const TReaderActorHandle impl)
            : Impl(impl.ActorId)
        {}

        void BecomeStateWork() {
            auto ptr = &TDerived::template DerivedStateFn<
                typename TDefaultClientBase<TDerived>::template TClientEventDispatcher<TClientDerivedCaller<TDerived>>,
                &TDefaultClientBase<TDerived>::template TClientEventDispatcher<TClientDerivedCaller<TDerived>>::StateWork>;
            this->Derived().Become(ptr);
        }

        template <class TIn, class TOut>
        void Reply(const TIn& in, const TOut& out) {
            this->Derived().Send(in->Sender, out);
        }

        NActors::IActorOps& ActorOps() {
            return static_cast<NActors::IActorOps&>(this->Derived());
        }

        std::unique_ptr<TEvReader::TEvPoll> Poll() const {
            return std::make_unique<TEvReader::TEvPoll>();
        }

        NActors::TActorId ReaderActor() const {
            return Impl;
        }

    private:
        NActors::TActorId Impl;
    };

    template <class TDerived>
    class TServerDerivedCaller
        : public TDerivedMixin<TDerived>
    {
    public:
        void OnPoll(TEvReader::TEvPoll::TPtr &ev) {
            this->DerivedPtr()->Poll(Tag{}, ev);
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
                hFunc(TEvReader::TEvPoll, TImpl::OnPoll);
            }
        }
    };

    template <class TDerived>
    using TServerEventDerivedDispatcher = TServerEventDispatcher<TServerDerivedCaller<TDerived>>;

    template <class TDerived>
    class TDefaultServerBase
        : public IServer
        , public TServerEventDerivedDispatcher<TDerived>
    {
    public:
        using TBase = TDefaultServerBase<TDerived>;

        void BecomeStateWork() {
            auto ptr = &TDerived::template DerivedStateFn<
                typename TDefaultServerBase<TDerived>::template TServerEventDispatcher<TServerDerivedCaller<TDerived>>,
                &TDefaultServerBase<TDerived>::template TServerEventDispatcher<TServerDerivedCaller<TDerived>>::StateWork>;
            this->Derived().Become(ptr);
        }

        template <class TIn, class TOut>
        void Reply(const TIn& in, const TOut& out) {
            this->Derived().Send(in->Sender, out);
        }

        NActors::IActorOps& ActorOps() {
            return static_cast<NActors::IActorOps&>(this->Derived());
        }

        std::unique_ptr<TEvReader::TEvPollResult> PollResult(TVector<TRcBuf> data) const {
            return std::make_unique<TEvReader::TEvPollResult>(data);
        }
    };
};

} // namespace NKikimr::NChangeMirroring
