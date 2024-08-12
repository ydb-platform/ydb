#pragma once

#include <ydb/core/persqueue/events/internal.h>

#include <util/generic/fwd.h>
#include <util/generic/maybe.h>

#include <variant>


namespace NKikimr::NPQ {

struct TUserCookie {
    TString User;
    ui64 Cookie;
};

struct TWriteMsg {
    ui64 Cookie;
    TMaybe<ui64> Offset;
    TEvPQ::TEvWrite::TMsg Msg;
    std::optional<ui64> InitialSeqNo;
    bool Internal = false;
};

struct TOwnershipMsg {
    ui64 Cookie;
    TString OwnerCookie;
};

struct TRegisterMessageGroupMsg {
    ui64 Cookie;
    TEvPQ::TEvRegisterMessageGroup::TBody Body;

    explicit TRegisterMessageGroupMsg(TEvPQ::TEvRegisterMessageGroup& ev)
        : Cookie(ev.Cookie)
        , Body(std::move(ev.Body))
    {
    }
};

struct TDeregisterMessageGroupMsg {
    ui64 Cookie;
    TEvPQ::TEvDeregisterMessageGroup::TBody Body;

    explicit TDeregisterMessageGroupMsg(TEvPQ::TEvDeregisterMessageGroup& ev)
        : Cookie(ev.Cookie)
        , Body(std::move(ev.Body))
    {
    }
};

struct TSplitMessageGroupMsg {
    ui64 Cookie;
    TVector<TEvPQ::TEvDeregisterMessageGroup::TBody> Deregistrations;
    TVector<TEvPQ::TEvRegisterMessageGroup::TBody> Registrations;

    explicit TSplitMessageGroupMsg(ui64 cookie)
        : Cookie(cookie)
    {
    }
};


struct TMessage {
    std::variant<
        TWriteMsg,
        TOwnershipMsg,
        TRegisterMessageGroupMsg,
        TDeregisterMessageGroupMsg,
        TSplitMessageGroupMsg
    > Body;
    TDuration QueueTime;    // baseline for request and duration for response
    TInstant WriteTimeBaseline;

    template <typename T>
    explicit TMessage(T&& body, TDuration queueTime, TInstant writeTimeBaseline = TInstant::Zero())
        : Body(std::forward<T>(body))
        , QueueTime(queueTime)
        , WriteTimeBaseline(writeTimeBaseline)
    {
    }

    ui64 GetCookie() const {
        switch (Body.index()) {
        case 0:
            return std::get<0>(Body).Cookie;
        case 1:
            return std::get<1>(Body).Cookie;
        case 2:
            return std::get<2>(Body).Cookie;
        case 3:
            return std::get<3>(Body).Cookie;
        case 4:
            return std::get<4>(Body).Cookie;
        default:
            Y_ABORT("unreachable");
        }
    }

    #define DEFINE_CHECKER_GETTER(name, i) \
        bool Is##name() const { \
            return Body.index() == i; \
        } \
        const auto& Get##name() const { \
            Y_ABORT_UNLESS(Is##name()); \
            return std::get<i>(Body); \
        } \
        auto& Get##name() { \
            Y_ABORT_UNLESS(Is##name()); \
            return std::get<i>(Body); \
        }

    DEFINE_CHECKER_GETTER(Write, 0)
    DEFINE_CHECKER_GETTER(Ownership, 1)
    DEFINE_CHECKER_GETTER(RegisterMessageGroup, 2)
    DEFINE_CHECKER_GETTER(DeregisterMessageGroup, 3)
    DEFINE_CHECKER_GETTER(SplitMessageGroup, 4)

    #undef DEFINE_CHECKER_GETTER

    size_t GetWriteSize() const {
        if (IsWrite()) {
            auto& w = GetWrite();
            return w.Msg.SourceId.size() + w.Msg.Data.size();
        } else {
            return 0;
        }
    }
};


} // namespace NKikimr::NPQ

