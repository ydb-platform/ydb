#pragma once
#include <library/cpp/actors/util/local_process_key.h>
#include <library/cpp/actors/prof/tag.h>

namespace NActors {

struct TActorActivityTag {};

}

template <>
class TLocalProcessKeyStateIndexConstructor<NActors::TActorActivityTag> {
public:
    static ui32 BuildCurrentIndex(const TStringBuf name, const ui32 /*currentNamesCount*/) {
        return NProfiling::MakeTag(name.data());
    }
};
namespace NActors {
enum class EInternalActorType {
    OTHER = 0,
    INCORRECT_ACTOR_TYPE_INDEX,
    ACTOR_SYSTEM,
    ACTORLIB_COMMON,
    ACTORLIB_STATS,
    LOG_ACTOR,
    INTERCONNECT_PROXY_TCP,
    INTERCONNECT_SESSION_TCP,
    INTERCONNECT_COMMON,
    SELF_PING_ACTOR,
    TEST_ACTOR_RUNTIME,
    INTERCONNECT_HANDSHAKE,
    INTERCONNECT_POLLER,
    INTERCONNECT_SESSION_KILLER,
    ACTOR_SYSTEM_SCHEDULER_ACTOR,
    ACTOR_FUTURE_CALLBACK,
    INTERCONNECT_MONACTOR,
    INTERCONNECT_LOAD_ACTOR,
    INTERCONNECT_LOAD_RESPONDER,
    NAMESERVICE,
    DNS_RESOLVER,
    INTERCONNECT_PROXY_WRAPPER,
    ACTOR_COROUTINE
};

class TActorTypeOperator {
public:
    static constexpr ui32 GetMaxAvailableActorsCount() {
        return TLocalProcessKeyStateIndexLimiter::GetMaxKeysCount();
    }

    template <class TEnum>
    static ui32 GetEnumActivityType(const TEnum enumValue) {
        return TEnumProcessKey<TActorActivityTag, TEnum>::GetIndex(enumValue);
    }

    static ui32 GetActorSystemIndex() {
        return TEnumProcessKey<TActorActivityTag, EInternalActorType>::GetIndex(EInternalActorType::ACTOR_SYSTEM);
    }

    static ui32 GetOtherActivityIndex() {
        return TEnumProcessKey<TActorActivityTag, EInternalActorType>::GetIndex(EInternalActorType::OTHER);
    }

    static ui32 GetActorActivityIncorrectIndex() {
        return TEnumProcessKey<TActorActivityTag, EInternalActorType>::GetIndex(EInternalActorType::INCORRECT_ACTOR_TYPE_INDEX);
    }
};
}
