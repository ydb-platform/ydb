#pragma once
#include "common.h"
#include "index_constructor.h"
#include <ydb/library/actors/util/local_process_key.h>

namespace NActors {

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

    static ui32 GetActorSystemSpinIndex() {
        return TEnumProcessKey<TActorActivityTag, EInternalActorType>::GetIndex(EInternalActorType::ACTOR_SYSTEM_SPIN);
    }

    static ui32 GetActorSystemHarmonizerIndex() {
        return TEnumProcessKey<TActorActivityTag, EInternalActorType>::GetIndex(EInternalActorType::ACTOR_SYSTEM_HARMONIZER);
    }
};
}
