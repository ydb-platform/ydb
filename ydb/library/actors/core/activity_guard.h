#pragma once

#include <ydb/library/actors/actor_type/common.h>
#include <ydb/library/actors/actor_type/index_constructor.h>
#include <ydb/library/actors/util/local_process_key.h>
#include <ydb/library/actors/util/datetime.h>

namespace NActors {


void ChangeActivity(NHPTimer::STime hpnow, ui32 &prevIndex, ui32 &index);

template <EInternalActorSystemActivity ActivityType, bool IsMainActivity=true>
class TInternalActorTypeGuard {
public:
    TInternalActorTypeGuard() {
        if (Allowed) {
            ChangeActivity(GetCycleCountFast(), NextIndex, Index);
        }
    }

    TInternalActorTypeGuard(ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Allowed) {
            ChangeActivity(GetCycleCountFast(), NextIndex, Index);
            NextIndex = nextIndex;
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow) {
        if (Allowed) {
            ChangeActivity(hpnow, NextIndex, Index);
        }
    }

    TInternalActorTypeGuard(NHPTimer::STime hpnow, ui32 nextIndex)
        : NextIndex(nextIndex)
    {
        if (Allowed) {
            ChangeActivity(hpnow, NextIndex, Index);
            NextIndex = nextIndex;
        }
    }

    ~TInternalActorTypeGuard() {
        if (Allowed) {
            ui32 prevIndex = Index;
            ChangeActivity(GetCycleCountFast(), prevIndex, NextIndex);
        }
    }

private:
    static constexpr bool ExtraActivitiesIsAllowed = false;
    static constexpr bool Allowed = ExtraActivitiesIsAllowed || IsMainActivity;
    static ui32 Index;
    ui32 NextIndex = 0;
};

template <EInternalActorSystemActivity ActivityType, bool IsMainActivity>
ui32 TInternalActorTypeGuard<ActivityType, IsMainActivity>::Index = TEnumProcessKey<TActorActivityTag, EInternalActorSystemActivity>::GetIndex(ActivityType);

}