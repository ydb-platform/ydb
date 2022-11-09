#pragma once
#include "common.h"

#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadataInitializer {

enum EInitializerEvents {
    EvInitializerPreparationStart = EventSpaceBegin(TKikimrEvents::ES_METADATA_INITIALIZER),
    EvInitializerPreparationFinished,
    EvInitializerPreparationProblem,
    EvEnd
};

static_assert(EInitializerEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER)");

class TEvInitializerPreparationStart: public TEventLocal<TEvInitializerPreparationStart, EInitializerEvents::EvInitializerPreparationStart> {
public:
};

class TEvInitializerPreparationFinished: public TEventLocal<TEvInitializerPreparationFinished, EInitializerEvents::EvInitializerPreparationFinished> {
private:
    YDB_READONLY_DEF(TVector<ITableModifier::TPtr>, Modifiers);
public:
    TEvInitializerPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers)
        : Modifiers(modifiers) {

    }
};

class TEvInitializerPreparationProblem: public TEventLocal<TEvInitializerPreparationProblem, EInitializerEvents::EvInitializerPreparationProblem> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
public:
    TEvInitializerPreparationProblem(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

}
