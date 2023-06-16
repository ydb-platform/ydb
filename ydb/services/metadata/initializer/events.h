#pragma once
#include "common.h"

#include <ydb/services/metadata/ds_table/config.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/actors/core/av_bootstrapped.h>

namespace NKikimr::NMetadata::NInitializer {

enum EEvents {
    EvInitializerPreparationStart = EventSpaceBegin(TKikimrEvents::ES_METADATA_INITIALIZER),
    EvInitializerPreparationFinished,
    EvInitializerPreparationProblem,
    EvInitializationFinished,
    EvAlterFinished,
    EvAlterProblem,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER)");

class TEvInitializerPreparationStart: public TEventLocal<TEvInitializerPreparationStart, EEvents::EvInitializerPreparationStart> {
public:
};

class TEvAlterFinished: public TEventLocal<TEvAlterFinished, EEvents::EvAlterFinished> {
public:
};

class TEvAlterProblem: public TEventLocal<TEvAlterProblem, EEvents::EvAlterProblem> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
public:
    TEvAlterProblem(const TString& errorMesage)
        : ErrorMessage(errorMesage) {

    }
};

class TEvInitializerPreparationFinished: public TEventLocal<TEvInitializerPreparationFinished, EEvents::EvInitializerPreparationFinished> {
private:
    YDB_READONLY_DEF(TVector<ITableModifier::TPtr>, Modifiers);
public:
    TEvInitializerPreparationFinished(const TVector<ITableModifier::TPtr>& modifiers)
        : Modifiers(modifiers) {

    }
};

class TEvInitializerPreparationProblem: public TEventLocal<TEvInitializerPreparationProblem, EEvents::EvInitializerPreparationProblem> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
public:
    TEvInitializerPreparationProblem(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

class TEvInitializationFinished: public TEventLocal<TEvInitializationFinished, EEvents::EvInitializationFinished> {
private:
    YDB_READONLY_DEF(TString, InitializationId);
public:
    TEvInitializationFinished(const TString& initializationId)
        : InitializationId(initializationId) {

    }
};

}
