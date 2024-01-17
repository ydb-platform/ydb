#pragma once
#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NMetadata::NInitializer {

enum EEvents {
    EvInitializationFinished = EventSpaceBegin(TKikimrEvents::ES_METADATA_INITIALIZER),
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_METADATA_INITIALIZER)");

class TEvInitializationFinished: public TEventLocal<TEvInitializationFinished, EEvents::EvInitializationFinished> {
private:
    YDB_READONLY_DEF(TString, InitializationId);
public:
    TEvInitializationFinished(const TString& initializationId)
        : InitializationId(initializationId) {

    }
};

}
