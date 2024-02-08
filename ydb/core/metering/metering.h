#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/logger/backend.h>

#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NMetering {


struct TEvMetering
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_YDB_METERING),

        // Request actors
        EvWriteMeteringJson = EvBegin + 0,

        EvEnd
    };

    static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_METERING),
                  "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_YDB_METERING)");

    //
    // WriteMeteringJson
    //

    struct TEvWriteMeteringJson
        : public NActors::TEventLocal<TEvWriteMeteringJson, EvWriteMeteringJson>
    {
        TString MeteringJson;

        TEvWriteMeteringJson(TString meteringJson)
            : MeteringJson(std::move(meteringJson))
        {}
    };
};

////////////////////////////////////////////////////////////////////////////////

void SendMeteringJson(const NActors::TActorContext& ctx, TString message);

inline NActors::TActorId MakeMeteringServiceID() {
    return NActors::TActorId(0, TStringBuf("YDB_METER"));
}

THolder<NActors::IActor> CreateMeteringWriter(
    THolder<TLogBackend> meteringFile);

}   // namespace NKikimr
}   // namespace NMetering
