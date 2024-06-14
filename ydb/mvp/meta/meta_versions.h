#pragma once

#include <vector>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/mvp/core/core_ydb.h>

namespace NMVP {

struct TVersionInfo {
    TString Version;
    ui32 ColorClass;

    TVersionInfo() = default;
    TVersionInfo(TVersionInfo&&) = default;
    TVersionInfo(const TString& version, ui32 colorClass)
        : Version(version)
        , ColorClass(colorClass)
    {}
};

using TVersionInfoCache = std::vector<TVersionInfo>;
using TVersionInfoCachePtr = std::shared_ptr<TVersionInfoCache>;

struct THandlerActorYdbMeta {
    struct TEvPrivateMeta {
        enum EEv {
            EvVersionListLoadResult  = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvVersionListSave,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvVersionListLoadResult : NActors::TEventLocal<TEvVersionListLoadResult, EvVersionListLoadResult> {
            TVersionInfoCachePtr Result;

            TEvVersionListLoadResult() = default;
            TEvVersionListLoadResult(TVersionInfoCachePtr&& result)
                : Result(std::move(result))
            {}
        };

        struct TEvVersionListSave : NActors::TEventLocal<TEvVersionListSave, EvVersionListSave> {
            TVersionInfoCachePtr Result;

            TEvVersionListSave(TVersionInfoCachePtr&& result)
                : Result(std::move(result))
            {}
        };
    };
};


NActors::TActorId CreateLoadVersionsActor(const NActors::TActorId& owner, std::shared_ptr<NYdb::NTable::TTableClient> client, const TString rootDomain, const NActors::TActorContext& ctx);

} // namespace NMVP
