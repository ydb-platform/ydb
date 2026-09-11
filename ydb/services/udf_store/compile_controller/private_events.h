#pragma once

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NUdfStore {

struct TEvControllerPrivate {
    enum EEv {
        EvReconcileResult = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvScheduleTick,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE),
        "expected EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvReconcileResult
        : public NActors::TEventLocal<TEvReconcileResult, EvReconcileResult>
    {
        TString CpuSpec;
        bool Success = false;
        //! Joined `(id, kind, uid)` of every finished artifact on this
        //! platform. Empty on failure: an unreadable artifact table must not
        //! be mistaken for a platform where nothing is compiled yet, or the
        //! controller would re-assign work that is already done.
        THashSet<TString> ArtifactKeys;
    };

    //! Periodic wakeup that expires assignments and drains the queue.
    struct TEvScheduleTick
        : public NActors::TEventLocal<TEvScheduleTick, EvScheduleTick>
    {};
};

//! Key used to compare a `modules` row against the artifact tables. The uid is
//! part of it, so a re-upload produces a different gap rather than silently
//! reusing the artifact of the upload it replaced.
inline TString MakeArtifactKey(TStringBuf name, TStringBuf kind, TStringBuf uid) {
    TString result;
    result.reserve(name.size() + kind.size() + uid.size() + 2);
    result.append(name).append('\0').append(kind).append('\0').append(uid);
    return result;
}

} // namespace NKikimr::NUdfStore
