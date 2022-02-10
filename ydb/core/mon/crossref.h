#pragma once

#include <ydb/core/base/events.h>

#include <library/cpp/cgiparam/cgiparam.h>

namespace NKikimr::NCrossRef {

    struct TEvents {
        enum {
            EvGenerateCrossRef = EventSpaceBegin(TKikimrEvents::ES_CROSSREF),
            EvCrossRef,
        };
    };

    struct TEvGenerateCrossRef : TEventLocal<TEvGenerateCrossRef, TEvents::EvGenerateCrossRef> {
        const TString PagePath;
        const ui32 TargetNodeId;
        const TString Path;
        const TInstant Deadline;

        TEvGenerateCrossRef(TString pagePath, ui32 targetNodeId, TString path, TInstant deadline)
            : PagePath(std::move(pagePath))
            , TargetNodeId(targetNodeId)
            , Path(std::move(path))
            , Deadline(deadline)
        {}
    };

    struct TEvCrossRef : TEventLocal<TEvCrossRef, TEvents::EvCrossRef> {
        const TString Url;

        TEvCrossRef(TString url)
            : Url(std::move(url))
        {}
    };

    IActor *CreateCrossRefActor();

    inline TActorId MakeCrossRefActorId() {
        return TActorId(0, TStringBuf("CrossRefActr", 12));
    }

} // NKikimr::NCrossRef
