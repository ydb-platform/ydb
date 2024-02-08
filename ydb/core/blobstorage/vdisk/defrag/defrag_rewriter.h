#pragma once

#include "defs.h"
#include "defrag_actor.h"
#include "defrag_search.h"

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TEvDefragRewritten
    ////////////////////////////////////////////////////////////////////////////
    struct TEvDefragRewritten : public TEventLocal<TEvDefragRewritten, TEvBlobStorage::EvDefragRewritten> {
        size_t RewrittenRecs = 0;
        size_t RewrittenBytes = 0;

        TEvDefragRewritten(size_t rewrittenRecs, size_t rewrittenBytes)
            : RewrittenRecs(rewrittenRecs)
            , RewrittenBytes(rewrittenBytes)
        {}

        void Output(IOutputStream &str) const {
            str << "RewrittenRecs# " << RewrittenRecs << " RewrittenBytes# " << RewrittenBytes;
        }

        TString ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // VDISK DEFRAG REWRITER
    // Rewrites selected huge blobs to free up some Huge Heap chunks
    ////////////////////////////////////////////////////////////////////////////
    IActor* CreateDefragRewriter(
        const std::shared_ptr<TDefragCtx> &dCtx,
        const TVDiskID &selfVDiskId,
        const TActorId &notifyId,
        std::vector<TDefragRecord> &&recs);

    bool IsDefragRewriter(const IActor* actor);

} // NKikimr
