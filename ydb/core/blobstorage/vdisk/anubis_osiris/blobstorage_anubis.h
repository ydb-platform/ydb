#pragma once

#include "defs.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TAnubisIssues
    ////////////////////////////////////////////////////////////////////////////
    struct TAnubisIssues {
        // have we got some not good answers from peers? It means we may want to repeat
        bool NotOKAnswersFromPeers = false;
        // have we got some errors during local writes
        bool LocalWriteErrors = false;

        TAnubisIssues() = default;

        TAnubisIssues(bool notOKAnswersFromPeers, bool localWriteErrors)
            : NotOKAnswersFromPeers(notOKAnswersFromPeers)
            , LocalWriteErrors(localWriteErrors)
        {}

        TAnubisIssues &Merge(const TAnubisIssues &i) {
            if (i.NotOKAnswersFromPeers)
                NotOKAnswersFromPeers = true;
            if (i.LocalWriteErrors)
                LocalWriteErrors = true;
            return *this;
        }

        bool HaveProblems() const {
            return NotOKAnswersFromPeers || LocalWriteErrors;
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvAnubisQuantumDone
    ////////////////////////////////////////////////////////////////////////////
    struct TEvAnubisQuantumDone :
        public TEventLocal<TEvAnubisQuantumDone, TEvBlobStorage::EvAnubisQuantumDone>
    {
        // have we finished Hull Db processing
        const bool Finished;
        // position at whish we stopped if not finished
        const TLogoBlobID Pos;
        // any issuses we during run
        const TAnubisIssues Issues;

        TEvAnubisQuantumDone(bool finished,
                             const TLogoBlobID &pos,
                             const TAnubisIssues &issues)
            : Finished(finished)
            , Pos(pos)
            , Issues(issues)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // ANUBIS ACTOR CREATOR
    // The actor finds and removes LogoBlobs with Keep flags that are not
    // needed anymore
    ////////////////////////////////////////////////////////////////////////////
    struct THullCtx;
    IActor* CreateAnubis(const TIntrusivePtr<THullCtx> &hullCtx,
                         const TIntrusivePtr<TBlobStorageGroupInfo> &ginfo,
                         const TActorId &parentId,
                         const TActorId &skeletonId,
                         ui32 replInterconnectChannel,
                         ui64 anubisOsirisMaxInFly);

} // NKikimr
