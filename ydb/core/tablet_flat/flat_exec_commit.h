#pragma once

#include <ydb/core/base/logoblob.h>
#include <ydb/core/base/tablet.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TSeat;

    enum class ECommit {
        Redo = 1,   /* Tables data redo log     */
        Snap = 2,   /* Executor states snapshot */
        Data = 3,   /* Page packets compaction  */
        Misc = 8,   /* The rest of commit types */
    };

    struct TGCBlobDelta {
        /* born -> [ Created ] -> hold -> [ Deleted ] -> gone */

        TVector<TLogoBlobID> Created;
        TVector<TLogoBlobID> Deleted;
    };

    struct TLogCommit {
        using ETactic = TEvBlobStorage::TEvPut::ETactic;

        TLogCommit(bool sync, ui32 step, ECommit type, NWilson::TTraceId traceId)
            : Step(step)
            , Type(type)
            , Sync(sync)
            , TraceId(std::move(traceId))
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Commit{" << Step << " orig " << ui32(Type)
                << ", " << (Sync ? "S" : "-")  << (Embedded ? "E" : "-")
                << (FollowerAux ? "X" : "-") << " " << Refs.size() << " refs"
                << ", Gc (+" << +GcDelta.Created.size()
                    << " -" << GcDelta.Deleted.size() << ")}";
        }

        void PushTx(TSeat *seat) noexcept;
        TSeat* PopTx() noexcept;

        const ui32 Step = Max<ui32>();
        const ECommit Type = ECommit::Misc;
        const bool Sync = false;
        bool WaitFollowerGcAck = false;
        TString Embedded;
        TString FollowerAux;
        TVector<TEvTablet::TLogEntryReference> Refs;
        TGCBlobDelta GcDelta;
        TVector<TEvTablet::TCommitMetadata> Metadata;
        NWilson::TTraceId TraceId;
        TSeat *FirstTx = nullptr;
        TSeat *LastTx = nullptr;
    };

}
}
