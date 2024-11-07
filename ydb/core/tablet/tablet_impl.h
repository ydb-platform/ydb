#pragma once
#include "defs.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/statestorage.h>

#include <ydb/core/protos/tablet.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tablet/tablet_metrics.h>

#include <ydb/library/actors/core/scheduler_cookie.h>
#include <util/generic/deque.h>
#include <ydb/core/base/tracing.h>

namespace NKikimr {

IActor* CreateTabletReqRebuildHistoryGraph(const TActorId &owner, TTabletStorageInfo *info, ui32 blockedGen, NTracing::ITrace *trace, ui64 followerCookie);
IActor* CreateTabletFindLastEntry(const TActorId &owner, bool readBody, TTabletStorageInfo *info, ui32 blockedGen, bool leader);
IActor* CreateTabletReqWriteLog(const TActorId &owner, const TLogoBlobID &entryId, NKikimrTabletBase::TTabletLogEntry *entry, TVector<TEvTablet::TLogEntryReference> &refs, TEvBlobStorage::TEvPut::ETactic commitTactic, TTabletStorageInfo *info, NWilson::TTraceId traceId = {});
IActor* CreateTabletReqBlockBlobStorage(const TActorId &owner, TTabletStorageInfo *info, ui32 generation, bool blockPrevEntry);
IActor* CreateTabletReqDelete(const TActorId &owner, const TIntrusivePtr<TTabletStorageInfo> &tabletStorageInfo, ui32 generation = std::numeric_limits<ui32>::max());

struct TEvTabletBase {
    enum EEv {
        EvBlockBlobStorageResult = EventSpaceBegin(TKikimrEvents::ES_TABLETBASE),
        EvRebuildGraphResult,
        EvFindLatestLogEntryResult,
        EvWriteLogResult,
        EvDeleteTabletResult,

        EvFollowerRetry = EvBlockBlobStorageResult + 512,
        EvTrySyncFollower,
        EvTryBuildFollowerGraph,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLETBASE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TABLETBASE)");

    struct TEvBlockBlobStorageResult : public TEventLocal<TEvBlockBlobStorageResult, EvBlockBlobStorageResult> {
        const NKikimrProto::EReplyStatus Status;
        const ui64 TabletId;
        const TString ErrorReason;

        TEvBlockBlobStorageResult(NKikimrProto::EReplyStatus status, ui64 tabletId, const TString &reason = TString())
            : Status(status)
            , TabletId(tabletId)
            , ErrorReason(reason)
        {}
    };

    struct TEvRebuildGraphResult : public TEventLocal<TEvRebuildGraphResult, EvRebuildGraphResult> {
        const NKikimrProto::EReplyStatus Status;
        const TString ErrorReason;

        TIntrusivePtr<TEvTablet::TDependencyGraph> DependencyGraph;
        NMetrics::TTabletThroughputRawValue GroupReadBytes;
        NMetrics::TTabletIopsRawValue GroupReadOps;
        THolder<NTracing::ITrace> Trace;

        TEvRebuildGraphResult(
            NKikimrProto::EReplyStatus status,
            NTracing::ITrace *trace,
            const TString &reason = TString()
        )
            : Status(status)
            , ErrorReason(reason)
            , Trace(trace)
        {}

        TEvRebuildGraphResult(
            const TIntrusivePtr<TEvTablet::TDependencyGraph> &graph,
            NMetrics::TTabletThroughputRawValue &&read,
            NMetrics::TTabletIopsRawValue &&readOps,
            NTracing::ITrace *trace
        )
            : Status(NKikimrProto::OK)
            , DependencyGraph(graph)
            , GroupReadBytes(std::move(read))
            , GroupReadOps(std::move(readOps))
            , Trace(trace)
        {}
    };

    struct TEvFindLatestLogEntryResult : public TEventLocal<TEvFindLatestLogEntryResult, EvFindLatestLogEntryResult> {
        const NKikimrProto::EReplyStatus Status;
        const TLogoBlobID Latest;
        const ui32 BlockedGeneration;
        const TString Buffer;
        const TString ErrorReason;

        TEvFindLatestLogEntryResult(NKikimrProto::EReplyStatus status, const TString &reason = TString())
            : Status(status)
            , BlockedGeneration(0)
            , ErrorReason(reason)
        {
            Y_DEBUG_ABORT_UNLESS(status != NKikimrProto::OK);
        }

        TEvFindLatestLogEntryResult(const TLogoBlobID &latest, ui32 blockedGeneration, const TString &buffer)
            : Status(NKikimrProto::OK)
            , Latest(latest)
            , BlockedGeneration(blockedGeneration)
            , Buffer(buffer)
        {}
    };

    struct TEvWriteLogResult : public TEventLocal<TEvWriteLogResult, EvWriteLogResult> {
        const NKikimrProto::EReplyStatus Status;
        const TLogoBlobID EntryId;
        TVector<ui32> YellowMoveChannels;
        TVector<ui32> YellowStopChannels;
        THashMap<ui32, float> ApproximateFreeSpaceShareByChannel;
        NMetrics::TTabletThroughputRawValue GroupWrittenBytes;
        NMetrics::TTabletIopsRawValue GroupWrittenOps;
        const TString ErrorReason;

        struct TErrorCondition {

        } ErrorCondition;

        TEvWriteLogResult(
                NKikimrProto::EReplyStatus status,
                const TLogoBlobID &entryId,
                TVector<ui32>&& yellowMoveChannels,
                TVector<ui32>&& yellowStopChannels,
                THashMap<ui32, float>&& approximateFreeSpaceShareByChannel,
                NMetrics::TTabletThroughputRawValue&& written,
                NMetrics::TTabletIopsRawValue&& writtenOps,
                const TString &reason = TString())
            : Status(status)
            , EntryId(entryId)
            , YellowMoveChannels(std::move(yellowMoveChannels))
            , YellowStopChannels(std::move(yellowStopChannels))
            , ApproximateFreeSpaceShareByChannel(std::move(approximateFreeSpaceShareByChannel))
            , GroupWrittenBytes(std::move(written))
            , GroupWrittenOps(std::move(writtenOps))
            , ErrorReason(reason)
        {}
    };

    struct TEvFollowerRetry : public TEventLocal<TEvFollowerRetry, EvFollowerRetry> {
        const ui32 Round;

        TEvFollowerRetry(ui32 round)
            : Round(round)
        {}
    };

    struct TEvTrySyncFollower : public TEventLocal<TEvTrySyncFollower, EvTrySyncFollower> {
        const TActorId FollowerId;
        TSchedulerCookieHolder CookieHolder;

        TEvTrySyncFollower(TActorId followerId, ISchedulerCookie *cookie)
            : FollowerId(followerId)
            , CookieHolder(cookie)
        {}
    };

    struct TEvTryBuildFollowerGraph : public TEventLocal<TEvTryBuildFollowerGraph, EvTryBuildFollowerGraph> {};

    struct TEvDeleteTabletResult : public TEventLocal<TEvDeleteTabletResult, EvDeleteTabletResult> {
        const NKikimrProto::EReplyStatus Status;
        const ui64 TabletId;

        TEvDeleteTabletResult(NKikimrProto::EReplyStatus status, ui64 tabletId)
            : Status(status)
            , TabletId(tabletId)
        {}
    };
};

}
