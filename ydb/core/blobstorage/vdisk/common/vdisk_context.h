#pragma once
#include "blobstorage_cost_tracker.h"
#include "defs.h"
#include "memusage.h"
#include "vdisk_config.h"
#include "vdisk_costmodel.h"
#include "vdisk_log.h"
#include "vdisk_pdisk_error.h"
#include "vdisk_outofspace.h"
#include "vdisk_histograms.h"
#include "vdisk_mongroups.h"
#include <ydb/core/base/id_wrapper.h>
#include <ydb/core/blobstorage/base/ptr.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {
    class TCostModel;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TBSProxyContext
    /////////////////////////////////////////////////////////////////////////////////////////
    struct TBSProxyContext : public TThrRefBase, TNonCopyable {
        TBSProxyContext(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& mon)
            : Queue(mon->GetCounter("MemTotal:Queue"))
        {}

        TMemoryConsumer Queue;
    };

    using TBSProxyContextPtr = TIntrusivePtr<TBSProxyContext>;

    /////////////////////////////////////////////////////////////////////////////////////////
    // TVDiskContext
    /////////////////////////////////////////////////////////////////////////////////////////
    class TVDiskContext : public TBSProxyContext {
    public:
        using TGroupId = TIdWrapper<ui32, TGroupIdTag>;
        // ActorId of the main VDisk actor (currently ActorId of SkeletonFront)
        const TActorId VDiskActorId;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> VDiskCounters;
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> VDiskMemCounters;
        // latency histograms
        NVDiskMon::THistograms Histograms;
        std::shared_ptr<NMonGroup::TVDiskIFaceGroup> IFaceMonGroup;
        // Self VDisk related info
        const TGroupId GroupId;
        const TVDiskIdShort ShortSelfVDisk;
        const TString VDiskLogPrefix;
        const ui32 NodeId;
        // Memory consumption
        TMemoryConsumer FreshIndex;
        TMemoryConsumer FreshData;
        TMemoryConsumer SstIndex;
        TMemoryConsumer CompDataFresh;
        TMemoryConsumer CompIndexFresh;
        TMemoryConsumer CompData;
        TMemoryConsumer CompIndex;
        TMemoryConsumer IteratorsCache;
        TMemoryConsumer Replication;
        TMemoryConsumer SyncLogCache;
        TActorSystem *ActorSystem;
        TReplQuoter::TPtr ReplPDiskReadQuoter;
        TReplQuoter::TPtr ReplPDiskWriteQuoter;
        TReplQuoter::TPtr ReplNodeRequestQuoter;
        TReplQuoter::TPtr ReplNodeResponseQuoter;

        // diagnostics
        TString LocalRecoveryErrorStr;

        std::unique_ptr<TCostModel> CostModel;
        std::shared_ptr<TBsCostTracker> CostTracker;

        // oos logging
        std::atomic<ui32> CurrentOOSStatusFlag = NKikimrBlobStorage::StatusIsValid;
        std::shared_ptr<NMonGroup::TOutOfSpaceGroup> OOSMonGroup;

    private:
        // Managing disk space
        TOutOfSpaceState OutOfSpaceState;
        // Global stat about huge heap fragmentation
        THugeHeapFragmentation HugeHeapFragmentation;
        // Tracks PDisk errors
        TPDiskErrorState PDiskErrorState;
        friend class TDskSpaceTrackerActor;

        NMonGroup::TCostGroup CostMonGroup;

    public:
        TLogger Logger;

        TPDiskErrorState::EState GetPDiskErrorState() const {
            return PDiskErrorState.GetState();
        }

    public:
        TVDiskContext(
                const TActorId &vdiskActorId,
                std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                const TIntrusivePtr<::NMonitoring::TDynamicCounters>& vdiskCounters,
                const TVDiskID &selfVDisk,
                TActorSystem *as,   // can be nullptr for tests
                NPDisk::EDeviceType type,
                bool donorMode = false,
                TReplQuoter::TPtr replPDiskReadQuoter = nullptr,
                TReplQuoter::TPtr replPDiskWriteQuoter = nullptr,
                TReplQuoter::TPtr replNodeRequestQuoter = nullptr,
                TReplQuoter::TPtr replNodeResponseQuoter = nullptr,
                ui64 burstThresholdNs = 1'000'000'000,
                float diskTimeAvailableScale = 1);

        // The function checks response from PDisk. Normally, it's OK.
        // Other alternatives are: 1) shutdown; 2) FAIL
        // true -- OK
        // false -- shutdown caller
        template <class T, class TActorSystemOrCtx>
        bool CheckPDiskResponse(const TActorSystemOrCtx &actorSystemOrCtx, const T &ev, const TString &message = {}) {
            // check status
            switch (ev.Status) {
                case NKikimrProto::OK:
                    if constexpr (T::EventType != TEvBlobStorage::EvLogResult) {
                        // we have different semantics for TEvLogResult StatusFlags
                        OutOfSpaceState.UpdateLocalChunk(ev.StatusFlags);
                    } else {
                        // update log space flags
                        OutOfSpaceState.UpdateLocalLog(ev.StatusFlags);
                    }
                    return true;
                case NKikimrProto::INVALID_OWNER:
                case NKikimrProto::INVALID_ROUND:
                    // BlobStorage group reconfiguration, just return false and wait until
                    // node warden restarts VDisk
                    LOG_NOTICE(actorSystemOrCtx, NKikimrServices::BS_VDISK_OTHER,
                            VDISKP(VDiskLogPrefix,
                                "CheckPDiskResponse: Group Reconfiguration: %s",
                                FormatMessage(ev.Status, ev.ErrorReason, ev.StatusFlags, message).data()));
                    return false;
                case NKikimrProto::CORRUPTED:
                case NKikimrProto::OUT_OF_SPACE: {
                    // Device is out of order
                    PDiskErrorState.Set(ev.Status, ev.StatusFlags);
                    auto newState = PDiskErrorState.GetState();
                    LOG_ERROR(actorSystemOrCtx, NKikimrServices::BS_VDISK_OTHER,
                            VDISKP(VDiskLogPrefix,
                                "CheckPDiskResponse: Recoverable error from PDisk: %s newState# %s",
                                FormatMessage(ev.Status, ev.ErrorReason, ev.StatusFlags, message).data(),
                                TPDiskErrorState::StateToString(newState)));
                    actorSystemOrCtx.Send(VDiskActorId, new TEvPDiskErrorStateChange(newState));
                    return false;
                }
                default:
                    // fail process, unrecoverable error
                    Y_ABORT("%s", VDISKP(VDiskLogPrefix, "CheckPDiskResponse: FATAL error from PDisk: %s",
                                FormatMessage(ev.Status, ev.ErrorReason, ev.StatusFlags, message).data()).data());
                    return false;
            }
        }

        template <class TActorSystemOrCtx>
        bool CheckPDiskResponseReadable(const TActorSystemOrCtx &actorSystemOrCtx, const NPDisk::TEvChunkReadResult &ev, const TString &message = {}) {
            if (!ev.Data.IsReadable()) {
                LOG_ERROR(actorSystemOrCtx, NKikimrServices::BS_VDISK_OTHER,
                        VDISKP(VDiskLogPrefix,
                            "CheckPDiskResponseReadable: not readable chunk from PDisk: %s",
                            FormatMessage(ev.Status, ev.ErrorReason, ev.StatusFlags, message).data()));
                return false;
            }
            return true;
        }

        TOutOfSpaceState &GetOutOfSpaceState() {
            return OutOfSpaceState;
        }

        THugeHeapFragmentation &GetHugeHeapFragmentation() {
            return HugeHeapFragmentation;
        }

        template<class TEvent>
        void CountDefragCost(const TEvent& ev) {
            if (CostModel) {
                CostMonGroup.DefragCostNs() += CostModel->GetCost(ev);
            }
            CostTracker->CountDefragRequest(ev);
        }

        template<class TEvent>
        void CountScrubCost(const TEvent& ev) {
            if (CostModel) {
                CostMonGroup.ScrubCostNs() += CostModel->GetCost(ev);
            }
            CostTracker->CountScrubRequest(ev);
        }

        template<class TEvent>
        void CountCompactionCost(const TEvent& ev) {
            if (CostModel) {
                CostMonGroup.CompactionCostNs() += CostModel->GetCost(ev);
            }
            CostTracker->CountCompactionRequest(ev);
        }

        void UpdateCostModel(std::unique_ptr<TCostModel>&& newCostModel) {
            CostModel = std::move(newCostModel);
            if (CostModel) {
                CostTracker->UpdateCostModel(*CostModel);
            }
        }

    private:
        TString FormatMessage(
                NKikimrProto::EReplyStatus status,
                const TString &errorReason,
                NPDisk::TStatusFlags statusFlags,
                const TString &message);
    };

    using TVDiskContextPtr = TIntrusivePtr<TVDiskContext>;

} // NKikimr

// NOTES on handling PDisk responses
// Every PDisk response MUST be handled with CHECK_PDISK_RESPONSE*
// macros. In case of OK answer an actor continue execution.
// In case of INVALID_OWNER or INVALID_ROUND (i.e. BlobStorage group
// reconfiguration), an actor switches to a special state where
// it waits for TEvPoisonPill message and ignores other messages.
// We can't just kill this actor because it may be her responsibility
// to notify some other actors (children) about VDisk/component death.
#define CHECK_PDISK_RESPONSE(VCtx, ev, ctx)                             \
do {                                                                    \
    if (!(VCtx)->CheckPDiskResponse((ctx), *(ev)->Get())) {             \
        TThis::Become(&TThis::TerminateStateFunc);                      \
        return;                                                         \
    }                                                                   \
} while (false)

#define CHECK_PDISK_RESPONSE_MSG(VCtx, ev, ctx, msg)                    \
do {                                                                    \
    if (!(VCtx)->CheckPDiskResponse((ctx), *(ev)->Get(), (msg))) {      \
        TThis::Become(&TThis::TerminateStateFunc);                      \
        return;                                                         \
    }                                                                   \
} while (false)

#define PDISK_TERMINATE_STATE_FUNC_DEF                                  \
STFUNC(TerminateStateFunc) {                                            \
    switch (ev->GetTypeRewrite()) {                                     \
        HFunc(TEvents::TEvPoisonPill, HandlePoison);                    \
    }                                                                   \
}

#define CHECK_PDISK_RESPONSE_READABLE(VCtx, ev, ctx)                    \
do {                                                                    \
    CHECK_PDISK_RESPONSE(VCtx, ev, ctx);                                \
    if (!(VCtx)->CheckPDiskResponseReadable((ctx), *(ev)->Get())) {     \
        TThis::Become(&TThis::TerminateStateFunc);                      \
        return;                                                         \
    }                                                                   \
} while (false)

#define CHECK_PDISK_RESPONSE_READABLE_MSG(VCtx, ev, ctx, msg)           \
do {                                                                    \
    CHECK_PDISK_RESPONSE_MSG(VCtx, ev, ctx, msg);                       \
    if (!(VCtx)->CheckPDiskResponseReadable((ctx), *(ev)->Get(), (msg))) { \
        TThis::Become(&TThis::TerminateStateFunc);                      \
        return;                                                         \
    }                                                                   \
} while (false)
