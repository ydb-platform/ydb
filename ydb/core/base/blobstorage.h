#pragma once
#include "defs.h"

#include "blobstorage_pdisk_category.h"
#include "events.h"
#include "tablet_types.h"
#include "logoblob.h"

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/blobstorage_grouptype.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_base3.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/library/actors/wilson/wilson_trace.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/util/shared_data_rope_backend.h>

#include <util/stream/str.h>
#include <util/generic/xrange.h>

#include <optional>

namespace NWilson {
    class TSpan;
} // NWilson

namespace NKikimr {

static constexpr ui32 MaxProtobufSize = 67108000;
static constexpr ui32 MaxVDiskBlobSize = 10 << 20; // 10 megabytes
static constexpr ui64 MaxCollectGarbageFlagsPerMessage = 10000;

static constexpr TDuration VDiskCooldownTimeout = TDuration::Seconds(15);
static constexpr TDuration VDiskCooldownTimeoutOnProxy = TDuration::Seconds(12);


struct TStorageStatusFlags {
    ui32 Raw = 0;

    TStorageStatusFlags()
    {}

    TStorageStatusFlags(ui32 raw)
        : Raw(raw)
    {}

    TStorageStatusFlags(const TStorageStatusFlags&) = default;
    TStorageStatusFlags& operator =(const TStorageStatusFlags&) = default;

    friend bool operator ==(const TStorageStatusFlags& x, const TStorageStatusFlags& y) { return x.Raw == y.Raw; }
    friend bool operator !=(const TStorageStatusFlags& x, const TStorageStatusFlags& y) { return x.Raw != y.Raw; }

    void Merge(ui32 raw) {
        if (raw & ui32(NKikimrBlobStorage::StatusIsValid)) {
            Raw |= (raw & (
                ui32(NKikimrBlobStorage::StatusIsValid)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceYellowStop)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceOrange)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceRed)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceBlack)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceCyan)
                | ui32(NKikimrBlobStorage::StatusDiskSpaceLightOrange)
                | ui32(NKikimrBlobStorage::StatusDiskSpacePreOrange)));
        }
    }

    bool Check(NKikimrBlobStorage::EStatusFlags statusToCheck) const {
        return (Raw & ui32(NKikimrBlobStorage::StatusIsValid)) && (Raw & ui32(statusToCheck));
    }

    TString ToString() const {
        TStringStream str;
        Output(str);
        return str.Str();
    }

    void Output(IOutputStream &out) const {
        out << "{"
            << ((Raw & NKikimrBlobStorage::StatusIsValid) ? " Valid" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceCyan) ? " Cyan" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceLightYellowMove) ? " LightYellow" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceYellowStop) ? " Yellow" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceLightOrange) ? " LightOrange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpacePreOrange) ? " PreOrange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceOrange) ? " Orange" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceRed) ? " Red" : "")
            << ((Raw & NKikimrBlobStorage::StatusDiskSpaceBlack) ? " Black" : "")
            << " }";
    }
};

NKikimrBlobStorage::EPDiskType PDiskTypeToPDiskType(const NPDisk::EDeviceType type);

NPDisk::EDeviceType PDiskTypeToPDiskType(const NKikimrBlobStorage::EPDiskType type);

enum class EGroupConfigurationType : ui32 {
    Static = 0,
    Dynamic = 1,
    Virtual = 2,
};

struct TGroupID {
    TGroupID() = default;
    TGroupID(const TGroupID&) = default;
    TGroupID(const TGroupId wrappedId)
        : Raw(wrappedId.GetRawId()) {}

    TGroupID(EGroupConfigurationType configurationType, ui32 dataCenterId, ui32 groupLocalId) {
        Set(configurationType, dataCenterId, groupLocalId);
    }

    explicit TGroupID(ui32 raw)
        : Raw(raw)
    {}

    EGroupConfigurationType ConfigurationType() const {
        const auto type = static_cast<EGroupConfigurationType>(Raw >> TypeShift & TypeMask);
        if (type == EGroupConfigurationType::Static) {
            return type;
        } else {
            const ui32 domainId = Raw >> DomainShift & DomainMask;
            return domainId == VirtualGroupDomain
                ? EGroupConfigurationType::Virtual
                : EGroupConfigurationType::Dynamic;
        }
    }

    ui32 AvailabilityDomainID() const {
        const auto type = static_cast<EGroupConfigurationType>(Raw >> TypeShift & TypeMask);
        const ui32 domainId = Raw >> DomainShift & DomainMask;
        return type == EGroupConfigurationType::Static ? domainId :
            domainId == VirtualGroupDomain ? 1 :
            domainId;
    }

    ui32 GroupLocalID() const {
        return Raw & GroupMask;
    }

    ui32 GetRaw() const {
        return Raw;
    }

    friend bool operator ==(const TGroupID& x, const TGroupID& y) { return x.Raw == y.Raw; }
    friend bool operator !=(const TGroupID& x, const TGroupID& y) { return x.Raw != y.Raw; }

    TGroupID& operator++() {
        Set(ConfigurationType(), AvailabilityDomainID(), NextValidLocalId());
        return *this;
    }

    TGroupID operator++(int) {
        TGroupID old(*this);
        ++*this;
        return old;
    }

    TString ToString() const;

private:
    static constexpr ui32 TypeWidth = 1;
    static constexpr ui32 TypeMask = (1 << TypeWidth) - 1;
    static constexpr ui32 TypeShift = 32 - TypeWidth;

    static constexpr ui32 DomainWidth = 6;
    static constexpr ui32 DomainMask = (1 << DomainWidth) - 1;
    static constexpr ui32 DomainShift = TypeShift - DomainWidth;
    static constexpr ui32 VirtualGroupDomain = DomainMask;
    static constexpr ui32 MaxValidDomain = DomainMask - 1;

    static constexpr ui32 GroupWidth = 25;
    static constexpr ui32 GroupMask = (1 << GroupWidth) - 1;
    static constexpr ui32 InvalidLocalId = GroupMask;
    static constexpr ui32 MaxValidGroup = GroupMask - 1;

    ui32 Raw = Max<ui32>();

    void Set(EGroupConfigurationType configurationType, ui32 availabilityDomainID, ui32 groupLocalId) {
        Y_ABORT_UNLESS(groupLocalId <= MaxValidGroup);

        switch (configurationType) {
            case EGroupConfigurationType::Static:
            case EGroupConfigurationType::Dynamic:
                Y_ABORT_UNLESS(availabilityDomainID <= MaxValidDomain);
                Raw = static_cast<ui32>(configurationType) << TypeShift | availabilityDomainID << DomainShift | groupLocalId;
                break;

            case EGroupConfigurationType::Virtual:
                Y_ABORT_UNLESS(availabilityDomainID == 1);
                Raw = static_cast<ui32>(EGroupConfigurationType::Dynamic) << TypeShift | VirtualGroupDomain << DomainShift | groupLocalId;
                break;
        }
    }

    ui32 NextValidLocalId() {
        const ui32 localId = GroupLocalID();
        return localId == InvalidLocalId ? localId :
            localId == MaxValidGroup ? 0 :
            localId + 1;
    }
};

// channel info for tablet
struct TTabletChannelInfo {
    struct THistoryEntry {
        ui32 FromGeneration;
        ui32 GroupID;
        TInstant Timestamp; // for diagnostics usage only

        THistoryEntry()
            : FromGeneration(0)
            , GroupID(0)
        {}

        THistoryEntry(ui32 fromGeneration, ui32 groupId, TInstant timestamp = TInstant()) // groupId could be zero
            : FromGeneration(fromGeneration)
            , GroupID(groupId)
            , Timestamp(timestamp)
        {}

        struct TCmp {
            bool operator()(ui32 gen, const THistoryEntry &x) const {
                return gen < x.FromGeneration;
            }
        };

        TString ToString() const {
            TStringStream str;
            str << "{FromGeneration# " << FromGeneration;
            str << " GroupID# " << GroupID;
            str << " Timestamp# " << Timestamp.ToString();
            str << "}";
            return str.Str();
        }

        bool operator ==(const THistoryEntry& other) const {
            return FromGeneration == other.FromGeneration
                    && (GroupID == other.GroupID || GroupID == 0 || other.GroupID == 0);
        }
    };

    ui32 Channel;
    TBlobStorageGroupType Type;
    TString StoragePool;
    TVector<THistoryEntry> History;

    TTabletChannelInfo()
        : Channel()
        , Type()
    {}

    TTabletChannelInfo(ui32 channel, TBlobStorageGroupType type)
        : Channel(channel)
        , Type(type)
    {}

    TTabletChannelInfo(ui32 channel, TBlobStorageGroupType::EErasureSpecies erasureSpecies)
        : Channel(channel)
        , Type(erasureSpecies)
    {}

    TTabletChannelInfo(ui32 channel, TString storagePool)
        : Channel(channel)
        , Type(TBlobStorageGroupType::ErasureNone)
        , StoragePool(storagePool)
    {}

    ui32 GroupForGeneration(ui32 gen) const {
        const size_t historySize = History.size();
        Y_ABORT_UNLESS(historySize > 0, "empty channel history");

        const THistoryEntry * const first = &*History.begin();
        if (historySize == 1) {
            if (first->FromGeneration <= gen)
                return first->GroupID;
            return Max<ui32>();
        }

        const THistoryEntry * const end = first + historySize;
        const THistoryEntry * const last = end - 1;
        if (last->FromGeneration <= gen) {
            return last->GroupID;
        }

        const THistoryEntry *x = UpperBound(first, end, gen, THistoryEntry::TCmp());
        if (x != first) {
            return (x - 1)->GroupID;
        }

        return Max<ui32>();
    }

    const THistoryEntry* LatestEntry() const {
        if (!History.empty())
            return &History.back();
        else
            return nullptr;
    }

    const THistoryEntry* PreviousEntry() const {
        if (History.size() > 1)
            return &*(History.rbegin() + 1);
        else
            return nullptr;
    }

    TString ToString() const {
        TStringStream str;
        str << "{Channel# " << Channel;
        str << " Type# " << Type.ToString();
        str << " StoragePool# " << StoragePool;
        str << " History# {";
        const size_t historySize = History.size();
        for (size_t historyIdx = 0; historyIdx < historySize; ++historyIdx) {
            if (historyIdx != 0) {
                str <<", ";
            }
            str << historyIdx << ":" << History[historyIdx].ToString();
        }
        str << "}";
        return str.Str();
    }
};

class TTabletStorageInfo : public TThrRefBase {
public:
    //
    TTabletStorageInfo()
        : TabletID(Max<ui64>())
        , TabletType(TTabletTypes::TypeInvalid)
        , Version(0)
    {}
    TTabletStorageInfo(ui64 tabletId, TTabletTypes::EType tabletType)
        : TabletID(tabletId)
        , TabletType(tabletType)
        , Version(0)
    {}
    virtual ~TTabletStorageInfo() {}

    const TTabletChannelInfo* ChannelInfo(ui32 channel) const {
        if (Channels.size() <= channel) {
            return nullptr;
        }
        const TTabletChannelInfo &info = Channels[channel];
        if (info.History.empty()) {
            return nullptr;
        }
        return &info;
    }

    ui32 GroupFor(ui32 channel, ui32 recordGen) const {
        if (const TTabletChannelInfo *channelInfo = ChannelInfo(channel))
            return channelInfo->GroupForGeneration(recordGen);
        else
            return Max<ui32>();
    }

    ui32 GroupFor(const TLogoBlobID& id) const {
        return GroupFor(id.Channel(), id.Generation());
    }

    TString ToString() const {
        TStringStream str;
        str << "{Version# " << Version;
        str << " TabletID# " << TabletID;
        str << " TabletType# " << TabletType;
        str << " Channels# {";
        const size_t channelsSize = Channels.size();
        for (size_t channelIdx = 0; channelIdx < channelsSize; ++channelIdx) {
            if (channelIdx != 0) {
                str <<", ";
            }
            str << channelIdx << ":" << Channels[channelIdx].ToString();
        }
        str << "}";
        if (TenantPathId)
            str << " Tenant: " << TenantPathId;
        return str.Str();
    }

    TActorId BSProxyIDForChannel(ui32 channel, ui32 generation) const;

    bool operator<(const TTabletStorageInfo &other) const noexcept {
        if (Version != 0 && other.Version != 0) {
            return Version < other.Version;
        }
        const size_t selfSize = Channels.size();
        const size_t otherSize = other.Channels.size();
        if (selfSize != otherSize)
            return (selfSize < otherSize);

        for (ui64 channelIdx : xrange(selfSize)) {
            const ui32 lastInSelf = Channels[channelIdx].History.back().FromGeneration;
            const ui32 lastInOther = other.Channels[channelIdx].History.back().FromGeneration;
            if (lastInSelf != lastInOther)
                return (lastInSelf < lastInOther);
        }

        return false;
    }

    //
    ui64 TabletID;
    TVector<TTabletChannelInfo> Channels;
    TTabletTypes::EType TabletType;
    ui32 Version;
    TPathId TenantPathId;
    ui64 HiveId = 0;
};

inline TActorId TTabletStorageInfo::BSProxyIDForChannel(ui32 channel, ui32 generation) const {
    const ui32 group = GroupFor(channel, generation);
    Y_ABORT_UNLESS(group != Max<ui32>());
    const TActorId proxy = MakeBlobStorageProxyID(group);
    return proxy;
}

inline ui32 GroupIDFromBlobStorageProxyID(TActorId actorId) {
    ui32 blobStorageGroup = ui32(
        ((actorId.RawX1() >> (7 * 8)) & 0xff) |
        (((actorId.RawX2() >> (0 * 8)) & 0xff) << 8) |
        (((actorId.RawX2() >> (1 * 8)) & 0xff) << 16) |
        (((actorId.RawX2() >> (2 * 8)) & 0xff) << 24));
    Y_ABORT_UNLESS(MakeBlobStorageProxyID(blobStorageGroup) == actorId);
    return blobStorageGroup;
}

inline IEventHandle *CreateEventForBSProxy(TActorId sender, TActorId recipient, IEventBase *ev, ui64 cookie,
        NWilson::TTraceId traceId = {}) {
    std::unique_ptr<IEventBase> ptr(ev);
    const ui32 flags = NActors::IEventHandle::FlagTrackDelivery | NActors::IEventHandle::FlagForwardOnNondelivery;
    const TActorId nw = MakeBlobStorageNodeWardenID(sender.NodeId());
    auto *res = new IEventHandle(recipient, sender, ptr.get(), flags, cookie, &nw, std::move(traceId));
    ptr.release();
    return res;
}

inline IEventHandle *CreateEventForBSProxy(TActorId sender, ui32 groupId, IEventBase *ev, ui64 cookie, NWilson::TTraceId traceId = {}) {
    return CreateEventForBSProxy(sender, MakeBlobStorageProxyID(groupId), ev, cookie, std::move(traceId));
}

inline IEventHandle *CreateEventForBSProxy(TActorId sender, TGroupId groupId, IEventBase *ev, ui64 cookie, NWilson::TTraceId traceId = {}) {
    return CreateEventForBSProxy(sender, MakeBlobStorageProxyID(groupId), ev, cookie, std::move(traceId));
}

inline bool SendToBSProxy(TActorId sender, TActorId recipient, IEventBase *ev, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
    return TActivationContext::Send(CreateEventForBSProxy(sender, recipient, ev, cookie, std::move(traceId)));
}

inline bool SendToBSProxy(const TActorContext &ctx, TActorId recipient, IEventBase *ev, ui64 cookie = 0,
        NWilson::TTraceId traceId = {}) {
    return ctx.Send(CreateEventForBSProxy(ctx.SelfID, recipient, ev, cookie, std::move(traceId)));
}

inline bool SendToBSProxy(TActorId sender, ui32 groupId, IEventBase *ev, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
    return TActivationContext::Send(CreateEventForBSProxy(sender, groupId, ev, cookie, std::move(traceId)));
}

inline bool SendToBSProxy(const TActorContext &ctx, ui32 groupId, IEventBase *ev, ui64 cookie = 0,
        NWilson::TTraceId traceId = {}) {
    return ctx.Send(CreateEventForBSProxy(ctx.SelfID, groupId, ev, cookie, std::move(traceId)));
}

inline bool SendToBSProxy(TActorId sender, TGroupId groupId, IEventBase *ev, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
    return TActivationContext::Send(CreateEventForBSProxy(sender, groupId, ev, cookie, std::move(traceId)));
}

inline bool SendToBSProxy(const TActorContext &ctx, TGroupId groupId, IEventBase *ev, ui64 cookie = 0,
        NWilson::TTraceId traceId = {}) {
    return ctx.Send(CreateEventForBSProxy(ctx.SelfID, groupId, ev, cookie, std::move(traceId)));
}

struct TEvBlobStorage {
    enum EEv {
        // user <-> proxy interface
        EvPut = EventSpaceBegin(TKikimrEvents::ES_BLOBSTORAGE), /// 268 632 064
        EvGet,
        EvBlock,
        EvDiscover,
        EvRange,
        EvProbe,
        EvCollectGarbage,
        EvStatus,
        EvVBaldSyncLog,
        EvPatch,
        EvInplacePatch,
        EvAssimilate,

        EvGetQueuesInfo,     // for debugging purposes

        //
        EvPutResult = EvPut + 512,                              /// 268 632 576
        EvGetResult,
        EvBlockResult,
        EvDiscoverResult,
        EvRangeResult,
        EvProbeResult,
        EvCollectGarbageResult,
        EvStatusResult,
        EvVBaldSyncLogResult,
        EvPatchResult,
        EvInplacePatchResult,
        EvAssimilateResult,

        EvQueuesInfo,  // for debugging purposes

        // proxy <-> vdisk interface
        EvVPut = EvPut + 2 * 512,                               /// 268 633 088
        EvVGet,
        EvVBlock,
        EvVGetBlock,
        EvVCollectGarbage,
        EvVGetBarrier,
        EvVReadyNotify,
        EvVStatus,
        EvVDbStat,
        EvVCheckReadiness,
        EvVCompact,                                             /// 268 633 098
        EvVMultiPut,
        EvVMovedPatch,
        EvVPatchStart,
        EvVPatchDiff,
        EvVPatchXorDiff,
        EvVDefrag,
        EvVInplacePatch,
        EvVAssimilate,
        EvVTakeSnapshot,
        EvVReleaseSnapshot,

        EvVPutResult = EvPut + 3 * 512,                         /// 268 633 600
        EvVGetResult,
        EvVBlockResult,
        EvVGetBlockResult,
        EvVCollectGarbageResult,
        EvVGetBarrierResult,
        EvVStatusResult,
        EvVDbStatResult,
        EvVWindowChange,
        EvVCheckReadinessResult,
        EvVCompactResult,
        EvVMultiPutResult,
        EvVMovedPatchResult,
        EvVPatchFoundParts,
        EvVPatchXorDiffResult,
        EvVPatchResult,
        EvVDefragResult,
        EvVInplacePatchResult,
        EvVAssimilateResult,
        EvVTakeSnapshotResult,
        EvVReleaseSnapshotResult,

        // vdisk <-> vdisk interface
        EvVDisk = EvPut + 4 * 512,                              /// 268 634 112
        EvVSync,
        EvVSyncFull,
        EvVSyncGuid,

        EvVDiskReply = EvPut + 5 * 512,                         /// 268 634 624
        EvVSyncResult,
        EvVSyncFullResult,
        EvVSyncGuidResult,

        // vdisk <-> controller interface,
        EvCnt = EvPut + 6 * 512,                                /// 268 635 136
        EvVGenerationChange,
        EvRegisterPDiskLoadActor,
        EvStatusUpdate,
        EvDropDonor,
        EvPutVDiskToReadOnly,

        EvCntReply = EvPut + 7 * 512,                           /// 268 635 648
        EvVGenerationChangeResult,
        EvRegisterPDiskLoadActorResult,

        // internal vdisk interface
        EvYardInit = EvPut + 8 * 512,                           /// 268 636 160
        EvLog,
        EvReadLog,
        EvChunkRead,
        EvChunkWrite,
        EvHarakiri,
        EvCheckSpace,
        EvConfigureScheduler,
        EvYardControl,
        EvCutLog,
        EvIPDiskGet,                                            /// 268 636 170
        EvSyncLogPut,
        EvSyncLogRead,
        EvSyncLogTrim,
        EvSyncLogFreeChunk,
        EvSyncLogCommitDone,
        EvSyncLogSnapshot,
        EvSyncLogReadFinished,
        EvSyncLogPutSst,
        EvChunkReserve,
        EvSkeletonBackSyncLogID,                                /// 268 636 180
        EvHullConfirmedLsn,
        EvHullChange,
        EvHullSegLoaded,
        EvHullSegmentsLoaded,
        EvHullIndexLoaded,
        EvHullFreeSlice,
        EvHullAdvanceLsn,
        EvHullCommitFinished,
        EvHullWriteHugeBlob,
        EvHullLogHugeBlob,                                      /// 268 636 190
        EvHullHugeBlobLogged,
        EvHullFreeHugeSlots,
        EvHullHugeChunkAllocated,
        EvHullHugeChunkFreed,
        EvHullHugeCommitted,
        EvHullHugeWritten,
        EvHullDelayedResult,
        EvHullCompSelected,
        EvHullReleaseSnapshot,
        EvSyncJobDone,                                          /// 268 636 200
        EvLocalSyncData,
        EvVDiskCutLog,
        EvRunRepl,
        EvRecoveredHugeBlob,
        EvDetectedPhantomBlob,
        EvAddBulkSst,
        EvReplProxyNext,
        EvSyncLogGetLastLsn,
        EvLocalStatus,
        EvLocalHandoff,                                         /// 268 636 210
        EvHandoffProxyMon,
        EvHandoffSyncLogDel,
        EvHandoffSyncLogFinished,
        EvVDiskRequestCompleted,
        EvFrontRecoveryStatus,
        EvPruneQueue,
        EvPDiskFairSchedulerWake,
        EvVDiskGuidObtained,
        EvCompactionFinished,
        EvKickEmergencyPutQueue,                                /// 268 636 220
        EvWakeupEmergencyPutQueue,
        EvTimeToUpdateWhiteboard,
        EvBulkSstsLoaded,
        EvVDiskGuidWritten,
        EvSyncerCommit,
        EvSyncerCommitDone,
        EvVDiskGuidRecovered,
        EvQueryReplToken,
        EvReplToken,
        EvReleaseReplToken,                                     /// 268 636 230
        OBSOLETE_EvQueryReplDataToken,
        OBSOLETE_EvReplDataToken,
        EvQueryReplMemToken,
        EvReplMemToken,
        EvUpdateReplMemToken,
        EvReleaseReplMemToken,
        EvSyncerCommitProxyDone,
        EvSyncerNeedFullRecovery,
        EvThroughputUpdate,
        EvThroughputAddRequest,                                 /// 268 636 240
        EvSyncerLostDataRecovered,
        EvSyncerGuidFirstRunDone,
        EvSyncerFullSyncedWithPeer,
        EvSyncerRLDWakeup,
        EvSlay,
        EvCallOsiris,
        EvAnubisOsirisPut,
        EvAnubisOsirisPutResult,
        EvSyncLogDbBirthLsn,
        EvSublogLine,                                           // 268 636 250
        EvDelayedRead,
        EvFullSyncedWith,
        EvAnubisDone,
        EvTakeHullSnapshot,
        EvTakeHullSnapshotResult,
        EvAnubisQuantumDone,
        EvAnubisCandidates,
        EvAnubisVGet,
        EvChunkLock,
        EvChunkUnlock,                                         // 268 636 260
        EvWhiteboardReportResult,
        EvHttpInfoResult,
        EvReadLogContinue,
        EvLogSectorRestore,
        EvLogInitResult,
        EvAskForCutLog,
        EvDelLogoBlobDataSyncLog,
        EvPDiskFormattingFinished,
        EvRecoveryLogReplayDone,
        EvMonStreamQuery,                                       // 268 636 270
        EvMonStreamActorDeathNote,
        EvPDiskErrorStateChange,
        EvMultiLog,
        EvVMultiPutItemResult,
        EvEnrichNotYet,
        EvCommenceRepl, // for debugging purposes
        EvRecoverBlob,
        EvRecoverBlobResult,
        EvScrubAwait, // for debugging purposes
        EvScrubNotify,                                          // 268 636 280
        EvDefragQuantumResult,
        EvDefragStartQuantum,
        EvReportScrubStatus,
        EvRestoreCorruptedBlob,
        EvRestoreCorruptedBlobResult,
        EvCaptureVDiskLayout, // for debugging purposes
        EvCaptureVDiskLayoutResult,
        OBSOLETE_EvTriggerCompaction,
        OBSOLETE_EvTriggerCompactionResult,
        EvHullCompact,                                          // 268 636 290
        EvHullCompactResult,
        EvCompactVDisk,
        EvCompactVDiskResult,
        EvDefragRewritten,
        EvVPatchDyingRequest,
        EvVPatchDyingConfirm,
        EvNonrestoredCorruptedBlobNotify,
        EvHugeLockChunks,
        EvHugeStat,
        EvForwardToSkeleton,                                    // 268 636 300
        EvHugeUnlockChunks,
        EvVDiskStatRequest,
        EvGetLogoBlobRequest,
        EvChunkForget,
        EvFormatReencryptionFinish,
        EvDetectedPhantomBlobCommitted,
        EvGetLogoBlobIndexStatRequest,
        EvReadMetadata,
        EvWriteMetadata,
        EvPermitGarbageCollection,
        EvReplInvoke,
        EvStartBalancing,
        EvReplCheckProgress,
        EvMinHugeBlobSizeUpdate,
        EvHugePreCompact,
        EvHugePreCompactResult,
        EvPDiskMetadataLoaded,

        EvYardInitResult = EvPut + 9 * 512,                     /// 268 636 672
        EvLogResult,
        EvReadLogResult,
        EvChunkReadResult,
        EvChunkWriteResult,
        EvHarakiriResult,
        EvCheckSpaceResult,
        EvConfigureSchedulerResult,
        EvYardControlResult,
        EvIPDiskGetResult,
        EvSyncLogSnapshotResult,                                /// 268 636 682
        EvLocalRecoveryDone,
        EvChunkReserveResult,
        EvLocalSyncDataResult,
        EvReadFormatResult,
        EvReplStarted,
        EvReplFinished,
        EvReplPlanFinished,
        EvReplProxyNextResult,
        EvSyncLogGetLastLsnResult,
        EvLocalStatusResult,                                    /// 268 636 692
        EvHandoffProxyMonResult,
        EvSyncGuidRecoveryDone,
        EvSlayResult,
        EvOsirisDone,
        EvSyncLogWriteDone,
        EvAnubisVGetResult,
        EvChunkLockResult,
        EvChunkUnlockResult,
        EvDelLogoBlobDataSyncLogResult,
        EvAddBulkSstResult,                                     /// 268 636 702
        EvAddBulkSstCommitted,
        EvBulkSstEssenceLoaded,
        EvCommitLogChunks,
        EvLogCommitDone,
        EvSyncLogLocalStatus,
        EvSyncLogLocalStatusResult,
        EvReplResume,
        EvReplDone,
        EvFreshAppendixCompactionDone,
        EvDeviceError,
        EvHugeLockChunksResult,
        EvHugeStatResult,
        EvVDiskStatResponse,
        EvGetLogoBlobResponse,
        EvChunkForgetResult,
        EvGetLogoBlobIndexStatResponse,
        EvReadMetadataResult,
        EvWriteMetadataResult,

        // internal proxy interface
        EvUnusedLocal1 = EvPut + 10 * 512, // Not used.    /// 268 637 184
        EvUnusedLocal2,                    // Not used.
        EvUnusedLocal3,                    // Not used.
        EvNotReadyRetryTimeout,
        EvConfigureQueryTimeout,
        EvEstablishingSessionTimeout,
        EvDeathNote,                       /// 268 637 191
        EvVDiskStateChanged,
        EvAccelerate,
        EvUnusedLocal4,                    /// 268 637 194
        EvProxyQueueState,
        EvAbortOperation,
        EvResume,
        EvTimeStats,
        EvOverseerRequest,                 // Not used
        EvOverseerLogLastLsn,              // Not used
        EvOverseerConfirm,                 // Not used
        EvLatencyReport,
        EvGroupStatReport,
        EvAccelerateGet,
        EvAcceleratePut,
        EvRequestProxyQueueState,
        EvRequestProxySessionsState,
        EvProxySessionsState,
        EvBunchOfEvents,
        EvDeadline,

        // blobstorage controller interface
        EvControllerRegisterNode                    = 0x10031602,
        EvControllerSelectGroups                    = 0x10031606,
        EvControllerGetGroup                        = 0x10031607,
        EvControllerUpdateDiskStatus                = 0x10031608,
        EvControllerConfigRequest                   = 0x1003160a,
        EvControllerConfigResponse                  = 0x1003160b,
        EvControllerProposeGroupKey                 = 0x10031614,
        EvControllerUpdateGroupStat                 = 0x10031616,
        EvControllerNotifyGroupChange               = 0x10031617,
        EvControllerCommitGroupLatencies            = 0x10031618,
        EvControllerUpdateSelfHealInfo              = 0x10031619,
        EvControllerScrubQueryStartQuantum          = 0x1003161a,
        EvControllerScrubQuantumFinished            = 0x1003161b,
        EvControllerScrubReportQuantumInProgress    = 0x1003161c,
        EvControllerUpdateNodeDrives                = 0x1003161d,
        EvControllerGroupDecommittedNotify          = 0x1003161e,
        EvControllerGroupDecommittedResponse        = 0x1003161f,
        EvControllerGroupMetricsExchange            = 0x10031620,

        // BSC interface result section
        EvControllerNodeServiceSetUpdate            = 0x10031802,
        EvControllerSelectGroupsResult              = 0x10031806,
        EvRequestControllerInfo                     = 0x10031807,
        EvResponseControllerInfo                    = 0x10031808,
        EvControllerNodeReport                      = 0x1003180d,
        EvControllerScrubStartQuantum               = 0x1003180e,
        EvControllerUpdateSystemViews               = 0x10031815,

        // proxy - node controller interface
        EvConfigureProxy = EvPut + 13 * 512,
        EvProxyConfigurationRequest, // DEPRECATED
        EvUpdateGroupInfo,
        EvNotifyVDiskGenerationChange, // DEPRECATED

        // node controller internal messages
        EvRegisterNodeRetry = EvPut + 14 * 512,
        EvAskWardenRestartPDisk,
        EvAskWardenRestartPDiskResult,
        EvNotifyWardenPDiskRestarted,
        EvNodeWardenQueryGroupInfo,
        EvNodeWardenGroupInfo,
        EvNodeConfigPush,
        EvNodeConfigReversePush,
        EvNodeConfigUnbind,
        EvNodeConfigScatter,
        EvNodeConfigGather,
        EvNodeWardenQueryStorageConfig,
        EvNodeWardenStorageConfig,
        EvAskRestartVDisk,
        EvNodeConfigInvokeOnRoot,
        EvNodeConfigInvokeOnRootResult,
        EvNodeWardenStorageConfigConfirm,
        EvNodeWardenQueryBaseConfig,
        EvNodeWardenBaseConfig,
        EvNodeWardenDynamicConfigSubscribe,
        EvNodeWardenDynamicConfigPush,
        EvNodeWardenReadMetadata,
        EvNodeWardenReadMetadataResult,
        EvNodeWardenWriteMetadata,
        EvNodeWardenWriteMetadataResult,

        // Other
        EvRunActor = EvPut + 15 * 512,
        EvVMockCtlRequest,
        EvVMockCtlResponse,
        EvDelayedMessageWrapper,

        // incremental huge blob keeper
        EvIncrHugeInit = EvPut + 17 * 512,
        EvIncrHugeInitResult,
        EvIncrHugeWrite,
        EvIncrHugeWriteResult,
        EvIncrHugeRead,
        EvIncrHugeReadResult,
        EvIncrHugeDelete,
        EvIncrHugeDeleteResult,
        EvIncrHugeHarakiri,
        EvIncrHugeHarakiriResult,
        EvIncrHugeCallback,
        EvIncrHugeControlDefrag,

        EvIncrHugeReadLogResult,
        EvIncrHugeScanResult,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOBSTORAGE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOBSTORAGE)");

    struct TExecutionRelay {};

    struct TEvPutResult;
    struct TEvGetResult;
    struct TEvBlockResult;
    struct TEvDiscoverResult;
    struct TEvRangeResult;
    struct TEvCollectGarbageResult;
    struct TEvStatusResult;
    struct TEvPatchResult;
    struct TEvInplacePatchResult;
    struct TEvAssimilateResult;

    struct TEvPut : public TEventLocal<TEvPut, EvPut> {
        enum ETactic {
            TacticMaxThroughput = 0,
            TacticMinLatency,
            TacticDefault, // Default depends on the erasure type
            TacticCount // This is not a tactic, but a number of tactics. Add new tactics before this line.
        };
        static const char* TacticName(ETactic tactic) {
            switch (tactic) {
                case TacticMaxThroughput:
                    return "MaxThroughput";
                case TacticMinLatency:
                    return "MinLatency";
                case TacticDefault:
                    return "Default";
                default:
                    return "unknown";
            }
        };
        const TLogoBlobID Id;
        const TRcBuf Buffer; //FIXME(innokentii) const members prevent usage of move-semantics elsewhere
        const TInstant Deadline;
        const NKikimrBlobStorage::EPutHandleClass HandleClass;
        const ETactic Tactic;
        mutable NLWTrace::TOrbit Orbit;
        ui32 RestartCounter = 0;
        std::vector<std::pair<ui64, ui32>> ExtraBlockChecks; // (TabletId, Generation) pairs
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvPut(const TLogoBlobID &id, TRcBuf &&buffer, TInstant deadline,
               NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog,
               ETactic tactic = TacticDefault)
            : Id(id)
            , Buffer(std::move(buffer))
            , Deadline(deadline)
            , HandleClass(handleClass)
            , Tactic(tactic)
        {
            Y_ABORT_UNLESS(Id, "EvPut invalid: LogoBlobId must have non-zero tablet field, id# %s", Id.ToString().c_str());
            Y_ABORT_UNLESS(Buffer.size() < (40 * 1024 * 1024),
                   "EvPut invalid: LogoBlobId# %s buffer.Size# %zu",
                   id.ToString().data(), Buffer.size());
            Y_ABORT_UNLESS(Buffer.size() == id.BlobSize(),
                   "EvPut invalid: LogoBlobId# %s buffer.Size# %zu",
                   id.ToString().data(), Buffer.size());
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&id, sizeof(id));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(Buffer.GetContiguousSpan().Data(), Buffer.size());
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&deadline, sizeof(deadline));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&handleClass, sizeof(handleClass));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&tactic, sizeof(tactic));
        }

        TEvPut(const TLogoBlobID &id, const TString &buffer, TInstant deadline,
               NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog,
               ETactic tactic = TacticDefault)
            : TEvPut(id, TRcBuf(buffer), deadline, handleClass, tactic)
        {}


        TEvPut(const TLogoBlobID &id, const TSharedData &buffer, TInstant deadline,
               NKikimrBlobStorage::EPutHandleClass handleClass = NKikimrBlobStorage::TabletLog,
               ETactic tactic = TacticDefault)
            : TEvPut(id, TRcBuf(buffer), deadline, handleClass, tactic)
        {}

        TString Print(bool isFull) const {
            TStringStream str;
            str << "TEvPut {Id# " << Id.ToString();
            str << " Size# " << Buffer.GetSize();
            if (isFull) {
                str << " Buffer# " << Buffer.ExtractUnderlyingContainerOrCopy<TString>().Quote();
            }
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " HandleClass# " << HandleClass;
            str << " Tactic# " << TacticName(Tactic);
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this) + Buffer.GetSize();
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvPutResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvPutResult : public TEventLocal<TEvPutResult, EvPutResult> {
        NKikimrProto::EReplyStatus Status;
        const TLogoBlobID Id;
        const TStorageStatusFlags StatusFlags;
        const ui32 GroupId;
        const float ApproximateFreeSpaceShare; // 0.f has special meaning 'data could not be obtained'
        TString ErrorReason;
        bool WrittenBeyondBarrier = false; // was this blob written beyond the barrier?
        mutable NLWTrace::TOrbit Orbit;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;
        const TString StorageId;

        TEvPutResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &id, const TStorageStatusFlags statusFlags,
                TGroupId groupId, float approximateFreeSpaceShare, const TString& storageId = Default<TString>())
            : Status(status)
            , Id(id)
            , StatusFlags(statusFlags)
            , GroupId(groupId.GetRawId())
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
            , StorageId(storageId)
        {}

        TEvPutResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &id, const TStorageStatusFlags statusFlags,
                ui32 groupId, float approximateFreeSpaceShare, const TString& storageId = Default<TString>())
            : Status(status)
            , Id(id)
            , StatusFlags(statusFlags)
            , GroupId(groupId)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
            , StorageId(storageId)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvPutResult {Id# " << Id.ToString();
            str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " StatusFlags# " << StatusFlags;
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvGet : public TEventLocal<TEvGet, EvGet> {
        struct TQuery {
            TLogoBlobID Id;
            ui32 Shift;
            ui32 Size;

            TQuery()
                : Shift(0)
                , Size(0)
            {}

            void Set(const TLogoBlobID &id, ui32 sh = 0, ui32 sz = 0) {
                Id = id;
                Shift = sh;
                Size = sz;

                Y_ABORT_UNLESS(id.BlobSize() > 0, "Please, don't read/write 0-byte blobs!");
                Y_ABORT_UNLESS(sh < id.BlobSize(),
                    "Please, don't read behind the end of the blob! BlobSize# %" PRIu32 " sh# %" PRIu32,
                    (ui32)id.BlobSize(), (ui32)sh);
            }

            TString ToString() const {
                TStringStream str;
                str << "TQuery {Id# " << Id.ToString();
                str << " Shift# " << Shift;
                str << " Size# " << Size;
                str << "}";
                return str.Str();
            }
        };

        // todo: replace with queue-like thing
        const ui32 QuerySize;
        TArrayHolder<TQuery> Queries;
        TInstant Deadline;
        bool MustRestoreFirst;
        NKikimrBlobStorage::EGetHandleClass GetHandleClass;
        mutable NLWTrace::TOrbit Orbit;
        ui64 TabletId = 0; // set to non-zero tablet id to get the blocked generation
        bool AcquireBlockedGeneration = false; // get the blocked generation along with the blobs
        bool IsIndexOnly;
        bool IsVerboseNoDataEnabled; // Debug use only
        bool IsInternal = false; // set to true if generated by ds proxy
        bool CollectDebugInfo = false; // collect query debug info and return in response
        bool ReportDetailedPartMap = false;
        ui32 RestartCounter = 0;
        bool PhantomCheck = false;
        bool Decommission = false; // is it generated by decommission actor and should be handled by the underlying proxy?
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        struct TTabletData {
            TTabletData() = default;
            TTabletData(ui64 id, ui32 generation) : Id(id), Generation(generation) {}

            ui64 Id = 0;            // tablet id
            ui32 Generation = 0;    // tablet generation
        };

        using TForceBlockTabletData = TTabletData;
        using TReaderTabletData = TTabletData;

        std::optional<TReaderTabletData> ReaderTabletData;
        std::optional<TForceBlockTabletData> ForceBlockTabletData;

        // NKikimrBlobStorage::EGetHandleClass::FastRead

        TEvGet(TArrayHolder<TQuery> &q, ui32 sz, TInstant deadline, NKikimrBlobStorage::EGetHandleClass getHandleClass,
                bool mustRestoreFirst = false, bool isIndexOnly = false, std::optional<TForceBlockTabletData> forceBlockTabletData = {},
                bool isInternal = false, bool isVerboseNoDataEnabled = false, bool collectDebugInfo = false,
                bool reportDetailedPartMap = false)
            : QuerySize(sz)
            , Queries(q.Release())
            , Deadline(deadline)
            , MustRestoreFirst(mustRestoreFirst)
            , GetHandleClass(getHandleClass)
            , IsIndexOnly(isIndexOnly)
            , IsVerboseNoDataEnabled(isVerboseNoDataEnabled)
            , IsInternal(isInternal)
            , CollectDebugInfo(collectDebugInfo)
            , ReportDetailedPartMap(reportDetailedPartMap)
            , ForceBlockTabletData(forceBlockTabletData)
        {
            Y_ABORT_UNLESS(QuerySize > 0, "can't execute empty get queries");
            VerifySameTabletId();
        }

        TEvGet(const TLogoBlobID &id, ui32 shift, ui32 size, TInstant deadline,
                NKikimrBlobStorage::EGetHandleClass getHandleClass,
                bool mustRestoreFirst = false, bool isIndexOnly = false,
                std::optional<TForceBlockTabletData> forceBlockTabletData = {})
            : QuerySize(1)
            , Queries(new TQuery[1])
            , Deadline(deadline)
            , MustRestoreFirst(mustRestoreFirst)
            , GetHandleClass(getHandleClass)
            , IsIndexOnly(isIndexOnly)
            , IsVerboseNoDataEnabled(false)
            , ForceBlockTabletData(forceBlockTabletData)
        {
            Queries[0].Id = id;
            Queries[0].Shift = shift;
            Queries[0].Size = size;
            Y_ABORT_UNLESS(id.BlobSize() > 0, "Please, don't read/write 0-byte blobs!");
            Y_ABORT_UNLESS(shift < id.BlobSize(),
                    "Please, don't read behind the end of the blob! Id# %s BlobSize# %" PRIu32 " shift# %" PRIu32,
                    id.ToString().c_str(), (ui32)id.BlobSize(), (ui32)shift);
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvGet {MustRestoreFirst# " << (MustRestoreFirst ? "true" : "false");
            str << " GetHandleClass# " << NKikimrBlobStorage::EGetHandleClass_Name(GetHandleClass);
            str << " IsVerboseNoDataEnabled# " << (IsVerboseNoDataEnabled ? "true" : "false");
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " QuerySize# " << QuerySize;
            for (ui32 i = 0; i < QuerySize; ++i) {
                TQuery &query = Queries[i];
                str << " {Id# " << query.Id.ToString();
                if (query.Shift) {
                    str << " Shift# " << query.Shift;
                }
                if (query.Size) {
                    str << " Size# " << query.Size;
                }
                str << "}";
            }
            if (ForceBlockTabletData) {
                str << " ForceBlockTabletId# : " << ForceBlockTabletData->Id;
                str << " ForceBlockTabletGeneration# : " << ForceBlockTabletData->Generation;
            }
            if (ReaderTabletData) {
                str << " ReaderTabletId# " << ReaderTabletData->Id;
                str << " ReaderTabletGeneration# " << ReaderTabletData->Generation;
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this) + QuerySize * sizeof(TQuery);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvGetResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);

    private:
        void VerifySameTabletId() const {
            for (ui32 i = 1; i < QuerySize; ++i) {
                Y_ABORT_UNLESS(Queries[i].Id.TabletID() == Queries[0].Id.TabletID(),
                        "Trying to request blobs for different tablets in one request: %" PRIu64 ", %" PRIu64,
                        Queries[0].Id.TabletID(), Queries[i].Id.TabletID());
            }
        }
    };

    struct TEvGetResult : public TEventLocal<TEvGetResult, EvGetResult> {
        struct TPartMapItem {
            ui32 DiskOrderNumber;
            ui32 PartIdRequested;
            ui32 RequestIndex;
            ui32 ResponseIndex;
            TVector<std::pair<ui32, NKikimrProto::EReplyStatus>> Status;
        };
        struct TResponse {
            NKikimrProto::EReplyStatus Status;

            TLogoBlobID Id;
            ui32 Shift;
            ui32 RequestedSize;
            TRope Buffer;
            TVector<TPartMapItem> PartMap;
            bool Keep = false;
            bool DoNotKeep = false;
            std::optional<bool> LooksLikePhantom; // filled only when PhantomCheck is true

            TResponse()
                : Status(NKikimrProto::UNKNOWN)
                , Shift(0)
                , RequestedSize(0)
            {}
        };

        NKikimrProto::EReplyStatus Status;

        // todo: replace with queue-like thing
        ui32 ResponseSz;
        TArrayHolder<TResponse> Responses;
        const ui32 GroupId;
        ui32 BlockedGeneration = 0; // valid only for requests with non-zero TabletId and true AcquireBlockedGeneration.
        TString DebugInfo;
        TString ErrorReason;
        mutable NLWTrace::TOrbit Orbit;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        // to measure blobstorage->client hop
        TInstant Sent;

        TEvGetResult(NKikimrProto::EReplyStatus status, ui32 sz, TGroupId groupId)
            : Status(status)
            , ResponseSz(sz)
            , Responses(sz == 0 ? nullptr : new TResponse[sz])
            , GroupId(groupId.GetRawId())
        {}

        TEvGetResult(NKikimrProto::EReplyStatus status, ui32 sz, ui32 groupId)
            : Status(status)
            , ResponseSz(sz)
            , Responses(sz == 0 ? nullptr : new TResponse[sz])
            , GroupId(groupId)
        {}

        TString Print(bool isFull) const {
            TStringStream str;
            str << "TEvGetResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " ResponseSz# " << ResponseSz;
            for (ui32 i = 0; i < ResponseSz; ++i) {
                TResponse &response = Responses[i];
                str << " {" << response.Id.ToString();
                str << " " << NKikimrProto::EReplyStatus_Name(response.Status).data();
                if (response.Shift) {
                    str << " Shift# " << response.Shift;
                }
                str << " Size# " << response.Buffer.size();
                if (response.RequestedSize) {
                    str << " RequestedSize# " << response.RequestedSize;
                }
                if (isFull) {
                    str << " Buffer# " << response.Buffer.ConvertToString().Quote();
                }
                str << "}";
                if (ErrorReason.size()) {
                    str << " ErrorReason# \"" << ErrorReason << "\"";
                }
            }
            if (BlockedGeneration) {
                str << " BlockedGeneration# " << BlockedGeneration;
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 PayloadSizeBytes() const {
            ui32 size = 0;
            for (ui32 i = 0; i < ResponseSz; ++i) {
                size += Responses[i].Buffer.size();
            }
            return size;
        }
    };

    struct TEvBlock : public TEventLocal<TEvBlock, EvBlock> {
        const ui64 TabletId;
        const ui32 Generation;
        const TInstant Deadline;
        const ui64 IssuerGuid = RandomNumber<ui64>() | 1;
        bool IsMonitored = true;
        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvBlock(ui64 tabletId, ui32 generation, TInstant deadline)
            : TabletId(tabletId)
            , Generation(generation)
            , Deadline(deadline)
        {}

        TEvBlock(ui64 tabletId, ui32 generation, TInstant deadline, ui64 issuerGuid)
            : TabletId(tabletId)
            , Generation(generation)
            , Deadline(deadline)
            , IssuerGuid(issuerGuid)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvBlock {TabletId# " << TabletId
                << " Generation# " << Generation
                << " Deadline# " << Deadline.MilliSeconds()
                << " IsMonitored# " << IsMonitored
                << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvBlockResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvBlockResult : public TEventLocal<TEvBlockResult, EvBlockResult> {
        NKikimrProto::EReplyStatus Status;
        TString ErrorReason;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvBlockResult(NKikimrProto::EReplyStatus status)
            : Status(status)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvBlockResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvPatch : public TEventLocal<TEvPatch, EvPatch> {
    private:
        static constexpr ui32 BaseDomainsCount = 8;
        static constexpr ui32 MaxStepsForFindingId = 128;

    public:
        struct TDiff {
            TRcBuf Buffer;
            ui32 Offset;

            TDiff()
                : Offset(0)
            {
            }

            void Set(const TString &buffer, ui32 offset) {
                Buffer = TRcBuf(buffer);
                Offset = offset;
                Y_VERIFY_S(buffer.Size(), "EvPatchDiff invalid: Diff size must be non-zero");
            }

            void Set(const TRcBuf &buffer, ui32 offset) {
                Buffer = buffer;
                Offset = offset;
                Y_VERIFY_S(buffer.Size(), "EvPatchDiff invalid: Diff size must be non-zero");
            }

            template <typename TOStream>
            void Output(TOStream &os) const {
                os << "TDiff {Offset# " << Offset << " Size# " << Buffer.Size() << '}';
            }

            TString ToString() const {
                TStringBuilder str;
                Output(str);
                return str;
            }
        };

        const ui32 OriginalGroupId;
        const TLogoBlobID OriginalId;
        const TLogoBlobID PatchedId;
        const ui32 MaskForCookieBruteForcing = 0;

        TArrayHolder<TDiff> Diffs;
        const ui64 DiffCount;
        const TInstant Deadline;
        mutable NLWTrace::TOrbit Orbit;
        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvPatch(ui32 originalGroupId, const TLogoBlobID &originalId, const TLogoBlobID &patchedId,
                ui32 maskForCookieBruteForcing, TArrayHolder<TDiff> &&diffs, ui64 diffCount, TInstant deadline)
            : OriginalGroupId(originalGroupId)
            , OriginalId(originalId)
            , PatchedId(patchedId)
            , MaskForCookieBruteForcing(maskForCookieBruteForcing)
            , Diffs(std::move(diffs))
            , DiffCount(diffCount)
            , Deadline(deadline)
        {
            CheckContructorArgs(originalId, patchedId, Diffs, DiffCount);
        }

        static void CheckContructorArgs(const TLogoBlobID &originalId, const TLogoBlobID &patchedId,
                const TArrayHolder<TDiff> &diffs, ui64 diffCount)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&originalId, sizeof(originalId));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&patchedId, sizeof(patchedId));
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(diffs.Get(), sizeof(*diffs.Get()) * diffCount);

            Y_VERIFY_S(originalId, "EvPatch invalid: LogoBlobId must have non-zero tablet field,"
                    << " OriginalId# " << originalId);
            Y_VERIFY_S(patchedId, "EvPatch invalid: LogoBlobId must have non-zero tablet field,"
                    << " PatchedId# " << patchedId);
            Y_VERIFY_S(originalId != patchedId, "EvPatch invalid: OriginalId and PatchedId mustn't be equal"
                    << " OriginalId# " << originalId
                    << " PatchedId# " << patchedId);
            Y_VERIFY_S(originalId.BlobSize() == patchedId.BlobSize(),
                    "EvPatch invalid: LogoBlobId must have non-zero tablet field,"
                    << " OriginalId# " << originalId
                    << " PatchedId# " << patchedId);

            for (ui32 idx = 0; idx < diffCount; ++idx) {
                REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(diffs[idx].Buffer.Data(), diffs[idx].Buffer.size());

                if (idx) {
                    Y_VERIFY_S(diffs[idx - 1].Offset + diffs[idx - 1].Buffer.Size() <= diffs[idx].Offset,
                            "EvPatch invalid: Diffs must not overlap,"
                            << " [" << idx - 1 << "].Offset# " << diffs[idx - 1].Offset
                            << " [" << idx - 1 << "].Size# " << diffs[idx - 1].Buffer.Size()
                            << " [" << idx << "].Offset# " << diffs[idx].Offset
                            << " [" << idx << "].Size# " << diffs[idx].Buffer.Size());
                }
                Y_VERIFY_S(diffs[idx].Offset + diffs[idx].Buffer.Size() <= originalId.BlobSize(),
                        "EvPatch invalid: Blob size bound was overflow by diff,"
                        << " [" << idx << "].Offset# " << diffs[idx].Offset
                        << " [" << idx << "].Size# " << diffs[idx].Buffer.Size()
                        << " [" << idx << "].EndIdx# " << diffs[idx].Offset + diffs[idx].Buffer.Size()
                        << " BlobSize# " << originalId.BlobSize());
                Y_VERIFY_S(diffs[idx].Buffer.Size(),
                        "EvPatch invalid: Diff size must be non-zero,"
                        << " [" << idx << "].Size# " << diffs[idx].Buffer.Size());
            }
        }

        static ui8 BlobPlacementKind(const TLogoBlobID &blob) {
            return blob.Hash() % BaseDomainsCount;
        }

        static bool GetBlobIdWithSamePlacement(const TLogoBlobID &originalId, TLogoBlobID *patchedId,
                ui32 bitsForBruteForce, ui32 originalGroupId, ui32 currentGroupId)
        {
            if (originalGroupId != currentGroupId) {
                return false;
            }

            ui32 expectedValue = originalId.Hash() % BaseDomainsCount;
            Y_ABORT_UNLESS(patchedId);
            if (patchedId->Hash() % BaseDomainsCount == expectedValue) {
                return true;
            }

            Y_ABORT_UNLESS(bitsForBruteForce <= TLogoBlobID::MaxCookie);
            ui32 baseCookie = ~bitsForBruteForce & patchedId->Cookie();
            ui32 extraCookie = TLogoBlobID::MaxCookie + 1;
            ui32 steps = 0;
            do {
                extraCookie = (extraCookie - 1) & bitsForBruteForce;
                ui32 cookie = baseCookie | (extraCookie ^ bitsForBruteForce);
                steps++;

                TLogoBlobID id(patchedId->TabletID(), patchedId->Generation(), patchedId->Step(), patchedId->Channel(),
                        patchedId->BlobSize(), cookie, 0, patchedId->CrcMode());
                if (id.Hash() % BaseDomainsCount == expectedValue) {
                    *patchedId = id;
                    return true;
                }

            } while(extraCookie && steps < MaxStepsForFindingId);

            return false;
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringBuilder str;
            str << "TEvPatch {OriginalGroupId# " << OriginalGroupId;
            str << " OriginalId# " << OriginalId;
            str << " PatchedId# " << PatchedId;
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " DiffCount# " << DiffCount;
            for (ui32 idx = 0; idx < DiffCount; ++idx) {
                str << ' ';
                Diffs[idx].Output(str);
            }
            str << '}';
            return str;
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this) + sizeof(TDiff) * DiffCount;
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvPatchResult> MakeErrorResponse(NKikimrProto::EReplyStatus status,
                const TString& errorReason, TGroupId groupId);
    };

    struct TEvPatchResult : public TEventLocal<TEvPatchResult, EvPatchResult> {
        NKikimrProto::EReplyStatus Status;
        const TLogoBlobID Id;
        const TStorageStatusFlags StatusFlags;
        const TGroupId GroupId;
        const float ApproximateFreeSpaceShare; // 0.f has special meaning 'data could not be obtained'
        TString ErrorReason;
        mutable NLWTrace::TOrbit Orbit;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvPatchResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &id, TStorageStatusFlags statusFlags,
                TGroupId groupId, float approximateFreeSpaceShare)
            : Status(status)
            , Id(id)
            , StatusFlags(statusFlags)
            , GroupId(groupId)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&id, sizeof(id));
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringBuilder str;
            str << "TEvPatchResult {Id# " << Id;
            str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " StatusFlags# " << StatusFlags;
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
            str << "}";
            return str;
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvInplacePatch : public TEventLocal<TEvInplacePatch, EvInplacePatch> {
        using TDiff = TEvPatch::TDiff;

        const TLogoBlobID OriginalId;
        const TLogoBlobID PatchedId;

        TArrayHolder<TDiff> Diffs;
        const ui64 DiffCount;
        const TInstant Deadline;
        mutable NLWTrace::TOrbit Orbit;
        ui32 RestartCounter = 0;

        TEvInplacePatch(const TLogoBlobID &originalId, const TLogoBlobID &patchedId, TArrayHolder<TDiff> &&diffs,
                ui64 diffCount, TInstant deadline)
            : OriginalId(originalId)
            , PatchedId(patchedId)
            , Diffs(std::move(diffs))
            , DiffCount(diffCount)
            , Deadline(deadline)
        {
            TEvPatch::CheckContructorArgs(originalId, patchedId, Diffs, DiffCount);
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringBuilder str;
            str << "TEvInplacePatch {OriginalId# " << OriginalId;
            str << " PatchedId# " << PatchedId;
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " DiffCount# " << DiffCount;
            for (ui32 idx = 0; idx < DiffCount; ++idx) {
                str << ' ';
                Diffs[idx].Output(str);
            }
            str << '}';
            return str;
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this) + sizeof(TDiff) * DiffCount;
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvInplacePatchResult> MakeErrorResponse(NKikimrProto::EReplyStatus status,
                const TString& errorReason);
    };

    struct TEvInplacePatchResult : public TEventLocal<TEvInplacePatchResult, EvInplacePatchResult> {
        NKikimrProto::EReplyStatus Status;
        const TLogoBlobID Id;
        const TStorageStatusFlags StatusFlags;
        const float ApproximateFreeSpaceShare; // 0.f has special meaning 'data could not be obtained'
        TString ErrorReason;
        mutable NLWTrace::TOrbit Orbit;

        TEvInplacePatchResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &id, TStorageStatusFlags statusFlags,
                float approximateFreeSpaceShare)
            : Status(status)
            , Id(id)
            , StatusFlags(statusFlags)
            , ApproximateFreeSpaceShare(approximateFreeSpaceShare)
        {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&id, sizeof(id));
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringBuilder str;
            str << "TEvPatchResult {Id# " << Id;
            str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " StatusFlags# " << StatusFlags;
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
            str << "}";
            return str;
        }

        TString ToString() const {
            return Print(false);
        }
    };

    // special kind of request, strictly used for tablet discovery
    // returns logoblobid of last known control-channel (zero) entry.
    struct TEvDiscover : public TEventLocal<TEvDiscover, EvDiscover> {
        const ui64 TabletId;
        const ui32 MinGeneration;
        const TInstant Deadline;
        const bool ReadBody;
        const bool DiscoverBlockedGeneration;
        const ui32 ForceBlockedGeneration;
        const bool FromLeader;
        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvDiscover(ui64 tabletId, ui32 minGeneration, bool readBody, bool discoverBlockedGeneration,
                TInstant deadline, ui32 forceBlockedGeneration, bool fromLeader)
            : TabletId(tabletId)
            , MinGeneration(minGeneration)
            , Deadline(deadline)
            , ReadBody(readBody)
            , DiscoverBlockedGeneration(discoverBlockedGeneration)
            , ForceBlockedGeneration(forceBlockedGeneration)
            , FromLeader(fromLeader)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvDiscover {TabletId# " << TabletId;
            str << " MinGeneration# " << MinGeneration;
            str << " ReadBody# " << (ReadBody ? "true" : "false");
            str << " DiscoverBlockedGeneration# " << (DiscoverBlockedGeneration ? "true" : "false");
            str << " ForceBlockedGeneration# " << ForceBlockedGeneration;
            str << " FromLeader# " << (FromLeader ? "true" : "false");
            str << " Deadline# " << Deadline.MilliSeconds();
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvDiscoverResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvDiscoverResult : public TEventLocal<TEvDiscoverResult, EvDiscoverResult> {
        NKikimrProto::EReplyStatus Status;

        TLogoBlobID Id;
        ui32 MinGeneration;
        TString Buffer;
        ui32 BlockedGeneration;
        TString ErrorReason;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvDiscoverResult(NKikimrProto::EReplyStatus status, ui32 minGeneration, ui32 blockedGeneration)
            : Status(status)
            , MinGeneration(minGeneration)
            , BlockedGeneration(blockedGeneration)
        {
            Y_DEBUG_ABORT_UNLESS(status != NKikimrProto::OK);
        }

        TEvDiscoverResult(const TLogoBlobID &id, ui32 minGeneration, const TString &buffer)
            : Status(NKikimrProto::OK)
            , Id(id)
            , MinGeneration(minGeneration)
            , Buffer(buffer)
            , BlockedGeneration(0)
        {}

        TEvDiscoverResult(const TLogoBlobID &id, ui32 minGeneration, const TString &buffer, ui32 blockedGeneration)
            : Status(NKikimrProto::OK)
            , Id(id)
            , MinGeneration(minGeneration)
            , Buffer(buffer)
            , BlockedGeneration(blockedGeneration)
        {}

        TString Print(bool isFull) const {
            TStringStream str;
            str << "TEvDiscoverResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " BlockedGeneration# " << BlockedGeneration;
            str << " Id# " << Id.ToString();
            str << " Size# " << Buffer.size();
            str << " MinGeneration# " << MinGeneration;
            if (isFull) {
                str << " Buffer# " << Buffer.Quote();
            }
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvRange : public TEventLocal<TEvRange, EvRange> {
        ui64 TabletId;
        TLogoBlobID From;
        TLogoBlobID To;
        const TInstant Deadline;
        bool MustRestoreFirst;
        bool IsIndexOnly;
        ui32 ForceBlockedGeneration;
        ui32 RestartCounter = 0;
        bool Decommission = false;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvRange(ui64 tabletId, const TLogoBlobID &from, const TLogoBlobID &to, const bool mustRestoreFirst,
                TInstant deadline, bool isIndexOnly = false, ui32 forceBlockedGeneration = 0)
            : TabletId(tabletId)
            , From(from)
            , To(to)
            , Deadline(deadline)
            , MustRestoreFirst(mustRestoreFirst)
            , IsIndexOnly(isIndexOnly)
            , ForceBlockedGeneration(forceBlockedGeneration)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvRange {TabletId# " << TabletId;
            str << " From# " << From.ToString();
            str << " To# " << To.ToString();
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " MustRestoreFirst# " << (MustRestoreFirst ? "true" : "false");
            if (ForceBlockedGeneration)
                str << " ForceBlock: " << ForceBlockedGeneration;
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvRangeResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvRangeResult : public TEventLocal<TEvRangeResult, EvRangeResult> {
        struct TResponse {
            TLogoBlobID Id;
            TString Buffer;
            bool Keep = false;
            bool DoNotKeep = false;

            TResponse()
            {}

            TResponse(const TLogoBlobID &id, const TString &x, bool keep = false, bool doNotKeep = false)
                : Id(id)
                , Buffer(x)
                , Keep(keep)
                , DoNotKeep(doNotKeep)
            {}
        };

        NKikimrProto::EReplyStatus Status;
        TLogoBlobID From;
        TLogoBlobID To;

        TVector<TResponse> Responses;
        const ui32 GroupId;
        TString ErrorReason;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvRangeResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &from, const TLogoBlobID &to, TGroupId groupId)
            : Status(status)
            , From(from)
            , To(to)
            , GroupId(groupId.GetRawId())
        {}

        TEvRangeResult(NKikimrProto::EReplyStatus status, const TLogoBlobID &from, const TLogoBlobID &to, ui32 groupId)
            : Status(status)
            , From(from)
            , To(to)
            , GroupId(groupId)
        {}

        TString Print(bool isFull) const {
            TStringStream str;
            str << "TEvRangeResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " From# " << From.ToString();
            str << " To# " << To.ToString();
            str << " Size# " << Responses.size();
            for (ui32 i = 0; i < Responses.size(); ++i) {
                const TResponse &response = Responses[i];
                str << " {" << response.Id.ToString();
                str << " Size# " << response.Buffer.size();
                if (isFull) {
                    str << " Buffer# " << response.Buffer.Quote();
                }
                str << "}";
            }
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvCollectGarbage : public TEventLocal<TEvCollectGarbage, EvCollectGarbage> {
        ui64 TabletId;
        ui32 RecordGeneration;
        ui32 PerGenerationCounter; // monotone increasing cmd counter for RecordGeneration
        ui32 Channel;

        THolder<TVector<TLogoBlobID> > Keep;
        THolder<TVector<TLogoBlobID> > DoNotKeep;
        TInstant Deadline;

        ui32 CollectGeneration;
        ui32 CollectStep;

        // if set to true, this barrier does not take keep flags into account and is treated separately from soft barriers;
        // this means that all data before the hard barrier is destroyed without taking keep flags into account
        bool Hard;

        bool Collect;

        bool IsMultiCollectAllowed;
        bool IsMonitored = true;

        bool Decommission = false;

        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvCollectGarbage(ui64 tabletId, ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel,
                bool collect, ui32 collectGeneration,
                ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep, TInstant deadline,
                bool isMultiCollectAllowed, bool hard = false)
            : TabletId(tabletId)
            , RecordGeneration(recordGeneration)
            , PerGenerationCounter(perGenerationCounter)
            , Channel(channel)
            , Keep(keep)
            , DoNotKeep(doNotKeep)
            , Deadline(deadline)
            , CollectGeneration(collectGeneration)
            , CollectStep(collectStep)
            , Hard(hard)
            , Collect(collect)
            , IsMultiCollectAllowed(isMultiCollectAllowed)
        {}

        TEvCollectGarbage(ui64 tabletId, ui32 recordGeneration, ui32 channel, bool collect, ui32 collectGeneration,
                ui32 collectStep, TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep, TInstant deadline)
            : TabletId(tabletId)
            , RecordGeneration(recordGeneration)
            , PerGenerationCounter(0)
            , Channel(channel)
            , Keep(keep)
            , DoNotKeep(doNotKeep)
            , Deadline(deadline)
            , CollectGeneration(collectGeneration)
            , CollectStep(collectStep)
            , Hard(false)
            , Collect(collect)
            , IsMultiCollectAllowed(true)
        {}

        static THolder<TEvCollectGarbage> CreateHardBarrier(ui64 tabletId, ui32 recordGeneration,
                ui32 perGenerationCounter, ui32 channel, ui32 collectGeneration, ui32 collectStep, TInstant deadline) {
            return MakeHolder<TEvCollectGarbage>(tabletId, recordGeneration, perGenerationCounter, channel,
                    true /*collect*/, collectGeneration, collectStep, nullptr /*keep*/, nullptr /*doNotKeep*/,
                    deadline, false /*isMultiCollectAllowed*/, true /*hard*/);
        }

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvCollectGarbage {TabletId# " << TabletId;
            str << " RecordGeneration# " << RecordGeneration;
            str << " PerGenerationCounter# " << PerGenerationCounter;
            str << " Channel# " << Channel;
            str << " Deadline# " << Deadline.MilliSeconds();
            str << " Collect# " << (Collect ? "true" : "false");
            if (Collect) {
                str << " CollectGeneration# " << CollectGeneration;
                str << " CollectStep# " << CollectStep;
            }
            str << " Hard# " << (Hard ? "true" : "false");
            str << " IsMultiCollectAllowed# " << IsMultiCollectAllowed;
            str << " IsMonitored# " << IsMonitored;
            if (Keep.Get()) {
                str << " KeepSize# " << Keep->size() << " {";
                for (ui32 i = 0; i < Keep->size(); ++i) {
                    str << "Id# " << (*Keep)[i].ToString();
                }
                str << "}";
            }
            if (DoNotKeep.Get()) {
                str << " DoNotKeepSize# " << DoNotKeep->size() << " {";
                for (ui32 i = 0; i < DoNotKeep->size(); ++i) {
                    str << "Id# " << (*DoNotKeep)[i].ToString();
                }
                str << "}";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        static ui64 PerGenerationCounterStepSize(TVector<TLogoBlobID> *keep, TVector<TLogoBlobID> *doNotKeep) {
            ui64 keepCount = keep ? keep->size() : 0;
            ui64 doNotKeepCount = doNotKeep ? doNotKeep->size() : 0;
            ui64 totalFlags = keepCount + doNotKeepCount;
            ui64 extraSteps = (totalFlags + MaxCollectGarbageFlagsPerMessage - 1) / MaxCollectGarbageFlagsPerMessage;
            ui64 stepSize = Max(ui64(1ull), extraSteps);
            return stepSize;
        }

        ui64 PerGenerationCounterStepSize() const {
            return PerGenerationCounterStepSize(Keep.Get(), DoNotKeep.Get());
        }

        ui32 CalculateSize() const {
            return sizeof(*this) + ((Keep ? Keep->size() : 0) + (DoNotKeep ? DoNotKeep->size() : 0)) * sizeof(TLogoBlobID);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvCollectGarbageResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvCollectGarbageResult : public TEventLocal<TEvCollectGarbageResult, EvCollectGarbageResult> {
        NKikimrProto::EReplyStatus Status;

        ui64 TabletId;
        ui32 RecordGeneration;
        ui32 PerGenerationCounter;
        ui32 Channel;
        TString ErrorReason;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvCollectGarbageResult(NKikimrProto::EReplyStatus status, ui64 tabletId,
                ui32 recordGeneration, ui32 perGenerationCounter, ui32 channel)
            : Status(status)
            , TabletId(tabletId)
            , RecordGeneration(recordGeneration)
            , PerGenerationCounter(perGenerationCounter)
            , Channel(channel)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvCollectGarbageResult {TabletId# " << TabletId;
            str << " RecordGeneration# " << RecordGeneration;
            str << " PerGenerationCounter# " << PerGenerationCounter;
            str << " Channel# " << Channel;
            str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvStatus : public TEventLocal<TEvStatus, EvStatus> {
        const TInstant Deadline;
        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvStatus(TInstant deadline)
            : Deadline(deadline)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvStatus {Deadline# " << Deadline.MilliSeconds()
                << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }

        ui32 CalculateSize() const {
            return sizeof(*this);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvStatusResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvStatusResult : public TEventLocal<TEvStatusResult, EvStatusResult> {
        NKikimrProto::EReplyStatus Status;
        TStorageStatusFlags StatusFlags;
        float ApproximateFreeSpaceShare = 0.0f; // zero means absence of correct data
        TString ErrorReason;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvStatusResult(NKikimrProto::EReplyStatus status, TStorageStatusFlags statusFlags)
            : Status(status)
            , StatusFlags(statusFlags)
        {}

        TString Print(bool isFull) const {
            Y_UNUSED(isFull);
            TStringStream str;
            str << "TEvStatusResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
            str << " StatusFlags# ";
            StatusFlags.Output(str);
            if (ErrorReason.size()) {
                str << " ErrorReason# \"" << ErrorReason << "\"";
            }
            str << "}";
            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvAssimilate : TEventLocal<TEvAssimilate, EvAssimilate> {
        std::optional<ui64> SkipBlocksUpTo;
        std::optional<std::tuple<ui64, ui8>> SkipBarriersUpTo;
        std::optional<TLogoBlobID> SkipBlobsUpTo;
        ui32 RestartCounter = 0;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvAssimilate(std::optional<ui64> skipBlocksUpTo, std::optional<std::tuple<ui64, ui8>> skipBarriersUpTo,
                std::optional<TLogoBlobID> skipBlobsUpTo)
            : SkipBlocksUpTo(skipBlocksUpTo)
            , SkipBarriersUpTo(skipBarriersUpTo)
            , SkipBlobsUpTo(skipBlobsUpTo)
        {}

        TString Print(bool /*isFull*/) const {
            return ToString();
        }

        TString ToString() const {
            TStringStream str;
            str << "TEvAssimilate {";
            const char *prefix = "";
            if (SkipBlocksUpTo) {
                str << std::exchange(prefix, " ") << "SkipBlocksUpTo# " << *SkipBlocksUpTo;
            }
            if (SkipBarriersUpTo) {
                str << std::exchange(prefix, " " ) << "SkipBarriersUpTo# " << std::get<0>(*SkipBarriersUpTo)
                    << ":" << int(std::get<1>(*SkipBarriersUpTo));
            }
            if (SkipBlobsUpTo) {
                str << std::exchange(prefix, " " ) << "SkipBlobsUpTo# ";
                SkipBlobsUpTo->Out(str);
            }
            str << "}";
            return str.Str();
        }

        ui32 CalculateSize() const {
            return sizeof(*this);
        }

        void ToSpan(NWilson::TSpan& span) const;

        std::unique_ptr<TEvAssimilateResult> MakeErrorResponse(NKikimrProto::EReplyStatus status, const TString& errorReason,
            TGroupId groupId);
    };

    struct TEvAssimilateResult : TEventLocal<TEvAssimilateResult, EvAssimilateResult> {
        struct TBlock {
            ui64 TabletId = 0;
            ui32 BlockedGeneration = 0;

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

            void Output(IOutputStream& s) const {
                s << "{" << TabletId << "=>" << BlockedGeneration << "}";
            }
        };

        struct TBarrier {
            struct TValue {
                ui32 RecordGeneration = 0;
                ui32 PerGenerationCounter = 0;
                ui32 CollectGeneration = 0;
                ui32 CollectStep = 0;

                void Output(IOutputStream& s) const {
                    if (RecordGeneration || PerGenerationCounter || CollectGeneration || CollectGeneration) {
                        s << "{" << RecordGeneration << ":" << PerGenerationCounter << "=>" << CollectGeneration
                            << ":" << CollectStep << "}";
                    }
                }
            };

            ui64 TabletId = 0;
            ui8 Channel = 0;
            TValue Soft;
            TValue Hard;

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

            void Output(IOutputStream& s) const {
                s << "{" << TabletId << ":" << int(Channel) << "=>soft";
                Soft.Output(s);
                s << "/hard";
                Hard.Output(s);
                s << "}";
            }
        };

        struct TBlob {
            TLogoBlobID Id;
            bool Keep = false;
            bool DoNotKeep = false;

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }

            void Output(IOutputStream& s) const {
                Id.Out(s);
                if (Keep) {
                    s << "k";
                }
                if (DoNotKeep) {
                    s << "d";
                }
            }
        };

        NKikimrProto::EReplyStatus Status;
        TString ErrorReason;
        std::deque<TBlock> Blocks;
        std::deque<TBarrier> Barriers;
        std::deque<TBlob> Blobs;
        std::shared_ptr<TExecutionRelay> ExecutionRelay;

        TEvAssimilateResult(NKikimrProto::EReplyStatus status, TString errorReason = {})
            : Status(status)
            , ErrorReason(std::move(errorReason))
        {}

        TString Print(bool isFull) const {
            TStringStream str;
            str << "TEvAssimilateResult {"
                << "Status# " << NKikimrProto::EReplyStatus_Name(Status)
                << " ErrorReason# '" << ErrorReason << "'";

            auto out = [&](const char *name, auto& container) {
                str << " " << name << "# ";
                if (isFull) {
                    str << "[";
                    for (auto it = container.begin(); it != container.end(); ++it) {
                        if (it != container.begin()) {
                            str << " ";
                        }
                        it->Output(str);
                    }
                    str << "]";
                } else {
                    str << "size=" << container.size();
                }
            };

            out("Blocks", Blocks);
            out("Barriers", Barriers);
            out("Blobs", Blobs);

            str << "}";

            return str.Str();
        }

        TString ToString() const {
            return Print(false);
        }
    };

    struct TEvConfigureProxy;
    struct TEvUpdateGroupInfo;

    struct TEvVMovedPatch;
    struct TEvVMovedPatchResult;
    struct TEvVInplacePatch;
    struct TEvVInplacePatchResult;
    struct TEvVPut;
    struct TEvVPutResult;
    struct TEvVMultiPut;
    struct TEvVMultiPutResult;
    struct TEvVGet;
    struct TEvVGetResult;
    struct TEvVPatchStart;
    struct TEvVPatchFoundParts;
    struct TEvVPatchDiff;
    struct TEvVPatchResult;
    struct TEvVPatchXorDiff;
    struct TEvVPatchXorDiffResult;
    struct TEvVBlock;
    struct TEvVBlockResult;
    struct TEvVGetBlock;
    struct TEvVGetBlockResult;
    struct TEvVCollectGarbage;
    struct TEvVCollectGarbageResult;
    struct TEvVGetBarrier;
    struct TEvVGetBarrierResult;
    struct TEvVSyncGuid;
    struct TEvVSyncGuidResult;
    struct TEvVSync;
    struct TEvVSyncResult;
    struct TEvVSyncFull;
    struct TEvVSyncFullResult;
    struct TEvVStatus;
    struct TEvVStatusResult;
    struct EvVBaldSyncLog;
    struct EvVBaldSyncLogResult;
    struct TEvVDbStat;
    struct TEvVDbStatResult;
    struct TEvVCheckReadiness;
    struct TEvVCheckReadinessResult;
    struct TEvVCompact;
    struct TEvVCompactResult;
    struct TEvVBaldSyncLog;
    struct TEvVBaldSyncLogResult;
    struct TEvVWindowChange;
    struct TEvLocalRecoveryDone;
    struct THullChange;
    struct TEvVReadyNotify;
    struct TEvEnrichNotYet;
    struct TEvCaptureVDiskLayout;
    struct TEvCaptureVDiskLayoutResult;
    struct TEvVDefrag;
    struct TEvVDefragResult;
    struct TEvVAssimilate;
    struct TEvVAssimilateResult;
    struct TEvVTakeSnapshot;
    struct TEvVTakeSnapshotResult;
    struct TEvVReleaseSnapshot;
    struct TEvVReleaseSnapshotResult;


    struct TEvControllerRegisterNode;
    struct TEvControllerSelectGroups;
    struct TEvControllerGetGroup;
    struct TEvControllerUpdateDiskStatus;
    struct TEvControllerUpdateGroupStat;
    struct TEvControllerUpdateNodeDrives;
    struct TEvControllerNodeServiceSetUpdate;
    struct TEvControllerProposeGroupKey;
    struct TEvControllerSelectGroupsResult;
    struct TEvControllerNodeReport;
    struct TEvControllerConfigRequest;
    struct TEvControllerConfigResponse;
    struct TEvControllerScrubQueryStartQuantum;
    struct TEvControllerScrubStartQuantum;
    struct TEvControllerScrubQuantumFinished;
    struct TEvControllerScrubReportQuantumInProgress;
    struct TEvRequestControllerInfo;
    struct TEvResponseControllerInfo;
    struct TEvControllerGroupDecommittedNotify;
    struct TEvControllerGroupDecommittedResponse;
    struct TEvControllerGroupMetricsExchange;
    struct TEvPutVDiskToReadOnly;

    struct TEvMonStreamQuery;
    struct TEvMonStreamActorDeathNote;

    struct TEvDropDonor;
    struct TEvBunchOfEvents;

    struct TEvAskRestartVDisk;
    struct TEvAskWardenRestartPDisk;
    struct TEvAskWardenRestartPDiskResult;
    struct TEvNotifyWardenPDiskRestarted;
};

// EPutHandleClass defines BlobStorage queue to a request to
static inline NKikimrBlobStorage::EVDiskQueueId HandleClassToQueueId(NKikimrBlobStorage::EPutHandleClass cls) {
    switch (cls) {
        case NKikimrBlobStorage::EPutHandleClass::TabletLog:
            return NKikimrBlobStorage::EVDiskQueueId::PutTabletLog;
        case NKikimrBlobStorage::EPutHandleClass::AsyncBlob:
            return NKikimrBlobStorage::EVDiskQueueId::PutAsyncBlob;
        case NKikimrBlobStorage::EPutHandleClass::UserData:
            return NKikimrBlobStorage::EVDiskQueueId::PutUserData;
        default:
            Y_ABORT("Unexpected case");
    }
}

    // EGetHandleClass defines BlobStorage queue to a request to
    static inline NKikimrBlobStorage::EVDiskQueueId HandleClassToQueueId(NKikimrBlobStorage::EGetHandleClass cls) {
        switch (cls) {
            case NKikimrBlobStorage::EGetHandleClass::AsyncRead:
                return NKikimrBlobStorage::EVDiskQueueId::GetAsyncRead;
            case NKikimrBlobStorage::EGetHandleClass::FastRead:
                return NKikimrBlobStorage::EVDiskQueueId::GetFastRead;
            case NKikimrBlobStorage::EGetHandleClass::Discover:
                return NKikimrBlobStorage::EVDiskQueueId::GetDiscover;
            case NKikimrBlobStorage::EGetHandleClass::LowRead:
                return NKikimrBlobStorage::EVDiskQueueId::GetLowRead;
            default:
                Y_ABORT("Unexpected case");
        }
    }


inline bool SendPutToGroup(const TActorContext &ctx, ui32 groupId, TTabletStorageInfo *storage,
        THolder<TEvBlobStorage::TEvPut> event, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
    auto checkGroupId = [&] {
        const TLogoBlobID &id = event->Id;
        const ui32 expectedGroupId = storage->GroupFor(id.Channel(), id.Generation());
        return id.TabletID() == storage->TabletID && expectedGroupId != Max<ui32>() && groupId == expectedGroupId;
    };
    Y_ABORT_UNLESS(checkGroupId(), "groupId# %" PRIu32 " does not match actual one LogoBlobId# %s", groupId,
        event->Id.ToString().data());
    return SendToBSProxy(ctx, groupId, event.Release(), cookie, std::move(traceId));
    // TODO(alexvru): check if return status is actually needed?
}

inline bool SendPatchToGroup(const TActorContext &ctx, ui32 groupId, TTabletStorageInfo *storage,
        THolder<TEvBlobStorage::TEvPatch> event, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
    auto checkGroupId = [&] {
        const TLogoBlobID &id = event->PatchedId;
        const ui32 expectedGroupId = storage->GroupFor(id.Channel(), id.Generation());
        const TLogoBlobID &originalId = event->OriginalId;
        const ui32 expectedOriginalGroupId = storage->GroupFor(originalId.Channel(), originalId.Generation());
        return id.TabletID() == storage->TabletID && expectedGroupId != Max<ui32>() && groupId == expectedGroupId && event->OriginalGroupId == expectedOriginalGroupId;
    };
    Y_VERIFY_S(checkGroupId(), "groupIds# (" << event->OriginalGroupId << ',' << groupId << ") does not match actual ones LogoBlobIds# (" <<
        event->OriginalId.ToString() << ',' << event->PatchedId.ToString() << ')');
    return SendToBSProxy(ctx, groupId, event.Release(), cookie, std::move(traceId));
    // TODO(alexvru): check if return status is actually needed?
}

} // NKikimr
