#pragma once
#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>
#include "defs.h"
#include <ydb/core/base/events.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/protos/node_whiteboard.pb.h>
#include <ydb/core/protos/whiteboard_disk_states.pb.h>
#include <ydb/core/protos/whiteboard_flags.pb.h>
#include <ydb/core/protos/memory_stats.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/library/actors/interconnect/events_local.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/tracing.h>
#include <ydb/core/protos/blobstorage_vdisk_config.pb.h>

namespace NKikimr {
    class TTabletStorageInfo;
    class TBlobStorageGroupInfo;
}

namespace NKikimr::NNodeWhiteboard {

using TTabletId = ui64;
using TFollowerId = ui32;
using TNodeId = ui32;

struct TEvWhiteboard {
    enum EEv {
        EvTabletStateUpdate = EventSpaceBegin(TKikimrEvents::ES_NODE_WHITEBOARD),
        EvTabletStateRequest,
        EvTabletStateResponse,
        EvNodeStateUpdate,
        EvNodeStateDelete,
        EvNodeStateRequest,
        EvNodeStateResponse,
        EvPDiskStateUpdate,
        EvPDiskStateRequest,
        EvPDiskStateResponse,
        EvVDiskStateUpdate,
        EvVDiskStateRequest,
        EvVDiskStateResponse,
        EvSystemStateUpdate,
        EvSystemStateRequest,
        EvSystemStateResponse,
        EvBSGroupStateUpdate,
        EvBSGroupStateRequest,
        EvBSGroupStateResponse,
        EvVDiskStateDelete,
        EvSystemStateAddEndpoint,
        EvSystemStateAddRole,
        EvSystemStateSetTenant,
        EvSystemStateRemoveTenant,
        EvBSGroupStateDelete,
        EvIntrospectionData,
        EvTabletLookupRequest,
        EvTabletLookupResponse,
        EvTraceLookupRequest,
        EvTraceLookupResponse,
        EvTraceRequest,
        EvTraceResponse,
        EvSignalBodyRequest,
        EvSignalBodyResponse,
        EvPDiskStateDelete,
        EvVDiskStateGenerationChange,
        EvVDiskDropDonors,
        EvClockSkewUpdate,
        EvMemoryStatsUpdate,
        EvBridgeInfoUpdate,
        EvBridgeInfoRequest,
        EvBridgeInfoResponse,
        EvDDiskStateUpdate,
        EvDDiskStateDelete,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_NODE_WHITEBOARD), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_NODE_WHITEBOARD)");

    struct TEvTabletStateUpdate : TEventPB<TEvTabletStateUpdate, NKikimrWhiteboard::TTabletStateInfo, EvTabletStateUpdate> {
        TEvTabletStateUpdate() = default;

        TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, const TIntrusivePtr<TTabletStorageInfo>& storageInfo, ui32 generation, bool leader);

        TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, ui32 generation);

        TEvTabletStateUpdate(TTabletId tabletId, TFollowerId followerId, NKikimrWhiteboard::TTabletStateInfo::ETabletState state, ui32 generation, bool leader);

        TEvTabletStateUpdate(TTabletId tabletId, ui32 userState);
    };

    struct TEvTabletStateRequest : public TEventPB<TEvTabletStateRequest, NKikimrWhiteboard::TEvTabletStateRequest, EvTabletStateRequest> {};

#pragma pack(push, 1)
    struct TEvTabletStateResponsePacked5 {
        ui64 TabletId;
        ui32 FollowerId;
        ui32 Generation;
        NKikimrTabletBase::TTabletTypes::EType Type;
        NKikimrWhiteboard::TTabletStateInfo::ETabletState State;

        TEvTabletStateResponsePacked5() = default;
        TEvTabletStateResponsePacked5(const NKikimrWhiteboard::TTabletStateInfo& elem)
            : TabletId(elem.GetTabletId())
            , FollowerId(elem.GetFollowerId())
            , Generation(elem.GetGeneration())
            , Type(elem.GetType())
            , State(elem.GetState())
        {}

        operator NKikimrWhiteboard::TTabletStateInfo() const {
            NKikimrWhiteboard::TTabletStateInfo result;
            Fill(result);
            return result;
        }

        void Fill(NKikimrWhiteboard::TTabletStateInfo& result) const;
    } Y_PACKED;

    static_assert(sizeof(TEvTabletStateResponsePacked5) == 24);
#pragma pack(pop)

    struct TEvTabletStateResponse : public TEventPB<TEvTabletStateResponse, NKikimrWhiteboard::TEvTabletStateResponse, EvTabletStateResponse> {
        TEvTabletStateResponsePacked5* AllocatePackedResponse(size_t count);
    };

    struct TEvPDiskStateUpdate : TEventPB<TEvPDiskStateUpdate, NKikimrWhiteboard::TPDiskStateInfo, EvPDiskStateUpdate> {
        TEvPDiskStateUpdate() = default;

        TEvPDiskStateUpdate(ui32 pDiskId, const TString& path, ui64 guid, ui64 category);

        TEvPDiskStateUpdate(ui32 pDiskId, ui64 availableSize, ui64 totalSize, ui32 state);

        TEvPDiskStateUpdate(ui32 pDiskId, NKikimrWhiteboard::EFlag realtime, NKikimrWhiteboard::EFlag device);
    };

    struct TEvPDiskStateRequest : public TEventPB<TEvPDiskStateRequest, NKikimrWhiteboard::TEvPDiskStateRequest, EvPDiskStateRequest> {};

    struct TEvPDiskStateResponse : public TEventPB<TEvPDiskStateResponse, NKikimrWhiteboard::TEvPDiskStateResponse, EvPDiskStateResponse> {};

    // Local publications only; requests never fan out to DDisk/PB actors.
    struct TEvDDiskStateUpdate : TEventLocal<TEvDDiskStateUpdate, EvDDiskStateUpdate> {
        NKikimrWhiteboard::TDDiskStateInfo Record;
        ui64 OwnerRound = 0;
        TDuration Lifetime;
    };

    struct TEvDDiskStateDelete : TEventLocal<TEvDDiskStateDelete, EvDDiskStateDelete> {
        ui32 PDiskId;
        ui32 DDiskSlotId;
        ui64 OwnerRound;

        TEvDDiskStateDelete(ui32 pdiskId, ui32 slotId, ui64 ownerRound)
            : PDiskId(pdiskId)
            , DDiskSlotId(slotId)
            , OwnerRound(ownerRound)
        {}
    };

    struct TEvVDiskStateUpdate : TEventPB<TEvVDiskStateUpdate, NKikimrWhiteboard::TVDiskStateInfo, EvVDiskStateUpdate> {
        const bool Initial = false;

        TEvVDiskStateUpdate() = default;

        // this message is generated by NodeWarden
        explicit TEvVDiskStateUpdate(const TVDiskID& vDiskId, const TMaybe<TString>& storagePoolName, ui32 pDiskId,
                ui32 vDiskSlotId, ui64 guid, NKikimrBlobStorage::TVDiskKind::EVDiskKind kind, bool donorMode,
                ui64 instanceGuid, std::vector<NKikimrBlobStorage::TVSlotId>&& donors, ui32 groupSizeInUnits);

        explicit TEvVDiskStateUpdate(NKikimrWhiteboard::TVDiskSatisfactionRank *satisfactionRank) {
            Record.MutableSatisfactionRank()->Swap(satisfactionRank);
        }

        explicit TEvVDiskStateUpdate(NKikimrWhiteboard::EVDiskState state,
                                     NKikimrWhiteboard::EFlag diskSpace,
                                     bool replicated,
                                     bool unreplicatedPhantoms,
                                     bool unreplicatedNonPhantoms,
                                     NKikimrWhiteboard::TVDiskDetailedReplicationStatus::E detailedReplicationStatus,
                                     ui64 unsyncedVDisks,
                                     NKikimrWhiteboard::EFlag frontQueuesLigth,
                                     bool hasUnreadableBlobs);

        static constexpr struct TUpdateIncarnationGuid {} UpdateIncarnationGuid{};

        explicit TEvVDiskStateUpdate(TUpdateIncarnationGuid, ui64 incarnationGuid) {
            Record.SetIncarnationGuid(incarnationGuid);
        }

        explicit TEvVDiskStateUpdate(NKikimrWhiteboard::TVDiskStateInfo&& rec) {
            Record = std::move(rec);
        }
    };

    struct TEvVDiskStateDelete : TEventPB<TEvVDiskStateDelete, NKikimrWhiteboard::TVDiskStateInfo, EvVDiskStateDelete> {
        TEvVDiskStateDelete() = default;

        explicit TEvVDiskStateDelete(const TVDiskID& vDiskId) {
            VDiskIDFromVDiskID(vDiskId, Record.MutableVDiskId());
        }
    };

    struct TEvVDiskStateGenerationChange : TEventLocal<TEvVDiskStateGenerationChange, EvVDiskStateGenerationChange> {
        const TVDiskID VDiskId;
        const ui32 Generation;
        const ui64 InstanceGuid;

        TEvVDiskStateGenerationChange(const TVDiskID& vdiskId, ui32 generation, ui64 instanceGuid)
            : VDiskId(vdiskId)
            , Generation(generation)
            , InstanceGuid(instanceGuid)
        {}
    };

    struct TEvVDiskDropDonors : TEventLocal<TEvVDiskDropDonors, EvVDiskDropDonors> {
        const TVDiskID VDiskId;
        const ui64 InstanceGuid;
        const std::vector<NKikimrBlobStorage::TVSlotId> DropDonors;
        const bool DropAllDonors = false;

        TEvVDiskDropDonors(TVDiskID vdiskId, ui64 instanceGuid, std::vector<NKikimrBlobStorage::TVSlotId> dropDonors)
            : VDiskId(vdiskId)
            , InstanceGuid(instanceGuid)
            , DropDonors(std::move(dropDonors))
        {}

        struct TDropAllDonors {};

        TEvVDiskDropDonors(TVDiskID vdiskId, ui64 instanceGuid, TDropAllDonors)
            : VDiskId(vdiskId)
            , InstanceGuid(instanceGuid)
            , DropAllDonors(true)
        {}
    };

    struct TEvPDiskStateDelete : TEventPB<TEvPDiskStateDelete, NKikimrWhiteboard::TPDiskStateInfo, EvPDiskStateDelete> {
        TEvPDiskStateDelete() = default;

        explicit TEvPDiskStateDelete(const ui32& pdiskId) {
            Record.SetPDiskId(pdiskId);
        }
    };


    struct TEvVDiskStateRequest : public TEventPB<TEvVDiskStateRequest, NKikimrWhiteboard::TEvVDiskStateRequest, EvVDiskStateRequest> {};

    struct TEvVDiskStateResponse : public TEventPB<TEvVDiskStateResponse, NKikimrWhiteboard::TEvVDiskStateResponse, EvVDiskStateResponse> {};

    struct TEvBSGroupStateUpdate : TEventPB<TEvBSGroupStateUpdate, NKikimrWhiteboard::TBSGroupStateInfo, EvBSGroupStateUpdate> {
        TEvBSGroupStateUpdate() = default;

        TEvBSGroupStateUpdate(const TIntrusivePtr<TBlobStorageGroupInfo>& groupInfo);
    };

    struct TEvBSGroupStateDelete : TEventPB<TEvBSGroupStateDelete, NKikimrWhiteboard::TBSGroupStateInfo, EvBSGroupStateDelete> {
        TEvBSGroupStateDelete() = default;

        TEvBSGroupStateDelete(ui32 groupID) {
            Record.SetGroupID(groupID);
        }
    };

    struct TEvBSGroupStateRequest : public TEventPB<TEvBSGroupStateRequest, NKikimrWhiteboard::TEvBSGroupStateRequest, EvBSGroupStateRequest> {};

    struct TEvBSGroupStateResponse : public TEventPB<TEvBSGroupStateResponse, NKikimrWhiteboard::TEvBSGroupStateResponse, EvBSGroupStateResponse> {};

    struct TEvSystemStateUpdate : TEventPB<TEvSystemStateUpdate, NKikimrWhiteboard::TSystemStateInfo, EvSystemStateUpdate> {
        TEvSystemStateUpdate() = default;

        TEvSystemStateUpdate(
                TInstant startTime,
                ui32 numberOfCpus,
                const TString& version);

        TEvSystemStateUpdate(
                TInstant startTime,
                ui32 numberOfCpus);

        TEvSystemStateUpdate(const TVector<double>& loadAverage);

        TEvSystemStateUpdate(const TVector<std::tuple<TString, double, ui32, ui32>>& poolStats);

        TEvSystemStateUpdate(const TNodeLocation& systemLocation) {
            systemLocation.Serialize(Record.MutableLocation(), false);
        }

        TEvSystemStateUpdate(const NKikimrWhiteboard::TSystemStateInfo& systemStateInfo) {
            Record.CopyFrom(systemStateInfo);
        }
    };

    struct TEvMemoryStatsUpdate : TEventPB<TEvMemoryStatsUpdate, NKikimrMemory::TMemoryStats, EvMemoryStatsUpdate> {};

    static TEvSystemStateUpdate *CreateTotalSessionsUpdateRequest(ui32 totalSessions);

    struct TEvSystemStateAddEndpoint : TEventLocal<TEvSystemStateAddEndpoint, EvSystemStateAddEndpoint> {
        TString Name;
        TString Address;

        TEvSystemStateAddEndpoint(const TString& name, const TString& address)
            : Name(name)
            , Address(address)
        {}
    };

    struct TEvSystemStateAddRole : TEventLocal<TEvSystemStateAddRole, EvSystemStateAddRole> {
        TString Role;

        TEvSystemStateAddRole(const TString& role)
            : Role(role)
        {}
    };

    struct TEvSystemStateSetTenant : TEventLocal<TEvSystemStateSetTenant, EvSystemStateSetTenant> {
        TString Tenant;

        TEvSystemStateSetTenant(const TString& tenant)
            : Tenant(tenant)
        {}
    };

    struct TEvSystemStateRemoveTenant : TEventLocal<TEvSystemStateRemoveTenant, EvSystemStateRemoveTenant> {
        TString Tenant;

        TEvSystemStateRemoveTenant(const TString& tenant)
            : Tenant(tenant)
        {}
    };

    struct TEvSystemStateRequest : public TEventPB<TEvSystemStateRequest, NKikimrWhiteboard::TEvSystemStateRequest, EvSystemStateRequest> {};

    struct TEvSystemStateResponse : public TEventPB<TEvSystemStateResponse, NKikimrWhiteboard::TEvSystemStateResponse, EvSystemStateResponse> {};

    struct TEvNodeStateUpdate : TEventPB<TEvNodeStateUpdate, NKikimrWhiteboard::TNodeStateInfo, EvNodeStateUpdate> {
        TEvNodeStateUpdate() = default;
    };

    struct TEvNodeStateDelete : TEventPB<TEvNodeStateDelete, NKikimrWhiteboard::TNodeStateInfo, EvNodeStateDelete> {
        TEvNodeStateDelete() = default;

        TEvNodeStateDelete(const TString& peerName) {
            Record.SetPeerName(peerName);
        }
    };

    struct TEvNodeStateRequest : TEventPB<TEvNodeStateRequest, NKikimrWhiteboard::TEvNodeStateRequest, EvNodeStateRequest>
    {};

    struct TEvNodeStateResponse : TEventPB<TEvNodeStateResponse, NKikimrWhiteboard::TEvNodeStateResponse, EvNodeStateResponse>
    {};

    struct TEvIntrospectionData : TEventLocal<TEvIntrospectionData, EvIntrospectionData> {

        THolder<NTracing::ITrace> Trace;
        TTabletId TabletId;

        TEvIntrospectionData(TTabletId tabletId, NTracing::ITrace* trace)
            : Trace(trace)
            , TabletId(tabletId)
        {}
    };

    struct TEvTabletLookupRequest : TEventPB<TEvTabletLookupRequest, NKikimrWhiteboard::TEvTabletLookupRequest, EvTabletLookupRequest> {};
    struct TEvTabletLookupResponse : TEventPB<TEvTabletLookupResponse, NKikimrWhiteboard::TEvTabletLookupResponse, EvTabletLookupResponse> {};

    struct TEvTraceLookupRequest : TEventPB<TEvTraceLookupRequest, NKikimrWhiteboard::TEvTraceLookupRequest, EvTraceLookupRequest> {};
    struct TEvTraceLookupResponse : TEventPB<TEvTraceLookupResponse, NKikimrWhiteboard::TEvTraceLookupResponse, EvTraceLookupResponse> {};

    struct TEvTraceRequest : TEventPB<TEvTraceRequest, NKikimrWhiteboard::TEvTraceRequest, EvTraceRequest> {};
    struct TEvTraceResponse : TEventPB<TEvTraceResponse, NKikimrWhiteboard::TEvTraceResponse, EvTraceResponse> {};

    struct TEvSignalBodyRequest : TEventPB<TEvSignalBodyRequest, NKikimrWhiteboard::TEvSignalBodyRequest, EvSignalBodyRequest> {};
    struct TEvSignalBodyResponse : TEventPB<TEvSignalBodyResponse, NKikimrWhiteboard::TEvSignalBodyResponse, EvSignalBodyResponse> {};
};

inline TActorId MakeNodeWhiteboardServiceId(ui32 node) {
    char x[12] = {'n','o','d','e','w','h','i','t','e','b','o','a'};
    return TActorId(node, TStringBuf(x, 12));
}

IActor* CreateNodeWhiteboardService();

template<typename TRequestType>
struct WhiteboardResponse {};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvTabletStateRequest> {
    using Type = TEvWhiteboard::TEvTabletStateResponse;
};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvPDiskStateRequest> {
    using Type = TEvWhiteboard::TEvPDiskStateResponse;
};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvVDiskStateRequest> {
    using Type = TEvWhiteboard::TEvVDiskStateResponse;
};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvSystemStateRequest> {
    using Type = TEvWhiteboard::TEvSystemStateResponse;
};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvBSGroupStateRequest> {
    using Type = TEvWhiteboard::TEvBSGroupStateResponse;
};

template<>
struct WhiteboardResponse<TEvWhiteboard::TEvNodeStateRequest> {
    using Type = TEvWhiteboard::TEvNodeStateResponse;
};

template<typename TResponseType>
::google::protobuf::RepeatedField<int> GetDefaultWhiteboardFields();

} // NKikimr::NNodeWhiteboard
