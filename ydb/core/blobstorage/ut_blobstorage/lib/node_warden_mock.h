#pragma once

#include "defs.h"

class TNodeWardenMockActor : public TActorBootstrapped<TNodeWardenMockActor> {
public:
    struct TPDiskId {
        ui32 NodeId;
        ui32 PDiskId;

        TPDiskId(ui32 nodeId, ui32 pdiskId)
            : NodeId(nodeId)
            , PDiskId(pdiskId)
        {}

        TString ToString() const {
            return TStringBuilder() << NodeId << ":" << PDiskId;
        }

        friend bool operator ==(const TPDiskId& x, const TPDiskId& y) {
            return x.NodeId == y.NodeId && x.PDiskId == y.PDiskId;
        }

        struct THash {
            size_t operator ()(const TPDiskId& x) const {
                return MultiHash(x.NodeId, x.PDiskId);
            }
        };
    };

    struct TVSlotId : TPDiskId {
        ui32 VSlotId;

        TVSlotId(ui32 nodeId, ui32 pdiskId, ui32 vslotId)
            : TPDiskId(nodeId, pdiskId)
            , VSlotId(vslotId)
        {}

        TString ToString() const {
            return TStringBuilder() << TPDiskId::ToString() << ":" << VSlotId;
        }

        friend bool operator ==(const TVSlotId& x, const TVSlotId& y) {
            return static_cast<const TPDiskId&>(x) == static_cast<const TPDiskId&>(y) && x.VSlotId == y.VSlotId;
        }

        struct THash {
            size_t operator ()(const TVSlotId& x) const {
                return MultiHash(TPDiskId::THash()(x), x.VSlotId);
            }
        };
    };

    struct TSetup : TThrRefBase {
        struct TPDiskInfo {
            ui64 Size; // total size of PDisk in bytes
        };
        struct TGroupInfo {
            ui64 SlotSize; // expected slot size of this group in bytes
        };

        ui64 TabletId;
        std::unordered_map<std::tuple<ui32, TString>, TPDiskInfo> PDisks;
        std::unordered_map<ui32, TGroupInfo> Groups;

        using TPtr = TIntrusivePtr<TSetup>;
    };

private:
    class TVDiskMockActor;

private:
    TSetup::TPtr Setup;

public:
    TNodeWardenMockActor(TSetup::TPtr setup);

    void Bootstrap();
    void OnConnected();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // State code
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct TGroupState;
    struct TVDiskState;
    struct TPDiskState;

    std::unordered_map<TPDiskId, std::shared_ptr<TPDiskState>, TPDiskId::THash> PDisks;
    std::unordered_map<TVSlotId, std::shared_ptr<TVDiskState>, TVSlotId::THash> VDisks;
    std::unordered_map<ui32, std::shared_ptr<TGroupState>> Groups;

    TPDiskState *GetPDisk(TPDiskId pdiskId);
    TVDiskState *GetVDisk(TVSlotId vslotId);
    TGroupState *GetGroup(ui32 groupId);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // BSC interface code
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendRegisterNode();
    void SendUpdateDiskStatus();
    void SendUpdateVDiskStatus(TVDiskState *vdisk);

    void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev);
    void Handle(TEvNodeWardenQueryStorageConfig::TPtr ev);
    void HandleUnsubscribe(STATEFN_SIG);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Pipe management code
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    TActorId PipeId; // actor id of the registered pipe client
    bool IsPipeConnected = false;

    void Connect();
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev);
    void ScheduleReconnect();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    STRICT_STFUNC(StateFunc,
        hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
        hFunc(TEvTabletPipe::TEvClientConnected, Handle);
        hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        cFunc(TEvents::TSystem::Wakeup, Connect);
        IgnoreFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate);
        hFunc(TEvNodeWardenQueryStorageConfig, Handle);
        fFunc(TEvents::TSystem::Unsubscribe, HandleUnsubscribe);
        IgnoreFunc(NStorage::TEvNodeWardenUpdateCache);
        IgnoreFunc(NStorage::TEvNodeWardenQueryCache);
        IgnoreFunc(NStorage::TEvNodeWardenUnsubscribeFromCache);
    )
};
