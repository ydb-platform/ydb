#pragma once

#include <util/stream/format.h>
#include <util/string/hex.h>

#include <ydb/core/base/group_stat.h>
#include <ydb/core/base/blobstorage.h>

#include <ydb/core/protos/blobstorage.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr {

    struct TEvGroupStatReport : TEventPB<TEvGroupStatReport, NKikimrBlobStorage::TEvGroupStatReport, TEvBlobStorage::EvGroupStatReport> {
        TEvGroupStatReport() = default;

        TEvGroupStatReport(const TActorId& vdiskServiceId, ui32 groupId, const TGroupStat& stat)
        {
            Record.SetGroupId(groupId);
            ActorIdToProto(vdiskServiceId, Record.MutableVDiskServiceId());
            stat.Serialize(&Record);
        }

        bool GetStat(TGroupStat& s) const {
            return s.Deserialize(Record);
        }

        ui32 GetGroupId() const {
            return Record.GetGroupId();
        }

        TActorId GetVDiskServiceId() const {
            return ActorIdFromProto(Record.GetVDiskServiceId());
        }
    };

    inline TActorId MakeGroupStatAggregatorId(ui32 nodeId, ui32 pdiskId, ui32 vdiskSlotId) {
        char x[12] = {'b','s','g','s'};
        x[4] = (char)pdiskId;
        x[5] = (char)(pdiskId >> 8);
        x[6] = (char)(pdiskId >> 16);
        x[7] = (char)(pdiskId >> 24);
        x[8] = (char)vdiskSlotId;
        x[9] = (char)(vdiskSlotId >> 8);
        x[10] = (char)(vdiskSlotId >> 16);
        x[11] = (char)(vdiskSlotId >> 24);
        return TActorId(nodeId, TStringBuf(x, 12));
    }

    inline TActorId MakeGroupStatAggregatorId(const TActorId& vdiskServiceId) {
        Y_ABORT_UNLESS(vdiskServiceId.IsService());
        char x[12];
        TStringBuf serviceId = vdiskServiceId.ServiceId();
        Y_VERIFY_S(serviceId[0] == 'b' && serviceId[1] == 's' && serviceId[2] == 'v' && serviceId[3] == 'd',
                "Invalid VDisk's, HexEncode(ServiceId)# " << HexEncode(serviceId));
        memcpy(x, serviceId.data(), serviceId.size());
        x[0] = 'b';
        x[1] = 's';
        x[2] = 'g';
        x[3] = 's';
        return TActorId(vdiskServiceId.NodeId(), TStringBuf(x, 12));
    }

    NActors::IActor *CreateGroupStatAggregatorActor(ui32 groupId, const TActorId& vdiskServiceId);

} // NKikimr
