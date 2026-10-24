#include "mon_restore_part.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/json/writer/json_value.h>

namespace NKikimr {
namespace {

    class TMonRestorePartRequestActor : public TActorBootstrapped<TMonRestorePartRequestActor> {
        NMon::TEvHttpInfo::TPtr Ev;
        TVDiskID VDiskId;
        TLogoBlobID BlobId;
        TInstant Deadline;
        TActorId TargetId;
        TActorId PipeClientId;

        bool ParseVDiskId(TStringBuf value) {
            if (!value.StartsWith('[') || !value.EndsWith(']')) {
                return false;
            }
            value.Skip(1);
            value.Chop(1);
            ui32 fields[5];
            for (ui32 i = 0; i < std::size(fields); ++i) {
                TStringBuf field;
                if (i + 1 == std::size(fields)) {
                    field = value;
                } else if (!value.TrySplit(':', field, value)) {
                    return false;
                }
                if (!(i ? TryFromString(field, fields[i]) : TryIntFromString<16>(field, fields[i]))
                        || (i >= 2 && fields[i] > Max<ui8>())) {
                    return false;
                }
            }
            VDiskId = TVDiskID(fields[0], fields[1], fields[2], fields[3], fields[4]);
            return VDiskId.GroupGeneration != 0;
        }

        // TLogoBlobID constructors assert on overflowing fields.
        bool ParseBlobId(TStringBuf value) {
            if (!value.StartsWith('[') || !value.EndsWith(']')) {
                return false;
            }
            value.Skip(1);
            value.Chop(1);
            const ui64 limits[] = {Max<ui64>(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxCookie, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxPartId};
            ui64 fields[std::size(limits)];
            for (ui32 i = 0; i < std::size(limits); ++i) {
                TStringBuf field;
                if (i + 1 == std::size(limits)) {
                    field = value;
                } else if (!value.TrySplit(':', field, value)) {
                    return false;
                }
                if (!TryFromString(field, fields[i]) || fields[i] > limits[i]) {
                    return false;
                }
            }
            BlobId = TLogoBlobID(fields[0], fields[1], fields[2], fields[3], fields[5], fields[4], fields[6], 0);
            return BlobId && BlobId.BlobSize() && BlobId.PartId();
        }

        void Reply(NKikimrProto::EReplyStatus status, const TString& error = {}, bool invalidRequest = false) {
            const char* httpStatus = "500 Internal Server Error";
            switch (status) {
                case NKikimrProto::OK: httpStatus = "200 OK"; break;
                case NKikimrProto::NODATA:
                case NKikimrProto::NOT_FOUND: httpStatus = "404 Not Found"; break;
                case NKikimrProto::RACE: httpStatus = "409 Conflict"; break;
                case NKikimrProto::DEADLINE: httpStatus = "504 Gateway Timeout"; break;
                case NKikimrProto::NOTREADY:
                case NKikimrProto::VDISK_ERROR_STATE: httpStatus = "503 Service Unavailable"; break;
                default: break;
            }
            if (invalidRequest) {
                httpStatus = "400 Bad Request";
            }
            NJson::TJsonValue json(NJson::JSON_MAP);
            json["Status"] = NKikimrProto::EReplyStatus_Name(status);
            json["BlobId"] = BlobId.ToString();
            json["VDiskId"] = VDiskId.ToString();
            if (error) {
                json["ErrorReason"] = error;
            }
            NJsonWriter::TBuf writer;
            writer.WriteJsonValue(&json);
            TStringStream out;
            out << "HTTP/1.1 " << httpStatus << "\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                << writer.Str();
            Send(Ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), Ev->Get()->SubRequestId,
                NMon::TEvHttpInfoRes::Custom), 0, Ev->Cookie);
            PassAway();
        }

        void QueryVSlots() {
            PipeClientId = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(),
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            NTabletPipe::SendData(SelfId(), PipeClientId, new NSysView::TEvSysView::TEvGetVSlotsRequest);
        }

        void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr ev) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClientId);
            // Donor VSlots may still carry the same position from an older generation.
            const NKikimrSysView::TVSlotEntry* target = nullptr;
            for (const auto& entry : ev->Get()->Record.GetEntries()) {
                const auto& info = entry.GetInfo();
                if (info.GetGroupId() == VDiskId.GroupID.GetRawId()
                        && info.GetFailRealm() == VDiskId.FailRealm && info.GetFailDomain() == VDiskId.FailDomain
                        && info.GetVDisk() == VDiskId.VDisk && !info.GetIsBeingDeleted() && !info.GetDDisk()
                        && (!target || info.GetGroupGeneration() > target->GetInfo().GetGroupGeneration())) {
                    target = &entry;
                }
            }
            if (!target) {
                return Reply(NKikimrProto::NOT_FOUND, "VDiskId was not found in BSController VSlots");
            }
            if (target->GetInfo().GetGroupGeneration() != VDiskId.GroupGeneration) {
                return Reply(NKikimrProto::RACE, "VDiskId does not match the current generation in BSController VSlots");
            }
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }
            const auto& key = target->GetKey();
            TargetId = MakeBlobStorageVDiskID(key.GetNodeId(), key.GetPDiskId(), key.GetVSlotId());
            auto request = std::make_unique<TEvVRestorePart>();
            VDiskIDFromVDiskID(VDiskId, request->Record.MutableVDiskID());
            LogoBlobIDFromLogoBlobID(BlobId, request->Record.MutableBlobID());
            request->Record.SetDeadline(Deadline.GetValue());
            ui32 flags = IEventHandle::FlagTrackDelivery;
            if (TargetId.NodeId() != SelfId().NodeId()) {
                flags |= IEventHandle::FlagSubscribeOnSession;
            }
            Send(TargetId, request.release(), flags);
            Become(&TThis::StateVRestorePart);
        }

        void Handle(TEvVRestorePartResult::TPtr ev) {
            const auto& record = ev->Get()->Record;
            Reply(record.HasStatus() ? record.GetStatus() : NKikimrProto::ERROR,
                record.GetErrorReason(), record.GetInvalidRequest());
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            if (ev->Get()->Status != NKikimrProto::OK) {
                Reply(NKikimrProto::NOTREADY, "BSController is unavailable");
            }
        }

        void HandleControllerUnavailable() {
            Reply(NKikimrProto::NOTREADY, "Connection to BSController was lost");
        }

        void HandleTimeout() {
            Reply(NKikimrProto::DEADLINE, "Timed out waiting for VRestorePart");
        }

        void HandleUnavailable() {
            Reply(NKikimrProto::NOTREADY, "Target VDisk is unavailable");
        }

        void PassAway() override {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClientId);
            if (TargetId && TargetId.NodeId() != SelfId().NodeId()) {
                Send(TActivationContext::InterconnectProxy(TargetId.NodeId()), new TEvents::TEvUnsubscribe);
            }
            TActorBootstrapped::PassAway();
        }

    public:
        explicit TMonRestorePartRequestActor(NMon::TEvHttpInfo::TPtr ev)
            : Ev(ev)
        {}

        void Bootstrap() {
            const auto& params = Ev->Get()->Request.GetParams();
            if (!ParseVDiskId(params.Get("VDiskId"))) {
                return Reply(NKikimrProto::ERROR,
                    "Invalid VDiskId; expected [groupIdHex:generation:realm:domain:vdisk] with a nonzero generation", true);
            }
            if (!ParseBlobId(params.Get("BlobId"))) {
                return Reply(NKikimrProto::ERROR,
                    "Invalid BlobId; expected [tablet:gen:step:channel:cookie:blobsize:partid] with a nonzero PartId", true);
            }
            ui32 timeout = 30;
            if (params.Has("timeout") && (!TryFromString(params.Get("timeout"), timeout) || !timeout || timeout > 300)) {
                return Reply(NKikimrProto::ERROR, "timeout must be an integer in [1, 300] seconds", true);
            }
            Deadline = TActivationContext::Now() + TDuration::Seconds(timeout);
            Become(&TThis::StateResolve, Deadline, new TEvents::TEvWakeup);
            QueryVSlots();
        }

        STRICT_STFUNC(StateResolve,
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvTabletPipe::EvClientDestroyed, HandleControllerUnavailable);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        )

        STRICT_STFUNC(StateVRestorePart,
            hFunc(TEvVRestorePartResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            cFunc(TEvents::TSystem::Undelivered, HandleUnavailable);
            cFunc(TEvInterconnect::EvNodeDisconnected, HandleUnavailable);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            IgnoreFunc(TEvTabletPipe::TEvClientConnected);
            IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
        )
    };

    class TMonRestorePartActor : public TActor<TMonRestorePartActor> {
        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            Register(new TMonRestorePartRequestActor(ev));
        }

    public:
        TMonRestorePartActor()
            : TActor(&TThis::StateFunc)
        {}

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
        )
    };

} // anonymous namespace

IActor* CreateMonRestorePartActor() {
    return new TMonRestorePartActor;
}

} // NKikimr
