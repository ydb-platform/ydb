#include "mon_restore_part.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <google/protobuf/text_format.h>

#include <util/string/strip.h>

#include <map>
#include <set>
#include <tuple>
#include <vector>

namespace NKikimr {
namespace {

    class TMonRestorePartRequestActor : public TActorBootstrapped<TMonRestorePartRequestActor> {
        using TResult = NKikimrBlobStorage::TEvVRestorePartResult;

        struct TItem {
            TVDiskID VDiskId;
            TLogoBlobID BlobId;
            TActorId TargetId;
            NKikimrProto::EReplyStatus Status = NKikimrProto::UNKNOWN;
            TString ErrorReason;
            TResult::EResponseOrigin ResponseOrigin = TResult::MON_RESTORE_PART_REQUEST_ACTOR;
            bool InFlight = false;
        };

        NMon::TEvHttpInfo::TPtr Ev;
        std::vector<TItem> Items;
        TInstant Deadline;
        TActorId PipeClientId;
        std::set<ui32> SubscribedNodes;
        size_t NextItem = 0;
        size_t NumInFlight = 0;
        ui32 InflightLimit = 1;

        static bool ParseVDiskId(TStringBuf value, TVDiskID& vdiskId) {
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
            vdiskId = TVDiskID(fields[0], fields[1], fields[2], fields[3], fields[4]);
            return vdiskId.GroupGeneration != 0;
        }

        // TLogoBlobID constructors assert on overflowing fields.
        static bool ParseBlobId(TStringBuf value, TLogoBlobID& blobId) {
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
            blobId = TLogoBlobID(fields[0], fields[1], fields[2], fields[3], fields[5], fields[4], fields[6], 0);
            return blobId && blobId.BlobSize();
        }

        void Reply(NKikimrProto::EReplyStatus status, const TString& error = {}, bool invalidRequest = false) {
            const char* httpStatus = "500 Internal Server Error";
            switch (status) {
                case NKikimrProto::ALREADY:
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
            TStringStream out;
            out << "HTTP/1.1 " << httpStatus << "\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n";
            NProtoBuf::TextFormat::Printer printer;
            printer.SetSingleLineMode(true);
            auto writeResult = [&](NKikimrProto::EReplyStatus itemStatus, const TString& reason, const TItem* item = nullptr) {
                TResult result;
                result.SetStatus(itemStatus);
                result.SetResponseOrigin(item ? item->ResponseOrigin : TResult::MON_RESTORE_PART_REQUEST_ACTOR);
                if (reason) {
                    result.SetErrorReason(reason);
                }
                if (item) {
                    out << item->VDiskId << ' ' << item->BlobId << ' ';
                }
                TString text;
                printer.PrintToString(result, &text);
                out << StripString(text) << '\n';
            };
            if (Items.empty()) {
                writeResult(status, error);
            } else {
                for (const auto& item : Items) {
                    writeResult(item.Status, item.ErrorReason, &item);
                }
            }
            Send(Ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), Ev->Get()->SubRequestId,
                NMon::TEvHttpInfoRes::Custom), 0, Ev->Cookie);
            PassAway();
        }

        void FinishPending(NKikimrProto::EReplyStatus status, const TString& error) {
            for (auto& item : Items) {
                if (item.Status == NKikimrProto::UNKNOWN) {
                    item.Status = status;
                    item.ErrorReason = error;
                }
            }
            Finish();
        }

        void Finish() {
            for (const auto& item : Items) {
                if (item.Status != NKikimrProto::OK && item.Status != NKikimrProto::ALREADY) {
                    return Reply(item.Status);
                }
            }
            Reply(NKikimrProto::OK);
        }

        void SendNext() {
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }
            while (NextItem < Items.size() && NumInFlight < InflightLimit) {
                const ui64 cookie = ++NextItem;
                auto& item = Items[cookie - 1];
                if (item.Status != NKikimrProto::UNKNOWN) {
                    continue;
                }
                auto request = std::make_unique<TEvVRestorePart>();
                VDiskIDFromVDiskID(item.VDiskId, request->Record.MutableVDiskID());
                LogoBlobIDFromLogoBlobID(item.BlobId, request->Record.MutableBlobID());
                request->Record.SetDeadline(Deadline.GetValue());
                ui32 flags = IEventHandle::FlagTrackDelivery;
                if (item.TargetId.NodeId() != SelfId().NodeId()) {
                    flags |= IEventHandle::FlagSubscribeOnSession;
                    SubscribedNodes.insert(item.TargetId.NodeId());
                }
                item.InFlight = true;
                ++NumInFlight;
                // The actor envelope cookie is echoed by TEvVRestorePartResult, including across nodes.
                Send(item.TargetId, request.release(), flags, cookie);
            }
            if (NextItem == Items.size() && !NumInFlight) {
                Finish();
            }
        }

        void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr ev) {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClientId);
            using TPosition = std::tuple<ui32, ui32, ui32, ui32>;
            std::map<TPosition, const NKikimrSysView::TVSlotEntry*> slots;
            for (const auto& entry : ev->Get()->Record.GetEntries()) {
                const auto& info = entry.GetInfo();
                if (info.GetIsBeingDeleted() || info.GetDDisk()) {
                    continue;
                }
                auto& target = slots[{info.GetGroupId(), info.GetFailRealm(), info.GetFailDomain(), info.GetVDisk()}];
                // Donor VSlots may still carry the same position from an older generation.
                if (!target || info.GetGroupGeneration() > target->GetInfo().GetGroupGeneration()) {
                    target = &entry;
                }
            }
            for (auto& item : Items) {
                const auto& id = item.VDiskId;
                auto it = slots.find({id.GroupID.GetRawId(), id.FailRealm, id.FailDomain, id.VDisk});
                if (it == slots.end()) {
                    item.Status = NKikimrProto::NOT_FOUND;
                    item.ErrorReason = "VDiskId was not found in BSController VSlots";
                } else if (it->second->GetInfo().GetGroupGeneration() != id.GroupGeneration) {
                    item.Status = NKikimrProto::RACE;
                    item.ErrorReason = "VDiskId does not match the current generation in BSController VSlots";
                } else {
                    const auto& key = it->second->GetKey();
                    item.TargetId = MakeBlobStorageVDiskID(key.GetNodeId(), key.GetPDiskId(), key.GetVSlotId());
                }
            }
            Become(&TThis::StateRestore);
            SendNext();
        }

        void Complete(ui64 cookie, NKikimrProto::EReplyStatus status, const TString& error,
                TResult::EResponseOrigin origin = TResult::MON_RESTORE_PART_REQUEST_ACTOR) {
            if (!cookie || cookie > Items.size() || !Items[cookie - 1].InFlight) {
                return; // A late or duplicate reply must not free another request's inflight slot.
            }
            auto& item = Items[cookie - 1];
            item.Status = status == NKikimrProto::UNKNOWN ? NKikimrProto::ERROR : status;
            item.ErrorReason = error;
            item.ResponseOrigin = origin;
            item.InFlight = false;
            --NumInFlight;
        }

        void Handle(TEvVRestorePartResult::TPtr ev) {
            const auto& record = ev->Get()->Record;
            Complete(ev->Cookie, record.HasStatus() ? record.GetStatus() : NKikimrProto::ERROR,
                record.GetErrorReason(), record.GetResponseOrigin());
            SendNext();
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev) {
            Complete(ev->Cookie, NKikimrProto::NOTREADY, "Target VDisk is unavailable");
            SendNext();
        }

        void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
            for (size_t i = 0; i < Items.size(); ++i) {
                auto& item = Items[i];
                if (item.Status == NKikimrProto::UNKNOWN && item.TargetId.NodeId() == ev->Get()->NodeId) {
                    if (item.InFlight) {
                        Complete(i + 1, NKikimrProto::NOTREADY, "Connection to target VDisk was lost");
                    } else {
                        item.Status = NKikimrProto::NOTREADY;
                        item.ErrorReason = "Connection to target VDisk was lost";
                    }
                }
            }
            SendNext();
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            if (ev->Get()->Status != NKikimrProto::OK) {
                FinishPending(NKikimrProto::NOTREADY, "BSController is unavailable");
            }
        }

        void HandleControllerUnavailable() {
            FinishPending(NKikimrProto::NOTREADY, "Connection to BSController was lost");
        }

        void HandleTimeout() {
            FinishPending(NKikimrProto::DEADLINE, "Timed out waiting for VRestorePart");
        }

        void PassAway() override {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeClientId);
            for (ui32 nodeId : SubscribedNodes) {
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe);
            }
            TActorBootstrapped::PassAway();
        }

    public:
        explicit TMonRestorePartRequestActor(NMon::TEvHttpInfo::TPtr ev)
            : Ev(ev)
        {}

        void Bootstrap() {
            const auto& params = Ev->Get()->Request.GetPostParams();
            ui32 timeout = 30;
            if (params.Has("timeout") && (!TryFromString(params.Get("timeout"), timeout) || !timeout)) {
                return Reply(NKikimrProto::ERROR, "timeout must be a positive integer in seconds", true);
            }
            if (params.Has("inflight") && (!TryFromString(params.Get("inflight"), InflightLimit)
                    || !InflightLimit)) {
                return Reply(NKikimrProto::ERROR, "inflight must be a positive integer", true);
            }
            TStringBuf text = params.Get("requests");
            ui32 lineNumber = 0;
            while (text) {
                auto line = StripString(text.NextTok('\n'));
                ++lineNumber;
                if (!line) {
                    continue;
                }
                const size_t separator = line.find_first_of(" \t");
                TItem item;
                const TStringBuf vdisk = line.SubStr(0, separator);
                const TStringBuf blob = separator == TStringBuf::npos ? TStringBuf() : StripString(line.SubStr(separator));
                if (!ParseVDiskId(vdisk, item.VDiskId) || !ParseBlobId(blob, item.BlobId)) {
                    Items.clear();
                    return Reply(NKikimrProto::ERROR, TStringBuilder() << "Invalid line " << lineNumber
                        << "; expected VDiskId BlobId: [groupIdHex:generation:realm:domain:vdisk] "
                        << "[tablet:gen:step:channel:cookie:blobsize:partid]", true);
                }
                Items.push_back(std::move(item));
            }
            if (Items.empty()) {
                return Reply(NKikimrProto::ERROR, "requests must contain at least one VDiskId BlobId line", true);
            }
            Deadline = TActivationContext::Now() + TDuration::Seconds(timeout);
            Become(&TThis::StateResolve, Deadline, new TEvents::TEvWakeup);
            PipeClientId = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(),
                NTabletPipe::TClientRetryPolicy::WithRetries()));
            NTabletPipe::SendData(SelfId(), PipeClientId, new NSysView::TEvSysView::TEvGetVSlotsRequest);
        }

        STRICT_STFUNC(StateResolve,
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvTabletPipe::EvClientDestroyed, HandleControllerUnavailable);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        )

        STRICT_STFUNC(StateRestore,
            hFunc(TEvVRestorePartResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            IgnoreFunc(TEvInterconnect::TEvNodeConnected);
            IgnoreFunc(TEvTabletPipe::TEvClientConnected);
            IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);
        )
    };

    class TMonRestorePartActor : public TActor<TMonRestorePartActor> {
        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            if (ev->Get()->Request.GetPath() != "/restore_part"
                    || ev->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
                Send(ev->Sender, new NMon::TEvHttpInfoRes(
                    "HTTP/1.1 405 Method Not Allowed\r\nAllow: POST\r\nConnection: close\r\n\r\n",
                    ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom), 0, ev->Cookie);
                return;
            }
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
