#include "mon_get_blob_page.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonVDiskStreamActor
        : public TActor<TMonVDiskStreamActor>
    {
        ui64 LastCookie = 0;
        THashMap<ui64, std::tuple<TActorId, ui64, int>> RequestsInFlight;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_GET_ACTOR;
        }

        TMonVDiskStreamActor()
            : TActor(&TThis::StateFunc)
        {}

        void Handle(NMon::TEvHttpInfo::TPtr ev) {
            // parse HTTP request
            const TCgiParameters& params = ev->Get()->Request.GetParams();

            auto generateError = [&](const TString& msg) {
                TStringStream out;

                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << msg << "\r\n";

                Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom),
                    0, ev->Cookie);
            };

            ui32 pdiskId = 0;
            if (!params.Has("pdiskId")) {
                return generateError("Missing pdiskId parameter");
            } else if (!TryFromString(params.Get("pdiskId"), pdiskId)) {
                return generateError("Invalid pdiskId value");
            }

            ui32 vdiskSlotId = 0;
            if (!params.Has("vdiskSlotId")) {
                return generateError("Missing vdiskSlotId parameter");
            } else if (!TryFromString(params.Get("vdiskSlotId"), vdiskSlotId)) {
                return generateError("Invalid vdiskSlotId value");
            }

            TString sessionId;
            if (!params.Has("sessionId")) {
                return generateError("Missing sessionId parameter");
            } else {
                sessionId = params.Get("sessionId");
            }

            const ui64 cookie = ++LastCookie;
            Send(MakeBlobStorageVDiskID(SelfId().NodeId(), pdiskId, vdiskSlotId),
                new TEvBlobStorage::TEvMonStreamQuery(sessionId, {}, {}),
                IEventHandle::FlagTrackDelivery, cookie);
            RequestsInFlight[cookie] = std::make_tuple(ev->Sender, ev->Cookie, ev->Get()->SubRequestId);
        }

        template<typename TCallback>
        void Reply(ui64 cookie, TCallback&& callback) {
            const auto it = RequestsInFlight.find(cookie);
            Y_ABORT_UNLESS(it != RequestsInFlight.end());
            const auto& [sender, senderCookie, subRequestId] = it->second;
            TStringStream out;
            callback(out);
            Send(sender, new NMon::TEvHttpInfoRes(out.Str(), subRequestId, NMon::TEvHttpInfoRes::Custom), 0, senderCookie);
            RequestsInFlight.erase(it);
        }

        void Handle(NMon::TEvHttpInfoRes::TPtr ev) {
            Reply(ev->Cookie, [&](IOutputStream& out) {
                out << "HTTP/1.1 200 OK\r\n"
                    << "Content-Type: application/octet-stream\r\n"
                    << "Connection: close\r\n"
                    << "\r\n";
                ev->Get()->Output(out);
            });
        }

        void Handle(TEvents::TEvUndelivered::TPtr ev) {
            Reply(ev->Cookie, [&](IOutputStream& out) {
                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << "VDisk actor not found\r\n";
            });
        }

        STRICT_STFUNC(StateFunc,
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(NMon::TEvHttpInfoRes, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        )
    };

} // anon

IActor *CreateMonVDiskStreamActor() {
    return new TMonVDiskStreamActor();
}

} // NKikimr
