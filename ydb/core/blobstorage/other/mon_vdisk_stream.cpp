#include "mon_get_blob_page.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/threading/future/future.h>

namespace NKikimr {

namespace {

    class TMonVDiskPage
        : public NMonitoring::IMonPage
    {
        class TRequestActor
            : public TActorBootstrapped<TRequestActor>
        {
            const ui32 PDiskId;
            const ui32 VDiskSlotId;
            const TString SessionId;
            NThreading::TPromise<TString> Promise;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_GET_ACTOR;
            }

            TRequestActor(ui32 pdiskId, ui32 vdiskSlotId, TString sessionId, NThreading::TPromise<TString> promise)
                : PDiskId(pdiskId)
                , VDiskSlotId(vdiskSlotId)
                , SessionId(sessionId)
                , Promise(promise)
            {}

            void Bootstrap(const TActorContext& ctx) {
                ctx.Send(MakeBlobStorageVDiskID(ctx.SelfID.NodeId(), PDiskId, VDiskSlotId),
                    new TEvBlobStorage::TEvMonStreamQuery(SessionId, {}, {}));
                Become(&TRequestActor::StateFunc);
            }

            void Handle(NMon::TEvHttpInfoRes::TPtr& ev, const TActorContext& ctx) {
                TStringStream s;
                ev->Get()->Output(s);
                Promise.SetValue(s.Str());
                Die(ctx);
            }

            STRICT_STFUNC(StateFunc,
                HFunc(NMon::TEvHttpInfoRes, Handle)
            )
        };

    private:
        TActorSystem *ActorSystem;

    public:
        TMonVDiskPage(const TString& path, TActorSystem *actorSystem)
            : IMonPage(path)
            , ActorSystem(actorSystem)
        {}

        void Output(NMonitoring::IMonHttpRequest& request) override {
            IOutputStream& out = request.Output();

            // parse HTTP request
            const TCgiParameters& params = request.GetParams();

            auto generateError = [&](const TString& msg) {
                out << "HTTP/1.1 400 Bad Request\r\n"
                    << "Content-Type: text/plain\r\n"
                    << "Connection: close\r\n"
                    << "\r\n"
                    << msg << "\r\n";
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

            // create promise & future to obtain query result
            auto promise = NThreading::NewPromise<TString>();
            auto future = promise.GetFuture();

            // register and start actor
            ActorSystem->Register(new TRequestActor(pdiskId, vdiskSlotId, sessionId, promise));

            // wait for query to complete
            future.Wait();

            // obtain result
            const TString& result = future.GetValue();

            out << "HTTP/1.1 200 OK\r\n"
                << "Content-Type: application/octet-stream\r\n"
                << "Connection: close\r\n"
                << "\r\n"
                << result;
        }
    };

} // anon

NMonitoring::IMonPage *CreateMonVDiskStreamPage(const TString& path, TActorSystem *actorSystem) {
    return new TMonVDiskPage(path, actorSystem);
}

} // NKikimr
