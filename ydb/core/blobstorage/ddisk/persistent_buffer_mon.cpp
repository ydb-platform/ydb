#include "persistent_buffer_mon.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/lwtrace/all.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/util/stlog.h>

#include <util/generic/queue.h>
#include <util/system/types.h>
#include <util/system/mutex.h>

using namespace NActors;

namespace NKikimr {

    using namespace NLWTrace;

    namespace {

        class TPersistentBufferMonActor : public TActorBootstrapped<TPersistentBufferMonActor> {
            struct TInflight {
                TActorId Sender;
                ui64 Cookie;
                int SubRequestId;
            };

            ui64 NextCookie = 0;
            std::unordered_map<ui64, TInflight> Inflight;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_PERSISTENT_BUFFER;
            }

            TPersistentBufferMonActor(const NKikimrConfig::TAppConfig& /*config*/, const NKikimr::TAppData& /*appData*/)
            {}

            void Bootstrap(const TActorContext& /*ctx*/) {
                Become(&TPersistentBufferMonActor::StateFunc);
            }

            void Handle(TEvNodeWardenListLocalDDisksResult::TPtr& ev) {
                auto cookie = ev->Cookie;
                STLOG(PRI_DEBUG, BS_DDISK, BSDD18, "TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult)", (cookie, cookie));
                auto it = Inflight.find(cookie);
                Y_ABORT_UNLESS(it != Inflight.end());
                auto& inflight = it->second;

                TStringStream str;
                HTML(str) {
                    str << "<h1>" << "Persistent buffers" << "</font></h1>";
                    for (auto r : ev->Get()->Infos) {
                        str << "<h2>" << "PB actorId: " << r.PersistentBufferId << "</font></h2>";

                    }
                }
                Send(inflight.Sender, new NMon::TEvHttpInfoRes(str.Str(), inflight.SubRequestId), inflight.Cookie);
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev) {
                const ui64 cookie = ++NextCookie;
                Inflight[cookie] = {ev->Sender, ev->Cookie, ev->Get()->SubRequestId};
                auto nwId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                Send(nwId, new TEvNodeWardenListLocalDDisks(), 0, cookie);
                STLOG(PRI_DEBUG, BS_DDISK, BSDD18, "TPersistentBufferMonActor::Handle(TEvHttpInfo)", (cookie, cookie));
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NMon::TEvHttpInfo, Handle)
                hFunc(TEvNodeWardenListLocalDDisksResult, Handle)
            )
        };

    } // anon

    IActor *CreateMonPersistentBufferActor(const NKikimrConfig::TAppConfig& config, const NKikimr::TAppData& appData) {
        return new TPersistentBufferMonActor(config, appData);
    }

} // NKikimr
