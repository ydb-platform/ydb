#include "persistent_buffer_mon.h"
#include "ddisk.h"

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
                std::vector<NDDisk::TEvPersistentBufferInfo::TPtr> Responses;
                std::unordered_map<ui64, TActorId> Requests;
            };

            ui64 NextCookie = 0;
            std::unordered_map<ui64, TInflight> Inflight;
            std::unordered_map<ui64, ui64> PBuffersInflight;

        public:
            static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
                return NKikimrServices::TActivity::BS_PERSISTENT_BUFFER;
            }

            TPersistentBufferMonActor(const NKikimrConfig::TAppConfig& /*config*/, const NKikimr::TAppData& /*appData*/)
            {}

            void Bootstrap(const TActorContext& /*ctx*/) {
                Become(&TPersistentBufferMonActor::StateFunc);
            }

            void HandleWakeup(TEvents::TEvWakeup::TPtr &ev) {
                STLOG(PRI_DEBUG, BS_DDISK, BSDD32, "TPersistentBufferMonActor::HandleWakeup", (cookie, ev->Get()->Tag));
                for (auto [cookie, _] : Inflight[ev->Get()->Tag].Requests) {
                    PBuffersInflight.erase(cookie);
                }
                Reply(ev->Get()->Tag);
            }

            void Reply(ui64 cookie) {
                auto it = Inflight.find(cookie);
                Y_ABORT_UNLESS(it != Inflight.end());
                auto& inflight = it->second;

                TStringStream str;
                HTML(str) {
                    for (auto& [_, id] : inflight.Requests) {
                        str << "<h2 style=\"color:red;\">" << "No response from PB actorId: " << id << " </font></h2>";
                    }
                    for (auto& v : inflight.Responses) {
                        auto b = v->Get();
                        TDuration uptime = TInstant::Now() - b->StartedAt;
                        str << "<h2>" << "PB actorId: " << v->Sender << "</font></h2>";
                        str << "Uptime:" << uptime.ToString();
                        str << " Free sectors: " << b->FreeSectors;
                        str << " of allocated" << (b->AllocatedChunks * b->ChunkSize / b->SectorSize);
                        str << " max" << (b->MaxChunks * b->ChunkSize / b->SectorSize);
                        TABLE_CLASS ("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() {str << "TabletId";}
                                    TABLEH() {str << "Generation";}
                                    TABLEH() {str << "First lsn";}
                                    TABLEH() {str << "Timestamp";}
                                    TABLEH() {str << "Last lsn";}
                                    TABLEH() {str << "Timestamp";}
                                }
                            }
                            TABLEBODY() {
                                for (auto& ti : b->TabletInfos) {
                                    TABLER() {
                                        TABLED() {str << ti.TabletId;}
                                        TABLED() {str << ti.Generation;}
                                        TABLED() {str << ti.FirstLsn;}
                                        TABLED() {str << ti.FirstLsnTimestamp;}
                                        TABLED() {str << ti.LastLsn;}
                                        TABLED() {str << ti.LastLsnTimestamp;}
                                    }
                                }
                            }
                        }
                    }
                }
                Send(inflight.Sender, new NMon::TEvHttpInfoRes(str.Str(), inflight.SubRequestId), 0, inflight.Cookie);
                STLOG(PRI_DEBUG, BS_DDISK, BSDD39, "TPersistentBufferMonActor::Reply()", (Responses, inflight.Responses.size()), (Requests, inflight.Requests.size()));
                Inflight.erase(it);
            }

            void Handle(NDDisk::TEvPersistentBufferInfo::TPtr& ev) {
                auto reqCookie = ev->Cookie;
                auto cookie = PBuffersInflight[reqCookie];
                PBuffersInflight.erase(reqCookie);
                STLOG(PRI_DEBUG, BS_DDISK, BSDD33, "TPersistentBufferMonActor::Handle(TEvPersistentBufferInfo)",
                    (Sender, ev->Sender), (reqCookie, reqCookie), (cookie, cookie));
                auto it = Inflight.find(cookie);
                if (it == Inflight.end()) {
                    return;
                }
                auto& inflight = it->second;
                if (inflight.Requests.count(ev->Cookie) == 0) {
                    STLOG(PRI_ERROR, BS_DDISK, BSDD34, "TPersistentBufferMonActor::Handle(TEvPersistentBufferInfo) unknown persistent buffer",
                        (Sender, ev->Sender), (cookie, cookie));
                } else {
                    inflight.Requests.erase(ev->Cookie);
                }
                inflight.Responses.push_back(ev);
                if (inflight.Requests.empty()) {
                    Reply(cookie);
                }
            }

            void Handle(TEvNodeWardenListLocalDDisksResult::TPtr& ev) {
                auto cookie = ev->Cookie;
                STLOG(PRI_DEBUG, BS_DDISK, BSDD35, "TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult)", (cookie, cookie));
                auto it = Inflight.find(cookie);
                Y_ABORT_UNLESS(it != Inflight.end());
                auto& inflight = it->second;

                for (auto r : ev->Get()->Infos) {
                    auto reqCookie = ++NextCookie;
                    inflight.Requests.insert({reqCookie, r.PersistentBufferId});
                    PBuffersInflight[reqCookie] = cookie;
                    Send(r.PersistentBufferId, new NDDisk::TEvGetPersistentBufferInfo(), reqCookie);
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD36, "TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult) Send",
                        (r.PersistentBufferId, r.PersistentBufferId), (reqCookie, reqCookie));
                }
                Schedule(TDuration::MilliSeconds(5000), new TEvents::TEvWakeup(cookie));
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev) {
                const ui64 cookie = ++NextCookie;
                Inflight[cookie] = TInflight{
                    .Sender = ev->Sender,
                    .Cookie = ev->Cookie,
                    .SubRequestId = ev->Get()->SubRequestId,
                };
                auto nwId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                Send(nwId, new TEvNodeWardenListLocalDDisks(), 0, cookie);
                STLOG(PRI_DEBUG, BS_DDISK, BSDD37, "TPersistentBufferMonActor::Handle(TEvHttpInfo)", (cookie, cookie));
            }

            STRICT_STFUNC(StateFunc,
                hFunc(NMon::TEvHttpInfo, Handle)
                hFunc(TEvNodeWardenListLocalDDisksResult, Handle)
                hFunc(NDDisk::TEvPersistentBufferInfo, Handle)
                hFunc(TEvents::TEvWakeup, HandleWakeup);
            )
        };

    } // anon

    IActor *CreateMonPersistentBufferActor(const NKikimrConfig::TAppConfig& config, const NKikimr::TAppData& appData) {
        return new TPersistentBufferMonActor(config, appData);
    }

} // NKikimr
