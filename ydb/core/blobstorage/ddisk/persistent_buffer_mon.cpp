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
                bool DescribeFreeSpace;
                bool ShowTablets;
                ui32 RefreshRate;
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

            static TString GenerateFreeSpaceSvg(const std::vector<std::vector<std::tuple<ui32, ui32>>>& freeSpace,
                    ui32 sectorsPerChunk) {
                if (freeSpace.empty() || sectorsPerChunk == 0) {
                    return {};
                }

                const ui32 chunkCount = freeSpace.size();
                const int svgWidth = 800;
                const int rowHeight = 5;
                const int rowGap = 1;
                const int barWidth = svgWidth - 10;
                const int svgHeight = chunkCount * (rowHeight + rowGap) + 10;

                TStringStream svg;
                svg << "<svg xmlns=\"http://www.w3.org/2000/svg\""
                    << " width=\"" << svgWidth << "\""
                    << " height=\"" << svgHeight << "\">\n";

                for (ui32 chunkIdx = 0; chunkIdx < chunkCount; ++chunkIdx) {
                    const int y = chunkIdx * (rowHeight + rowGap) + 5;

                    svg << "<rect x=\"" << 0 << "\" y=\"" << y
                        << "\" width=\"" << barWidth << "\" height=\"" << rowHeight
                        << "\" fill=\"#d9534f\"/>\n";

                    const auto& ranges = freeSpace[chunkIdx];
                    for (const auto& [left, right] : ranges) {
                        const double xFrac = static_cast<double>(left) / sectorsPerChunk;
                        const double wFrac = static_cast<double>(right - left + 1) / sectorsPerChunk;
                        const int rx = static_cast<int>(xFrac * barWidth);
                        const int rw = std::max(1, static_cast<int>(wFrac * barWidth));
                        svg << "<rect x=\"" << rx << "\" y=\"" << y
                            << "\" width=\"" << rw << "\" height=\"" << rowHeight
                            << "\" fill=\"#5cb85c\"/>\n";
                    }
                }

                svg << "</svg>\n";
                return svg.Str();
            }

            void Reply(ui64 cookie) {
                auto it = Inflight.find(cookie);
                Y_ABORT_UNLESS(it != Inflight.end());
                auto& inflight = it->second;
                auto beautyDuration = [](TDuration v) {
                    TStringBuilder str;
                    if (v.Days() > 1) {
                        str << v.Days() << 'd';
                    } else if (v.Hours() > 1) {
                        str << v.Hours() << 'h';
                    } else if (v.Minutes() > 1) {
                        str << v.Minutes() << 'm';
                    } else if (v.Seconds() > 1) {
                        str << v.Seconds() << 's';
                    } else {
                        str << "Just now";
                    }
                    return str;
                };
                auto beautySize = [](ui32 s) {
                    TStringBuilder str;
                    if (s > 1e6) {
                        str << (ui32)(s / 1e6) << "Mb";
                    } else if (s > 1e3) {
                        str << (ui32)(s / 1e3) << "Kb";
                    } else {
                        str << s << "b";
                    }
                    return str;
                };
                TStringStream str;
                HTML(str) {
                    if (inflight.RefreshRate > 0) {
                        str << "<script>setTimeout(function(){window.location.reload(1);}, " << (inflight.RefreshRate * 1000) << ");</script>";
                    }
                    for (auto& [_, id] : inflight.Requests) {
                        str << "<h2 style=\"color:red;\">" << "No response from PB actorId: " << id << " </h2>";
                    }
                    std::sort(inflight.Responses.begin(), inflight.Responses.end(), [](auto &o1, auto &o2) -> bool {
                        return o1->Sender < o2->Sender;
                    });
                    for (auto& v : inflight.Responses) {
                        auto b = v->Get();
                        ui32 sectorsInChunk = b->ChunkSize / b->SectorSize;
                        TDuration uptime = TInstant::Now() - b->StartedAt;
                        str << "<h2>" << "PB actorId: " << v->Sender << "</h2>";
                        str << "Uptime: " << beautyDuration(uptime);
                        str << "<br> Allocated chunks: " << b->AllocatedChunks << " of " << b->MaxChunks << " by " << (ui32)(b->ChunkSize / 1e6) << "Mb";
                        str << "<br> Free sectors: " << b->FreeSectors;
                        str << " of allocated " << (b->AllocatedChunks * sectorsInChunk);
                        str << " max " << (b->MaxChunks * sectorsInChunk);
                        str << "<br> InMemory cache: " << beautySize(b->InMemoryCacheSize) << " of " << beautySize(b->InMemoryCacheLimit);
                        str << "<br> Pending events: " << b->PendingEvents;
                        str << "<br> Disk operations inflight: " << b->DiskOperationsInflight;

                        if (inflight.DescribeFreeSpace && !b->FreeSpace.empty() && b->SectorSize > 0 && b->ChunkSize > 0) {
                            const ui32 sectorsPerChunk = b->ChunkSize / b->SectorSize;
                            str << "<br><br><b>Free space map (green = free, red = used):</b><br>";
                            str << GenerateFreeSpaceSvg(b->FreeSpace, sectorsPerChunk);
                        }
                        if (inflight.ShowTablets) {
                            TABLE_CLASS ("table") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {str << "TabletId";}
                                        TABLEH() {str << "Generation";}
                                        TABLEH() {str << "Barrier";}
                                        TABLEH() {str << "Lsns count";}
                                        TABLEH() {str << "Total space";}
                                        TABLEH() {str << "First lsn";}
                                        TABLEH() {str << "Uptime";}
                                        TABLEH() {str << "Last lsn";}
                                        TABLEH() {str << "Uptime";}
                                    }
                                }
                                TABLEBODY() {
                                    for (auto& ti : b->TabletInfos) {
                                        TABLER() {
                                            TABLED() {str << ti.TabletId;}
                                            TABLED() {str << ti.Generation;}
                                            if (auto it = b->EraseBarriers.find(ti.TabletId); it != b->EraseBarriers.end()) {
                                                TABLED() {str << b->EraseBarriers[ti.TabletId];}
                                            } else {
                                                TABLED() {str << "No barrier";}
                                            }
                                            TABLED() {str << ti.LsnsCount;}
                                            TABLED() {str << beautySize(ti.Size);}
                                            TABLED() {str << ti.FirstLsn;}
                                            TABLED() {str << beautyDuration(TInstant::Now() - ti.FirstLsnTimestamp);}
                                            TABLED() {str << ti.LastLsn;}
                                            TABLED() {str << beautyDuration(TInstant::Now() - ti.LastLsnTimestamp);}
                                        }
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
                    auto infoReq = std::make_unique<NDDisk::TEvGetPersistentBufferInfo>();
                    infoReq->DescribeFreeSpace = inflight.DescribeFreeSpace;
                    infoReq->DescribeTablets = inflight.ShowTablets;
                    Send(r.PersistentBufferId, infoReq.release(), 0, reqCookie);
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD36, "TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult) Send",
                        (r.PersistentBufferId, r.PersistentBufferId), (reqCookie, reqCookie));
                }
                Schedule(TDuration::MilliSeconds(5000), new TEvents::TEvWakeup(cookie));
            }

            void Handle(NMon::TEvHttpInfo::TPtr& ev) {
                const TCgiParameters& params = ev->Get()->Request.GetParams();

                auto generateError = [&](const TString& msg) {
                    TStringStream out;

                    out << "HTTP/1.1 400 Bad Request\r\n"
                        << "Content-Type: text/plain\r\n"
                        << "Connection: close\r\n"
                        << "\r\n"
                        << msg << "\r\n";

                    Send(ev->Sender, new NMon::TEvHttpInfoRes(out.Str(), ev->Get()->SubRequestId, NMon::TEvHttpInfoRes::Custom), 0,
                        ev->Cookie);
                };
                bool describeFreeSpace = true;
                if (params.Has("describeFreeSpace")) {
                    int value;
                    if (!TryFromString(params.Get("describeFreeSpace"), value) || !(value >= 0 && value <= 1)) {
                        return generateError("Failed to parse describeFreeSpace parameter -- must be an integer in range [0, 1]");
                    } else {
                        describeFreeSpace = value != 0;
                    }
                }

                bool showTablets = true;
                if (params.Has("showTablets")) {
                    int value;
                    if (!TryFromString(params.Get("showTablets"), value) || !(value >= 0 && value <= 1)) {
                        return generateError("Failed to parse showTablets parameter -- must be an integer in range [0, 1]");
                    } else {
                        showTablets = value != 0;
                    }
                }
                ui32 refreshRate = 0;
                if (params.Has("refreshRate")) {
                    int value;
                    if (!TryFromString(params.Get("refreshRate"), value) || !(value >= 0)) {
                        return generateError("Failed to parse refreshRate parameter -- must be an integer in seconds");
                    } else {
                        refreshRate = value;
                    }
                }
                const ui64 cookie = ++NextCookie;
                Inflight[cookie] = TInflight{
                    .Sender = ev->Sender,
                    .Cookie = ev->Cookie,
                    .SubRequestId = ev->Get()->SubRequestId,
                    .DescribeFreeSpace = describeFreeSpace,
                    .ShowTablets = showTablets,
                    .RefreshRate = refreshRate,
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
