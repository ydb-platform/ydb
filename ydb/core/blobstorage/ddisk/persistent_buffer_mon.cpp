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
#include <util/generic/hash_set.h>
#include <util/string/split.h>
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
                bool AutoRefresh;
                ui32 RefreshRate;
                THashSet<TString> SelectedPBs; // empty == show all
                std::vector<TActorId> AllPBs;
                // Responses are paired with the *service* id (the well-known PB actor id we
                // sent the request to), not ev->Sender (the actor that replied), so the UI
                // displays the same id the user sees in the PB selection panel.
                std::vector<std::pair<TActorId, NDDisk::TEvPersistentBufferInfo::TPtr>> Responses;
                std::unordered_map<ui64, TActorId> Requests;  // cookie -> serviceId (in-flight only)
                std::unordered_map<ui64, TActorId> CookieToService; // cookie -> serviceId (full lifecycle)
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

                auto beautySize = [](ui64 s) {
                    TStringBuilder str;
                    constexpr ui64 KiB = 1ull << 10;
                    constexpr ui64 MiB = 1ull << 20;
                    constexpr ui64 GiB = 1ull << 30;
                    if (s >= GiB) {
                        str << Sprintf("%.1fGiB", static_cast<double>(s) / GiB);
                    } else if (s >= MiB) {
                        str << Sprintf("%.1fMiB", static_cast<double>(s) / MiB);
                    } else if (s >= KiB) {
                        str << Sprintf("%.1fKiB", static_cast<double>(s) / KiB);
                    } else {
                        str << s << "B";
                    }
                    return str;
                };
                // Decode the PB service id (built by MakeBlobStoragePersistentBufferId) back
                // into node:pdisk:slot. The name layout is:
                //   bytes 0..3 = "NPB_"
                //   bytes 4..7 = pdiskId (little-endian uint32)
                //   bytes 8..11 = ddiskSlotId (little-endian uint32)
                // Falls back to the raw ToString() form if the prefix doesn't match.
                auto formatPBId = [](const TActorId& id) {
                    if (id.IsService()) {
                        const TStringBuf name = id.ServiceId();
                        if (name.size() >= 12 && name[0] == 'N' && name[1] == 'P'
                                && name[2] == 'B' && name[3] == '_') {
                            const auto* b = reinterpret_cast<const ui8*>(name.data());
                            const ui32 pdiskId =
                                ui32(b[4]) | (ui32(b[5]) << 8) | (ui32(b[6]) << 16) | (ui32(b[7]) << 24);
                            const ui32 slotId =
                                ui32(b[8]) | (ui32(b[9]) << 8) | (ui32(b[10]) << 16) | (ui32(b[11]) << 24);
                            return TStringBuilder() << id.NodeId() << ":" << pdiskId << ":" << slotId;
                        }
                    }
                    return TStringBuilder() << id;
                };
                auto htmlEscape = [](const TString& s) {
                    TStringBuilder out;
                    for (char c : s) {
                        switch (c) {
                            case '&': out << "&amp;"; break;
                            case '<': out << "&lt;"; break;
                            case '>': out << "&gt;"; break;
                            case '"': out << "&quot;"; break;
                            case '\'': out << "&#39;"; break;
                            default: out << c;
                        }
                    }
                    return TString(out);
                };
                TStringStream str;
                HTML(str) {
                    // Settings panel (lives OUTSIDE #pb-mon-content so user edits are not
                    // wiped by AJAX refresh). Changes apply immediately via JS — no submit
                    // button. The hidden formPresent flag lets the server distinguish
                    // "checkbox cleared by user" from "first visit, no params at all".
                    str << "<div id=\"pb-mon-settings\" style=\"margin-bottom:1em; padding:8px;"
                        << " border:1px solid #ddd; border-radius:4px;\">";
                    str << "<input type=\"hidden\" id=\"pb-mon-formPresent\" value=\"1\">";
                    str << "<label style=\"margin-right:1em;\">"
                        << "<input type=\"checkbox\" id=\"pb-mon-autoRefresh\""
                        << (inflight.AutoRefresh ? " checked" : "") << "> Auto-refresh</label>";
                    str << "<label style=\"margin-right:1em;\">Refresh rate (sec): "
                        << "<input type=\"number\" id=\"pb-mon-refreshRate\" min=\"1\" value=\""
                        << (inflight.RefreshRate ? inflight.RefreshRate : 1)
                        << "\" style=\"width:70px;\"></label>";
                    str << "<label style=\"margin-right:1em;\">"
                        << "<input type=\"checkbox\" id=\"pb-mon-describeFreeSpace\""
                        << (inflight.DescribeFreeSpace ? " checked" : "") << "> Show free space</label>";
                    str << "<label style=\"margin-right:1em;\">"
                        << "<input type=\"checkbox\" id=\"pb-mon-showTablets\""
                        << (inflight.ShowTablets ? " checked" : "") << "> Show tablets</label>";
                    if (!inflight.AllPBs.empty()) {
                        str << "<br><br><b>Persistent Buffers to display:</b> ";
                        str << "<a href=\"#\" id=\"pb-mon-selectAll\">[select all]</a> ";
                        str << "<a href=\"#\" id=\"pb-mon-clearAll\">[clear]</a><br>";
                        for (const auto& pbId : inflight.AllPBs) {
                            TString pbStr = ToString(pbId);
                            const bool selected = inflight.SelectedPBs.contains(pbStr);
                            const TString pbEsc = htmlEscape(pbStr);
                            // Checkbox VALUE keeps the raw service-id form (so the server's
                            // pb= filter still matches); the LABEL shows the human triplet.
                            const TString pbLabel = htmlEscape(formatPBId(pbId));
                            str << "<label style=\"margin-right:1em;\">"
                                << "<input type=\"checkbox\" class=\"pb-mon-pb\" value=\"" << pbEsc << "\""
                                << (selected ? " checked" : "") << "> " << pbLabel << "</label>";
                        }
                    }
                    str << "</div>";

                    // Single-install controller: builds query string from current control
                    // state, updates URL, triggers an AJAX refresh, and (re)schedules the
                    // auto-refresh timer. On every settings change we call applyNow(), so
                    // params take effect with no submit button and no page reload.
                    // Guarded by window.__pbMonInstalled so AJAX-fetched HTML doesn't
                    // re-execute this script.
                    str << "<script>(function(){"
                        << "if (window.__pbMonInstalled) { return; }"
                        << "window.__pbMonInstalled = true;"
                        << "var timer = null;"
                        << "var inFlight = false;"
                        << "function buildUrl(){"
                        << "  var p = new URLSearchParams();"
                        << "  p.set('formPresent','1');"
                        << "  if (document.getElementById('pb-mon-autoRefresh').checked) p.set('autoRefresh','1');"
                        << "  if (document.getElementById('pb-mon-describeFreeSpace').checked) p.set('describeFreeSpace','1');"
                        << "  if (document.getElementById('pb-mon-showTablets').checked) p.set('showTablets','1');"
                        << "  var rr = document.getElementById('pb-mon-refreshRate').value;"
                        << "  if (rr) p.set('refreshRate', rr);"
                        << "  document.querySelectorAll('.pb-mon-pb').forEach(function(c){"
                        << "    if (c.checked) p.append('pb', c.value);"
                        << "  });"
                        << "  return window.location.pathname + '?' + p.toString();"
                        << "}"
                        << "function refresh(){"
                        << "  if (inFlight) return;"
                        << "  inFlight = true;"
                        << "  fetch(window.location.href, {headers:{'Accept':'text/html'}, cache:'no-store'})"
                        << "    .then(function(r){return r.text();})"
                        << "    .then(function(html){"
                        << "      var doc = new DOMParser().parseFromString(html, 'text/html');"
                        << "      var fresh = doc.getElementById('pb-mon-content');"
                        << "      var cur = document.getElementById('pb-mon-content');"
                        << "      if (fresh && cur) { cur.innerHTML = fresh.innerHTML; }"
                        << "    })"
                        << "    .catch(function(){})"
                        << "    .finally(function(){ inFlight = false; });"
                        << "}"
                        << "function reschedule(){"
                        << "  if (timer) { clearInterval(timer); timer = null; }"
                        << "  var on = document.getElementById('pb-mon-autoRefresh').checked;"
                        << "  var sec = parseInt(document.getElementById('pb-mon-refreshRate').value, 10);"
                        << "  if (on && sec > 0) { timer = setInterval(refresh, sec * 1000); }"
                        << "}"
                        << "function applyNow(){"
                        << "  var url = buildUrl();"
                        << "  history.replaceState(null, '', url);"
                        << "  reschedule();"
                        << "  refresh();"
                        << "}"
                        << "var settings = document.getElementById('pb-mon-settings');"
                        << "settings.addEventListener('change', applyNow);"
                        << "settings.addEventListener('input', function(e){"
                        << "  if (e.target && e.target.id === 'pb-mon-refreshRate') { applyNow(); }"
                        << "});"
                        << "var selAll = document.getElementById('pb-mon-selectAll');"
                        << "if (selAll) selAll.addEventListener('click', function(e){"
                        << "  e.preventDefault();"
                        << "  document.querySelectorAll('.pb-mon-pb').forEach(function(c){c.checked=true;});"
                        << "  applyNow();"
                        << "});"
                        << "var clrAll = document.getElementById('pb-mon-clearAll');"
                        << "if (clrAll) clrAll.addEventListener('click', function(e){"
                        << "  e.preventDefault();"
                        << "  document.querySelectorAll('.pb-mon-pb').forEach(function(c){c.checked=false;});"
                        << "  applyNow();"
                        << "});"
                        << "reschedule();"
                        << "})();</script>";

                    str << "<div id=\"pb-mon-content\">";
                    for (auto& [_, id] : inflight.Requests) {
                        str << "<h2 style=\"color:red;\">" << "No response from PB " << formatPBId(id) << " </h2>";
                    }
                    std::sort(inflight.Responses.begin(), inflight.Responses.end(),
                        [](const auto& o1, const auto& o2) -> bool {
                            return o1.first < o2.first;
                        });
                    for (auto& [serviceId, v] : inflight.Responses) {
                        auto b = v->Get();
                        ui32 sectorsInChunk = b->ChunkSize / b->SectorSize;
                        TDuration uptime = TInstant::Now() - b->StartedAt;
                        str << "<h2>" << "PB " << formatPBId(serviceId) << "</h2>";
                        str << "Uptime: " << beautyDuration(uptime);
                        str << "<br> Allocated chunks: " << b->AllocatedChunks << " of " << b->MaxChunks << " by " << beautySize(b->ChunkSize);
                        str << "<br> Free sectors: " << b->FreeSectors;
                        str << " of allocated " << (b->AllocatedChunks * sectorsInChunk);
                        str << " max " << (b->MaxChunks * sectorsInChunk);
                        str << "<br> InMemory cache: " << beautySize(b->InMemoryCacheSize) << " of " << beautySize(b->InMemoryCacheLimit);
                        str << "<br> Pending events: " << b->PendingEvents;
                        str << "<br> Disk operations inflight: " << b->DiskOperationsInflight;

                        if (!b->OpStats.empty()) {
                            str << "<h3>Operation latency &amp; IOPS (last 15 sec)</h3>";
                            TABLE_CLASS ("table") {
                                TABLEHEAD() {
                                    TABLER() {
                                        TABLEH() {str << "Operation";}
                                        TABLEH() {str << "InFlight";}
                                        TABLEH() {str << "IOPS";}
                                        TABLEH() {str << "Latency p50, ms";}
                                        TABLEH() {str << "Latency p99, ms";}
                                        TABLEH() {str << "Latency max, ms";}
                                    }
                                }
                                TABLEBODY() {
                                    for (const auto& op : b->OpStats) {
                                        const ui64 iops = op.WindowSeconds > 0
                                            ? static_cast<ui64>(op.Requests / op.WindowSeconds + 0.5)
                                            : 0;
                                        TABLER() {
                                            TABLED() {str << op.Name;}
                                            TABLED() {str << op.RequestsInFlight;}
                                            TABLED() {str << iops;}
                                            TABLED() {str << Sprintf("%.3f", op.LatencyP50Ms);}
                                            TABLED() {str << Sprintf("%.3f", op.LatencyP99Ms);}
                                            TABLED() {str << Sprintf("%.3f", op.LatencyMaxMs);}
                                        }
                                    }
                                }
                            }
                        }

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
                                        TABLEH() {str << "Fast erases";}
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
                                                TABLED() {str << it->second;}
                                            } else {
                                                TABLED() {str << "No barrier";}
                                            }
                                            TABLED() {str << ti.FastErasesCount;}
                                            TABLED() {str << ti.LsnsCount;}
                                            TABLED() {str << beautySize(ti.Size) << " of " << beautySize(b->PerTabletStorageLimit);}
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
                    str << "</div>";
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
                // Look up the service id (well-known PB actor id) we sent this request to,
                // and pair it with the response. ev->Sender is the actor that replied — it
                // can differ from the service id (e.g. when the service forwards), so we
                // must NOT use it for display.
                TActorId serviceId;
                if (auto cit = inflight.CookieToService.find(ev->Cookie);
                        cit != inflight.CookieToService.end()) {
                    serviceId = cit->second;
                } else {
                    // Fallback: shouldn't normally happen.
                    serviceId = ev->Sender;
                }
                inflight.Responses.emplace_back(serviceId, ev);
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

                inflight.AllPBs.reserve(ev->Get()->Infos.size());
                for (auto r : ev->Get()->Infos) {
                    inflight.AllPBs.push_back(r.PersistentBufferId);
                }
                std::sort(inflight.AllPBs.begin(), inflight.AllPBs.end());

                for (auto r : ev->Get()->Infos) {
                    if (!inflight.SelectedPBs.contains(ToString(r.PersistentBufferId))) {
                        continue;
                    }
                    auto reqCookie = ++NextCookie;
                    inflight.Requests.insert({reqCookie, r.PersistentBufferId});
                    inflight.CookieToService.insert({reqCookie, r.PersistentBufferId});
                    PBuffersInflight[reqCookie] = cookie;
                    auto infoReq = std::make_unique<NDDisk::TEvGetPersistentBufferInfo>();
                    infoReq->DescribeFreeSpace = inflight.DescribeFreeSpace;
                    infoReq->DescribeTablets = inflight.ShowTablets;
                    Send(r.PersistentBufferId, infoReq.release(), 0, reqCookie);
                    STLOG(PRI_DEBUG, BS_DDISK, BSDD36, "TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult) Send",
                        (r.PersistentBufferId, r.PersistentBufferId), (reqCookie, reqCookie));
                }
                if (inflight.Requests.empty()) {
                    Reply(cookie);
                    return;
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
                // When the settings form is submitted (formPresent=1), an absent checkbox
                // parameter means the user explicitly unchecked it. On the very first visit
                // (no form submission), checkboxes default to ON.
                const bool formPresent = params.Has("formPresent");

                auto parseBoolParam = [&](const TString& name, bool defaultValue, bool& result) -> std::optional<TString> {
                    if (params.Has(name)) {
                        int value;
                        if (!TryFromString(params.Get(name), value) || !(value >= 0 && value <= 1)) {
                            return TString("Failed to parse " + name + " parameter -- must be an integer in range [0, 1]");
                        }
                        result = value != 0;
                    } else {
                        // Missing checkbox after form submission means "unchecked".
                        result = formPresent ? false : defaultValue;
                    }
                    return std::nullopt;
                };

                bool describeFreeSpace = true;
                if (auto err = parseBoolParam("describeFreeSpace", true, describeFreeSpace)) {
                    return generateError(*err);
                }
                bool showTablets = true;
                if (auto err = parseBoolParam("showTablets", true, showTablets)) {
                    return generateError(*err);
                }
                bool autoRefresh = true;
                if (auto err = parseBoolParam("autoRefresh", true, autoRefresh)) {
                    return generateError(*err);
                }

                ui32 refreshRate = 1; // default 1 sec
                if (params.Has("refreshRate")) {
                    int value;
                    if (!TryFromString(params.Get("refreshRate"), value) || value < 1) {
                        return generateError("Failed to parse refreshRate parameter -- must be a positive integer in seconds");
                    }
                    refreshRate = value;
                }

                THashSet<TString> selectedPBs;
                for (const auto& v : params.Range("pb")) {
                    if (!v.empty()) {
                        selectedPBs.insert(v);
                    }
                }

                const ui64 cookie = ++NextCookie;
                Inflight[cookie] = TInflight{
                    .Sender = ev->Sender,
                    .Cookie = ev->Cookie,
                    .SubRequestId = ev->Get()->SubRequestId,
                    .DescribeFreeSpace = describeFreeSpace,
                    .ShowTablets = showTablets,
                    .AutoRefresh = autoRefresh,
                    .RefreshRate = refreshRate,
                    .SelectedPBs = std::move(selectedPBs),
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
