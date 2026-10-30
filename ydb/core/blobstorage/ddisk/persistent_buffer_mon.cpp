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
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>


#include <util/generic/queue.h>
#include <util/generic/hash_set.h>
#include <util/string/split.h>
#include <util/system/types.h>
#include <util/system/mutex.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_DDISK

using namespace NActors;

namespace NKikimr {

    using namespace NLWTrace;

    namespace {

        class TPersistentBufferMonActor : public TActorBootstrapped<TPersistentBufferMonActor> {
            static constexpr ui32 DefaultTabletsPageSize = 20;
            static constexpr ui32 MaxTabletsPageSize = 1000;

            struct TInflight {
                TActorId Sender;
                ui64 Cookie;
                int SubRequestId;
                bool DescribeFreeSpace;
                bool TabletsOnly;
                ui64 TabletPage;
                ui32 TabletsPageSize;
                std::optional<ui64> TabletIdFilter;
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
                YDB_LOG_DEBUG("TPersistentBufferMonActor::HandleWakeup",
                    {"marker", "BSDD32"},
                    {"cookie", ev->Get()->Tag});
                auto it = Inflight.find(ev->Get()->Tag);
                if (it == Inflight.end()) {
                    return;
                }
                for (auto [cookie, _] : it->second.Requests) {
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
                if (inflight.TabletsOnly) {
                    const bool timeout = !inflight.Requests.empty();
                    NJson::TJsonValue result(NJson::JSON_MAP);
                    if (inflight.Responses.empty()) {
                        result["error"] = timeout ? "Persistent buffer did not respond" : "Persistent buffer not found";
                    } else {
                        const auto* info = inflight.Responses.front().second->Get();
                        result["page"] = info->TabletsOffset / inflight.TabletsPageSize;
                        result["pages"] = info->TabletsTotal ? (info->TabletsTotal - 1) / inflight.TabletsPageSize + 1 : 1;
                        result["total"] = ToString(info->TabletsTotal);
                        result["pageSize"] = inflight.TabletsPageSize;
                        auto& tablets = result["tablets"];
                        tablets.SetType(NJson::JSON_ARRAY);
                        for (const auto& ti : info->TabletInfos) {
                            NJson::TJsonValue row(NJson::JSON_MAP);
                            row["tabletId"] = ToString(ti.TabletId);
                            row["generation"] = ToString(ti.Generation);
                            const auto barrier = info->EraseBarriers.find({ti.TabletId, ti.DirectBlockGroupIndex});
                            row["barrier"] = barrier == info->EraseBarriers.end() ? TString("No barrier") : ToString(barrier->second);
                            row["fastErasesCount"] = ToString(ti.FastErasesCount);
                            row["lsnsCount"] = ToString(ti.LsnsCount);
                            row["space"] = TString(beautySize(ti.Size)) + " of " + TString(beautySize(info->PerTabletStorageLimit));
                            row["firstLsn"] = ToString(ti.FirstLsn);
                            row["firstLsnUptime"] = TString(beautyDuration(TInstant::Now() - ti.FirstLsnTimestamp));
                            row["lastLsn"] = ToString(ti.LastLsn);
                            row["lastLsnUptime"] = TString(beautyDuration(TInstant::Now() - ti.LastLsnTimestamp));
                            tablets.AppendValue(std::move(row));
                        }
                    }
                    TStringStream response;
                    response << "HTTP/1.1 " << (timeout ? "504 Gateway Timeout" :
                        inflight.Responses.empty() ? "404 Not Found" : "200 OK")
                        << "\r\nContent-Type: application/json\r\nCache-Control: no-store\r\n\r\n"
                        << NJson::WriteJson(result, false);
                    Send(inflight.Sender, new NMon::TEvHttpInfoRes(response.Str(), inflight.SubRequestId,
                        NMon::TEvHttpInfoRes::Custom), 0, inflight.Cookie);
                    Inflight.erase(it);
                    return;
                }
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

                    str << R"JS(<script>(function(){
if (window.__pbMonInstalled) return;
window.__pbMonInstalled = true;
var timer = null;
var inFlight = false;
var pending = false;
function state(pb) {
    var p = new URLSearchParams(window.location.search);
    var page = Number(p.get('tabletPage.' + pb) || 0);
    var pageSize = Number(p.get('tabletPageSize.' + pb) || 20);
    return {open: p.get('tabletOpen.' + pb) === '1', page: Number.isSafeInteger(page) && page >= 0 ? page : 0,
        pageSize: Number.isInteger(pageSize) && pageSize >= 1 && pageSize <= 1000 ? pageSize : 20,
        tabletId: p.get('tabletId.' + pb) || ''};
}
function saveState(pb, open, page) {
    var url = new URL(window.location.href);
    if (open) url.searchParams.set('tabletOpen.' + pb, '1');
    else url.searchParams.delete('tabletOpen.' + pb);
    url.searchParams.set('tabletPage.' + pb, page);
    history.replaceState(null, '', url);
}
function tabletStateParam(k) {
    return ['tabletPage.', 'tabletOpen.', 'tabletPageSize.', 'tabletId.'].some(function(prefix) {
        return k.indexOf(prefix) === 0;
    });
}
function buildUrl() {
    var p = new URLSearchParams();
    new URLSearchParams(window.location.search).forEach(function(v, k) {
        if (tabletStateParam(k)) p.set(k, v);
    });
    p.set('formPresent', '1');
    if (document.getElementById('pb-mon-autoRefresh').checked) p.set('autoRefresh', '1');
    if (document.getElementById('pb-mon-describeFreeSpace').checked) p.set('describeFreeSpace', '1');
    var rr = document.getElementById('pb-mon-refreshRate').value;
    if (rr) p.set('refreshRate', rr);
    document.querySelectorAll('.pb-mon-pb').forEach(function(c) {
        if (c.checked) p.append('pb', c.value);
    });
    return window.location.pathname + '?' + p.toString();
}
function pageButton(pb, label, page, disabled) {
    var button = document.createElement('button');
    button.type = 'button';
    button.className = 'pb-mon-tablet-page';
    button.dataset.pb = pb;
    button.dataset.page = page;
    button.disabled = disabled;
    button.textContent = label;
    return button;
}
function tabletsCacheKey(pb) {
    return 'pb-mon-tablets:' + window.location.pathname + ':' + pb;
}
function renderTablets(panel, data) {
    var pb = panel.dataset.pb;
    var body = panel.querySelector('.pb-mon-tablets-body');
    var status = panel.querySelector('.pb-mon-tablets-status');
    // Preserve the table, controls and row nodes on background refresh.
    if (!panel.table) {
        panel.previous = body.appendChild(pageButton(pb, 'Previous', 0, true));
        panel.pageLabel = body.appendChild(document.createTextNode(''));
        panel.next = body.appendChild(pageButton(pb, 'Next', 0, true));
        panel.table = document.createElement('table');
        panel.table.className = 'table pb-mon-tablet-table';
        var header = panel.table.createTHead().insertRow();
        ['TabletId', 'Generation', 'Barrier', 'Fast erases', 'Lsns count', 'Total space',
            'First lsn', 'Uptime', 'Last lsn', 'Uptime'].forEach(function(label, index) {
            var th = document.createElement('th');
            th.textContent = label;
            th.style.width = [18, 6, 9, 7, 7, 16, 10, 8, 10, 9][index] + '%';
            header.appendChild(th);
        });
        panel.rows = panel.table.createTBody();
        body.appendChild(panel.table);
    }
    var previousHeight = panel.resetHeight ? 0 : body.getBoundingClientRect().height;
    panel.resetHeight = false;
    panel.previous.dataset.page = Math.max(0, data.page - 1);
    panel.previous.disabled = data.page === 0;
    panel.next.dataset.page = data.page + 1;
    panel.next.disabled = data.page + 1 >= data.pages;
    var label = ' Page ' + (data.page + 1) + ' of ' + data.pages +
        ' (' + data.total + ' tablets, ' + data.pageSize + ' per page) ';
    if (panel.pageLabel.textContent !== label) panel.pageLabel.textContent = label;
    data.tablets.forEach(function(tablet, index) {
        var row = panel.rows.rows[index] || panel.rows.insertRow();
        ['tabletId', 'generation', 'barrier', 'fastErasesCount', 'lsnsCount', 'space',
            'firstLsn', 'firstLsnUptime', 'lastLsn', 'lastLsnUptime'].forEach(function(key, column) {
            var cell = row.cells[column] || row.insertCell();
            if (cell.textContent !== tablet[key]) {
                cell.textContent = tablet[key];
                cell.title = tablet[key];
            }
        });
    });
    while (panel.rows.rows.length > data.tablets.length) panel.rows.deleteRow(-1);
    // Avoid shifting later buffers when a background refresh temporarily has fewer rows.
    body.style.minHeight = Math.max(previousHeight, body.getBoundingClientRect().height) + 'px';
    status.textContent = '';
}
function loadTablets(panel) {
    var pb = panel.dataset.pb;
    var selected = state(pb);
    if (!selected.open || panel.request) return;
    var controller = new AbortController();
    panel.request = controller;
    var body = panel.querySelector('.pb-mon-tablets-body');
    var status = panel.querySelector('.pb-mon-tablets-status');
    status.textContent = 'Loading...';
    var url = new URL(window.location.pathname, window.location.origin);
    url.searchParams.set('action', 'tablets');
    url.searchParams.set('pb', pb);
    url.searchParams.set('page', selected.page);
    url.searchParams.set('pageSize', selected.pageSize);
    if (selected.tabletId) url.searchParams.set('tabletId', selected.tabletId);
    fetch(url, {cache: 'no-store', signal: controller.signal})
        .then(function(r) { return r.json().then(function(data) {
            if (!r.ok) throw new Error(data.error || 'Failed to load tablets');
            return data;
        }); })
        .then(function(data) {
            if (panel.request !== controller || !panel.isConnected || !state(pb).open) return;
            saveState(pb, true, data.page);
            renderTablets(panel, data);
            try {
                sessionStorage.setItem(tabletsCacheKey(pb), JSON.stringify({page: data.page,
                    pageSize: data.pageSize, tabletId: selected.tabletId, data: data}));
            } catch (error) {}
        })
        .catch(function(error) {
            if (panel.request === controller && panel.isConnected && error.name !== 'AbortError') {
                status.textContent = error.message;
            }
        })
        .finally(function() { if (panel.request === controller) panel.request = null; });
}
function restorePanel(panel) {
    var selected = state(panel.dataset.pb);
    var open = selected.open;
    var pageSize = panel.querySelector('.pb-mon-tablets-page-size');
    var search = panel.querySelector('.pb-mon-tablets-search');
    // Preserve unfinished edits and keyboard focus while refreshing the summary.
    if (document.activeElement !== pageSize) pageSize.value = selected.pageSize;
    if (document.activeElement !== search) search.value = selected.tabletId;
    panel.querySelector('.pb-mon-tablets-content').hidden = !open;
    var toggle = panel.querySelector('.pb-mon-tablets-toggle');
    toggle.textContent = open ? 'Hide tablets' : 'Show tablets';
    toggle.setAttribute('aria-expanded', open);
    if (open) {
        // Show the last loaded page synchronously after a browser reload, then refresh it.
        if (!panel.table) {
            try {
                var cached = JSON.parse(sessionStorage.getItem(tabletsCacheKey(panel.dataset.pb)));
                if (cached && cached.page === selected.page && cached.pageSize === selected.pageSize
                        && cached.tabletId === selected.tabletId) renderTablets(panel, cached.data);
            } catch (error) {}
        }
        loadTablets(panel);
    }
}
function restoreTablets() {
    document.querySelectorAll('.pb-mon-tablets').forEach(restorePanel);
}
function summaryUrl() {
    var url = new URL(buildUrl(), window.location.origin);
    Array.from(url.searchParams.keys()).forEach(function(k) {
        if (tabletStateParam(k)) url.searchParams.delete(k);
    });
    return url.pathname + url.search;
}
function refresh() {
    if (inFlight) { pending = true; return; }
    inFlight = true;
    // Tablet state is kept independently of the summary request, so a click during
    // this request must not discard the new summary or reset a tablet panel.
    var requestedUrl = summaryUrl();
    fetch(requestedUrl, {headers: {'Accept': 'text/html'}, cache: 'no-store'})
        .then(function(r) { if (!r.ok) throw new Error('Failed to refresh'); return r.text(); })
        .then(function(html) {
            if (requestedUrl !== summaryUrl()) { pending = true; return; }
            var doc = new DOMParser().parseFromString(html, 'text/html');
            var fresh = doc.getElementById('pb-mon-content');
            var cur = document.getElementById('pb-mon-content');
            if (!fresh || !cur) return;
            // Keep existing buffer containers and their expanded lists attached to the live DOM.
            var existing = new Map();
            cur.querySelectorAll('.pb-mon-buffer').forEach(function(buffer) { existing.set(buffer.dataset.pb, buffer); });
            fresh.querySelectorAll('.pb-mon-buffer').forEach(function(buffer) {
                var current = existing.get(buffer.dataset.pb);
                if (current) {
                    var currentSummary = current.querySelector('.pb-mon-summary');
                    var freshSummary = buffer.querySelector('.pb-mon-summary');
                    if (currentSummary && freshSummary) currentSummary.innerHTML = freshSummary.innerHTML;
                } else {
                    cur.appendChild(buffer);
                }
            });
            var selectedPBs = new Set();
            document.querySelectorAll('.pb-mon-pb').forEach(function(c) { if (c.checked) selectedPBs.add(c.value); });
            existing.forEach(function(buffer, pb) {
                if (!selectedPBs.has(pb)) {
                    var panel = buffer.querySelector('.pb-mon-tablets');
                    if (panel.request) panel.request.abort();
                    buffer.remove();
                }
            });
            var currentErrors = cur.querySelector('.pb-mon-errors');
            var freshErrors = fresh.querySelector('.pb-mon-errors');
            if (currentErrors && freshErrors) currentErrors.innerHTML = freshErrors.innerHTML;
            restoreTablets();
        })
        .catch(function() {})
        .finally(function() { inFlight = false; if (pending) { pending = false; refresh(); } });
}
function reschedule() {
    if (timer) { clearInterval(timer); timer = null; }
    var on = document.getElementById('pb-mon-autoRefresh').checked;
    var sec = parseInt(document.getElementById('pb-mon-refreshRate').value, 10);
    if (on && sec > 0) timer = setInterval(refresh, sec * 1000);
}
function applyNow() {
    history.replaceState(null, '', buildUrl());
    reschedule();
    refresh();
}
var settings = document.getElementById('pb-mon-settings');
settings.addEventListener('change', applyNow);
settings.addEventListener('input', function(e) {
    if (e.target && e.target.id === 'pb-mon-refreshRate') applyNow();
});
var selAll = document.getElementById('pb-mon-selectAll');
if (selAll) selAll.addEventListener('click', function(e) {
    e.preventDefault();
    document.querySelectorAll('.pb-mon-pb').forEach(function(c) { c.checked = true; });
    applyNow();
});
var clrAll = document.getElementById('pb-mon-clearAll');
if (clrAll) clrAll.addEventListener('click', function(e) {
    e.preventDefault();
    document.querySelectorAll('.pb-mon-pb').forEach(function(c) { c.checked = false; });
    applyNow();
});
document.addEventListener('click', function(e) {
    var button = e.target.closest('.pb-mon-tablets-toggle, .pb-mon-tablet-page');
    if (!button || button.disabled) return;
    var panel = button.closest('.pb-mon-tablets');
    var selected = state(panel.dataset.pb);
    var open = button.classList.contains('pb-mon-tablets-toggle') ? !selected.open : true;
    var page = button.classList.contains('pb-mon-tablet-page') ? Number(button.dataset.page) : selected.page;
    if (panel.request) { panel.request.abort(); panel.request = null; }
    saveState(panel.dataset.pb, open, page);
    panel.querySelector('.pb-mon-tablets-body').style.minHeight = '';
    panel.resetHeight = true;
    restorePanel(panel);
});
function applyTabletOptions(panel) {
    if (panel.request) { panel.request.abort(); panel.request = null; }
    var url = new URL(window.location.href);
    url.searchParams.set('tabletPageSize.' + panel.dataset.pb, panel.querySelector('.pb-mon-tablets-page-size').value);
    var tabletId = panel.querySelector('.pb-mon-tablets-search').value.trim();
    if (tabletId) url.searchParams.set('tabletId.' + panel.dataset.pb, tabletId);
    else url.searchParams.delete('tabletId.' + panel.dataset.pb);
    url.searchParams.set('tabletPage.' + panel.dataset.pb, '0');
    history.replaceState(null, '', url);
    panel.querySelector('.pb-mon-tablets-body').style.minHeight = '';
    panel.resetHeight = true;
    restorePanel(panel);
}
document.addEventListener('change', function(e) {
    if (e.target.matches('.pb-mon-tablets-page-size') && e.target.checkValidity()) {
        applyTabletOptions(e.target.closest('.pb-mon-tablets'));
    }
});
document.addEventListener('submit', function(e) {
    if (e.target.matches('.pb-mon-tablets-options')) {
        e.preventDefault();
        applyTabletOptions(e.target.closest('.pb-mon-tablets'));
    }
});
if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', restoreTablets);
else restoreTablets();
reschedule();
})();</script>)JS";

                    str << "<style>"
                        << ".pb-mon-tablets-status{height:1.5em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}"
                        << ".pb-mon-tablets-body{overflow-x:auto;}"
                        << ".pb-mon-tablet-table{table-layout:fixed;width:100%;min-width:1100px;}"
                        << ".pb-mon-tablet-table th,.pb-mon-tablet-table td{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}"
                        << "</style>";
                    str << "<div id=\"pb-mon-content\"><div class=\"pb-mon-errors\">";
                    for (auto& [_, id] : inflight.Requests) {
                        str << "<h2 style=\"color:red;\">" << "No response from PB " << formatPBId(id) << " </h2>";
                    }
                    str << "</div>";
                    std::sort(inflight.Responses.begin(), inflight.Responses.end(),
                        [](const auto& o1, const auto& o2) -> bool {
                            return o1.first < o2.first;
                        });
                    for (auto& [serviceId, v] : inflight.Responses) {
                        auto b = v->Get();
                        ui32 sectorsInChunk = b->ChunkSize / b->SectorSize;
                        TDuration uptime = TInstant::Now() - b->StartedAt;
                        str << "<div class=\"pb-mon-buffer\" data-pb=\"" << htmlEscape(ToString(serviceId)) << "\">"
                            << "<div class=\"pb-mon-summary\">";
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
                        str << "</div>";
                        str << "<div class=\"pb-mon-tablets\" data-pb=\"" << htmlEscape(ToString(serviceId)) << "\">"
                            << "<button type=\"button\" class=\"pb-mon-tablets-toggle\" aria-expanded=\"false\">Show tablets</button>"
                            << "<div class=\"pb-mon-tablets-content\" hidden>"
                            << "<form class=\"pb-mon-tablets-options\" style=\"margin:8px 0;\">"
                            << "<label>Per page: <input class=\"pb-mon-tablets-page-size\" type=\"number\" min=\"1\" max=\"1000\" value=\"20\" style=\"width:70px;\"></label> "
                            << "<label>TabletId: <input class=\"pb-mon-tablets-search\" type=\"search\" inputmode=\"numeric\" pattern=\"[0-9]*\" placeholder=\"All tablets\"></label> "
                            << "<button type=\"submit\">Find</button></form>"
                            << "<div class=\"pb-mon-tablets-status\" role=\"status\"></div>"
                            << "<div class=\"pb-mon-tablets-body\"></div></div></div></div>";
                    }
                    str << "</div>";
                }
                Send(inflight.Sender, new NMon::TEvHttpInfoRes(str.Str(), inflight.SubRequestId), 0, inflight.Cookie);
                YDB_LOG_DEBUG("TPersistentBufferMonActor::Reply()",
                    {"marker", "BSDD39"},
                    {"responses", inflight.Responses.size()},
                    {"requests", inflight.Requests.size()});
                Inflight.erase(it);
            }

            void Handle(NDDisk::TEvPersistentBufferInfo::TPtr& ev) {
                auto reqCookie = ev->Cookie;
                auto request = PBuffersInflight.find(reqCookie);
                if (request == PBuffersInflight.end()) {
                    return;
                }
                auto cookie = request->second;
                PBuffersInflight.erase(request);
                YDB_LOG_DEBUG("TPersistentBufferMonActor::Handle(TEvPersistentBufferInfo)",
                    {"marker", "BSDD33"},
                    {"sender", ev->Sender},
                    {"reqCookie", reqCookie},
                    {"cookie", cookie});
                auto it = Inflight.find(cookie);
                if (it == Inflight.end()) {
                    return;
                }
                auto& inflight = it->second;
                if (inflight.Requests.count(ev->Cookie) == 0) {
                    YDB_LOG_ERROR("TPersistentBufferMonActor::Handle(TEvPersistentBufferInfo) unknown persistent buffer",
                        {"marker", "BSDD34"},
                        {"sender", ev->Sender},
                        {"cookie", cookie});
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
                YDB_LOG_DEBUG("TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult)",
                    {"marker", "BSDD35"},
                    {"cookie", cookie});
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
                    infoReq->DescribeTablets = inflight.TabletsOnly;
                    infoReq->TabletsLimit = inflight.TabletsPageSize;
                    infoReq->TabletsOffset = inflight.TabletPage * inflight.TabletsPageSize;
                    infoReq->TabletIdFilter = inflight.TabletIdFilter;
                    Send(r.PersistentBufferId, infoReq.release(), 0, reqCookie);
                    YDB_LOG_DEBUG("TPersistentBufferMonActor::Handle(TEvNodeWardenListLocalDDisksResult) Send",
                        {"marker", "BSDD36"},
                        {"persistentBufferId", r.PersistentBufferId},
                        {"reqCookie", reqCookie});
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

                    out << "HTTP/1.1 400 Bad Request\r\n";
                    if (params.Get("action") == "tablets") {
                        NJson::TJsonValue result(NJson::JSON_MAP);
                        result["error"] = msg;
                        out << "Content-Type: application/json\r\nCache-Control: no-store\r\n\r\n"
                            << NJson::WriteJson(result, false);
                    } else {
                        out << "Content-Type: text/plain\r\nConnection: close\r\n\r\n" << msg << "\r\n";
                    }

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

                const bool tabletsOnly = params.Get("action") == "tablets";
                if (params.Has("action") && !tabletsOnly) {
                    return generateError("Unknown action");
                }
                ui64 tabletPage = 0;
                ui32 tabletsPageSize = DefaultTabletsPageSize;
                std::optional<ui64> tabletIdFilter;
                if (tabletsOnly) {
                    if (params.Has("pageSize") && (!TryFromString(params.Get("pageSize"), tabletsPageSize)
                            || !tabletsPageSize || tabletsPageSize > MaxTabletsPageSize)) {
                        return generateError("pageSize must be an integer in range [1, 1000]");
                    }
                    if (params.Has("tabletId") && !params.Get("tabletId").empty()) {
                        ui64 tabletId;
                        if (!TryFromString(params.Get("tabletId"), tabletId)) {
                            return generateError("tabletId must be an unsigned 64-bit integer");
                        }
                        tabletIdFilter = tabletId;
                    }
                    if (selectedPBs.size() != 1 || params.NumOfValues("pb") != 1) {
                        return generateError("Tablets API requires exactly one pb parameter");
                    }
                    if (params.Has("page") && (!TryFromString(params.Get("page"), tabletPage)
                            || tabletPage > Max<ui64>() / tabletsPageSize)) {
                        return generateError(TStringBuilder() << "page must be an integer in range [0, "
                            << Max<ui64>() / tabletsPageSize << "]");
                    }
                }

                const ui64 cookie = ++NextCookie;
                Inflight[cookie] = TInflight{
                    .Sender = ev->Sender,
                    .Cookie = ev->Cookie,
                    .SubRequestId = ev->Get()->SubRequestId,
                    .DescribeFreeSpace = tabletsOnly ? false : describeFreeSpace,
                    .TabletsOnly = tabletsOnly,
                    .TabletPage = tabletPage,
                    .TabletsPageSize = tabletsPageSize,
                    .TabletIdFilter = tabletIdFilter,
                    .AutoRefresh = autoRefresh,
                    .RefreshRate = refreshRate,
                    .SelectedPBs = std::move(selectedPBs),
                };
                auto nwId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                Send(nwId, new TEvNodeWardenListLocalDDisks(), 0, cookie);
                YDB_LOG_DEBUG("TPersistentBufferMonActor::Handle(TEvHttpInfo)",
                    {"marker", "BSDD37"},
                    {"cookie", cookie});
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
