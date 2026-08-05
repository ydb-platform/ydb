#include "mon_render.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/core/base/services/blobstorage_service_id.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/subst.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString HtmlEscape(TStringBuf in)
{
    TString escaped(in);
    SubstGlobal(escaped, "&", "&amp;");
    SubstGlobal(escaped, "<", "&lt;");
    SubstGlobal(escaped, ">", "&gt;");
    SubstGlobal(escaped, "\"", "&quot;");
    return escaped;
}

const char* PageParam(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "overview";
        case EMonPage::Dbg:
            return "dbg";
        case EMonPage::LocalDb:
            return "localdb";
        case EMonPage::VChunk:
            return "vchunk";
        case EMonPage::Latency:
            return "latency";
    }
    return "overview";
}

const char* PageTitle(EMonPage page)
{
    switch (page) {
        case EMonPage::Overview:
            return "Overview";
        case EMonPage::Dbg:
            return "DBGs";
        case EMonPage::LocalDb:
            return "Local DB";
        case EMonPage::VChunk:
            return "VChunk";
        case EMonPage::Latency:
            return "Latency";
    }
    return "";
}

// Mon page of the DDisk actor behind the id; the "/node/<id>" prefix makes the
// link work from any node's mon. The path format mirrors
// TDDiskActor::RegisterMonPage.
TString MakeDDiskMonPageUrl(const NKikimr::NBsController::TDDiskId& ddiskId)
{
    return TStringBuilder()
           << "/node/" << ddiskId.NodeId
           << Sprintf(
                  "/actors/ddisks/ddisk_p%09" PRIu32 "_s%09" PRIu32,
                  ddiskId.PDiskId,
                  ddiskId.DDiskSlotId);
}

void RenderDDiskLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& ddiskId)
{
    str << "<a href='" << MakeDDiskMonPageUrl(ddiskId) << "'>"
        << HtmlEscape(ddiskId.ToString()) << "</a>";
}

// Mon page of the persistent buffer behind the id: the node's "Persistent
// Buffer" page filtered to this pbuffer's service actor (its "pb" filter
// matches ToString of the well-known service id).
TString MakePBufferMonPageUrl(const NKikimr::NBsController::TDDiskId& pbufferId)
{
    const auto serviceId = NKikimr::MakeBlobStoragePersistentBufferId(
        pbufferId.NodeId,
        pbufferId.PDiskId,
        pbufferId.DDiskSlotId);
    return TStringBuilder()
           << "/node/" << pbufferId.NodeId << "/actors/persistent_buffer?pb="
           << CGIEscapeRet(serviceId.ToString());
}

void RenderPBufferLink(
    IOutputStream& str,
    const NKikimr::NBsController::TDDiskId& pbufferId)
{
    str << "<a href='" << MakePBufferMonPageUrl(pbufferId) << "'>"
        << HtmlEscape(pbufferId.ToString()) << "</a>";
}

// "6 Online" or "4 Online / 2 Sufferer".
TString HealthRollup(const TMap<EHostHealth, size_t>& counts)
{
    TStringBuilder sb;
    for (const auto& [health, count]: counts) {
        if (!sb.empty()) {
            sb << " / ";
        }
        sb << count << " " << ToString(health);
    }
    return sb.empty() ? TString("-") : TString(sb);
}

////////////////////////////////////////////////////////////////////////////////

void RenderHeader(IOutputStream& str, const TTabletInfo& tabletInfo)
{
    HTML (str) {
        TAG (TH3) {
            str << "partition_direct tablet " << tabletInfo.TabletId;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "TabletId";
                    }
                    TABLED () {
                        str << tabletInfo.TabletId;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Generation";
                    }
                    TABLED () {
                        str << tabletInfo.Generation;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "DiskId";
                    }
                    TABLED () {
                        str << HtmlEscape(tabletInfo.DiskId);
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "State";
                    }
                    TABLED () {
                        str << HtmlEscape(tabletInfo.State);
                    }
                }
            }
        }
    }
}

void RenderMenu(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    EMonPage current)
{
    static const EMonPage pages[] = {
        EMonPage::Overview,
        EMonPage::Dbg,
        EMonPage::LocalDb,
        EMonPage::VChunk,
        EMonPage::Latency,
    };
    str << "<div style='margin:0.5em 0 1em;'>";
    for (EMonPage page: pages) {
        const char* btnClass =
            (page == current) ? "btn btn-primary" : "btn btn-default";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.4em;'"
               " href='?TabletID="
            << tabletInfo.TabletId << "&page=" << PageParam(page) << "'>"
            << PageTitle(page) << "</a>";
    }
    str << "</div>";
}

void RenderOverview(IOutputStream& str, const TFastPathServiceInfo& info)
{
    HTML (str) {
        TAG (TH3) {
            str << "Overview";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "DirectBlockGroups";
                    }
                    TABLED () {
                        str << info.DbgCount;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "VChunks (total)";
                    }
                    TABLED () {
                        str << info.TotalVChunks;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "LSN counter";
                    }
                    TABLED () {
                        str << info.LsnCounter;
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Last safe barrier";
                    }
                    TABLED () {
                        if (info.LastSafeBarrier != 0) {
                            str << info.LastSafeBarrier;
                        } else {
                            str << "-";
                        }
                    }
                }
            }
        }
    }
}

void RenderDbgList(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TVector<TDbgSnapshot>& dbgs)
{
    HTML (str) {
        TAG (TH3) {
            str << "Direct Block Groups";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "DBG";
                    }
                    TABLEH () {
                        str << "Hosts";
                    }
                    TABLEH () {
                        str << "VChunks";
                    }
                    TABLEH () {
                        str << "Host health";
                    }
                    TABLEH () {
                        str << "Inflight";
                    }
                    TABLEH () {
                        str << "Consecutive errors";
                    }
                    TABLEH () {
                        str << "Consecutive success";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& dbg: dbgs) {
                    TMap<EHostHealth, size_t> healthCounts;
                    size_t inflight = 0;
                    size_t consecutiveErrors = 0;
                    size_t consecutiveSuccesses = 0;
                    for (const auto& host: dbg.Hosts) {
                        ++healthCounts[host.Health];
                        consecutiveErrors += host.Errors.ConsecutiveErrorCount;
                        consecutiveSuccesses +=
                            host.Errors.ConsecutiveSuccessCount;
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            inflight += host.InflightByOperation[operation];
                        }
                    }
                    TABLER () {
                        TABLED () {
                            str << "<a href='?TabletID=" << tabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>#"
                                << dbg.Index << "</a>";
                        }
                        TABLED () {
                            str << dbg.Hosts.size();
                        }
                        TABLED () {
                            str << dbg.VChunkCount;
                        }
                        TABLED () {
                            str << HealthRollup(healthCounts);
                        }
                        TABLED () {
                            str << inflight;
                        }
                        TABLED () {
                            str << consecutiveErrors;
                        }
                        TABLED () {
                            str << consecutiveSuccesses;
                        }
                    }
                }
            }
        }
    }
}

void RenderDbgDetail(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TDbgSnapshot& dbg)
{
    str << "<div style='margin-bottom:0.5em;'><a href='?TabletID="
        << tabletInfo.TabletId << "&page=dbg'>&larr; back to DBGs</a></div>";
    // POST, not a link: link prefetching must not add hosts.
    //
    // The same parameters go into both the action URL and the hidden fields
    // because the request has two readers, each looking at one place only:
    // the mon proxy picks the target tablet from the POST body, while the
    // tablet's Cgi() reads the URL query.
    str << "<form method='post' action='?TabletID=" << tabletInfo.TabletId
        << "&page=dbg&dbg=" << dbg.Index
        << "&action=addhost' style='margin-bottom:0.5em;'>"
           "<input type='hidden' name='TabletID' value='"
        << tabletInfo.TabletId
        << "'/>"
           "<input type='hidden' name='page' value='dbg'/>"
           "<input type='hidden' name='dbg' value='"
        << dbg.Index
        << "'/>"
           "<input type='hidden' name='action' value='addhost'/>"
           "<button type='submit' class='btn btn-default'>Add host</button>"
           "</form>";
    HTML (str) {
        TAG (TH3) {
            str << "DBG #" << dbg.Index;
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "State";
                    }
                    TABLEH () {
                        str << "Health";
                    }
                    TABLEH () {
                        str << "PBuffer used";
                    }
                    TABLEH () {
                        str << "Consecutive errors";
                    }
                    TABLEH () {
                        str << "Consecutive success";
                    }
                    for (size_t operation = 0; operation < OperationCount;
                         ++operation)
                    {
                        TABLEH () {
                            str << ToString(static_cast<EOperation>(operation));
                        }
                    }
                }
            }
            TABLEBODY () {
                for (const auto& host: dbg.Hosts) {
                    TABLER () {
                        TABLED () {
                            str << PrintHostIndex(host.Index);
                        }
                        TABLED () {
                            str << ToString(host.State);
                        }
                        TABLED () {
                            str << ToString(host.Health);
                        }
                        TABLED () {
                            str << host.PBufferUsedSize;
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveErrorCount;
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveSuccessCount;
                        }
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            TABLED () {
                                str << host.InflightByOperation[operation];
                            }
                        }
                    }
                }
            }
        }
        TAG (TH4) {
            str << "Connections";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "DDisk id";
                    }
                    TABLEH () {
                        str << "PBuffer id";
                    }
                    TABLEH () {
                        str << "DDisk session";
                    }
                    TABLEH () {
                        str << "PBuffer connected";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& connection: dbg.Connections) {
                    TABLER () {
                        TABLED () {
                            str << PrintHostIndex(connection.HostIndex);
                        }
                        TABLED () {
                            RenderDDiskLink(str, connection.DDiskId);
                        }
                        TABLED () {
                            if (connection.PBufferId) {
                                RenderPBufferLink(str, *connection.PBufferId);
                            }
                        }
                        TABLED () {
                            str << connection.DDiskSession;
                        }
                        TABLED () {
                            str << (connection.PBufferConnected ? "yes" : "no");
                        }
                    }
                }
            }
        }
    }
}

void RenderProtoDump(
    IOutputStream& str,
    const char* name,
    const std::optional<TString>& dump)
{
    if (!dump) {
        str << "<div style='margin-bottom:0.5em;'>" << name << " (none)</div>";
        return;
    }
    // display:list-item brings back the fold triangle that the page CSS
    // hides; the pointer marks the line as clickable.
    str << "<details style='margin-bottom:0.5em;'>"
           "<summary style='display:list-item; cursor:pointer;'>"
        << name << "</summary><pre>" << HtmlEscape(*dump) << "</pre></details>";
}

void RenderLocalDb(IOutputStream& str, const TLocalDbContents& db)
{
    HTML (str) {
        TAG (TH3) {
            str << "Local DB";
        }
        RenderProtoDump(str, "VolumeConfig", db.VolumeConfig);
        RenderProtoDump(
            str,
            "DirectBlockGroupsConnections",
            db.DirectBlockGroupsConnections);
        RenderProtoDump(str, "AddHostInProgress", db.AddHostInProgress);
        TAG (TH4) {
            str << "VChunkConfigs (persisted overrides)";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "VChunkIndex";
                    }
                    TABLEH () {
                        str << "Config";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& config: db.VChunkConfigs) {
                    TABLER () {
                        TABLED () {
                            str << config.GetVChunkIndex();
                        }
                        TABLED () {
                            str << "<pre>" << HtmlEscape(config.DebugPrint())
                                << "</pre>";
                        }
                    }
                }
            }
        }
    }
}

void RenderVChunk(IOutputStream& str, const TMonPageData& data)
{
    // Looking up a vchunk changes nothing, so this is a GET form. On submit a
    // GET form rebuilds the query string from its fields ALONE and drops the
    // current one - so TabletID and page (which live in the URL as
    // ?TabletID=..&page=vchunk) must be repeated as hidden fields, otherwise
    // the submit lands on ?vchunk=N with no tablet and no page.
    str << "<form method='get' action='' style='margin-bottom:1em;'>"
           "<input type='hidden' name='TabletID' value='"
        << data.TabletInfo.TabletId
        << "'/>"
           "<input type='hidden' name='page' value='vchunk'/>"
           "VChunk index: <input type='number' name='vchunk' min='0' value='";
    if (data.SelectedVChunk) {
        str << *data.SelectedVChunk;
    }
    str << "'/> <button type='submit' class='btn btn-default'>Show</button>"
           "</form>";

    if (!data.SelectedVChunk) {
        return;
    }
    if (!data.VChunk) {
        HTML (str) {
            DIV_CLASS ("alert alert-warning") {
                str << "VChunk #" << *data.SelectedVChunk << " not found.";
            }
        }
        return;
    }

    const TVChunkSnapshot& vchunk = *data.VChunk;
    const TVChunkConfig& config = vchunk.VChunkConfig;
    HTML (str) {
        TAG (TH3) {
            str << "VChunk #" << config.GetVChunkIndex();
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        str << "DBG";
                    }
                    TABLED () {
                        str << "<a href='?TabletID=" << data.TabletInfo.TabletId
                            << "&page=dbg&dbg=" << config.GetDBGIndex() << "'>#"
                            << config.GetDBGIndex() << "</a>";
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Safe barrier";
                    }
                    TABLED () {
                        if (vchunk.SafeBarrier) {
                            str << *vchunk.SafeBarrier;
                        } else {
                            str << "-";
                        }
                    }
                }
            }
        }
        TAG (TH4) {
            str << "Host roles";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
                    }
                    TABLEH () {
                        str << "PBuffer role";
                    }
                    TABLEH () {
                        str << "DDisk role";
                    }
                    TABLEH () {
                        str << "Enabled";
                    }
                    TABLEH () {
                        str << "Watermark";
                    }
                }
            }
            TABLEBODY () {
                const auto disabled = config.GetDisabledHosts();
                for (THostIndex host = 0; host < config.GetHostCount(); ++host)
                {
                    TABLER () {
                        TABLED () {
                            str << PrintHostIndex(host);
                        }
                        TABLED () {
                            str << ToString(config.GetPBufferRole(host));
                        }
                        TABLED () {
                            str << ToString(config.GetDDiskRole(host));
                        }
                        TABLED () {
                            str << (disabled.Get(host) ? "no" : "yes");
                        }
                        TABLED () {
                            const auto watermark = config.GetWatermark(host);
                            if (watermark) {
                                str << *watermark;
                            } else {
                                str << "-";
                            }
                        }
                    }
                }
            }
        }
        str << "<details style='margin-bottom:0.5em;'>"
               "<summary style='display:list-item; cursor:pointer;'>"
               "Dirty map dump</summary><pre>"
            << HtmlEscape(vchunk.DirtyMapDump) << "</pre></details>";
    }
}

void RenderDbg(IOutputStream& str, const TMonPageData& data)
{
    if (!data.SelectedDbg) {
        RenderDbgList(str, data.TabletInfo, data.Dbgs);
        return;
    }
    for (const auto& dbg: data.Dbgs) {
        if (dbg.Index == *data.SelectedDbg) {
            RenderDbgDetail(str, data.TabletInfo, dbg);
            return;
        }
    }
    HTML (str) {
        DIV_CLASS ("alert alert-warning") {
            str << "DBG #" << *data.SelectedDbg << " not found.";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Latency page
////////////////////////////////////////////////////////////////////////////////

const char* PercentileParam(ELatencyPercentile p)
{
    switch (p) {
        case ELatencyPercentile::P50:
            return "50";
        case ELatencyPercentile::P90:
            return "90";
        case ELatencyPercentile::P99:
            return "99";
        case ELatencyPercentile::Max:
            return "max";
    }
    return "99";
}

const char* PercentileTitle(ELatencyPercentile p)
{
    switch (p) {
        case ELatencyPercentile::P50:
            return "p50";
        case ELatencyPercentile::P90:
            return "p90";
        case ELatencyPercentile::P99:
            return "p99";
        case ELatencyPercentile::Max:
            return "max";
    }
    return "p99";
}

TDuration SelectedLatencyValue(
    const TLatencyStats& stats,
    ELatencyPercentile percentile)
{
    switch (percentile) {
        case ELatencyPercentile::P50:
            return stats.P50;
        case ELatencyPercentile::P90:
            return stats.P90;
        case ELatencyPercentile::P99:
            return stats.P99;
        case ELatencyPercentile::Max:
            return stats.Max;
    }
    return stats.P99;
}

// Fold rhs into lhs. Count sums; Min/Max are true extrema; percentiles take
// the max across contributors (worst-slot indicator — samples never leave
// their DBG executor, so a true node-wide percentile is unavailable).
void MergeLatencyStats(TLatencyStats& lhs, const TLatencyStats& rhs)
{
    if (rhs.Count == 0) {
        return;
    }
    if (lhs.Count == 0) {
        lhs = rhs;
        return;
    }
    lhs.Count += rhs.Count;
    lhs.Min = Min(lhs.Min, rhs.Min);
    lhs.Max = Max(lhs.Max, rhs.Max);
    lhs.P50 = Max(lhs.P50, rhs.P50);
    lhs.P90 = Max(lhs.P90, rhs.P90);
    lhs.P99 = Max(lhs.P99, rhs.P99);
}

// light green <500us, dark green <1ms, yellow <5ms, red <20ms, dark red ≥20ms
const char* LatencyColor(TDuration value)
{
    const ui64 us = value.MicroSeconds();
    if (us == 0) {
        return "#f0f0f0";
    }
    if (us < 500) {
        return "#90ee90";   // light green
    }
    if (us < 1000) {
        return "#228b22";   // dark green
    }
    if (us < 5000) {
        return "#ffd54f";   // yellow
    }
    if (us < 20000) {
        return "#e74c3c";   // red
    }
    return "#8b0000";   // dark red
}

// Bar width 0..100% on a linear scale capped at 20ms.
ui32 LatencyBarWidthPct(TDuration value)
{
    const ui64 us = value.MicroSeconds();
    if (us == 0) {
        return 0;
    }
    constexpr ui64 CapUs = 20000;
    return static_cast<ui32>(Min<ui64>(100, Max<ui64>(4, us * 100 / CapUs)));
}

TString LatencyStatsTitle(const TLatencyStats& stats)
{
    if (stats.Count == 0) {
        return "no samples";
    }
    return TStringBuilder()
           << "n=" << stats.Count << " min=" << FormatDuration(stats.Min)
           << " p50=" << FormatDuration(stats.P50)
           << " p90=" << FormatDuration(stats.P90)
           << " p99=" << FormatDuration(stats.P99)
           << " max=" << FormatDuration(stats.Max);
}

// Value text plus a proportional coloured bar. All percentiles are embedded
// as data-* so the client can redraw without a server round-trip.
void RenderLatencyValueBar(
    IOutputStream& str,
    const TLatencyStats& stats,
    ELatencyPercentile percentile)
{
    const TDuration value = SelectedLatencyValue(stats, percentile);
    const ui32 width = LatencyBarWidthPct(value);
    str << "<div class='lat-bar' style='min-width:4.5em;'"
           " data-count='"
        << stats.Count << "' data-min='" << stats.Min.MicroSeconds()
        << "' data-p50='" << stats.P50.MicroSeconds() << "' data-p90='"
        << stats.P90.MicroSeconds() << "' data-p99='"
        << stats.P99.MicroSeconds() << "' data-max='"
        << stats.Max.MicroSeconds() << "' title='"
        << HtmlEscape(LatencyStatsTitle(stats))
        << "'>"
           "<div class='lat-bar-text'>"
        << FormatDuration(value)
        << "</div>"
           "<div style='height:4px; background:#eee; margin-top:2px;'>"
           "<div class='lat-bar-fill' style='height:100%; width:"
        << width << "%; background:" << LatencyColor(value)
        << ";'></div>"
           "</div></div>";
}

TString LatencyPageHref(
    const TMonPageData& data,
    ELatencyPercentile percentile,
    const std::optional<EOperation>& operation)
{
    TStringBuilder sb;
    sb << "?TabletID=" << data.TabletInfo.TabletId
       << "&page=latency&p=" << PercentileParam(percentile);
    if (operation) {
        sb << "&op=" << static_cast<size_t>(*operation);
    }
    return sb;
}

struct THostConnections
{
    NKikimr::NBsController::TDDiskId DDiskId;
    std::optional<NKikimr::NBsController::TDDiskId> PBufferId;
};

struct TSlotLatency
{
    NKikimr::NBsController::TDDiskId SlotId;
    bool IsDDisk = true;
    // Per-operation folded stats for this slot.
    TLatencyByOperation ByOperation{};
};

struct TNodeLatency
{
    ui32 NodeId = 0;
    // Per-operation folded stats across all slots of this node.
    TLatencyByOperation ByOperation{};
    TMap<NKikimr::NBsController::TDDiskId, TSlotLatency> Slots;
};

TString SlotOpsJson(const TSlotLatency& slot)
{
    TStringBuilder sb;
    sb << "[";
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        if (operation != 0) {
            sb << ",";
        }
        const auto& stats = slot.ByOperation[operation];
        if (stats.Count == 0) {
            sb << "null";
        } else {
            sb << "{\"c\":" << stats.Count
               << ",\"min\":" << stats.Min.MicroSeconds()
               << ",\"p50\":" << stats.P50.MicroSeconds()
               << ",\"p90\":" << stats.P90.MicroSeconds()
               << ",\"p99\":" << stats.P99.MicroSeconds()
               << ",\"max\":" << stats.Max.MicroSeconds() << "}";
        }
    }
    sb << "]";
    return sb;
}

TMap<ui32, TNodeLatency> AggregateLatencyByNode(
    const TVector<TDbgSnapshot>& dbgs)
{
    TMap<ui32, TNodeLatency> nodes;
    for (const auto& dbg: dbgs) {
        THashMap<THostIndex, THostConnections> hostConnections;
        for (const auto& connection: dbg.Connections) {
            hostConnections[connection.HostIndex] = {
                .DDiskId = connection.DDiskId,
                .PBufferId = connection.PBufferId,
            };
        }

        for (const auto& host: dbg.Hosts) {
            const auto* connections = hostConnections.FindPtr(host.Index);
            if (!connections) {
                continue;
            }
            for (size_t operation = 0; operation < OperationCount; ++operation)
            {
                const auto& stats = host.LatencyByOperation[operation];
                if (stats.Count == 0) {
                    continue;
                }
                const auto op = static_cast<EOperation>(operation);
                const bool isDDisk = IsDDiskOperation(op);
                std::optional<NKikimr::NBsController::TDDiskId> slotId;
                if (isDDisk) {
                    slotId = connections->DDiskId;
                } else if (connections->PBufferId) {
                    slotId = *connections->PBufferId;
                }
                if (!slotId) {
                    continue;
                }

                auto& node = nodes[slotId->NodeId];
                node.NodeId = slotId->NodeId;
                MergeLatencyStats(node.ByOperation[operation], stats);

                auto& slot = node.Slots[*slotId];
                slot.SlotId = *slotId;
                slot.IsDDisk = isDDisk;
                MergeLatencyStats(slot.ByOperation[operation], stats);
            }
        }
    }
    return nodes;
}

TLatencyStats SlotDisplayStats(
    const TSlotLatency& slot,
    const std::optional<EOperation>& selectedOp)
{
    if (selectedOp) {
        return slot.ByOperation[static_cast<size_t>(*selectedOp)];
    }
    // Worst across operations by p99.
    TLatencyStats worst;
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        const auto& stats = slot.ByOperation[operation];
        if (stats.Count == 0) {
            continue;
        }
        if (worst.Count == 0 || stats.P99 > worst.P99) {
            worst = stats;
        }
    }
    return worst;
}

void RenderLatencyLegend(IOutputStream& str)
{
    str << "<div style='margin-bottom:0.5em; font-size:0.9em;'>";

    struct TBucket
    {
        const char* Label;
        const char* Color;
    };

    static const TBucket buckets[] = {
        {"&lt;500us", "#90ee90"},
        {"&lt;1ms", "#228b22"},
        {"&lt;5ms", "#ffd54f"},
        {"&lt;20ms", "#e74c3c"},
        {"&ge;20ms", "#8b0000"},
    };
    for (const auto& bucket: buckets) {
        str << "<span style='display:inline-block; width:1.2em; height:1.2em;"
               " background:"
            << bucket.Color
            << "; border:1px solid #ccc; margin-right:0.2em;"
               " vertical-align:middle;'></span>"
            << bucket.Label << "&nbsp;&nbsp;";
    }
    str << "</div>";
}

void RenderPercentileSelector(IOutputStream& str, const TMonPageData& data)
{
    str << "<div style='margin:0.5em 0;'>Percentile: ";
    static const ELatencyPercentile percentiles[] = {
        ELatencyPercentile::P50,
        ELatencyPercentile::P90,
        ELatencyPercentile::P99,
        ELatencyPercentile::Max,
    };
    for (ELatencyPercentile p: percentiles) {
        const char* btnClass = (p == data.SelectedPercentile)
                                   ? "btn btn-primary btn-sm lat-nav"
                                   : "btn btn-default btn-sm lat-nav";
        str << "<a class='" << btnClass << "' data-p='" << PercentileParam(p)
            << "' style='margin-right:0.3em;'"
               " href='"
            << LatencyPageHref(data, p, data.SelectedLatencyOperation) << "'>"
            << PercentileTitle(p) << "</a>";
    }
    str << "</div>";
}

void RenderSlotOperationSelector(IOutputStream& str, const TMonPageData& data)
{
    str << "<div style='margin:0.5em 0;'>Slot grid operation: ";
    {
        const char* btnClass = !data.SelectedLatencyOperation
                                   ? "btn btn-primary btn-sm lat-nav"
                                   : "btn btn-default btn-sm lat-nav";
        str << "<a class='" << btnClass
            << "' data-op='' style='margin-right:0.3em;'"
               " href='"
            << LatencyPageHref(data, data.SelectedPercentile, std::nullopt)
            << "'>worst</a>";
    }
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        const auto op = static_cast<EOperation>(operation);
        const bool selected = data.SelectedLatencyOperation &&
                              *data.SelectedLatencyOperation == op;
        const char* btnClass = selected ? "btn btn-primary btn-sm lat-nav"
                                        : "btn btn-default btn-sm lat-nav";
        str << "<a class='" << btnClass << "' data-op='" << operation
            << "' style='margin-right:0.3em;'"
               " href='"
            << LatencyPageHref(data, data.SelectedPercentile, op) << "'>"
            << ToString(op) << "</a>";
    }
    str << "</div>";
}

void RenderLatencyAutoRefreshControls(IOutputStream& str)
{
    str << "<div style='margin:0.5em 0;'>"
           "<label style='margin-right:0.6em;'>"
           "<input type='checkbox' id='latencyAutoRefresh'/> Auto refresh"
           "</label>"
           "<label>every <input type='number' id='latencyRefreshRate' "
           "min='1' value='1' style='width:4em;'/> sec</label>"
           "</div>";
}

void RenderLatencyClientScript(IOutputStream& str)
{
    // Must run AFTER #latencyLiveContent is in the DOM so bindUi can find
    // Show slots / Show data. Auto-refresh swaps only the live region's
    // innerHTML; binders live here so they survive that swap.
    // Percentile / Slot grid operation only redraw client-side (no fetch).
    TStringBuilder opNamesJs;
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        if (operation != 0) {
            opNamesJs << ",";
        }
        opNamesJs << "'" << ToString(static_cast<EOperation>(operation)) << "'";
    }

    str << "<script>"
           "(function(){"
           "var cb=document.getElementById('latencyAutoRefresh');"
           "var inp=document.getElementById('latencyRefreshRate');"
           "var key='pdLatencyAutoRefresh';"
           "var rateKey='pdLatencyRefreshRate';"
           "var uiKey='pdLatencyUiState';"
           "var timer=null;"
           "var refreshing=false;"
           "var opNames=["
        << opNamesJs
        << "];"
           "function val(id){var el=document.getElementById(id);return "
           "el?el.value:'';}"
           "function checked(id){var el=document.getElementById(id);return "
           "!!(el&&el.checked);}"
           "function setChecked(id,v){var "
           "el=document.getElementById(id);if(el)el.checked=!!v;}"
           "function setVal(id,v){var el=document.getElementById(id);if(el){"
           "for(var "
           "i=0;i<el.options.length;i++){if(el.options[i].value===v){el.value="
           "v;return;}}"
           "}}"
           "function saveUi(){"
           "var "
           "s={showSlots:checked('latShowSlots'),slotNode:val('"
           "latSlotNodeFilter'),"
           "showDetail:checked('latShowDetail'),fNode:val('latFilterNode'),"
           "fPdisk:val('latFilterPdisk'),fType:val('latFilterType'),fOp:val('"
           "latFilterOp')};"
           "sessionStorage.setItem(uiKey,JSON.stringify(s));"
           "return s;"
           "}"
           "function loadUi(){"
           "try{return "
           "JSON.parse(sessionStorage.getItem(uiKey)||'{}');}catch(e){return "
           "{};}"
           "}"
           "function restoreUi(s){"
           "if(!s)s=loadUi();"
           "setChecked('latShowSlots',s.showSlots);"
           "setVal('latSlotNodeFilter',s.slotNode||'');"
           "setChecked('latShowDetail',s.showDetail);"
           "setVal('latFilterNode',s.fNode||'');"
           "setVal('latFilterPdisk',s.fPdisk||'');"
           "setVal('latFilterType',s.fType||'');"
           "setVal('latFilterOp',s.fOp||'');"
           "}"
           "function applySlots(){"
           "var show=document.getElementById('latShowSlots');"
           "var body=document.getElementById('latSlotsBody');"
           "var nodeSel=document.getElementById('latSlotNodeFilter');"
           "if(!show||!body||!nodeSel)return;"
           "body.style.display=show.checked?'':'none';"
           "var filter=nodeSel.value;"
           "var nodes=body.querySelectorAll('.lat-slot-node');"
           "for(var i=0;i<nodes.length;i++){"
           "var n=nodes[i];"
           "n.style.display=(!filter||n.dataset.node===filter)?'':'none';"
           "}"
           "}"
           "function applyDetail(){"
           "var show=document.getElementById('latShowDetail');"
           "var body=document.getElementById('latDetailBody');"
           "var table=document.getElementById('latencyDetailTable');"
           "if(!show||!body)return;"
           "body.style.display=show.checked?'':'none';"
           "if(!show.checked||!table)return;"
           "var tbody=table.tBodies[0];"
           "var fNode=document.getElementById('latFilterNode');"
           "var fPdisk=document.getElementById('latFilterPdisk');"
           "var fType=document.getElementById('latFilterType');"
           "var fOp=document.getElementById('latFilterOp');"
           "var rows=tbody.rows;"
           "for(var i=0;i<rows.length;i++){"
           "var r=rows[i];"
           "var ok=(!fNode.value||r.dataset.node===fNode.value)"
           "&&(!fPdisk.value||r.dataset.pdisk===fPdisk.value)"
           "&&(!fType.value||r.dataset.type===fType.value)"
           "&&(!fOp.value||r.dataset.op===fOp.value);"
           "r.style.display=ok?'':'none';"
           "}"
           "}"
           "function bindUi(){"
           "var showSlots=document.getElementById('latShowSlots');"
           "var slotNode=document.getElementById('latSlotNodeFilter');"
           "var showDetail=document.getElementById('latShowDetail');"
           "var table=document.getElementById('latencyDetailTable');"
           "if(showSlots){showSlots.onchange=function(){saveUi();applySlots();}"
           ";}"
           "if(slotNode){slotNode.onchange=function(){saveUi();applySlots();};}"
           "if(showDetail){showDetail.onchange=function(){saveUi();applyDetail("
           ");};}"
           "['latFilterNode','latFilterPdisk','latFilterType','latFilterOp']."
           "forEach(function(id){"
           "var el=document.getElementById(id);"
           "if(el){el.onchange=function(){saveUi();applyDetail();};}"
           "});"
           "if(table){"
           "table.tHead.rows[0].onclick=function(ev){"
           "var th=ev.target.closest('th[data-sort]');"
           "if(!th)return;"
           "var key=th.getAttribute('data-sort');"
           "var asc=th.getAttribute('data-asc')!=='1';"
           "th.setAttribute('data-asc',asc?'1':'0');"
           "var tbody=table.tBodies[0];"
           "var rows=Array.prototype.slice.call(tbody.rows);"
           "rows.sort(function(a,b){"
           "var av=Number(a.dataset[key]||0),bv=Number(b.dataset[key]||0);"
           "return asc?av-bv:bv-av;"
           "});"
           "rows.forEach(function(r){tbody.appendChild(r);});"
           "};"
           "}"
           "applySlots();"
           "applyDetail();"
           "}"
           "function selectedP(){"
           "var a=document.querySelector('a.lat-nav[data-p].btn-primary');"
           "return a?a.getAttribute('data-p'):'99';"
           "}"
           "function selectedOp(){"
           "var a=document.querySelector('a.lat-nav[data-op].btn-primary');"
           "return a?a.getAttribute('data-op'):'';"
           "}"
           "function pTitle(p){"
           "return p==='max'?'max':('p'+p);"
           "}"
           "function fmtUs(us){"
           "us=Number(us)||0;"
           "if(us===0)return '0';"
           "if(us<1000)return us+'us';"
           "if(us<1000000)return (us/1000).toFixed(3)+'ms';"
           "return (us/1000000).toFixed(3)+'s';"
           "}"
           "function latColor(us){"
           "us=Number(us)||0;"
           "if(us===0)return '#f0f0f0';"
           "if(us<500)return '#90ee90';"
           "if(us<1000)return '#228b22';"
           "if(us<5000)return '#ffd54f';"
           "if(us<20000)return '#e74c3c';"
           "return '#8b0000';"
           "}"
           "function latWidth(us){"
           "us=Number(us)||0;"
           "if(us===0)return 0;"
           "return Math.min(100,Math.max(4,Math.floor(us*100/20000)));"
           "}"
           "function pickUs(stats,p){"
           "if(p==='50')return Number(stats.p50);"
           "if(p==='90')return Number(stats.p90);"
           "if(p==='max')return Number(stats.max);"
           "return Number(stats.p99);"
           "}"
           "function paintBar(el,p){"
           "var stats={p50:el.dataset.p50,p90:el.dataset.p90,"
           "p99:el.dataset.p99,max:el.dataset.max};"
           "var us=pickUs(stats,p);"
           "var text=el.querySelector('.lat-bar-text');"
           "var fill=el.querySelector('.lat-bar-fill');"
           "if(text)text.textContent=fmtUs(us);"
           "if(fill){fill.style.width=latWidth(us)+'%';"
           "fill.style.background=latColor(us);}"
           "}"
           "function barHtml(stats,p){"
           "var us=pickUs(stats,p);"
           "var title='n='+stats.c+' min='+fmtUs(stats.min)"
           "+' p50='+fmtUs(stats.p50)+' p90='+fmtUs(stats.p90)"
           "+' p99='+fmtUs(stats.p99)+' max='+fmtUs(stats.max);"
           "return \"<div class='lat-bar' style='min-width:4.5em;'\"+"
           "\" data-count='\"+stats.c+\"' data-min='\"+stats.min+\"'\"+"
           "\" data-p50='\"+stats.p50+\"' data-p90='\"+stats.p90+\"'\"+"
           "\" data-p99='\"+stats.p99+\"' data-max='\"+stats.max+\"'\"+"
           "\" title='\"+title+\"'>\"+"
           "\"<div class='lat-bar-text'>\"+fmtUs(us)+\"</div>\"+"
           "\"<div style='height:4px; background:#eee; margin-top:2px;'>\"+"
           "\"<div class='lat-bar-fill' style='height:100%; width:\"+"
           "latWidth(us)+\"%; background:\"+latColor(us)+\";'></div>\"+"
           "\"</div></div>\";"
           "}"
           "function slotStats(ops,op){"
           "if(op!==''&&op!=null){"
           "var i=Number(op);"
           "return (ops&&ops[i])?ops[i]:null;"
           "}"
           "var worst=null;"
           "for(var i=0;i<(ops||[]).length;i++){"
           "var s=ops[i];if(!s)continue;"
           "if(!worst||Number(s.p99)>Number(worst.p99))worst=s;"
           "}"
           "return worst;"
           "}"
           "function redrawViews(){"
           "var p=selectedP();"
           "var op=selectedOp();"
           "var title=document.getElementById('latHeatmapTitle');"
           "if(title)title.textContent='Latency by node ('+pTitle(p)+')';"
           "var desc=document.getElementById('latSlotDesc');"
           "if(desc){"
           "var opPart=(op==='')?' (worst op by p99)':"
           "(' for '+(opNames[Number(op)]||op));"
           "desc.textContent='Each cell is one ddisk / pbuffer actor slot "
           "(node:pdisk:slot), one pdisk per row. Uses the selected percentile'"
           "+opPart+'.';"
           "}"
           "var heat=document.getElementById('latHeatmapTable');"
           "if(heat){"
           "var bars=heat.querySelectorAll('.lat-bar');"
           "for(var i=0;i<bars.length;i++)paintBar(bars[i],p);"
           "}"
           "var slots=document.querySelectorAll('a.lat-slot');"
           "for(var i=0;i<slots.length;i++){"
           "var a=slots[i];"
           "var ops=[];"
           "try{ops=JSON.parse(a.getAttribute('data-ops')||'[]');}"
           "catch(e){ops=[];}"
           "var stats=slotStats(ops,op);"
           "var box=a.querySelector('.lat-slot-val');"
           "if(!box)continue;"
           "if(!stats){box.innerHTML=\"<span style='color:#999;'>-</span>\";}"
           "else{box.innerHTML=barHtml(stats,p);}"
           "}"
           "}"
           "function setNavActive(a){"
           "if(a.hasAttribute('data-p')){"
           "document.querySelectorAll('a.lat-nav[data-p]').forEach("
           "function(el){el.className=el===a?"
           "'btn btn-primary btn-sm lat-nav':'btn btn-default btn-sm lat-nav';"
           "});"
           "}else if(a.hasAttribute('data-op')){"
           "document.querySelectorAll('a.lat-nav[data-op]').forEach("
           "function(el){el.className=el===a?"
           "'btn btn-primary btn-sm lat-nav':'btn btn-default btn-sm lat-nav';"
           "});"
           "}"
           "}"
           "function syncNavFromUrl(){"
           "var q=new URLSearchParams(location.search);"
           "var p=q.get('p')||'99';"
           "var op=q.has('op')?q.get('op'):'';"
           "document.querySelectorAll('a.lat-nav[data-p]').forEach("
           "function(el){el.className=(el.getAttribute('data-p')===p)?"
           "'btn btn-primary btn-sm lat-nav':'btn btn-default btn-sm lat-nav';"
           "});"
           "document.querySelectorAll('a.lat-nav[data-op]').forEach("
           "function(el){"
           "var v=el.getAttribute('data-op');"
           "el.className=(v===op)?"
           "'btn btn-primary btn-sm lat-nav':'btn btn-default btn-sm lat-nav';"
           "});"
           "redrawViews();"
           "}"
           "function refreshLive(){"
           "if(refreshing)return;"
           "refreshing=true;"
           "var ui=saveUi();"
           "fetch(location.href,{credentials:'same-origin',cache:'no-store'})"
           ".then(function(r){if(!r.ok)throw new Error('http "
           "'+r.status);return r.text();})"
           ".then(function(html){"
           "var doc=new DOMParser().parseFromString(html,'text/html');"
           "var next=doc.getElementById('latencyLiveContent');"
           "var cur=document.getElementById('latencyLiveContent');"
           "if(next&&cur){cur.innerHTML=next.innerHTML;}"
           "restoreUi(ui);"
           "bindUi();"
           "})"
           ".catch(function(){})"
           ".then(function(){refreshing=false;});"
           "}"
           "function schedule(){"
           "if(timer){clearInterval(timer);timer=null;}"
           "if(!cb.checked){return;}"
           "var sec=parseInt(inp.value,10);if(!(sec>0))sec=1;"
           "timer=setInterval(refreshLive,sec*1000);"
           "}"
           // Percentile / Slot grid operation: pushState + client redraw only.
           "document.addEventListener('click',function(ev){"
           "var a=ev.target.closest('a.lat-nav');"
           "if(!a)return;"
           "var live=document.getElementById('latencyLiveContent');"
           "if(!live||!live.contains(a))return;"
           "ev.preventDefault();"
           "var href=a.getAttribute('href');"
           "if(!href)return;"
           "setNavActive(a);"
           "if(href!==location.pathname+location.search){"
           "history.pushState(null,'',href);"
           "}"
           "redrawViews();"
           "});"
           "window.addEventListener('popstate',function(){syncNavFromUrl();});"
           "cb.checked=sessionStorage.getItem(key)==='1';"
           "var saved=sessionStorage.getItem(rateKey);"
           "if(saved){inp.value=saved;}"
           "cb.addEventListener('change',function(){"
           "sessionStorage.setItem(key,cb.checked?'1':'0');"
           "if(cb.checked){refreshLive();}schedule();});"
           "inp.addEventListener('change',function(){"
           "sessionStorage.setItem(rateKey,inp.value);schedule();});"
           "restoreUi(loadUi());"
           "bindUi();"
           "schedule();"
           "})();"
           "</script>";
}

void RenderLatencyHeatmap(
    IOutputStream& str,
    const TMonPageData& data,
    const TMap<ui32, TNodeLatency>& nodes)
{
    str << "<h3 id='latHeatmapTitle'>Latency by node ("
        << PercentileTitle(data.SelectedPercentile) << ")</h3>";
    str << "<p style='font-size:0.9em; color:#666;'>"
           "Node-level p50/p90/p99 is the max across contributing slots "
           "(worst-slot indicator); samples stay on each DBG executor."
           "</p>";
    RenderPercentileSelector(str, data);
    RenderLatencyLegend(str);

    str << "<table class='table table-condensed table-bordered' "
           "id='latHeatmapTable'><thead><tr><th>Node</th>";
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        str << "<th>" << ToString(static_cast<EOperation>(operation))
            << "</th>";
    }
    str << "</tr></thead><tbody>";
    for (const auto& [nodeId, node]: nodes) {
        str << "<tr><td>" << nodeId << "</td>";
        for (size_t operation = 0; operation < OperationCount; ++operation) {
            const auto& stats = node.ByOperation[operation];
            str << "<td>";
            if (stats.Count == 0) {
                str << "<span style='color:#999;'>-</span>";
            } else {
                RenderLatencyValueBar(str, stats, data.SelectedPercentile);
            }
            str << "</td>";
        }
        str << "</tr>";
    }
    str << "</tbody></table>";
}

void RenderLatencySlotGrid(
    IOutputStream& str,
    const TMonPageData& data,
    const TMap<ui32, TNodeLatency>& nodes)
{
    HTML (str) {
        TAG (TH3) {
            str << "Latency by slot";
        }
    }
    str << "<p id='latSlotDesc' style='font-size:0.9em; color:#666;'>"
           "Each cell is one ddisk / pbuffer actor slot (node:pdisk:slot), "
           "one pdisk per row. Uses the selected percentile"
        << (data.SelectedLatencyOperation
                ? TStringBuilder()
                      << " for " << ToString(*data.SelectedLatencyOperation)
                : TString(" (worst op by p99)"))
        << ".</p>";
    RenderSlotOperationSelector(str, data);
    RenderLatencyLegend(str);

    // Show-slots checkbox (off by default) + node filter (all / one node).
    str << "<div style='margin:0.5em 0;'>"
           "<label style='margin-right:0.8em;'>"
           "<input type='checkbox' id='latShowSlots'/> Show slots</label>"
           "<label>Node <select id='latSlotNodeFilter'>"
           "<option value=''>all</option>";
    for (const auto& [nodeId, _]: nodes) {
        str << "<option value='" << nodeId << "'>" << nodeId << "</option>";
    }
    str << "</select></label></div>";

    str << "<div id='latSlotsBody' style='display:none;'>";
    for (const auto& [nodeId, node]: nodes) {
        str << "<div class='lat-slot-node' data-node='" << nodeId
            << "' style='margin-bottom:0.8em;'>"
               "<div style='font-weight:bold; margin-bottom:0.3em;'>node "
            << nodeId << "</div>";

        // One row per pdisk.
        std::optional<ui32> currentPDisk;
        bool rowOpen = false;
        auto closeRow = [&]()
        {
            if (rowOpen) {
                str << "</div>";
                rowOpen = false;
            }
        };

        for (const auto& [slotId, slot]: node.Slots) {
            if (!currentPDisk || *currentPDisk != slotId.PDiskId) {
                closeRow();
                str << "<div style='margin:0.2em 0 0.4em 1em;'>"
                       "<span style='display:inline-block; color:#666; "
                       "font-size:0.85em; margin-right:0.5em; min-width:5em;'>"
                       "pdisk "
                    << slotId.PDiskId << "</span>";
                currentPDisk = slotId.PDiskId;
                rowOpen = true;
            }

            const TLatencyStats stats =
                SlotDisplayStats(slot, data.SelectedLatencyOperation);
            const TString url = slot.IsDDisk
                                    ? MakeDDiskMonPageUrl(slot.SlotId)
                                    : MakePBufferMonPageUrl(slot.SlotId);
            // data-ops holds every operation's stats so Slot grid operation /
            // Percentile can redraw without a server round-trip.
            str << "<a class='lat-slot' href='" << url << "' data-ops='"
                << SlotOpsJson(slot)
                << "' style='display:inline-block; vertical-align:top;"
                   " margin:0 0.25em 0.2em 0; text-decoration:none;"
                   " color:inherit; min-width:5em;'>";
            str << "<div style='font-size:0.75em; color:#666;'>"
                << slot.SlotId.DDiskSlotId << "</div>";
            str << "<div class='lat-slot-val'>";
            if (stats.Count == 0) {
                str << "<span style='color:#999;'>-</span>";
            } else {
                RenderLatencyValueBar(str, stats, data.SelectedPercentile);
            }
            str << "</div></a>";
        }
        closeRow();
        str << "</div>";   // lat-slot-node
    }
    str << "</div>";   // latSlotsBody
}

void RenderLatencyDetailTable(
    IOutputStream& str,
    const TMap<ui32, TNodeLatency>& nodes)
{
    HTML (str) {
        TAG (TH3) {
            str << "Latency detail";
        }
    }

    str << "<div style='margin:0.5em 0;'>"
           "<label><input type='checkbox' id='latShowDetail'/> Show data"
           "</label></div>";

    // Collect distinct filter values for the dropdowns.
    TMap<ui32, bool> nodesFilter;
    TMap<ui32, bool> pdisksFilter;
    TMap<TString, bool> typesFilter;
    TMap<TString, bool> opsFilter;
    for (const auto& [nodeId, node]: nodes) {
        for (const auto& [slotId, slot]: node.Slots) {
            for (size_t operation = 0; operation < OperationCount; ++operation)
            {
                if (slot.ByOperation[operation].Count == 0) {
                    continue;
                }
                nodesFilter[nodeId] = true;
                pdisksFilter[slotId.PDiskId] = true;
                typesFilter[slot.IsDDisk ? "ddisk" : "pbuffer"] = true;
                opsFilter[ToString(static_cast<EOperation>(operation))] = true;
            }
        }
    }

    str << "<div id='latDetailBody' style='display:none;'>";

    str << "<div style='margin-bottom:0.5em;'>"
           "<label style='margin-right:0.8em;'>Node "
           "<select id='latFilterNode'><option value=''>all</option>";
    for (const auto& [nodeId, _]: nodesFilter) {
        str << "<option value='" << nodeId << "'>" << nodeId << "</option>";
    }
    str << "</select></label>"
           "<label style='margin-right:0.8em;'>PDisk "
           "<select id='latFilterPdisk'><option value=''>all</option>";
    for (const auto& [pdiskId, _]: pdisksFilter) {
        str << "<option value='" << pdiskId << "'>" << pdiskId << "</option>";
    }
    str << "</select></label>"
           "<label style='margin-right:0.8em;'>Type "
           "<select id='latFilterType'><option value=''>all</option>";
    for (const auto& [type, _]: typesFilter) {
        str << "<option value='" << type << "'>" << type << "</option>";
    }
    str << "</select></label>"
           "<label style='margin-right:0.8em;'>Operation "
           "<select id='latFilterOp'><option value=''>all</option>";
    for (const auto& [op, _]: opsFilter) {
        str << "<option value='" << op << "'>" << op << "</option>";
    }
    str << "</select></label></div>";

    str << "<table class='table table-condensed' id='latencyDetailTable'>"
           "<thead><tr>"
           "<th>Node</th>"
           "<th>PDisk</th>"
           "<th>Slot</th>"
           "<th>Type</th>"
           "<th>Operation</th>"
           "<th style='cursor:pointer;' data-sort='count'>Count</th>"
           "<th style='cursor:pointer;' data-sort='min'>Min</th>"
           "<th style='cursor:pointer;' data-sort='p50'>P50</th>"
           "<th style='cursor:pointer;' data-sort='p90'>P90</th>"
           "<th style='cursor:pointer;' data-sort='p99'>P99</th>"
           "<th style='cursor:pointer;' data-sort='max'>Max</th>"
           "</tr></thead><tbody>";

    for (const auto& [nodeId, node]: nodes) {
        for (const auto& [slotId, slot]: node.Slots) {
            for (size_t operation = 0; operation < OperationCount; ++operation)
            {
                const auto& stats = slot.ByOperation[operation];
                if (stats.Count == 0) {
                    continue;
                }
                const char* type = slot.IsDDisk ? "ddisk" : "pbuffer";
                const TString opName =
                    ToString(static_cast<EOperation>(operation));
                str << "<tr data-node='" << nodeId << "' data-pdisk='"
                    << slotId.PDiskId << "' data-type='" << type
                    << "' data-op='" << opName << "' data-count='"
                    << stats.Count << "' data-min='" << stats.Min.MicroSeconds()
                    << "' data-p50='" << stats.P50.MicroSeconds()
                    << "' data-p90='" << stats.P90.MicroSeconds()
                    << "' data-p99='" << stats.P99.MicroSeconds()
                    << "' data-max='" << stats.Max.MicroSeconds() << "'>"
                    << "<td>" << nodeId << "</td>"
                    << "<td>" << slotId.PDiskId << "</td>"
                    << "<td>";
                if (slot.IsDDisk) {
                    RenderDDiskLink(str, slot.SlotId);
                } else {
                    RenderPBufferLink(str, slot.SlotId);
                }
                str << "</td>"
                    << "<td>" << type << "</td>"
                    << "<td>" << opName << "</td>"
                    << "<td>" << stats.Count << "</td>"
                    << "<td>" << FormatDuration(stats.Min) << "</td>"
                    << "<td>" << FormatDuration(stats.P50) << "</td>"
                    << "<td>" << FormatDuration(stats.P90) << "</td>"
                    << "<td>" << FormatDuration(stats.P99) << "</td>"
                    << "<td>" << FormatDuration(stats.Max) << "</td>"
                    << "</tr>";
            }
        }
    }
    str << "</tbody></table></div>";   // latDetailBody
}

void RenderLatency(IOutputStream& str, const TMonPageData& data)
{
    bool anyCapacity = false;
    for (const auto& dbg: data.Dbgs) {
        if (dbg.LatencyHistoryCapacity > 0) {
            anyCapacity = true;
            break;
        }
    }
    if (!anyCapacity) {
        HTML (str) {
            DIV_CLASS ("alert alert-warning") {
                str << "Latency history is disabled "
                       "(OracleConfig.TimePredictionHistorySize = 0). "
                       "Set OracleConfig.TimePredictionHistorySize &gt; 0 to "
                       "collect "
                       "per-host sliding windows.";
            }
        }
        return;
    }

    const auto nodes = AggregateLatencyByNode(data.Dbgs);
    if (nodes.empty()) {
        HTML (str) {
            DIV_CLASS ("alert alert-info") {
                str << "No latency samples in the current window.";
            }
        }
        return;
    }

    // Auto-refresh controls at the top; live data in #latencyLiveContent;
    // client script AFTER the live region so bindUi sees Show slots / Show
    // data on first paint (not only after an auto-refresh swap).
    RenderLatencyAutoRefreshControls(str);
    str << "<div id='latencyLiveContent'>";
    RenderLatencyHeatmap(str, data, nodes);
    RenderLatencySlotGrid(str, data, nodes);
    RenderLatencyDetailTable(str, nodes);
    str << "</div>";
    RenderLatencyClientScript(str);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString RenderMonPage(const TMonPageData& data)
{
    TStringStream str;

    RenderHeader(str, data.TabletInfo);
    RenderMenu(str, data.TabletInfo, data.Page);

    if (data.RuntimeError) {
        HTML (str) {
            DIV_CLASS ("alert alert-warning") {
                str << HtmlEscape(*data.RuntimeError);
            }
        }
        return str.Str();
    }

    switch (data.Page) {
        case EMonPage::Overview:
            if (data.FastPathServiceInfo) {
                RenderOverview(str, *data.FastPathServiceInfo);
            }
            break;
        case EMonPage::Dbg:
            RenderDbg(str, data);
            break;
        case EMonPage::LocalDb:
            if (data.LocalDb) {
                RenderLocalDb(str, *data.LocalDb);
            }
            break;
        case EMonPage::VChunk:
            RenderVChunk(str, data);
            break;
        case EMonPage::Latency:
            RenderLatency(str, data);
            break;
    }
    return str.Str();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
