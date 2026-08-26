#include "mon_render.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/core/base/services/blobstorage_service_id.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>
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

void AddResource(IOutputStream& str, TStringBuf tag, TStringBuf resourceName)
{
    TString content;
    if (!NResource::FindExact(resourceName, &content)) {
        str << "<!-- resource " << resourceName << " not found -->";
        return;
    }
    str << "<" << tag << ">" << content << "</" << tag << ">";
}

void AddScript(IOutputStream& str, TStringBuf resourceName)
{
    AddResource(str, "script", resourceName);
}

void AddStyle(IOutputStream& str, TStringBuf resourceName)
{
    AddResource(str, "style", resourceName);
}

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
    str << "<div class='pd-menu'>";
    for (EMonPage page: pages) {
        const char* btnClass =
            (page == current) ? "btn btn-primary" : "btn btn-default";
        str << "<a class='" << btnClass
            << " pd-menu-btn' href='?TabletID=" << tabletInfo.TabletId
            << "&page=" << PageParam(page) << "'>" << PageTitle(page) << "</a>";
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

void RenderFreshPercentage(IOutputStream& str, const TDbgSnapshot& dbg)
{
    for (const auto& [vChunkId, vChunkConfig]: dbg.VChunkConfigs) {
        TStringBuilder w;
        for (auto host: vChunkConfig.GetDDisks()) {
            if (auto watermark = vChunkConfig.GetWatermark(host)) {
                w << PrintHostIndex(host) << ":" << *watermark;
            }
        }

        if (w) {
            str << vChunkId << "[" << w << "] ";
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
                        str << "Consecutive success / errors";
                    }
                    TABLEH () {
                        str << "PBuffers usage";
                    }
                    TABLEH () {
                        str << "Ahead";
                    }
                    TABLEH () {
                        str << "Behind";
                    }
                    TABLEH () {
                        str << "Fresh";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& dbg: dbgs) {
                    TMap<EHostHealth, size_t> healthCounts;
                    size_t inflight = 0;
                    size_t consecutiveErrors = 0;
                    size_t consecutiveSuccesses = 0;
                    TCountAndSize pBuffersUsage;
                    TCountAndSize aheadBlocks;
                    TCountAndSize behindBlocks;
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
                        pBuffersUsage += host.PBuffersUsage;
                        aheadBlocks += host.AheadBlocks;
                        behindBlocks += host.BehindBlocks;
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
                            str << consecutiveErrors << " / "
                                << consecutiveSuccesses;
                        }
                        TABLED () {
                            str << pBuffersUsage.Print(true);
                        }
                        TABLED () {
                            str << aheadBlocks.Print(true);
                        }
                        TABLED () {
                            str << behindBlocks.Print(true);
                        }
                        TABLED () {
                            RenderFreshPercentage(str, dbg);
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
    str << "<div class='pd-block'><a href='?TabletID=" << tabletInfo.TabletId
        << "&page=dbg'>&larr; back to DBGs</a></div>";
    // POST, not a link: link prefetching must not add hosts.
    //
    // The same parameters go into both the action URL and the hidden fields
    // because the request has two readers, each looking at one place only:
    // the mon proxy picks the target tablet from the POST body, while the
    // tablet's Cgi() reads the URL query.
    str << "<form method='post' action='?TabletID=" << tabletInfo.TabletId
        << "&page=dbg&dbg=" << dbg.Index
        << "&action=addhost' class='pd-block'>"
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
                        str << "Ahead blocks";
                    }
                    TABLEH () {
                        str << "Behind blocks";
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
                            str << host.PBuffersUsage.Print(true);
                        }
                        TABLED () {
                            str << host.AheadBlocks.Print(true);
                        }
                        TABLED () {
                            str << host.BehindBlocks.Print(true);
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveErrorCount;
                        }
                        TABLED () {
                            str << host.Errors.ConsecutiveSuccessCount;
                        }
                    }
                }
            }
        }
        TAG (TH4) {
            str << "Inflight by operation";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Host";
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
        str << "<div class='pd-block'>" << name << " (none)</div>";
        return;
    }
    // display:list-item brings back the fold triangle that the page CSS
    // hides; the pointer marks the line as clickable.
    str << "<details class='pd-details'>"
           "<summary class='pd-summary'>"
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
                for (const auto& [vChunkIndex, config]: db.VChunkConfigs) {
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
    str << "<form method='get' action='' class='pd-form'>"
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
                            str << vchunk.SafeBarrier->Print();
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
        str << "<details class='pd-details'>"
               "<summary class='pd-summary'>"
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
    str << "<div class='lat-bar'"
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
           "<div class='lat-bar-track'>"
           "<div class='lat-bar-fill' style='width:"
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
    str << "<div class='lat-legend'>";

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
        str << "<span class='lat-legend-swatch' style='background:"
            << bucket.Color << ";'></span>" << bucket.Label << "&nbsp;&nbsp;";
    }
    str << "</div>";
}

void RenderPercentileSelector(IOutputStream& str, const TMonPageData& data)
{
    str << "<div class='lat-controls'>Percentile: ";
    static const ELatencyPercentile percentiles[] = {
        ELatencyPercentile::P50,
        ELatencyPercentile::P90,
        ELatencyPercentile::P99,
        ELatencyPercentile::Max,
    };
    for (ELatencyPercentile p: percentiles) {
        const char* btnClass =
            (p == data.SelectedPercentile)
                ? "btn btn-primary btn-sm lat-nav lat-nav-btn"
                : "btn btn-default btn-sm lat-nav lat-nav-btn";
        str << "<a class='" << btnClass << "' data-p='" << PercentileParam(p)
            << "' href='"
            << LatencyPageHref(data, p, data.SelectedLatencyOperation) << "'>"
            << PercentileTitle(p) << "</a>";
    }
    str << "</div>";
}

void RenderSlotOperationSelector(IOutputStream& str, const TMonPageData& data)
{
    str << "<div class='lat-controls'>Slot grid operation: ";
    {
        const char* btnClass =
            !data.SelectedLatencyOperation
                ? "btn btn-primary btn-sm lat-nav lat-nav-btn"
                : "btn btn-default btn-sm lat-nav lat-nav-btn";
        str << "<a class='" << btnClass << "' data-op='' href='"
            << LatencyPageHref(data, data.SelectedPercentile, std::nullopt)
            << "'>worst</a>";
    }
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        const auto op = static_cast<EOperation>(operation);
        const bool selected = data.SelectedLatencyOperation &&
                              *data.SelectedLatencyOperation == op;
        const char* btnClass =
            selected ? "btn btn-primary btn-sm lat-nav lat-nav-btn"
                     : "btn btn-default btn-sm lat-nav lat-nav-btn";
        str << "<a class='" << btnClass << "' data-op='" << operation
            << "' href='" << LatencyPageHref(data, data.SelectedPercentile, op)
            << "'>" << ToString(op) << "</a>";
    }
    str << "</div>";
}

void RenderLatencyAutoRefreshControls(IOutputStream& str)
{
    str << "<div class='lat-controls'>"
           "<label class='lat-label'>"
           "<input type='checkbox' id='latencyAutoRefresh'/> Auto refresh"
           "</label>"
           "<label>every <input type='number' id='latencyRefreshRate' "
           "min='1' value='1' class='lat-refresh-rate'/> sec</label>"
           "</div>";
}

void RenderLatencyClientScript(IOutputStream& str)
{
    // Must run AFTER #latencyLiveContent is in the DOM so bindUi can find
    // Show slots / Show data. Auto-refresh swaps only the live region's
    // innerHTML; binders live in the script so they survive that swap.
    // Percentile / Slot grid operation only redraw client-side (no fetch).
    AddScript(str, "partition_direct/mon_page/latency.js");
}

void RenderLatencyHeatmap(
    IOutputStream& str,
    const TMonPageData& data,
    const TMap<ui32, TNodeLatency>& nodes)
{
    str << "<h3 id='latHeatmapTitle'>Latency by node ("
        << PercentileTitle(data.SelectedPercentile) << ")</h3>";
    str << "<p class='lat-hint'>"
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
                str << "<span class='lat-none'>-</span>";
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
    str << "<p id='latSlotDesc' class='lat-hint'>"
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
    str << "<div class='lat-controls'>"
           "<label class='lat-label-wide'>"
           "<input type='checkbox' id='latShowSlots'/> Show slots</label>"
           "<label>Node <select id='latSlotNodeFilter'>"
           "<option value=''>all</option>";
    for (const auto& [nodeId, _]: nodes) {
        str << "<option value='" << nodeId << "'>" << nodeId << "</option>";
    }
    str << "</select></label></div>";

    str << "<div id='latSlotsBody' class='lat-hidden'>";
    for (const auto& [nodeId, node]: nodes) {
        str << "<div class='lat-slot-node' data-node='" << nodeId
            << "'>"
               "<div class='lat-node-title'>node "
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
                str << "<div class='lat-pdisk-row'>"
                       "<span class='lat-pdisk-label'>"
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
                << SlotOpsJson(slot) << "'>";
            str << "<div class='lat-slot-id'>" << slot.SlotId.DDiskSlotId
                << "</div>";
            str << "<div class='lat-slot-val'>";
            if (stats.Count == 0) {
                str << "<span class='lat-none'>-</span>";
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

    str << "<div class='lat-controls'>"
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

    str << "<div id='latDetailBody' class='lat-hidden'>";

    str << "<div class='lat-filter-row'>"
           "<label class='lat-label-wide'>Node "
           "<select id='latFilterNode'><option value=''>all</option>";
    for (const auto& [nodeId, _]: nodesFilter) {
        str << "<option value='" << nodeId << "'>" << nodeId << "</option>";
    }
    str << "</select></label>"
           "<label class='lat-label-wide'>PDisk "
           "<select id='latFilterPdisk'><option value=''>all</option>";
    for (const auto& [pdiskId, _]: pdisksFilter) {
        str << "<option value='" << pdiskId << "'>" << pdiskId << "</option>";
    }
    str << "</select></label>"
           "<label class='lat-label-wide'>Type "
           "<select id='latFilterType'><option value=''>all</option>";
    for (const auto& [type, _]: typesFilter) {
        str << "<option value='" << type << "'>" << type << "</option>";
    }
    str << "</select></label>"
           "<label class='lat-label-wide'>Operation "
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
           "<th class='lat-sortable' data-sort='count'>Count</th>"
           "<th class='lat-sortable' data-sort='min'>Min</th>"
           "<th class='lat-sortable' data-sort='p50'>P50</th>"
           "<th class='lat-sortable' data-sort='p90'>P90</th>"
           "<th class='lat-sortable' data-sort='p99'>P99</th>"
           "<th class='lat-sortable' data-sort='max'>Max</th>"
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
    // opNames are exposed via data-op-names so the static JS resource can
    // read them; auto-refresh only swaps innerHTML, so the attribute stays.
    TStringBuilder opNamesJson;
    opNamesJson << "[";
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        if (operation != 0) {
            opNamesJson << ",";
        }
        opNamesJson << "\"" << ToString(static_cast<EOperation>(operation))
                    << "\"";
    }
    opNamesJson << "]";

    RenderLatencyAutoRefreshControls(str);
    str << "<div id='latencyLiveContent' data-op-names='" << opNamesJson
        << "'>";
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

    AddStyle(str, "partition_direct/mon_page/mon_page.css");
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
