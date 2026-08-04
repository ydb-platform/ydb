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

// Pale log-scale palette for heatmap / slot cells.
const char* LatencyColor(TDuration value)
{
    const ui64 us = value.MicroSeconds();
    if (us == 0) {
        return "#f0f0f0";
    }
    if (us < 200) {
        return "#c6efce";   // green
    }
    if (us < 500) {
        return "#ffeb9c";   // yellow
    }
    if (us < 1000) {
        return "#ffc000";   // amber
    }
    if (us < 5000) {
        return "#ff6b6b";   // orange-red
    }
    if (us < 20000) {
        return "#e74c3c";   // red
    }
    return "#8b0000";   // dark red
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

void RenderLatencySelectors(IOutputStream& str, const TMonPageData& data)
{
    str << "<div style='margin-bottom:0.5em;'>Percentile: ";
    static const ELatencyPercentile percentiles[] = {
        ELatencyPercentile::P50,
        ELatencyPercentile::P90,
        ELatencyPercentile::P99,
        ELatencyPercentile::Max,
    };
    for (ELatencyPercentile p: percentiles) {
        const char* btnClass = (p == data.SelectedPercentile)
                                   ? "btn btn-primary btn-sm"
                                   : "btn btn-default btn-sm";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.3em;'"
               " href='?TabletID="
            << data.TabletInfo.TabletId
            << "&page=latency&p=" << PercentileParam(p);
        if (data.SelectedLatencyOperation) {
            str << "&op="
                << static_cast<size_t>(*data.SelectedLatencyOperation);
        }
        str << "'>" << PercentileTitle(p) << "</a>";
    }
    str << "</div>";

    str << "<div style='margin-bottom:0.5em;'>Slot grid operation: ";
    {
        const char* btnClass = !data.SelectedLatencyOperation
                                   ? "btn btn-primary btn-sm"
                                   : "btn btn-default btn-sm";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.3em;'"
               " href='?TabletID="
            << data.TabletInfo.TabletId
            << "&page=latency&p=" << PercentileParam(data.SelectedPercentile)
            << "'>worst</a>";
    }
    for (size_t operation = 0; operation < OperationCount; ++operation) {
        const auto op = static_cast<EOperation>(operation);
        const bool selected = data.SelectedLatencyOperation &&
                              *data.SelectedLatencyOperation == op;
        const char* btnClass =
            selected ? "btn btn-primary btn-sm" : "btn btn-default btn-sm";
        str << "<a class='" << btnClass
            << "' style='margin-right:0.3em;'"
               " href='?TabletID="
            << data.TabletInfo.TabletId
            << "&page=latency&p=" << PercentileParam(data.SelectedPercentile)
            << "&op=" << operation << "'>" << ToString(op) << "</a>";
    }
    str << "</div>";
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
        {"&lt;200us", "#c6efce"},
        {"&lt;500us", "#ffeb9c"},
        {"&lt;1ms", "#ffc000"},
        {"&lt;5ms", "#ff6b6b"},
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

void RenderLatencyHeatmap(
    IOutputStream& str,
    const TMonPageData& data,
    const TMap<ui32, TNodeLatency>& nodes)
{
    HTML (str) {
        TAG (TH3) {
            str << "Latency by node ("
                << PercentileTitle(data.SelectedPercentile) << ")";
        }
    }
    str << "<p style='font-size:0.9em; color:#666;'>"
           "Node-level p50/p90/p99 is the max across contributing slots "
           "(worst-slot indicator); samples stay on each DBG executor."
           "</p>";
    RenderLatencyLegend(str);

    HTML (str) {
        TABLE_CLASS ("table table-condensed table-bordered") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Node";
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
                for (const auto& [nodeId, node]: nodes) {
                    TABLER () {
                        TABLED () {
                            str << nodeId;
                        }
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            const auto& stats = node.ByOperation[operation];
                            TABLED () {
                                if (stats.Count == 0) {
                                    str << "<span style='color:#999;'>-</span>";
                                } else {
                                    const TDuration value =
                                        SelectedLatencyValue(
                                            stats,
                                            data.SelectedPercentile);
                                    str << "<span title='"
                                        << HtmlEscape(LatencyStatsTitle(stats))
                                        << "' style='display:block; padding:"
                                           "0.2em 0.4em; background:"
                                        << LatencyColor(value) << ";'>"
                                        << FormatDuration(value) << "</span>";
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderLatencySlotGrid(
    IOutputStream& str,
    const TMonPageData& data,
    const TMap<ui32, TNodeLatency>& nodes)
{
    HTML (str) {
        TAG (TH3) {
            str << "Slots by node";
        }
    }
    str << "<p style='font-size:0.9em; color:#666;'>"
           "Each square is one ddisk / pbuffer actor slot. Color uses the "
           "selected percentile"
        << (data.SelectedLatencyOperation
                ? TStringBuilder()
                      << " for " << ToString(*data.SelectedLatencyOperation)
                : TString(" (worst op by p99)"))
        << ".</p>";

    for (const auto& [nodeId, node]: nodes) {
        str << "<div style='margin-bottom:0.4em;'>"
               "<span style='display:inline-block; width:5em;'>node "
            << nodeId << "</span>";
        for (const auto& [slotId, slot]: node.Slots) {
            const TLatencyStats stats =
                SlotDisplayStats(slot, data.SelectedLatencyOperation);
            const TDuration value =
                stats.Count == 0
                    ? TDuration()
                    : SelectedLatencyValue(stats, data.SelectedPercentile);
            const TString url = slot.IsDDisk
                                    ? MakeDDiskMonPageUrl(slot.SlotId)
                                    : MakePBufferMonPageUrl(slot.SlotId);
            const TString title = TStringBuilder()
                                  << slot.SlotId.ToString() << " "
                                  << (slot.IsDDisk ? "ddisk" : "pbuffer") << " "
                                  << LatencyStatsTitle(stats);
            str << "<a href='" << url << "' title='" << HtmlEscape(title)
                << "' style='display:inline-block; width:1.4em; height:1.4em;"
                   " margin:1px; border:1px solid #888; background:"
                << LatencyColor(value) << ";'></a>";
        }
        str << "</div>";
    }
}

void RenderLatencyDetailTable(
    IOutputStream& str,
    const TMap<ui32, TNodeLatency>& nodes)
{
    HTML (str) {
        TAG (TH3) {
            str << "Latency detail";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Node";
                    }
                    TABLEH () {
                        str << "Slot";
                    }
                    TABLEH () {
                        str << "Type";
                    }
                    TABLEH () {
                        str << "Operation";
                    }
                    TABLEH () {
                        str << "Count";
                    }
                    TABLEH () {
                        str << "Min";
                    }
                    TABLEH () {
                        str << "P50";
                    }
                    TABLEH () {
                        str << "P90";
                    }
                    TABLEH () {
                        str << "P99";
                    }
                    TABLEH () {
                        str << "Max";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& [nodeId, node]: nodes) {
                    for (const auto& [slotId, slot]: node.Slots) {
                        for (size_t operation = 0; operation < OperationCount;
                             ++operation)
                        {
                            const auto& stats = slot.ByOperation[operation];
                            if (stats.Count == 0) {
                                continue;
                            }
                            TABLER () {
                                TABLED () {
                                    str << nodeId;
                                }
                                TABLED () {
                                    if (slot.IsDDisk) {
                                        RenderDDiskLink(str, slot.SlotId);
                                    } else {
                                        RenderPBufferLink(str, slot.SlotId);
                                    }
                                }
                                TABLED () {
                                    str << (slot.IsDDisk ? "ddisk" : "pbuffer");
                                }
                                TABLED () {
                                    str << ToString(
                                        static_cast<EOperation>(operation));
                                }
                                TABLED () {
                                    str << stats.Count;
                                }
                                TABLED () {
                                    str << FormatDuration(stats.Min);
                                }
                                TABLED () {
                                    str << FormatDuration(stats.P50);
                                }
                                TABLED () {
                                    str << FormatDuration(stats.P90);
                                }
                                TABLED () {
                                    str << FormatDuration(stats.P99);
                                }
                                TABLED () {
                                    str << FormatDuration(stats.Max);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

void RenderLatency(IOutputStream& str, const TMonPageData& data)
{
    HTML (str) {
        TAG (TH3) {
            str << "Latency";
        }
    }

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
                       "Set TimePredictionHistorySize &gt; 0 to collect "
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

    RenderLatencySelectors(str, data);
    RenderLatencyHeatmap(str, data, nodes);
    RenderLatencySlotGrid(str, data, nodes);
    RenderLatencyDetailTable(str, nodes);
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
