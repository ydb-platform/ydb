#include "mon_render_dbg.h"

#include "mon_util.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/map.h>
#include <util/stream/str.h>
#include <util/string/builder.h>

#include <array>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

//////////////////////////////////////////////////////////////////////////////////

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
                        str << "Inflight";
                    }
                    TABLEH () {
                        str << "DirtyMap<br>inflight/flush/erase";
                    }
                    TABLEH () {
                        str << "Reads<br>DDisk/PBuffer";
                    }
                    TABLEH () {
                        str << "Flushes<br>in-node/cross-node";
                    }
                    TABLEH () {
                        str << "Consecutive<br>success/errors";
                    }
                    TABLEH () {
                        str << "PBuffers<br>usage";
                    }
                    TABLEH () {
                        str << "DDisks<br>Used/Rotten/Fresh";
                    }
                }
            }
            TABLEBODY () {
                size_t totalInflight = 0;
                size_t totalConsecutiveErrors = 0;
                size_t totalConsecutiveSuccesses = 0;
                TCountAndSize totalPBuffersUsage;
                ui64 totalDDiskBytes = 0;
                ui64 totalFreshBytes = 0;
                ui64 totalRottenBytes = 0;
                TDirtyMapStats totalDirtyMapStats;

                for (const auto& dbg: dbgs) {
                    TMap<EHostHealth, size_t> healthCounts;
                    size_t inflight = 0;
                    size_t consecutiveErrors = 0;
                    size_t consecutiveSuccesses = 0;
                    ui64 ddiskTotalBytes = 0;
                    ui64 freshTotalBytes = 0;
                    ui64 rottenTotalBytes = 0;
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
                        ddiskTotalBytes += host.DirtyMapStats.DDiskTotalBytes;
                        freshTotalBytes += host.DirtyMapStats.FreshTotalBytes;
                        rottenTotalBytes += host.DirtyMapStats.RottenTotalBytes;
                    }
                    totalInflight += inflight;
                    totalConsecutiveErrors += consecutiveErrors;
                    totalConsecutiveSuccesses += consecutiveSuccesses;
                    totalPBuffersUsage += dbg.PBuffersUsage;
                    totalDDiskBytes += ddiskTotalBytes;
                    totalFreshBytes += freshTotalBytes;
                    totalRottenBytes += rottenTotalBytes;
                    totalDirtyMapStats.Aggregate(dbg.DirtyMapStats);
                    TABLER () {
                        TABLED () {
                            str << "<a href='?TabletID=" << tabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>#"
                                << dbg.Index << "</a>";
                        }
                        TABLED () {
                            str << HealthRollup(healthCounts);
                        }
                        TABLED () {
                            str << inflight;
                        }
                        TABLED () {
                            str << dbg.DirtyMapStats.InflightCount;
                            str << " / ";
                            str << dbg.DirtyMapStats.ReadyToFlushCount;
                            str << " / ";
                            str << dbg.DirtyMapStats.ReadyToEraseCount;
                        }
                        TABLED () {
                            str << dbg.DirtyMapStats.ReadFromDDiskCount;
                            str << " / ";
                            str << dbg.DirtyMapStats.ReadFromPBufferCount;
                        }
                        TABLED () {
                            str << dbg.DirtyMapStats.InNodeFlushCount;
                            str << " / ";
                            str << dbg.DirtyMapStats.CrossNodeFlushCount;
                        }
                        TABLED () {
                            str << consecutiveSuccesses;
                            str << " / ";
                            str << consecutiveErrors;
                        }
                        TABLED () {
                            str << dbg.PBuffersUsage.Print(true);
                        }
                        TABLED () {
                            str << FormatByteSize(ddiskTotalBytes);
                            str << " / ";
                            str << FormatByteSize(rottenTotalBytes);
                            str << " / ";
                            str << FormatByteSize(freshTotalBytes);
                        }
                    }
                }
                TABLER () {
                    TABLED () {
                        str << "Total";
                    }
                    TABLED () {
                        str << "-";
                    }
                    TABLED () {
                        str << totalInflight;
                    }
                    TABLED () {
                        str << totalDirtyMapStats.InflightCount << " / "
                            << totalDirtyMapStats.ReadyToFlushCount << " / "
                            << totalDirtyMapStats.ReadyToEraseCount;
                    }
                    TABLED () {
                        str << totalDirtyMapStats.ReadFromDDiskCount << " / "
                            << totalDirtyMapStats.ReadFromPBufferCount;
                    }
                    TABLED () {
                        str << totalDirtyMapStats.InNodeFlushCount << " / "
                            << totalDirtyMapStats.CrossNodeFlushCount;
                    }
                    TABLED () {
                        str << totalConsecutiveSuccesses << " / "
                            << totalConsecutiveErrors;
                    }
                    TABLED () {
                        str << totalPBuffersUsage.Print(true);
                    }
                    TABLED () {
                        str << FormatByteSize(totalDDiskBytes) << " / "
                            << FormatByteSize(totalRottenBytes) << " / "
                            << FormatByteSize(totalFreshBytes);
                    }
                }
            }
        }
    }
}

// Returns the CSS color class for a compact VChunk host state.
const char* GetVChunkHostStateClass(
    TVChunkConfig::EHostHumanReadableState state)
{
    switch (state) {
        case TVChunkConfig::EHostHumanReadableState::Primary:
            return "vchunk-host-primary";
        case TVChunkConfig::EHostHumanReadableState::Fresh:
            return "vchunk-host-fresh";
        case TVChunkConfig::EHostHumanReadableState::HandOff:
            return "vchunk-host-handoff";
        case TVChunkConfig::EHostHumanReadableState::Rotten:
            return "vchunk-host-rotten";
        case TVChunkConfig::EHostHumanReadableState::Disabled:
            return "vchunk-host-disabled";
        case TVChunkConfig::EHostHumanReadableState::Demoted:
            return "vchunk-host-demoted";
    }
    return "";
}

enum class EVChunkHostStateFormat
{
    Brief,    // A compact symbol in a VChunk row.
    Legend,   // A symbol and full state name in the legend.
};

constexpr std::array<TVChunkConfig::EHostHumanReadableState, 6>
    VChunkHostStates = {
        TVChunkConfig::EHostHumanReadableState::Primary,
        TVChunkConfig::EHostHumanReadableState::Fresh,
        TVChunkConfig::EHostHumanReadableState::HandOff,
        TVChunkConfig::EHostHumanReadableState::Rotten,
        TVChunkConfig::EHostHumanReadableState::Disabled,
        TVChunkConfig::EHostHumanReadableState::Demoted,
};

void RenderVChunkHostState(
    IOutputStream& str,
    TVChunkConfig::EHostHumanReadableState state,
    EVChunkHostStateFormat format)
{
    const TString fullName = Print(state, false);
    str << "<td class='vchunk-host-state " << GetVChunkHostStateClass(state)
        << "' title='" << fullName << "'>" << Print(state, true);
    if (format == EVChunkHostStateFormat::Legend) {
        str << " = " << fullName;
    }
    str << "</td>";
}

void RenderVChunkHostStateLegend(IOutputStream& str)
{
    HTML (str) {
        TABLE_CLASS ("table table-condensed table-bordered vchunk-host-legend")
        {
            TABLEBODY () {
                TABLER () {
                    for (const auto state: VChunkHostStates) {
                        RenderVChunkHostState(
                            str,
                            state,
                            EVChunkHostStateFormat::Legend);
                    }
                }
            }
        }
    }
}

void RenderDDiskImbalance(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TDbgSnapshot& dbg)
{
    HTML (str) {
        TAG (TH4) {
            str << "DDisk imbalance";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Strategy";
                    }
                    TABLEH () {
                        str << "Need move";
                    }
                    TABLEH () {
                        str << "Total DDisks";
                    }
                    TABLEH () {
                        str << "Imbalance";
                    }
                    TABLEH () {
                        str << "Action";
                    }
                }
            }
            TABLEBODY () {
                const auto renderRow = [&](TStringBuf name,
                                           const TDDiskImbalance& imbalance,
                                           EDDiskBalanceStrategy strategy)
                {
                    TABLER () {
                        TABLED () {
                            str << name;
                        }
                        TABLED () {
                            str << imbalance.Moves;
                        }
                        TABLED () {
                            str << imbalance.TotalDDiskCount;
                        }
                        TABLED () {
                            str << imbalance.Percent << "%";
                        }
                        TABLED () {
                            RenderBalanceDDisksButton(
                                str,
                                tabletInfo.TabletId,
                                EMonPage::Dbg,
                                dbg.Index,
                                dbg.Index + 1,
                                strategy);
                        }
                    }
                };

                renderRow(
                    "Touched",
                    dbg.TouchedDDiskImbalance,
                    EDDiskBalanceStrategy::Touched);
                renderRow(
                    "Configured",
                    dbg.ConfiguredDDiskImbalance,
                    EDDiskBalanceStrategy::Configured);
            }
        }
    }
}

void RenderVChunks(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const TDbgSnapshot& dbg)
{
    HTML (str) {
        TAG (TH4) {
            str << "VChunks";
        }
        RenderVChunkHostStateLegend(str);
        TABLE_CLASS ("table table-condensed table-bordered") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "VChunk";
                    }
                    for (const auto& connection: dbg.Connections) {
                        TABLEH () {
                            str << PrintHostIndex(connection.HostIndex);
                        }
                    }
                    TABLEH () {
                        str << "Fresh<br>bytes";
                    }
                    TABLEH () {
                        str << "Rotten<br>bytes";
                    }
                    TABLEH () {
                        str << "PBuffer<br>bytes";
                    }
                    TABLEH () {
                        str << "Enabled<br>DDisks";
                    }
                }
            }
            TABLEBODY () {
                for (const auto& vchunk: dbg.VChunks) {
                    const auto& config = vchunk.Config;
                    const auto* freshDDisks =
                        dbg.FreshDDisks.FindPtr(config.GetVChunkIndex());
                    TABLER () {
                        TABLED () {
                            str << "<span class='vchunk-touched-marker ";
                            if (vchunk.Touched) {
                                str << "vchunk-touched-on' title='Touched";
                            } else {
                                str << "vchunk-touched-off' title='Not touched";
                            }
                            str << "'></span><a href='?TabletID="
                                << tabletInfo.TabletId << "&page=vchunk&vchunk="
                                << config.GetVChunkIndex() << "'>#"
                                << config.GetVChunkIndex() << "</a>";
                        }
                        for (const auto& connection: dbg.Connections) {
                            const THostIndex host = connection.HostIndex;
                            const bool fresh = freshDDisks != nullptr &&
                                               freshDDisks->Get(host);
                            const auto state =
                                config.GetHostHumanReadableState(host, fresh);
                            RenderVChunkHostState(
                                str,
                                state,
                                EVChunkHostStateFormat::Brief);
                        }
                        TABLED () {
                            str << FormatByteSize(vchunk.FreshBytes);
                        }
                        TABLED () {
                            str << FormatByteSize(vchunk.RottenBytes);
                        }
                        TABLED () {
                            str << FormatByteSize(vchunk.PBufferBytes);
                        }
                        TABLED () {
                            str << config.GetEnabledDDisks().Count();
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
        TAG (TH4) {
            str << "Dirty map statistics";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLER () {
                TABLED () {
                    str << "Inflight";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.InflightCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Ready to flush";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.ReadyToFlushCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Ready to erase";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.ReadyToEraseCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Read requests";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.ReadRequestCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Reads from DDisk";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.ReadFromDDiskCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Reads from PBuffer";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.ReadFromPBufferCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "In-node flushes";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.InNodeFlushCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Cross-node flushes";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.CrossNodeFlushCount;
                }
            }
            TABLER () {
                TABLED () {
                    str << "Allocated size";
                }
                TABLED () {
                    str << FormatByteSize(
                        dbg.DirtyMapStats.DDisksMemoryStats.ReservedSize);
                }
            }
            TABLER () {
                TABLED () {
                    str << "Used size";
                }
                TABLED () {
                    str << FormatByteSize(
                        dbg.DirtyMapStats.DDisksMemoryStats.UsedSize);
                }
            }
            TABLER () {
                TABLED () {
                    str << "DDisk state allocations";
                }
                TABLED () {
                    str << dbg.DirtyMapStats.DDisksMemoryStats.AllocationCount;
                }
            }
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
                        str << "DDisk total";
                    }
                    TABLEH () {
                        str << "Fresh blocks";
                    }
                    TABLEH () {
                        str << "Rotten blocks";
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
                            str << host.DirtyMapStats.PBuffersUsage.Print(true);
                        }
                        TABLED () {
                            str << FormatByteSize(
                                host.DirtyMapStats.DDiskTotalBytes);
                        }
                        TABLED () {
                            str << FormatByteSize(
                                host.DirtyMapStats.FreshTotalBytes);
                        }
                        TABLED () {
                            str << FormatByteSize(
                                host.DirtyMapStats.RottenTotalBytes);
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
                        str << "DDisk";
                    }
                    TABLEH () {
                        str << "PBuffer";
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
                            if (connection.DDiskConnected) {
                                str << " connected";
                            }
                            str << " " << connection.DDiskSession;
                        }
                        TABLED () {
                            RenderPBufferLink(str, connection.PBufferId);
                            if (connection.PBufferConnected) {
                                str << " connected";
                            }
                        }
                    }
                }
            }
        }
        RenderDDiskImbalance(str, tabletInfo, dbg);
        RenderVChunks(str, tabletInfo, dbg);
    }
}

}   // namespace

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

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
