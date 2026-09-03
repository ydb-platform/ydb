#include "mon_render_overview.h"

#include "mon_model.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Per-node placement and vchunk usage within one direct block group.
struct TDbgConfigCell
{
    TMap<TVChunkConfig::EHostHumanReadableState, size_t> DDiskStates;
    size_t PBufferCount = 0;
};

using TDbgConfigColumn = TMap<ui32, TDbgConfigCell>;

enum class EDbgConfigCellKind
{
    Placement,   // A node placement in one DBG.
    Empty,       // The node is not used by this DBG.
    Total,       // An aggregate across nodes or DBGs.
};

TDbgConfigColumn BuildDbgConfigColumn(const TDbgSnapshot& dbg)
{
    TDbgConfigColumn result;

    // Iterate over all connections and create the nodes.
    for (const auto& connection: dbg.Connections) {
        result[connection.DDiskId.NodeId];
    }

    // Iterate over all VChunks and mark the nodes used for DDisk and PBuffer.
    for (const auto& [vChunkIndex, config]: dbg.VChunkConfigs) {
        for (THostIndex host = 0; host < config.GetHostCount(); ++host) {
            const auto& dDiskId = dbg.Connections[host].DDiskId;
            const auto& pBufferId = dbg.Connections[host].PBufferId;
            if (config.GetDDiskRole(host) != EHostRole::None) {
                const auto state = config.GetHostHumanReadableState(host);
                ++result[dDiskId.NodeId].DDiskStates[state];
            }
            if (config.GetPBufferRole(host) != EHostRole::None) {
                ++result[pBufferId.NodeId].PBufferCount;
            }
        }
    }

    return result;
}

TStringBuf DbgConfigCellClass(const TDbgConfigCell& cell)
{
    const bool hasDDisks = !cell.DDiskStates.empty();
    const bool hasPBuffers = cell.PBufferCount != 0;
    if (hasDDisks && hasPBuffers) {
        return "dbg-config-cell dbg-config-both";
    }
    if (hasDDisks) {
        return "dbg-config-cell dbg-config-ddisk";
    }
    return "dbg-config-cell dbg-config-pbuffer";
}

void MergeDbgConfigCell(
    const TDbgConfigCell& source,
    TDbgConfigCell* destination)
{
    for (const auto& [state, count]: source.DDiskStates) {
        destination->DDiskStates[state] += count;
    }

    destination->PBufferCount += source.PBufferCount;
}

TString BuildDDisksStates(const TDbgConfigCell& cell, bool brief)
{
    if (brief && cell.DDiskStates.size() == 1 &&
        cell.DDiskStates.begin()->first ==
            TVChunkConfig::EHostHumanReadableState::Primary)
    {
        return ToString(cell.DDiskStates.begin()->second);
    }

    TStringBuilder result;
    for (auto [state, count]: cell.DDiskStates) {
        result << Print(state, brief) << ":" << count << "&#10;";
    }
    return result;
}

TString BuildDbgConfigTooltip(const TDbgConfigCell& cell)
{
    TStringBuilder result;
    result << "DDisk:&#10;" << BuildDDisksStates(cell, false);
    result << "PBuffer: " << cell.PBufferCount;
    return result;
}

void RenderDbgConfigCell(
    IOutputStream& str,
    const TDbgConfigCell& cell,
    EDbgConfigCellKind kind)
{
    TStringBuf cellClass = "dbg-config-cell";
    if (kind == EDbgConfigCellKind::Placement) {
        cellClass = DbgConfigCellClass(cell);
    } else if (kind == EDbgConfigCellKind::Total) {
        cellClass = "dbg-config-cell dbg-config-total";
    }

    str << "<td class=\"" << cellClass << "\" title=\""
        << BuildDbgConfigTooltip(cell) << "\">";
    if (kind == EDbgConfigCellKind::Empty) {
        str << "-";
    } else {
        str << BuildDDisksStates(cell, true);
    }
    str << "</td>";
}

void RenderOverviewHeader(IOutputStream& str, const TFastPathServiceInfo& info)
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

void RenderDbgConfig(
    IOutputStream& str,
    const TVector<TDbgSnapshot>& dbgs,
    const TTabletInfo& tabletInfo)
{
    TSet<ui32> nodeIds;
    TVector<TDbgConfigColumn> columns;
    TVector<TDbgConfigCell> columnTotals;
    TMap<ui32, TDbgConfigCell> rowTotals;
    TDbgConfigCell grandTotal;
    columns.reserve(dbgs.size());
    columnTotals.reserve(dbgs.size());
    for (const auto& dbg: dbgs) {
        auto column = BuildDbgConfigColumn(dbg);
        TDbgConfigCell columnTotal;
        for (const auto& [nodeId, cell]: column) {
            nodeIds.insert(nodeId);
            MergeDbgConfigCell(cell, &columnTotal);
            MergeDbgConfigCell(cell, &rowTotals[nodeId]);
        }
        MergeDbgConfigCell(columnTotal, &grandTotal);
        columns.push_back(std::move(column));
        columnTotals.push_back(std::move(columnTotal));
    }

    HTML (str) {
        TAG (TH3) {
            str << "Direct Block Group config";
        }

        DIV_CLASS ("pd-block") {
            SPAN_CLASS ("dbg-config-legend-item dbg-config-both") {
                str << "DDisk + PBuffer";
            }
            SPAN_CLASS ("dbg-config-legend-item dbg-config-ddisk") {
                str << "DDisk only";
            }
            SPAN_CLASS ("dbg-config-legend-item dbg-config-pbuffer") {
                str << "PBuffer only";
            }
        }
        if (dbgs.empty()) {
            DIV_CLASS ("alert alert-info") {
                str << "No Direct Block Groups.";
            }
            return;
        }
        TABLE_CLASS ("table table-condensed table-bordered") {
            TABLEHEAD () {
                TABLER () {
                    TABLEH () {
                        str << "Node";
                    }
                    for (const auto& dbg: dbgs) {
                        TABLEH () {
                            str << "<a href='?TabletID=" << tabletInfo.TabletId
                                << "&page=dbg&dbg=" << dbg.Index << "'>DBG #"
                                << dbg.Index << "</a>";
                        }
                    }
                    TABLEH () {
                        str << "Total";
                    }
                }
            }
            TABLEBODY () {
                const TDbgConfigCell emptyCell;
                for (ui32 nodeId: nodeIds) {
                    TABLER () {
                        TABLED () {
                            str << "Node " << nodeId;
                        }
                        for (const auto& column: columns) {
                            const auto* cell = column.FindPtr(nodeId);
                            if (!cell) {
                                RenderDbgConfigCell(
                                    str,
                                    emptyCell,
                                    EDbgConfigCellKind::Empty);
                                continue;
                            }
                            RenderDbgConfigCell(
                                str,
                                *cell,
                                EDbgConfigCellKind::Placement);
                        }
                        RenderDbgConfigCell(
                            str,
                            rowTotals[nodeId],
                            EDbgConfigCellKind::Total);
                    }
                }
                TABLER_CLASS ("dbg-config-total-row") {
                    TABLEH () {
                        str << "Total";
                    }
                    for (const auto& total: columnTotals) {
                        RenderDbgConfigCell(
                            str,
                            total,
                            EDbgConfigCellKind::Total);
                    }
                    RenderDbgConfigCell(
                        str,
                        grandTotal,
                        EDbgConfigCellKind::Total);
                }
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderOverview(IOutputStream& str, const TMonPageData& data)
{
    if (data.FastPathServiceInfo) {
        RenderOverviewHeader(str, *data.FastPathServiceInfo);
    }
    RenderDbgConfig(str, data.Dbgs, data.TabletInfo);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
