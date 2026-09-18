#include "mon_render_overview.h"

#include "mon_model.h"
#include "mon_util.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <array>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TNodeId = ui32;
using TVChunkId = size_t;
using TDbgId = size_t;

// Per-node DDisk and PBuffer counters within one rendered column.
struct TDbgTableCell
{
    TMap<TVChunkConfig::EHostHumanReadableState, size_t> DDiskStates;
    size_t PBufferCount = 0;
};

// DBG indices represented by one rendered column.
struct TDbgHeaderCell
{
    TVector<TDbgId> DbgIds;
};

// A reusable default config and the number of VChunks matching it.
struct TDefaultConfigEntry
{
    TVChunkConfig Config;
    size_t VChunkCount = 0;
};

using TDbgConfigHeaders = std::array<TDbgHeaderCell, VChunkPerRegionCount>;
using TDbgConfigRow = std::array<TDbgTableCell, VChunkPerRegionCount>;
using TDbgConfigTable = THashMap<TNodeId, TDbgConfigRow>;
using TDbgRowTotals = THashMap<TNodeId, TDbgTableCell>;
using TDefaultConfigs =
    std::array<TDefaultConfigEntry, DirectBlockGroupHostCount>;
using TDefaultConfigCache = TVector<TDefaultConfigs>;

// Contains the fixed columns and node rows of the DBG table.
struct TDbgConfigTableData
{
    TDbgConfigHeaders Headers;
    TDbgConfigTable Table;
    TDbgRowTotals RowTotals;
    TDbgConfigRow ColumnTotals;
    TDbgTableCell GrandTotal;
    TDefaultConfigCache DefaultConfigs;

    // Returns the cached default config entry matching the VChunk placement.
    TDefaultConfigEntry& GetDefaultConfig(TDbgId dbgId, TVChunkId vChunkId)
    {
        Y_ABORT_UNLESS(dbgId < DefaultConfigs.size());
        return DefaultConfigs[dbgId][vChunkId % DirectBlockGroupHostCount];
    }
};

enum class EDbgConfigCellKind
{
    Placement,   // A node placement in one rendered column.
    Total,       // An aggregate across nodes or rendered columns.
};

enum class EDDiskStatesFormat
{
    Brief,   // Omits the Primary state name when it is the only state.
    Full,    // Prints every state name for a tooltip.
};

void RenderValue(IOutputStream& str, TStringBuf name, const TString& value)
{
    HTML (str) {
        TABLER () {
            TABLED () {
                str << name;
            }
            TABLED () {
                str << value;
            }
        }
    }
}

TCountAndSize GetPBuffersUsage(const TVector<TDbgSnapshot>& dbgs)
{
    TCountAndSize result;
    for (const auto& dbg: dbgs) {
        result += dbg.PBuffersUsage;
    }
    return result;
}

// Builds all distinct default configs for a DBG. Default host roles repeat
// every hostCount VChunks.
TDefaultConfigs BuildDefaultConfigCache(const TDbgSnapshot& dbg)
{
    TDefaultConfigs result;
    for (TVChunkId vChunkId = 0; vChunkId < DirectBlockGroupHostCount;
         ++vChunkId)
    {
        auto config = TVChunkConfig::MakeDefault(
            vChunkId,
            DirectBlockGroupHostCount,
            DefaultPrimaryCount);
        config.SetDBGIndex(dbg.Index);
        result[vChunkId].Config = std::move(config);
    }
    return result;
}

// Counts touched VChunks matching every cached default config.
void FillDefaultConfigs(
    size_t regionCount,
    size_t directBlockGroupCount,
    const ITouchedProvider& touchedProvider,
    TDbgConfigTableData* tableData)
{
    for (ui32 regionIndex = 0; regionIndex < regionCount; ++regionIndex) {
        const auto touchedVChunks =
            touchedProvider.GetTouchedVChunks(regionIndex);
        Y_FOR_EACH_BIT(vChunkIndexInRegion, touchedVChunks)
        {
            const TVChunkId vChunkId =
                GetVChunkIndex(regionIndex, vChunkIndexInRegion);
            const TDbgId dbgId =
                GetDirectBlockGroupIndex(vChunkId, directBlockGroupCount);
            ++tableData->GetDefaultConfig(dbgId, vChunkId).VChunkCount;
        }
    }
}

// Adds cached default config counters to node cells.
void TransferDefaultConfigsToTable(
    const TVector<TDbgSnapshot>& dbgs,
    TDbgConfigTableData* tableData)
{
    for (const auto& dbg: dbgs) {
        Y_ABORT_UNLESS(dbg.Index < tableData->DefaultConfigs.size());
        const size_t columnIndex = dbg.Index % VChunkPerRegionCount;
        for (const auto& entry: tableData->DefaultConfigs[dbg.Index]) {
            if (entry.VChunkCount == 0) {
                continue;
            }

            const auto& config = entry.Config;
            Y_ABORT_UNLESS(dbg.Connections.size() >= config.GetHostCount());
            for (THostIndex host = 0; host < config.GetHostCount(); ++host) {
                const auto& connection = dbg.Connections[host];
                if (config.GetDDiskRole(host) != EHostRole::None) {
                    const auto state = config.GetHostHumanReadableState(host);
                    tableData->Table[connection.DDiskId.NodeId][columnIndex]
                        .DDiskStates[state] += entry.VChunkCount;
                }
                if (config.GetPBufferRole(host) != EHostRole::None) {
                    tableData->Table[connection.PBufferId.NodeId][columnIndex]
                        .PBufferCount += entry.VChunkCount;
                }
            }
        }
    }
}

// Replaces cached defaults with touched persisted config counters.
void ApplyRealConfigs(
    const TVector<TDbgSnapshot>& dbgs,
    size_t directBlockGroupCount,
    const TVChunkConfigs& vChunkConfigs,
    const ITouchedProvider& touchedProvider,
    TDbgConfigTableData* tableData)
{
    for (const auto& [vChunkId, config]: vChunkConfigs) {
        Y_ABORT_UNLESS(vChunkId == config.GetVChunkIndex());
        if (!touchedProvider.Get(vChunkId)) {
            continue;
        }

        const TDbgId dbgId =
            GetDirectBlockGroupIndex(vChunkId, directBlockGroupCount);
        auto& entry = tableData->GetDefaultConfig(dbgId, vChunkId);
        Y_ABORT_UNLESS(entry.VChunkCount != 0);
        --entry.VChunkCount;

        Y_ABORT_UNLESS(dbgId < dbgs.size() && dbgs[dbgId].Index == dbgId);
        const auto& dbg = dbgs[dbgId];
        Y_ABORT_UNLESS(dbg.Connections.size() >= config.GetHostCount());
        const size_t columnIndex = dbg.Index % VChunkPerRegionCount;
        for (THostIndex host = 0; host < config.GetHostCount(); ++host) {
            const auto& connection = dbg.Connections[host];
            if (config.GetDDiskRole(host) != EHostRole::None) {
                const auto state = config.GetHostHumanReadableState(host);
                ++tableData->Table[connection.DDiskId.NodeId][columnIndex]
                      .DDiskStates[state];
            }
            if (config.GetPBufferRole(host) != EHostRole::None) {
                ++tableData->Table[connection.PBufferId.NodeId][columnIndex]
                      .PBufferCount;
            }
        }
    }
}

// Builds table columns and node rows without calculating cell contents.
TDbgConfigTableData BuildDbgConfigTable(const TVector<TDbgSnapshot>& dbgs)
{
    TDbgConfigTableData result;
    result.DefaultConfigs.resize(dbgs.size());
    for (const auto& dbg: dbgs) {
        result.Headers[dbg.Index % VChunkPerRegionCount].DbgIds.push_back(
            dbg.Index);
        result.DefaultConfigs[dbg.Index] = BuildDefaultConfigCache(dbg);
        for (const auto& connection: dbg.Connections) {
            result.Table[connection.DDiskId.NodeId];
            result.Table[connection.PBufferId.NodeId];
        }
    }
    return result;
}

void AddTableCell(const TDbgTableCell& source, TDbgTableCell* destination)
{
    for (const auto& [state, count]: source.DDiskStates) {
        destination->DDiskStates[state] += count;
    }
    destination->PBufferCount += source.PBufferCount;
}

// Calculates totals after all table cells have been filled.
void CalculateTableTotals(TDbgConfigTableData* tableData)
{
    for (auto& [nodeId, row]: tableData->Table) {
        auto& rowTotal = tableData->RowTotals[nodeId];
        for (size_t columnIndex = 0; columnIndex < row.size(); ++columnIndex) {
            const auto& cell = row[columnIndex];
            AddTableCell(cell, &rowTotal);
            AddTableCell(cell, &tableData->ColumnTotals[columnIndex]);
        }
        AddTableCell(rowTotal, &tableData->GrandTotal);
    }
}

void RenderDbgConfigHeader(
    IOutputStream& str,
    ui64 tabletId,
    const TDbgHeaderCell& cell)
{
    if (cell.DbgIds.empty()) {
        str << "-";
        return;
    }

    for (size_t i = 0; i < cell.DbgIds.size(); ++i) {
        if (i != 0) {
            str << "<br>";
        }
        const TDbgId dbgIndex = cell.DbgIds[i];
        str << "<a href='?TabletID=" << tabletId << "&page=dbg&dbg=" << dbgIndex
            << "'>DBG #" << dbgIndex << "</a>";
    }
}

bool IsEmpty(const TDbgTableCell& cell)
{
    return cell.DDiskStates.empty() && cell.PBufferCount == 0;
}

TStringBuf GetDbgConfigCellClass(
    const TDbgTableCell& cell,
    EDbgConfigCellKind kind)
{
    if (kind == EDbgConfigCellKind::Total) {
        return "dbg-config-cell dbg-config-total";
    }

    const bool hasDDisks = !cell.DDiskStates.empty();
    const bool hasPBuffers = cell.PBufferCount != 0;
    if (hasDDisks && hasPBuffers) {
        return "dbg-config-cell dbg-config-both";
    }
    if (hasDDisks) {
        return "dbg-config-cell dbg-config-ddisk";
    }
    if (hasPBuffers) {
        return "dbg-config-cell dbg-config-pbuffer";
    }
    return "dbg-config-cell";
}

TString BuildDDisksStates(const TDbgTableCell& cell, EDDiskStatesFormat format)
{
    if (format == EDDiskStatesFormat::Brief && cell.DDiskStates.size() == 1 &&
        cell.DDiskStates.begin()->first ==
            TVChunkConfig::EHostHumanReadableState::Primary)
    {
        return ToString(cell.DDiskStates.begin()->second);
    }

    TStringBuilder result;
    for (const auto& [state, count]: cell.DDiskStates) {
        result << Print(state, format == EDDiskStatesFormat::Brief) << ":"
               << count << "&#10;";
    }
    return result;
}

TString BuildDbgConfigTooltip(const TDbgTableCell& cell)
{
    TStringBuilder result;
    result << "DDisk:&#10;"
           << BuildDDisksStates(cell, EDDiskStatesFormat::Full);
    result << "PBuffer: " << cell.PBufferCount;
    return result;
}

void RenderDbgConfigCell(
    IOutputStream& str,
    const TDbgTableCell& cell,
    EDbgConfigCellKind kind)
{
    str << "<td class=\"" << GetDbgConfigCellClass(cell, kind) << "\"";
    if (!IsEmpty(cell)) {
        str << " title=\"" << BuildDbgConfigTooltip(cell) << "\"";
    }
    str << ">";
    if (IsEmpty(cell)) {
        str << "-";
    } else {
        str << BuildDDisksStates(cell, EDDiskStatesFormat::Brief);
    }
    str << "</td>";
}

void RenderDbgConfigTable(
    IOutputStream& str,
    const TVector<TDbgSnapshot>& dbgs,
    const TTabletInfo& tabletInfo,
    const TVChunkConfigs& vChunkConfigs,
    const ITouchedProvider& touchedProvider)
{
    auto tableData = BuildDbgConfigTable(dbgs);
    const size_t regionCount = GetRegionCount(
        tabletInfo.BlockCount,
        tabletInfo.BlockSize,
        tabletInfo.VChunkSize);
    FillDefaultConfigs(
        regionCount,
        tabletInfo.VolumeDirectBlockGroupCount,
        touchedProvider,
        &tableData);
    ApplyRealConfigs(
        dbgs,
        tabletInfo.VolumeDirectBlockGroupCount,
        vChunkConfigs,
        touchedProvider,
        &tableData);
    TransferDefaultConfigsToTable(dbgs, &tableData);
    CalculateTableTotals(&tableData);

    TVector<TNodeId> nodeIds;
    nodeIds.reserve(tableData.Table.size());
    for (const auto& [nodeId, row]: tableData.Table) {
        Y_UNUSED(row);
        nodeIds.push_back(nodeId);
    }
    Sort(nodeIds);

    HTML (str) {
        TAG (TH3) {
            str << "Direct Block Group config";
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
                    for (const auto& headerCell: tableData.Headers) {
                        TABLEH () {
                            RenderDbgConfigHeader(
                                str,
                                tabletInfo.TabletId,
                                headerCell);
                        }
                    }
                    TABLEH () {
                        str << "Total";
                    }
                }
            }
            TABLEBODY () {
                for (const TNodeId nodeId: nodeIds) {
                    const auto& row = *tableData.Table.FindPtr(nodeId);
                    TABLER () {
                        TABLED () {
                            str << "Node " << nodeId;
                        }
                        for (const auto& cell: row) {
                            RenderDbgConfigCell(
                                str,
                                cell,
                                EDbgConfigCellKind::Placement);
                        }
                        RenderDbgConfigCell(
                            str,
                            *tableData.RowTotals.FindPtr(nodeId),
                            EDbgConfigCellKind::Total);
                    }
                }
                TABLER_CLASS ("dbg-config-total-row") {
                    TABLEH () {
                        str << "Total";
                    }
                    for (const auto& cell: tableData.ColumnTotals) {
                        RenderDbgConfigCell(
                            str,
                            cell,
                            EDbgConfigCellKind::Total);
                    }
                    RenderDbgConfigCell(
                        str,
                        tableData.GrandTotal,
                        EDbgConfigCellKind::Total);
                }
            }
        }
    }
}

void RenderOverviewInfo(
    IOutputStream& str,
    const TTabletInfo& tabletInfo,
    const std::optional<TFastPathServiceInfo>& serviceInfo,
    const TCountAndSize& pBuffersUsage)
{
    const ui64 regionSize = GetRegionSize(tabletInfo.VChunkSize);
    const ui64 blocksPerVChunk =
        GetVChunkBlockCount(tabletInfo.BlockSize, tabletInfo.VChunkSize);
    const ui64 blocksPerRegion =
        GetRegionBlockCount(tabletInfo.BlockSize, tabletInfo.VChunkSize);
    const ui64 regionCount = GetRegionCount(
        tabletInfo.BlockCount,
        tabletInfo.BlockSize,
        tabletInfo.VChunkSize);
    const ui64 totalVChunkCount = GetVChunkCount(
        tabletInfo.BlockCount,
        tabletInfo.BlockSize,
        tabletInfo.VChunkSize);
    const size_t touchedDDiskCount = tabletInfo.TouchedEnabledDDiskCount +
                                     tabletInfo.TouchedDisabledDDiskCount;
    const ui64 totalDDiskSize = tabletInfo.VChunkSize * touchedDDiskCount;

    HTML (str) {
        TAG (TH3) {
            str << "Overview";
        }
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                RenderValue(
                    str,
                    "TabletId",
                    TStringBuilder() << tabletInfo.TabletId);
                RenderValue(
                    str,
                    "Generation",
                    TStringBuilder() << tabletInfo.Generation);
                RenderValue(str, "DiskId", HtmlEscape(tabletInfo.DiskId));
                RenderValue(str, "State", HtmlEscape(tabletInfo.State));
                RenderValue(
                    str,
                    "Block size",
                    FormatByteSize(tabletInfo.BlockSize));
                RenderValue(
                    str,
                    "Block count",
                    TStringBuilder() << tabletInfo.BlockCount);
                RenderValue(
                    str,
                    "VChunk size",
                    TStringBuilder()
                        << FormatByteSize(tabletInfo.VChunkSize) << " = "
                        << FormatByteSize(tabletInfo.BlockSize) << " * "
                        << blocksPerVChunk << " (block)");
                RenderValue(
                    str,
                    "Volume DirectBlockGroup Count",
                    TStringBuilder() << tabletInfo.VolumeDirectBlockGroupCount);
                RenderValue(
                    str,
                    "Region size",
                    TStringBuilder()
                        << FormatByteSize(regionSize) << " = "
                        << FormatByteSize(tabletInfo.VChunkSize) << " * "
                        << VChunkPerRegionCount << " (VChunkPerRegion)" << " = "
                        << FormatByteSize(tabletInfo.BlockSize) << " * "
                        << blocksPerRegion << " (block)");
                RenderValue(
                    str,
                    "VChunk count",
                    TStringBuilder() << totalVChunkCount << " = " << regionCount
                                     << " (region) * " << VChunkPerRegionCount
                                     << " (VChunkPerRegion)");
                RenderValue(
                    str,
                    "Touched VChunks",
                    TStringBuilder() << tabletInfo.TouchedVChunkCount << " / "
                                     << totalVChunkCount);
                RenderValue(
                    str,
                    "Touched DDisks size",
                    TStringBuilder()
                        << FormatByteSize(totalDDiskSize) << " = "
                        << FormatByteSize(tabletInfo.VChunkSize) << " * "
                        << tabletInfo.TouchedEnabledDDiskCount
                        << " (Enabled DDisk) + "
                        << FormatByteSize(tabletInfo.VChunkSize) << " * "
                        << tabletInfo.TouchedDisabledDDiskCount
                        << " (Disabled DDisk)");
                RenderValue(
                    str,
                    "Used PBuffers size",
                    TStringBuilder()
                        << FormatByteSize(pBuffersUsage.Size) << " "
                        << pBuffersUsage.Count << " (count)");
                RenderValue(
                    str,
                    "Disk size",
                    TStringBuilder()
                        << FormatByteSize(
                               tabletInfo.BlockSize * tabletInfo.BlockCount)
                        << " = " << FormatByteSize(tabletInfo.BlockSize)
                        << " * " << tabletInfo.BlockCount << " (block)" << " = "
                        << FormatByteSize(tabletInfo.VChunkSize) << " * "
                        << totalVChunkCount << " (vchunk)" << " = "
                        << FormatByteSize(regionSize) << " * " << regionCount
                        << " (region)");
                RenderValue(
                    str,
                    "Space usage %",
                    TStringBuilder()
                        << static_cast<double>(
                               totalDDiskSize + pBuffersUsage.Size) /
                               static_cast<double>(
                                   tabletInfo.BlockSize *
                                   tabletInfo.BlockCount) *
                               100.0
                        << "% = (" << FormatByteSize(totalDDiskSize) << " + "
                        << FormatByteSize(pBuffersUsage.Size) << ") / "
                        << FormatByteSize(
                               tabletInfo.BlockSize * tabletInfo.BlockCount));
                RenderValue(
                    str,
                    "VChunks per region",
                    TStringBuilder() << VChunkPerRegionCount);
                RenderValue(str, "Regions", TStringBuilder() << regionCount);
                if (serviceInfo) {
                    RenderValue(
                        str,
                        "LSN counter",
                        TStringBuilder() << serviceInfo->LsnCounter);
                }
            }
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RenderOverview(
    IOutputStream& str,
    const TMonPageData& data,
    const TVChunkConfigs& vChunkConfigs,
    const ITouchedProvider& touchedProvider)
{
    RenderOverviewInfo(
        str,
        data.TabletInfo,
        data.FastPathServiceInfo,
        GetPBuffersUsage(data.Dbgs));
    RenderDbgConfigTable(
        str,
        data.Dbgs,
        data.TabletInfo,
        vChunkConfigs,
        touchedProvider);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
