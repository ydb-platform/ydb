#include "mon_render_overview.h"

#include "mon_model.h"
#include "mon_util.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/region_geometry.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

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
    Y_UNUSED(vChunkConfigs);
    Y_UNUSED(touchedProvider);

    RenderOverviewInfo(
        str,
        data.TabletInfo,
        data.FastPathServiceInfo,
        GetPBuffersUsage(data.Dbgs));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
