#pragma once

#include "hull.h"

namespace NKikimr::NPDiskTool {

struct TListFilter {
    TMaybe<TLogoBlobID> From;
    TMaybe<TLogoBlobID> To;
    TMaybe<ui64> TabletId;
    TMaybe<ui32> Channel;
    ui32 Limit = 10000;
    TString ContinueToken;
    bool DataOnly = true; // index records that carry no local data are rarely what recovery needs
};

// Where one copy of one blob part lives. Several copies show up when more than one SST still holds a
// record for the blob, or when the log re-adds a part the index already knows about.
struct TPartCopy {
    TBlobType::EType Type = TBlobType::DiskBlob;
    TDiskPart Where;     // byte range of this part alone, header already skipped
    TString InlineData;  // set instead of Where for parts carried in the log
    bool Packed = false; // this range holds a multi-part DiskBlob that could not be split

    ui32 Size() const {
        return InlineData ? InlineData.size() : Where.Size;
    }
};

struct TBlobPartView {
    ui32 PartId = 0;
    TVector<TPartCopy> Copies;
};

struct TBlobView {
    TLogoBlobID Id; // part id stripped
    ui64 Ingress = 0;
    TVector<TBlobPartView> Parts;

    bool HasData() const {
        return !Parts.empty();
    }
};

// Collapses the per-SST index records of a snapshot into one entry per blob, resolving each part to
// the exact bytes holding it so that duplicate copies can be compared.
TVector<TBlobView> BuildBlobViews(const THullSnapshot& snap, TIssueLog& issues);

void ListBlobs(
    const THullSnapshot& snap,
    const TListFilter& filter,
    TIssueLog& issues,
    NKikimr::NPdiskTool::TBlobsResult& out);

void ListBarriers(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBarriersResult& out);

void ListBlocks(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBlocksResult& out);

struct TExportStats {
    ui32 Blobs = 0;
    ui32 Parts = 0;
    ui32 PartsWithSeveralCopies = 0;
    ui32 PartsWithDifferingCopies = 0;
};

// Exports every part of the selected blobs. When a part has several copies they are all read and
// compared; identical copies are written once, differing ones are all kept and reported.
bool ExportBlobParts(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const THullSnapshot& snap,
    const TLogoBlobID& from,
    const TMaybe<TLogoBlobID>& to,
    const TListFilter& filter,
    const TString& outputDir,
    TIssueLog& issues,
    TExportStats& stats);

} // namespace NKikimr::NPDiskTool
