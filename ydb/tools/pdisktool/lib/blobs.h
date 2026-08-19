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
};

void ListBlobs(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBlobsResult& out);

void ListBarriers(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBarriersResult& out);

void ListBlocks(
    const THullSnapshot& snap,
    const TListFilter& filter,
    NKikimr::NPdiskTool::TBlocksResult& out);

bool ExportBlobParts(
    IDeviceReader& device,
    const TDiskFormat& format,
    const TParsedSysLog& state,
    const THullSnapshot& snap,
    const TLogoBlobID& from,
    const TMaybe<TLogoBlobID>& to,
    const TString& outputDir,
    TIssueLog& issues,
    ui32& exported);

} // namespace NKikimr::NPDiskTool
