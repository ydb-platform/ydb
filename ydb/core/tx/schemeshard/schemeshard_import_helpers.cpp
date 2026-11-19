#include "schemeshard_import_helpers.h"
#include "schemeshard_info_types.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

THolder<NBackup::TEvTableChecksum::TEvPrepareChecksum> MakeImportChecksumPrepareRequest(
    TSchemeShard* ss, TImportInfo& info, ui32 itemIdx, TRowVersion snapshot) {
    auto& item = info.Items.at(itemIdx);
    if (info.State != TImportInfo::EState::Waiting || !info.ShouldValidateTableChecksum() ||
        item.ParentIdx != Max<ui32>() || !item.Metadata.GetTableChecksum().has_value() ||
        item.Metadata.GetTableChecksum()->Error || !ss->Tables.contains(item.DstPathId)) {
        return {};
    }
    item.ChecksumSnapshot = snapshot;
    if (item.ChecksumActor) {
        return {};
    }
    return MakeHolder<NBackup::TEvTableChecksum::TEvPrepareChecksum>(info.Id, itemIdx, snapshot);
}

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx) {
    return TStringBuilder() << importInfo.Id << "-" << itemIdx << "-" << indexIdx;
}

TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    return MakeIndexBuildUid(importInfo, itemIdx, item.NextIndexIdx);
}

} // NKikimr::NSchemeShard
