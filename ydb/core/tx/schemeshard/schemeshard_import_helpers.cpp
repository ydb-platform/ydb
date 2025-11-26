#include "schemeshard_import_helpers.h"

TString MakeIndexBuildUid(const NKikimr::NSchemeShard::TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx) {
    return TStringBuilder() << importInfo.Id << "-" << itemIdx << "-" << indexIdx;
}

TString MakeIndexBuildUid(const NKikimr::NSchemeShard::TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    return MakeIndexBuildUid(importInfo, itemIdx, item.NextIndexIdx);
}
