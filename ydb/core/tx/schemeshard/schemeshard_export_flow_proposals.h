#pragma once

#include "schemeshard_impl.h"

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MkDirPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CopyTablesPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
);

THolder<TEvSchemeShard::TEvCancelTx> CancelPropose(
    const TExportInfo::TPtr exportInfo,
    TTxId backupTxId
);

TString ExportItemPathName(TSchemeShard* ss, const TExportInfo::TPtr exportInfo, ui32 itemIdx);
TString ExportItemPathName(const TString& exportPathName, ui32 itemIdx);

} // NSchemeShard
} // NKikimr
