#pragma once

#include "schemeshard_impl.h"

#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MkDirPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CopyTablesPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
);

THolder<TEvSchemeShard::TEvCancelTx> CancelPropose(
    const TExportInfo& exportInfo,
    TTxId backupTxId
);

TString ExportItemPathName(TSchemeShard* ss, const TExportInfo& exportInfo, ui32 itemIdx);
TString ExportItemPathName(const TString& exportPathName, ui32 itemIdx);

void PrepareDropping(TSchemeShard* ss, TExportInfo& exportInfo, NIceDb::TNiceDb& db,
    TExportInfo::EState droppingState, std::function<void(ui64)> func);
void PrepareDropping(TSchemeShard* ss, TExportInfo& exportInfo, NIceDb::TNiceDb& db);

} // NSchemeShard
} // NKikimr
