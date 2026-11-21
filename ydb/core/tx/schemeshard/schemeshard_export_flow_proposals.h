#pragma once

#include "schemeshard_impl.h"

#include <ydb/core/backup/common/metadata.h>
#include <ydb/library/conclusion/result.h>

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
    ui32 itemIdx,
    TString& error
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
TConclusion<TString> ExportItemDestinationPrefix(TSchemeShard* ss, const TExportInfo& exportInfo, ui32 itemIdx);

// IV of a separate index impl-table export item
NBackup::TEncryptionIV MakeExportIndexItemIV(const NBackup::TEncryptionIV& exportIV, ui32 itemIdx);

// Index impl tables that DataShard lists in the table metadata.json on export
TConclusion<TVector<NBackup::TIndexMetadata>> GetTableIndexesExportMetadata(TSchemeShard* ss, const TPathId& tablePathId,
    bool encrypted, NKikimrSchemeOp::TBackupTask* snapshot = nullptr);

void PrepareDropping(TSchemeShard* ss, TExportInfo& exportInfo, NIceDb::TNiceDb& db,
    TExportInfo::EState droppingState, std::function<void(ui64)> func);
void PrepareDropping(TSchemeShard* ss, TExportInfo& exportInfo, NIceDb::TNiceDb& db);

} // NSchemeShard
} // NKikimr
