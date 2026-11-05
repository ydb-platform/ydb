#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

// `indexesToBuild` receives the names of the indexes whose materialized data from the backup is
// incompatible with the layout created in this database: they are left out of the create request
// and must be built from the restored table data.
THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error,
    TVector<TString>& indexesToBuild
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestoreTableDataPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvCancelTx> CancelRestoreTableDataPropose(
    const TImportInfo& importInfo,
    TTxId restoreTxId
);

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    const TString& uid
);

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    TTxId indexBuildId
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateChangefeedPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    const TImportInfo::TItem& item,
    TString& error
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateConsumersPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    TImportInfo::TItem& item
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTopicPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

} // NSchemeShard
} // NKikimr
