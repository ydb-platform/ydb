#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

THolder<NEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx,
    TString& error
);

THolder<NEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx
);

THolder<NEvSchemeShard::TEvModifySchemeTransaction> RestorePropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx
);

THolder<NEvSchemeShard::TEvCancelTx> CancelRestorePropose(
    TImportInfo::TPtr importInfo,
    TTxId restoreTxId
);

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    TImportInfo::TPtr importInfo,
    ui32 itemIdx,
    const TString& uid
);

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    TImportInfo::TPtr importInfo,
    TTxId indexBuildId
);

} // NSchemeShard
} // NKikimr
