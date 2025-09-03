#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestorePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

THolder<TEvSchemeShard::TEvCancelTx> CancelRestorePropose(
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

} // NSchemeShard
} // NKikimr
