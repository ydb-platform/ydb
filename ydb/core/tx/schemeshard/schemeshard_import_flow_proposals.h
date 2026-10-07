#pragma once

#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> RestoreTableDataPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
);

std::unique_ptr<TEvSchemeShard::TEvCancelTx> CancelRestoreTableDataPropose(
    const TImportInfo& importInfo,
    TTxId restoreTxId
);

std::unique_ptr<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    const TString& uid
);

std::unique_ptr<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    TTxId indexBuildId
);

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateChangefeedPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    const TImportInfo::TItem& item,
    TString& error
);

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateConsumersPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    TImportInfo::TItem& item
);

std::unique_ptr<TEvSchemeShard::TEvModifySchemeTransaction> CreateTopicPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
);

} // NSchemeShard
} // NKikimr
