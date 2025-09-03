#pragma once
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <util/generic/string.h>

namespace Ydb::Operations {
    class OperationParams;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TExportInfo;
struct TImportInfo;

TString GetUid(const Ydb::Operations::OperationParams& operationParams);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TExportInfo& exportInfo);
THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TImportInfo& importInfo);

}  // NKikimr::NSchemeShard
