#pragma once
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>

#include <util/generic/string.h>

namespace Ydb::Operations {
    class OperationParams;
}

namespace NKikimr::NBackup {
    enum class EBackupFileType : unsigned char;
}

namespace NKikimrSchemeOp {
    enum EPathType : int;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TExportInfo;
struct TImportInfo;

namespace TEvSchemeShard {
struct TEvModifySchemeTransaction;
}

TString GetUid(const Ydb::Operations::OperationParams& operationParams);

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TExportInfo& exportInfo);
THolder<TEvSchemeShard::TEvModifySchemeTransaction> MakeModifySchemeTransaction(TSchemeShard* ss, TTxId txId, const TImportInfo& importInfo);

struct XxportProperties {
    TString FileName;
    NBackup::EBackupFileType FileType;
    NKikimrSchemeOp::EPathType PathType;
};

const TVector<XxportProperties>& GetXxportProperties();

TMaybe<XxportProperties> PathTypeToXxportProperties(NKikimrSchemeOp::EPathType pathType);

}  // NKikimr::NSchemeShard
