#pragma once

#include <util/generic/string.h>

namespace NKikimrScheme {
class TEvModifySchemeTransaction;
class TEvModifySchemeTransactionResult;

class TEvLogin;
class TEvLoginResult;
}

namespace NKikimrExport {
class TEvCreateExportRequest;
class TEvCreateExportResponse;
}

namespace NKikimrImport {
class TEvCreateImportRequest;
class TEvCreateImportResponse;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TExportInfo;
struct TImportInfo;

void AuditLogModifySchemeTransaction(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID);
void AuditLogModifySchemeTransactionDeprecated(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID);

void AuditLogExportStart(const NKikimrExport::TEvCreateExportRequest& request, const NKikimrExport::TEvCreateExportResponse& response, TSchemeShard* SS);
void AuditLogExportEnd(const TExportInfo& exportInfo, TSchemeShard* SS);

void AuditLogImportStart(const NKikimrImport::TEvCreateImportRequest& request, const NKikimrImport::TEvCreateImportResponse& response, TSchemeShard* SS);
void AuditLogImportEnd(const TImportInfo& importInfo, TSchemeShard* SS);

void AuditLogLogin(const NKikimrScheme::TEvLogin& request, const NKikimrScheme::TEvLoginResult& response, TSchemeShard* SS);
}
