#pragma once

#include <util/generic/string.h>

namespace NKikimrScheme {
class TEvModifySchemeTransaction;
class TEvModifySchemeTransactionResult;

class TEvLogin;
class TEvLoginResult;
}

namespace NKikimr::NSchemeShard {

class TSchemeShard;

void AuditLogModifySchemeTransaction(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID);
void AuditLogModifySchemeTransactionDeprecated(const NKikimrScheme::TEvModifySchemeTransaction& request, const NKikimrScheme::TEvModifySchemeTransactionResult& response, TSchemeShard* SS, const TString& userSID);

void AuditLogLogin(const NKikimrScheme::TEvLogin& request, const NKikimrScheme::TEvLoginResult& response, TSchemeShard* SS);
}
