#pragma once
#include <util/generic/string.h>

namespace NKikimrLdap {

enum class EScope : int {
    BASE,
    ONE_LEVEL,
    SUBTREE,
};

int Bind(LDAP* ld, const TString& dn, const TString& password);
int Unbind(LDAP* ld);
LDAP* Init(const TString& host, ui32 port);
int Search(LDAP* ld,
           const TString& base,
           const EScope& scope,
           const TString& filter,
           char** attrs,
           int attrsonly,
           LDAPMessage** res);
TString LdapError(LDAP* ld);
TString ErrorToString(int err);

LDAPMessage* FirstEntry(LDAP* ld, LDAPMessage* chain);
char* FirstAttribute(LDAP* ld, LDAPMessage* entry, BerElement** berout);
void MemFree(char* p);
void BerFree(BerElement* ber, int freebuf);
void MsgFree(LDAPMessage* lm);
int CountEntries(LDAP *ld, LDAPMessage *chain);
std::vector<TString> GetAllValuesOfAttribute(LDAP* ld, LDAPMessage* entry, char* target);

int SetProtocolVersion(LDAP* ld);
ui32 GetPort();
int GetScope(const EScope& scope);
bool IsSuccess(int result);


NKikimr::TEvLdapAuthProvider::EStatus ErrorToStatus(int err);
bool IsRetryableError(int error);
}
