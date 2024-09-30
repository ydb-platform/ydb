#pragma once
#include <util/generic/string.h>

#ifndef LDAP_NO_ATTRS
#define LDAP_NO_ATTRS "1.1"
#endif

namespace NKikimrLdap {

extern char* noAttributes[];
extern const TString LDAPS_SCHEME;

enum class EScope : int {
    BASE,
    ONE_LEVEL,
    SUBTREE,
};

enum class EOption {
    DEBUG,
    TLS_CACERTFILE,
    TLS_CACERTDIR,
    TLS_REQUIRE_CERT,
    PROTOCOL_VERSION,
};

int Bind(LDAP* ld, const TString& dn, const TString& password);
int Unbind(LDAP* ld);
int Init(LDAP** ld, const TString& scheme, const TString& uris, ui32 port);
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
LDAPMessage* NextEntry(LDAP* ld, LDAPMessage* entry);
char* FirstAttribute(LDAP* ld, LDAPMessage* entry, BerElement** berout);
void MemFree(char* p);
void BerFree(BerElement* ber, int freebuf);
void MsgFree(LDAPMessage* lm);
int CountEntries(LDAP *ld, LDAPMessage *chain);
std::vector<TString> GetAllValuesOfAttribute(LDAP* ld, LDAPMessage* entry, char* target);
int SetProtocolVersion(LDAP* ld);
int SetOption(LDAP* ld, const EOption& option, const void* value);
ui32 GetPort(const TString& scheme);
int GetScope(const EScope& scope);
bool IsSuccess(int result);
NKikimr::TEvLdapAuthProvider::EStatus ErrorToStatus(int err);
bool IsRetryableError(int error);
char* GetDn(LDAP* ld, LDAPMessage* entry);
int StartTLS(LDAP* ld);
int ConvertRequireCert(const NKikimrProto::TLdapAuthentication::TUseTls::TCertRequire& requireCertOption);
}
