#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/ticket_parser.h>
#include "ticket_parser_log.h"
#include "ldap_auth_provider.h"

#include <winldap.h>
#include <winber.h>

#include "ldap_compat.h"

namespace NKikimrLdap {

int Bind(LDAP* ld, const TString& dn, const TString& password) {
    char* bindDn = const_cast<char*>(dn.c_str());
    char* bindPassword = const_cast<char*>(password.c_str());
    return ldap_simple_bind_s(ld, bindDn, bindPassword);
}

int Unbind(LDAP* ld) {
    return ldap_unbind(ld);
}

LDAP* Init(const TString& host, ui32 port) {
    char* ldapHost = const_cast<char*>(host.c_str());
    return ldap_init(ldapHost, port);
}

int Search(LDAP* ld,
           const TString& base,
           const EScope& scope,
           const TString& filter,
           char** attrs,
           int attrsonly,
           LDAPMessage** res) {
    char* baseDn = const_cast<char*>(base.c_str());
    char* serachFilter = const_cast<char*>(filter.c_str());
    return ldap_search_s(ld, baseDn, GetScope(scope), serachFilter, attrs, attrsonly, res);
}

TString LdapError(LDAP* ld) {
    Y_UNUSED(ld);
    return TString(ldap_err2string(LdapGetLastError()));
}

TString ErrorToString(int err) {
    return ldap_err2string(err);
}

LDAPMessage* FirstEntry(LDAP* ld, LDAPMessage* chain) {
    return ldap_first_entry(ld, chain);
}

char* FirstAttribute(LDAP* ld, LDAPMessage* entry, BerElement** berout) {
    return ldap_first_attribute(ld, entry, berout);
}

void MemFree(char* p) {
    ldap_memfree(p);
}

void BerFree(BerElement* ber, int freebuf) {
    ber_free(ber, freebuf);
}

void MsgFree(LDAPMessage* lm) {
    ldap_msgfree(lm);
}

int CountEntries(LDAP *ld, LDAPMessage *chain) {
    return ldap_count_entries(ld, chain);
}

std::vector<TString> GetAllValuesOfAttribute(LDAP* ld, LDAPMessage* entry, char* target) {
    auto attributeValues = ldap_get_values_len(ld, entry, target);
    std::vector<TString> response;
    if (attributeValues != nullptr) {
        int countValues = ldap_count_values_len(attributeValues);
        response.reserve(countValues);
        for (int i = 0; i < countValues; i++) {
            response.emplace_back(attributeValues[i]->bv_val, attributeValues[i]->bv_len);
        }
        ldap_value_free_len(attributeValues);
    }
    return response;
}

ui32 GetPort() {
    return LDAP_PORT;
}

int GetScope(const EScope& scope) {
    switch (scope) {
        case EScope::BASE:
            return LDAP_SCOPE_BASE;
        case EScope::ONE_LEVEL:
            return LDAP_SCOPE_ONELEVEL;
        case EScope::SUBTREE:
            return LDAP_SCOPE_SUBTREE;
    }
}

bool IsSuccess(int result) {
    return result == LDAP_SUCCESS;
}

int SetProtocolVersion(LDAP* ld) {
    static const ui32 USED_LDAP_VERSION = LDAP_VERSION3;
    return ldap_set_option(ld, LDAP_OPT_PROTOCOL_VERSION, &USED_LDAP_VERSION);
}

NKikimr::TEvLdapAuthProvider::EStatus ErrorToStatus(int err) {
    switch (err) {
        case LDAP_SUCCESS:
            return NKikimr::TEvLdapAuthProvider::EStatus::SUCCESS;
        case LDAP_INVALID_CREDENTIALS:
            return NKikimr::TEvLdapAuthProvider::EStatus::UNAUTHORIZED;
        case LDAP_FILTER_ERROR:
            return NKikimr::TEvLdapAuthProvider::EStatus::BAD_REQUEST;
        default:
            return NKikimr::TEvLdapAuthProvider::EStatus::UNAVAILABLE;
    }
}

bool IsRetryableError(int error) {
    switch (error) {
        case LDAP_SERVER_DOWN:
            return true;
    }
    return false;
}

}

