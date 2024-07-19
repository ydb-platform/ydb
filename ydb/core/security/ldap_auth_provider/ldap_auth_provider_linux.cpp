#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/security/ticket_parser_log.h>
#include "ldap_auth_provider.h"

#define LDAP_DEPRECATED 1
#include <ldap.h>

#include "ldap_compat.h"

namespace NKikimrLdap {

namespace {

char ldapNoAttribute[] = LDAP_NO_ATTRS;

int ConvertOption(const EOption& option) {
    switch(option) {
        case EOption::DEBUG: {
            return LDAP_OPT_DEBUG_LEVEL;
        }
        case EOption::TLS_CACERTDIR: {
            return LDAP_OPT_X_TLS_CACERTDIR;
        }
        case EOption::TLS_CACERTFILE: {
            return LDAP_OPT_X_TLS_CACERTFILE;
        }
        case EOption::TLS_REQUIRE_CERT: {
            return LDAP_OPT_X_TLS_REQUIRE_CERT;
        }
        case EOption::PROTOCOL_VERSION: {
            return LDAP_OPT_PROTOCOL_VERSION;
        }
    }
}

}

char* noAttributes[] = {ldapNoAttribute, nullptr};
const TString LDAPS_SCHEME = "ldaps";

int Bind(LDAP* ld, const TString& dn, const TString& password) {
    return ldap_simple_bind_s(ld, dn.c_str(), password.c_str());
}

int Unbind(LDAP* ld) {
    return ldap_unbind(ld);
}

int Init(LDAP** ld, const TString& scheme, const TString& uris, ui32 port) {
    Y_UNUSED(scheme, port);
    return ldap_initialize(ld, uris.c_str());
}

int Search(LDAP* ld,
           const TString& base,
           const EScope& scope,
           const TString& filter,
           char** attrs,
           int attrsonly,
           LDAPMessage** res) {
    return ldap_search_s(ld, base.c_str(), GetScope(scope), filter.c_str(), attrs, attrsonly, res);
}

TString LdapError(LDAP* ld) {
    int errorCode = LDAP_SUCCESS;
    ldap_get_option(ld, LDAP_OPT_ERROR_NUMBER, &errorCode);
    return TString(ldap_err2string(errorCode));
}

TString ErrorToString(int err) {
    return ldap_err2string(err);
}

LDAPMessage* FirstEntry(LDAP* ld, LDAPMessage* chain) {
    return ldap_first_entry(ld, chain);
}

LDAPMessage* NextEntry(LDAP* ld, LDAPMessage* entry) {
    return ldap_next_entry(ld, entry);
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

ui32 GetPort(const TString& scheme) {
    if (scheme == LDAPS_SCHEME) {
        return LDAPS_PORT;
    }
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
    return SetOption(ld, EOption::PROTOCOL_VERSION, &USED_LDAP_VERSION);
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

char* GetDn(LDAP* ld, LDAPMessage* entry) {
    return ldap_get_dn(ld, entry);
}

int SetOption(LDAP* ld, const EOption& option, const void* value) {
    return ldap_set_option(ld, ConvertOption(option), value);
}

int StartTLS(LDAP* ld) {
    return ldap_start_tls_s(ld, nullptr, nullptr);
}

int ConvertRequireCert(const NKikimrProto::TLdapAuthentication::TUseTls::TCertRequire& requireCertOption) {
    switch (requireCertOption) {
        case NKikimrProto::TLdapAuthentication::TUseTls::NEVER: {
            return LDAP_OPT_X_TLS_NEVER;
        }
        case NKikimrProto::TLdapAuthentication::TUseTls::HARD: {
            return LDAP_OPT_X_TLS_HARD;
        }
        case NKikimrProto::TLdapAuthentication::TUseTls::DEMAND: {
            return LDAP_OPT_X_TLS_DEMAND;
        }
        case NKikimrProto::TLdapAuthentication::TUseTls::ALLOW: {
            return LDAP_OPT_X_TLS_ALLOW;
        }
        case NKikimrProto::TLdapAuthentication::TUseTls::TRY: {
            return LDAP_OPT_X_TLS_TRY;
        }
    }
}

}
