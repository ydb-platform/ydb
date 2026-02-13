#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/login/protos/login.pb.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/security/ticket_parser_log.h>
#include "ldap_auth_provider.h"

#include <winldap.h>
#include <winber.h>

#include "ldap_compat.h"

namespace NKikimrLdap {

namespace {

char ldapNoAttribute[] = LDAP_NO_ATTRS;

const char* ConvertSaslMechanism(const NLoginProto::ESaslAuthMech::SaslAuthMech& mechanism);

}

char* noAttributes[] = {ldapNoAttribute, nullptr};
const TString LDAPS_SCHEME = "ldaps";

int Bind(LDAP* ld, const TString& dn, const NLoginProto::ESaslAuthMech::SaslAuthMech& mechanism, std::vector<char>* credentials) {
    static char initBvVal[] = "";
    static constexpr BerValue defaultCredentials {.bv_len = 0, .bv_val = initBvVal};
    BerValue cred = defaultCredentials;
    BerValue* credPtr = &cred;
    if (credentials) {
        cred.bv_len = credentials->size();
        cred.bv_val = credentials->data();
    }
    struct berval* servercredp = nullptr;
    char* bindDn = (dn.empty() ? nullptr : const_cast<char*>(dn.c_str()));
    return ldap_sasl_bind_sA(ld, bindDn, const_cast<char*>(ConvertSaslMechanism(mechanism)), credPtr, nullptr, nullptr, &servercredp);
}

int Unbind(LDAP* ld) {
    return ldap_unbind(ld);
}

int Init(LDAP** ld, const TString& scheme, const TString& uris, ui32 port) {
    char* hostName = const_cast<char*>(uris.c_str());
    if (scheme == LDAPS_SCHEME) {
        static const int isSecure = 1;
        *ld = ldap_sslinit(hostName, port, isSecure);
    } else {
        *ld = ldap_init(hostName, port);
    }
    return LdapGetLastError();
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
        return LDAP_SSL_PORT;
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
    case LDAP_TIMEOUT:
    case LDAP_CONNECT_ERROR:
    case LDAP_BUSY:
    case LDAP_UNAVAILABLE:
    case LDAP_ADMIN_LIMIT_EXCEEDED:
        return true;
    }
    return false;
}

char* GetDn(LDAP* ld, LDAPMessage* entry) {
    return ldap_get_dn(ld, entry);
}

int SetOption(LDAP*, const EOption&, const void*) {
    // stub
    return LDAP_SUCCESS;
}

int StartTLS(LDAP* ld) {
    // Do not use tls on Windows
    return LDAP_SERVER_DOWN;
}

int ConvertRequireCert(const NKikimrProto::TLdapAuthentication::TUseTls::TCertRequire&) {
    // stub
    return LDAP_SUCCESS;
}

namespace {

const char* ConvertSaslMechanism(const NLoginProto::ESaslAuthMech::SaslAuthMech& mechanism) {
    switch (mechanism) {
    case NLoginProto::ESaslAuthMech::Simple: {
        return nullptr;
    }
    case NLoginProto::ESaslAuthMech::Plain: {
        return "PLAIN";
    }
    case NLoginProto::ESaslAuthMech::External: {
        return "EXTERNAL";
    }
    default: {
        return "UNKNOWN";
    }
    }
}

} // namespace

}
