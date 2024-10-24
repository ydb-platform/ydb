#pragma once
#include <util/generic/string.h>
#include <vector>
#include <utility>

namespace LdapMock {

constexpr const char* LDAP_EXOP_START_TLS = "1.3.6.1.4.1.1466.20037";

enum EStatus {
    SUCCESS = 0x00,
    PROTOCOL_ERROR = 0x02,
    INVALID_CREDENTIALS = 0x31,
};

enum EProtocolOp {
    UNKNOWN_OP = 0,
    UNBIND_OP_REQUEST = 0x42,
    BIND_OP_REQUEST = 0x60,
    BIND_OP_RESPONSE = 0x61,
    SEARCH_OP_REQUEST = 0x63,
    SEARCH_OP_ENTRY_RESPONSE = 0x64,
    SEARCH_OP_DONE_RESPONSE = 0x65,
    EXTENDED_OP_REQUEST = 0x77,
    EXTENDED_OP_RESPONSE = 0x78,
};

enum EFilterType {
    UNKNOWN = 0x0,
    LDAP_FILTER_AND = 0xA0,
    LDAP_FILTER_OR = 0xA1,
    LDAP_FILTER_NOT	= 0xA2,
    LDAP_FILTER_EQUALITY = 0xA3,
    LDAP_FILTER_SUBSTRINGS  = 0xA4,
    LDAP_FILTER_GE = 0xa5,
    LDAP_FILTER_LE = 0xA6,
    LDAP_FILTER_PRESENT = 0x87,
    LDAP_FILTER_APPROX = 0xA8,
    LDAP_FILTER_EXT = 0xA9,
};

enum EExtendedFilterType {
    LDAP_FILTER_EXT_OID = 0x81U,
    LDAP_FILTER_EXT_TYPE = 0x82U,
    LDAP_FILTER_EXT_VALUE = 0x83U,
    LDAP_FILTER_EXT_DNATTRS = 0x84U,
};

enum EElementType {
    BOOL = 0x01,
    STRING = 0x04,
    INTEGER = 0x02,
    ENUMERATED = 0x0A,
    SEQUENCE = 0x30,
    SET = 0x31,
};

struct TBindRequestInfo {
    struct TInitializeList {
        TString Login;
        TString Password;
    };

    TString Login;
    TString Password;

    TBindRequestInfo() = default;
    TBindRequestInfo(const TString& login, const TString& password);
    TBindRequestInfo(const TInitializeList& list);

    bool operator==(const TBindRequestInfo& otherRequest) const;
};

struct TSearchRequestInfo {
    struct TSearchFilter {
        EFilterType Type;
        TString Attribute;
        TString Value;
        TString MatchingRule;
        bool DnAttributes = false;

        std::vector<std::shared_ptr<TSearchFilter>> NestedFilters;
    };

    struct TInitializeList {
        TString BaseDn;
        size_t Scope;
        size_t DerefAliases;
        bool TypesOnly = false;
        TSearchFilter Filter;
        std::vector<TString> Attributes;
    };

    TString BaseDn;
    size_t Scope;
    size_t DerefAliases;
    bool TypesOnly = false;
    TSearchFilter Filter;
    std::vector<TString> Attributes;

    TSearchRequestInfo() = default;
    TSearchRequestInfo(const TString& baseDn,
                       size_t scope,
                       size_t derefAliases,
                       bool typesOnly,
                       const TSearchFilter& filter,
                       const std::vector<TString>& attributes);

    TSearchRequestInfo(const TInitializeList& list);

    bool operator==(const TSearchRequestInfo& otherRequest) const;
};

struct TSearchEntry {
    TString Dn;
    std::vector<std::pair<TString, std::vector<TString>>> AttributeList;
};

struct TLdapFinalResponse {
    EStatus Status;
    TString MatchedDN = "";
    TString DiagnosticMsg = "";
};

struct TSearchResponseInfo {
    std::vector<TSearchEntry> ResponseEntries;
    TLdapFinalResponse ResponseDone;
};

using TBindResponseInfo = TLdapFinalResponse;

struct TLdapMockResponses {
    std::vector<std::pair<TBindRequestInfo, TBindResponseInfo>> BindResponses;
    std::vector<std::pair<TSearchRequestInfo, TSearchResponseInfo>> SearchResponses;
};

}
