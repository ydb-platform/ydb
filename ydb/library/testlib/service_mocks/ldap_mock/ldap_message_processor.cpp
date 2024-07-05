#include <util/stream/format.h>
#include <utility>
#include <algorithm>
#include "ldap_message_processor.h"
#include "ldap_defines.h"
#include "ber.h"

namespace LdapMock {

namespace {

struct TResponseInfo {
    EStatus Status;
    TString MatchedDN = "";
    TString DiagnosticMsg = "";
};

TString CreateResponse(const TResponseInfo& responseInfo) {
    TString result = EncodeEnum(static_cast<int>(responseInfo.Status));
    result += EncodeString(responseInfo.MatchedDN);
    result += EncodeString(responseInfo.DiagnosticMsg);
    return result;
}

TString CreateExtendedResponse(const TResponseInfo& responseInfo, const TString& oid = "", const TString& oidValue = "") {
    TString result = CreateResponse(responseInfo);
    result += EncodeString(oid);
    result += EncodeString(oidValue);
    return result;
}

TString CreateSetOfAttributeValue(const std::vector<TString>& values) {
    TString result;
    result += EElementType::SET;
    TString body;
    for (const TString& str : values) {
        body += EncodeString(str);
    }
    result += EncodeSize(body.size());
    result += body;
    return result;
}

TString CreateSequenceOfAttribute(const std::vector<std::pair<TString, std::vector<TString>>>& attributes) {
    TString result;
    result += EElementType::SEQUENCE;
    TString partialAttributes;
    for (const auto& entry : attributes) {
        partialAttributes += EElementType::SEQUENCE;
        TString body = EncodeString(entry.first);
        body += CreateSetOfAttributeValue(entry.second);
        partialAttributes += EncodeSize(body.size());
        partialAttributes += body;
    }
    result += EncodeSize(partialAttributes.size());
    result += partialAttributes;
    return result;
}

std::vector<TLdapRequestProcessor::TProtocolOpData> CreateSearchEntryResponses(const std::vector<TSearchEntry>& entries) {
    std::vector<TLdapRequestProcessor::TProtocolOpData> result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        TLdapRequestProcessor::TProtocolOpData protocolData;
        protocolData.Type = EProtocolOp::SEARCH_OP_ENTRY_RESPONSE;
        protocolData.Data = EncodeString(entry.Dn);
        protocolData.Data += CreateSequenceOfAttribute(entry.AttributeList);
        result.push_back(protocolData);
    }
    return result;
}

} // namespace

TLdapRequestProcessor::TLdapRequestProcessor(TAtomicSharedPtr<TLdapSocketWrapper> socket)
    : Socket(socket)
{}

unsigned char TLdapRequestProcessor::GetByte() {
    unsigned char res;
    GetNBytes(&res, 1);
    return res;
}

void TLdapRequestProcessor::GetNBytes(unsigned char* buf, size_t n) {
    Socket->Receive(buf, n);
    ReadBytes += n;
}

int TLdapRequestProcessor::GetInt() {
    unsigned char data = GetByte();
    data = GetByte();

    int res = 0;
    for (size_t i = data; i > 0; --i) {
        unsigned char octet = GetByte();
        res |= octet << (8 * (i - 1));
    }
    return res;
}

size_t TLdapRequestProcessor::GetLength() {
    unsigned char data = GetByte();
    if (data < 0x80) {
        return data;
    }

    size_t res = 0;
    for (size_t i = data - 0x80; i > 0; --i) {
        unsigned char octet = GetByte();
        res |= octet << (8 * (i - 1));
    }
    return res;
}

TString TLdapRequestProcessor::GetString() {
    size_t length = GetLength();
    if (length == 0) {
        return {};
    }

    TString res;
    for (size_t i = 0; i < length; i++) {
        res += GetByte();
    }
    return res;
}

int TLdapRequestProcessor::ExtractMessageId() {
    int res = GetInt();
    return res;
}

std::vector<TLdapRequestProcessor::TProtocolOpData> TLdapRequestProcessor::Process(const TLdapMockResponses& responses) {
    unsigned char protocolOp = GetByte();
    switch (protocolOp) {
        case EProtocolOp::BIND_OP_REQUEST: {
            return ProcessBindRequest(responses.BindResponses);
        }
        case EProtocolOp::SEARCH_OP_REQUEST: {
            return ProcessSearchRequest(responses.SearchResponses);
        }
        case EProtocolOp::UNBIND_OP_REQUEST: {
            return {{.Type = EProtocolOp::UNBIND_OP_REQUEST}};
        }
        case EProtocolOp::EXTENDED_OP_REQUEST: {
            return ProcessExtendedRequest();
        }
        default: {
            return {{.Type = EProtocolOp::UNKNOWN_OP, .Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR})}};
        }
    }
}

std::vector<TLdapRequestProcessor::TProtocolOpData> TLdapRequestProcessor::ProcessExtendedRequest() {
    TProtocolOpData responseOpData;
    responseOpData.Type  = EProtocolOp::EXTENDED_OP_RESPONSE;

    size_t length = GetLength();

    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    TString oid = GetString();
    Y_UNUSED(oid);
    TString oidValue = GetString();

    EStatus status = EStatus::PROTOCOL_ERROR;
    if (oidValue == LDAP_EXOP_START_TLS) {
        status = EStatus::SUCCESS;
    }

    responseOpData.Data = CreateExtendedResponse({.Status = status}, oidValue, "");

    return {responseOpData};
}

std::vector<TLdapRequestProcessor::TProtocolOpData> TLdapRequestProcessor::ProcessBindRequest(const std::vector<std::pair<TBindRequestInfo, TBindResponseInfo>>& responses) {
    TBindRequestInfo requestInfo;

    TProtocolOpData responseOpData;
    responseOpData.Type = EProtocolOp::BIND_OP_RESPONSE;

    size_t length = GetLength();

    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    int version = GetInt();
    if (version > 127) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    unsigned char elementType = GetByte();
    if (elementType != EElementType::STRING) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    requestInfo.Login = GetString();

    unsigned char authType = GetByte();
    Y_UNUSED(authType);

    requestInfo.Password = GetString();

    const auto it = std::find_if(responses.begin(), responses.end(), [&requestInfo] (const std::pair<TBindRequestInfo, TBindResponseInfo>& el) {
        const auto& expectedRequestInfo = el.first;
        return requestInfo == expectedRequestInfo;
    });

    if (it == responses.end()) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    responseOpData.Data = CreateResponse({.Status = it->second.Status, .MatchedDN = it->second.MatchedDN, .DiagnosticMsg = it->second.DiagnosticMsg});
    return {responseOpData};
}

std::vector<TLdapRequestProcessor::TProtocolOpData> TLdapRequestProcessor::ProcessSearchRequest(const std::vector<std::pair<TSearchRequestInfo, TSearchResponseInfo>>& responses) {
    TProtocolOpData responseOpData;
    responseOpData.Type = EProtocolOp::SEARCH_OP_DONE_RESPONSE;

    TSearchRequestInfo requestInfo;

    size_t length = GetLength();
    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    // Extract BaseDn
    unsigned char elementType = GetByte();
    if (elementType != EElementType::STRING) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    requestInfo.BaseDn = GetString();

    // Extract scope
    elementType = GetByte();
    if (elementType != EElementType::ENUMERATED) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    length = GetLength();
    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    requestInfo.Scope = GetByte();

    // Extract derefAliases
    elementType = GetByte();
    if (elementType != EElementType::ENUMERATED) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    length = GetLength();
    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    requestInfo.DerefAliases = GetByte();

    // Extract sizeLimit
    int sizeLimit = GetInt();
    Y_UNUSED(sizeLimit);

    // Extract timeLimit
    int timeLimit = GetInt();
    Y_UNUSED(timeLimit);

    // Extract typesOnly
    elementType = GetByte();
    if (elementType != EElementType::BOOL) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    length = GetLength();
    if (length == 0) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    requestInfo.TypesOnly = GetByte();

    requestInfo.Filter = ProcessFilter();

    // Extract Attributes
    elementType = GetByte();
    if (elementType != EElementType::SEQUENCE) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }
    length = GetLength();
    const size_t limit = ReadBytes + length;
    while (ReadBytes < limit) {
        elementType = GetByte();
        if (elementType != EElementType::STRING) {
            responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
            return {responseOpData};
        }
        requestInfo.Attributes.push_back(GetString());
    }

    const auto it = std::find_if(responses.begin(), responses.end(), [&requestInfo] (const std::pair<TSearchRequestInfo, TSearchResponseInfo>& el) {
        const auto& expectedRequestInfo = el.first;
        return requestInfo == expectedRequestInfo;
    });

    if (it == responses.end()) {
        responseOpData.Data = CreateResponse({.Status = EStatus::PROTOCOL_ERROR});
        return {responseOpData};
    }

    std::vector<TLdapRequestProcessor::TProtocolOpData> res = CreateSearchEntryResponses(it->second.ResponseEntries);

    const auto& responseDoneInfo = it->second.ResponseDone;
    responseOpData.Data = CreateResponse({.Status = responseDoneInfo.Status, .MatchedDN = responseDoneInfo.MatchedDN, .DiagnosticMsg = responseDoneInfo.DiagnosticMsg});
    res.push_back(std::move(responseOpData));
    return res;
}

TSearchRequestInfo::TSearchFilter TLdapRequestProcessor::ProcessFilter() {
    unsigned char filterType = GetByte();

    size_t filterLength = GetLength();
    Y_UNUSED(filterLength);

    TSearchRequestInfo::TSearchFilter filter;
    auto FillFilter = [&filter] (const EFilterType& type, const TString& value) {
        filter.Type = type;
        filter.Attribute = "attribute: " + value;
        filter.Value = "value: " + value;
    };

    switch (filterType) {
        case EFilterType::LDAP_FILTER_AND: {
            FillFilter(EFilterType::LDAP_FILTER_AND, "and");
            return filter;
        }
        case EFilterType::LDAP_FILTER_OR: {
            filter.Type = EFilterType::LDAP_FILTER_OR;
            ProcessFilterOr(&filter, filterLength);
            return filter;
        }
        case EFilterType::LDAP_FILTER_NOT: {
            FillFilter(EFilterType::LDAP_FILTER_NOT, "not");
            return filter;
        }
        case EFilterType::LDAP_FILTER_EQUALITY: {
            filter.Type = EFilterType::LDAP_FILTER_EQUALITY;
            ProcessFilterEquality(&filter);
            return filter;
        }
        case EFilterType::LDAP_FILTER_SUBSTRINGS: {
            FillFilter(EFilterType::LDAP_FILTER_SUBSTRINGS, "substrings");
            return filter;
        }
        case EFilterType::LDAP_FILTER_GE: {
            FillFilter(EFilterType::LDAP_FILTER_GE, "ge");
            return filter;
        }
        case EFilterType::LDAP_FILTER_LE: {
            FillFilter(EFilterType::LDAP_FILTER_LE, "le");
            return filter;
        }
        case EFilterType::LDAP_FILTER_PRESENT: {
            FillFilter(EFilterType::LDAP_FILTER_PRESENT, "present");
            return filter;
        }
        case EFilterType::LDAP_FILTER_APPROX: {
            FillFilter(EFilterType::LDAP_FILTER_APPROX, "approx");
            return filter;
        }
        case EFilterType::LDAP_FILTER_EXT: {
            filter.Type = EFilterType::LDAP_FILTER_EXT;
            ProcessFilterExtensibleMatch(&filter, filterLength);
            return filter;
        }
    }
    FillFilter(EFilterType::UNKNOWN, "unknown");
    return filter;
}

void TLdapRequestProcessor::ProcessFilterEquality(TSearchRequestInfo::TSearchFilter* filter) {
    unsigned char elementType = GetByte();
    if (elementType == EElementType::STRING) {
        filter->Attribute = GetString();
    }

    elementType = GetByte();
    if (elementType == EElementType::STRING) {
        filter->Value = GetString();
    }
}

void TLdapRequestProcessor::ProcessFilterExtensibleMatch(TSearchRequestInfo::TSearchFilter* filter, size_t lengthFilter) {
    const size_t limit = ReadBytes + lengthFilter;
    size_t lastCheckedField = 0;
    unsigned char elementType = GetByte();
    if (elementType == EExtendedFilterType::LDAP_FILTER_EXT_OID) {
        filter->MatchingRule = GetString();
        lastCheckedField = 1;
    }

    if (lastCheckedField == 1 && ReadBytes < limit) {
        elementType = GetByte();
    }
    if (elementType == EExtendedFilterType::LDAP_FILTER_EXT_TYPE) {
        filter->Attribute = GetString();
        lastCheckedField = 2;
    }

    if (lastCheckedField == 2 && ReadBytes < limit) {
        elementType = GetByte();
    }
    if (elementType == EExtendedFilterType::LDAP_FILTER_EXT_VALUE) {
        filter->Value = GetString();
    }

    if (ReadBytes < limit) {
        elementType = GetByte();
        if (elementType == EExtendedFilterType::LDAP_FILTER_EXT_DNATTRS) {
            size_t length = GetLength();
            Y_UNUSED(length);
            filter->DnAttributes = GetByte();
        }
    }
}

void TLdapRequestProcessor::ProcessFilterOr(TSearchRequestInfo::TSearchFilter* filter, size_t lengthFilter) {
    const size_t limit = ReadBytes + lengthFilter;
    while (ReadBytes < limit) {
        filter->NestedFilters.push_back(std::make_shared<TSearchRequestInfo::TSearchFilter>(ProcessFilter()));
    }
}

}
