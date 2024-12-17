#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include "ldap_defines.h"
#include "ldap_socket_wrapper.h"

class TStreamSocket;

namespace LdapMock {

class TLdapRequestProcessor {
public:
    struct TProtocolOpData {
        EProtocolOp Type;
        TString Data;
    };

    TLdapRequestProcessor(TAtomicSharedPtr<TLdapSocketWrapper> socket);
    int ExtractMessageId();
    std::vector<TProtocolOpData> Process(const TLdapMockResponses& responses);
    unsigned char GetByte();
    size_t GetLength();

private:
    int GetInt();
    void GetNBytes(unsigned char* buf, size_t n);
    TString GetString();
    std::vector<TProtocolOpData> ProcessBindRequest(const std::vector<std::pair<TBindRequestInfo, TBindResponseInfo>>& responses);
    std::vector<TProtocolOpData> ProcessSearchRequest(const std::vector<std::pair<TSearchRequestInfo, TSearchResponseInfo>>& responses);
    std::vector<TProtocolOpData> ProcessExtendedRequest();
    TSearchRequestInfo::TSearchFilter ProcessFilter();
    void ProcessFilterEquality(TSearchRequestInfo::TSearchFilter* filter);
    void ProcessFilterExtensibleMatch(TSearchRequestInfo::TSearchFilter* filter, size_t lengthFilter);
    void ProcessFilterOr(TSearchRequestInfo::TSearchFilter* filter, size_t lengthFilter);

private:
    TAtomicSharedPtr<TLdapSocketWrapper> Socket;
    size_t ReadBytes = 0;
};

}
