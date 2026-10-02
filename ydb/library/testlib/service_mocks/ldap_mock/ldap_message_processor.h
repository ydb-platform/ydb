#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include "ldap_defines.h"
#include "socket.h"

class TStreamSocket;

namespace LdapMock {

class TLdapRequestProcessor {
public:
    struct TProtocolOpData {
        EProtocolOp Type;
        TString Data;
    };

    TLdapRequestProcessor(std::shared_ptr<TSocket> socket, const THashMap<TString, TString>& externalAuthMap);
    int ExtractMessageId();
    std::vector<TProtocolOpData> Process(std::shared_ptr<const TLdapMockResponses> responses);
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
    TString GetBindDnFromClientCert();

private:
    std::shared_ptr<TSocket> Socket;
    size_t ReadBytes = 0;
    THashMap<TString, TString> ExternalAuthMap;
};

}
