#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include "ldap_defines.h"

class TStreamSocket;

namespace LdapMock {

class TLdapRequestProcessor {
public:
    struct TProtocolOpData {
        EProtocolOp Type;
        TString Data;
    };

    TLdapRequestProcessor(TAtomicSharedPtr<TStreamSocket> socket);
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
    TSearchRequestInfo::TSearchFilter ProcessFilter();
    void ProcessFilterEquality(TSearchRequestInfo::TSearchFilter* filter);

private:
    TAtomicSharedPtr<TStreamSocket> Socket;
    size_t ReadBytes = 0;
};

}
