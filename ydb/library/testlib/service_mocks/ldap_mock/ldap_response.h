#pragma once
#include <util/generic/ptr.h>
#include <util/stream/format.h>
#include <vector>
#include "ldap_message_processor.h"
#include "ldap_socket_wrapper.h"
#include "ber.h"

class TStreamSocket;

namespace LdapMock {

class TLdapResponse {
    std::vector<TString> DataResponses;
    bool NeedEnableTls = false;

    void EncodeLdapMsg(int msgId, const std::vector<TLdapRequestProcessor::TProtocolOpData>& protocolResults);

public:
    TLdapResponse();
    TLdapResponse(int msgId, const std::vector<TLdapRequestProcessor::TProtocolOpData>& protocolResults);

    bool Send(TAtomicSharedPtr<TLdapSocketWrapper> socket);
    bool EnableTls();
};

}
