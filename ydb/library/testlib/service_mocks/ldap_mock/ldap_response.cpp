#include <util/network/sock.h>
#include "ldap_response.h"
#include <util/stream/format.h>

namespace LdapMock {

TLdapResponse::TLdapResponse() {
    const char protocolErrorResponse[] = {0xA, 0x1, 0x0, 0x4, 0x0, 0x4, 0x0};
    TLdapRequestProcessor::TProtocolOpData protocolData {
        .Type = EProtocolOp::UNKNOWN_OP,
        .Data = TString(protocolErrorResponse)
    };
    EncodeLdapMsg(0, {protocolData});
}

TLdapResponse::TLdapResponse(int msgId, const std::vector<TLdapRequestProcessor::TProtocolOpData>& protocolResults) {
    EncodeLdapMsg(msgId, protocolResults);
}

bool TLdapResponse::Send(TAtomicSharedPtr<TLdapSocketWrapper> socket) {
    if (DataResponses.empty()) {
        return false;
    }
    try {
        for (const auto& data : DataResponses) {
            socket->Send(data.data(), data.size());
        }
    } catch (TSystemError e) {
        return false;
    }
    return true;
}

void TLdapResponse::EncodeLdapMsg(int msgId, const std::vector<TLdapRequestProcessor::TProtocolOpData>& protocolResults) {
    if (protocolResults.size() == 1) {
        const auto& result = protocolResults.front();
        if (result.Type == EProtocolOp::UNBIND_OP_REQUEST) {
            return;
        }
        if (result.Type == EProtocolOp::EXTENDED_OP_RESPONSE) {
            NeedEnableTls = true;
        }
    }
    for (const auto& result : protocolResults) {
        TString sequenceBody = EncodeInt(msgId);
        sequenceBody += static_cast<char>(result.Type);
        sequenceBody += EncodeSize(result.Data.size());
        sequenceBody += result.Data;

        DataResponses.push_back(EncodeSequence(sequenceBody));
    }
}

bool TLdapResponse::EnableTls() {
    return NeedEnableTls;
}

}
