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

bool TLdapResponse::Send(TAtomicSharedPtr<TStreamSocket> socket) {
    if (DataResponses.empty()) {
        return false;
    }
    try {
        for (const auto& data : DataResponses) {
            TBaseSocket::Check(socket->Send(data.data(), data.size()), "send");
        }
    } catch (TSystemError e) {
        return false;
    }
    return true;
}

void TLdapResponse::EncodeLdapMsg(int msgId, const std::vector<TLdapRequestProcessor::TProtocolOpData>& protocolResults) {
    if (protocolResults.size() == 1 && protocolResults.front().Type == EProtocolOp::UNBIND_OP_REQUEST) {
        return;
    }
    for (const auto& result : protocolResults) {
        TString sequenceBody = EncodeInt(msgId);
        sequenceBody += static_cast<char>(result.Type);
        sequenceBody += EncodeSize(result.Data.size());
        sequenceBody += result.Data;

        DataResponses.push_back(EncodeSequence(sequenceBody));
    }
}

}
