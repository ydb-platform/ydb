#include <util/stream/output.h>
#include <util/network/sock.h>
#include "ldap_mock.h"
#include "ldap_response.h"
#include "ldap_message_processor.h"
#include "ldap_defines.h"

namespace LdapMock {

namespace {
    TLdapResponse HandleLdapMessage(TAtomicSharedPtr<TStreamSocket> socket, const TLdapMockResponses& responses) {
        TLdapRequestProcessor requestProcessor(socket);
        unsigned char elementType = requestProcessor.GetByte();
        if (elementType != EElementType::SEQUENCE) {
            return TLdapResponse();
        }
        size_t messageLength = requestProcessor.GetLength();
        if (messageLength == 0) {
            return TLdapResponse();
        }
        int messageId = requestProcessor.ExtractMessageId();
        std::vector<TLdapRequestProcessor::TProtocolOpData> operationData = requestProcessor.Process(responses);
        return TLdapResponse(messageId, operationData);
    }
}

void LdapRequestHandler(TAtomicSharedPtr<TStreamSocket> socket, const TLdapMockResponses& responses) {
    while (true) {
        TLdapResponse response = HandleLdapMessage(socket, responses);
        if (!response.Send(socket)) {
            break;
        }
    }
}

}
