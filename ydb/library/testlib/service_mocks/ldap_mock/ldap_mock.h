#pragma once
#include <util/generic/ptr.h>
#include "ldap_defines.h"
#include "ldap_socket_wrapper.h"

class TStreamSocket;

namespace LdapMock {

void LdapRequestHandler(TAtomicSharedPtr<TLdapSocketWrapper> socket, const TLdapMockResponses& responses);

}
