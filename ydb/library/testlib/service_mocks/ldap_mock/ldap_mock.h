#pragma once
#include <util/generic/ptr.h>
#include "ldap_defines.h"

class TStreamSocket;

namespace LdapMock {

void LdapRequestHandler(TAtomicSharedPtr<TStreamSocket> socket, const TLdapMockResponses& responses);

}
