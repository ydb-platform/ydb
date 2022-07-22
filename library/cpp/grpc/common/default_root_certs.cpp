#include "default_root_certs.h"

#include <contrib/libs/grpc/src/core/lib/security/security_connector/ssl_utils.h>

namespace NGrpc {

const char* GetDefaultPemRootCerts() {
    return grpc_core::DefaultSslRootStore::GetPemRootCerts();
}

} // namespace NGrpc
