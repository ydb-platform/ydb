#include "common.h"

namespace NYT::NRpc {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

TPemBlobConfigPtr RpcCACert = CreateTestKeyBlob("rpc_ca.pem");
TPemBlobConfigPtr RpcServerCert = CreateTestKeyBlob("rpc_server_cert.pem");
TPemBlobConfigPtr RpcServerKey = CreateTestKeyBlob("rpc_server_key.pem");
TPemBlobConfigPtr RpcClientCert = CreateTestKeyBlob("rpc_client_cert.pem");
TPemBlobConfigPtr RpcClientKey = CreateTestKeyBlob("rpc_client_key.pem");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
