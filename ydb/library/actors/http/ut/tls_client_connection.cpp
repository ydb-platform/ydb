#include "tls_client_connection.h"

#include <Poco/Net/SecureStreamSocket.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/NetSSL.h>

#include <util/string/cast.h>

#include <cstdlib>
#include <mutex>

namespace NHttp::NTest {

namespace {

void EnsureSslInitialized() {
    static std::once_flag once;
    std::call_once(once, [] {
        Poco::Net::initializeSSL();
        std::atexit([] { Poco::Net::uninitializeSSL(); });
    });
}

} // namespace

void SendTlsRequest(
    const TIpPort port,
    const TString& clientCertFile,
    const TString& clientKeyFile,
    const TString& caCertFile,
    const TString& httpRequest
) {
    EnsureSslInitialized();

    // Create SSL context for client with mTLS
    Poco::Net::Context::Params params;
    params.privateKeyFile = clientKeyFile.empty() ? "" : clientKeyFile.data();
    params.certificateFile = clientCertFile.empty() ? "" : clientCertFile.data();
    params.caLocation = caCertFile.empty() ? "" : caCertFile.data();
    Poco::Net::Context::Ptr pContext = new Poco::Net::Context(
        Poco::Net::Context::CLIENT_USE,
        params
    );

    // Send HTTP request via secure socket
    Poco::Net::SocketAddress address("127.0.0.1", port);
    Poco::Net::SecureStreamSocket socket(address, pContext);
    socket.sendBytes(httpRequest.data(), httpRequest.size());

    // Close connection (server will process the request asynchronously)
    socket.shutdownSend();
    socket.close();
}

} // namespace NHttp::NTest
