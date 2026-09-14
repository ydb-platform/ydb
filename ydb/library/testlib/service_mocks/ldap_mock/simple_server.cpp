#include "simple_server.h"
#include "socket.h"
#include "ldap_response.h"
#include "ldap_message_processor.h"
#include "ldap_defines.h"

#include <openssl/err.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace LdapMock {

TSimpleServer::TSimpleServer(const TOptions& options, TLdapMockResponses responses)
    : Opt(options)
    , Responses(std::make_shared<const TLdapMockResponses>(std::move(responses)))
{}

bool TSimpleServer::Start() {
    if (Running.exchange(true)) {
        return true;
    }

    InitOpenSsl();

    if (!InitListenSocket()) {
        Running = false;
        return false;
    }
    if (!InitTlsCtx()) {
        Running = false;
        if (Ctx) {
            Ctx.Destroy();
        }
        return false;
    }
    Worker = std::thread([this]{ ThreadMain(); });
    return true;
}

void TSimpleServer::Stop() {
    if (!Running.exchange(false)) {
        return;
    }

    if (ListenSocket >= 0) {
        ::shutdown(ListenSocket, SHUT_RDWR);
        ::close(ListenSocket);
    }

    if (Worker.joinable()) {
        Worker.join();
    }
    ListenSocket = -1;
    if (Ctx) {
        Ctx.Destroy();
    }
}

ui16 TSimpleServer::GetPort() const {
    return Port;
}

void TSimpleServer::ReplaceResponses(TLdapMockResponses&& responses) {
    auto p = std::make_shared<const TLdapMockResponses>(std::move(responses));
    std::lock_guard<std::mutex> g(Mutex);
    Responses = std::move(p);
}

void TSimpleServer::ThreadMain() {
    while (Running) {
        int fd = ::accept(ListenSocket, nullptr, nullptr);
        if (fd < 0) continue;
        HandleClient_(fd);

        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
}

void TSimpleServer::HandleClient_(int fd) {
    std::shared_ptr<TSocket> socket = std::make_shared<TSocket>(fd);
    if (Opt.UseTls) {
        socket->UpgradeToTls(Ctx.Get());
    }
    TLdapRequestProcessor requestProcessor(socket, Opt.ExternalAuthMap);

    while (Running) {
        unsigned char elementType = requestProcessor.GetByte();
        if (elementType != EElementType::SEQUENCE) {
            if (TLdapResponse().Send(socket)) {
                break;
            }
        }
        size_t messageLength = requestProcessor.GetLength();
        if (messageLength == 0) {
            if (TLdapResponse().Send(socket)) {
                break;
            }
        }
        int messageId = requestProcessor.ExtractMessageId();
        std::shared_ptr<const TLdapMockResponses> responsesSnapshot;
        {
            std::lock_guard<std::mutex> g(Mutex);
            responsesSnapshot = Responses;
        }
        std::vector<TLdapRequestProcessor::TProtocolOpData> operationData = requestProcessor.Process(responsesSnapshot);
        TLdapResponse response = TLdapResponse(messageId, operationData);
        if (!response.Send(socket)) {
            break;
        }
        if (!socket->isTls() && response.EnableTls()) {
            if (!socket->UpgradeToTls(Ctx.Get())) {
                break;
            }
        }
    }
}

void TSimpleServer::InitOpenSsl() {
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
}

bool TSimpleServer::InitListenSocket() {
    ListenSocket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (ListenSocket < 0) {
        return false;
    }

    int one = 1;
    ::setsockopt(ListenSocket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(Opt.Port);
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
        return false;
    }

    if (::bind(ListenSocket, (sockaddr*)&addr, sizeof(addr)) != 0) {
        return false;
    }

    if (::listen(ListenSocket, 16) != 0) {
        return false;
    }

    socklen_t len = sizeof(addr);
    if (::getsockname(ListenSocket, (sockaddr*)&addr, &len) != 0) {
        return false;
    }

    Port = ntohs(addr.sin_port);

    return true;
}

int TSimpleServer::VerifyCb(int ok, X509_STORE_CTX* store) {
    int err = X509_STORE_CTX_get_error(store);
    int depth = X509_STORE_CTX_get_error_depth(store);
    X509* cert = X509_STORE_CTX_get_current_cert(store);

    char subj[512];
    X509_NAME_oneline(X509_get_subject_name(cert), subj, sizeof(subj));

    Cerr <<  "verify: ok=" << ok << " depth=" << depth << " err=" << err << " ("<< X509_verify_cert_error_string(err) << ") subject="<< subj << "\n" << Endl;

    return ok;
}

bool TSimpleServer::InitTlsCtx() {
    Ctx.Reset(SSL_CTX_new(TLS_server_method()));
    if (!Ctx) {
        return false;
    }

    if (!Opt.CertFile.empty() && !Opt.KeyFile.empty()) {
        if (SSL_CTX_use_certificate_file(Ctx.Get(), Opt.CertFile.c_str(), SSL_FILETYPE_PEM) != 1) {
            ERR_print_errors_fp(stderr);
            return false;
        }
        if (SSL_CTX_use_PrivateKey_file(Ctx.Get(), Opt.KeyFile.c_str(), SSL_FILETYPE_PEM) != 1) {
            ERR_print_errors_fp(stderr);
            return false;
        }
    }

    int mode = SSL_VERIFY_NONE;
    if (Opt.RequireClientCert) {
        mode = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
    } else if (!Opt.CaCertFile.empty()) {
        mode = SSL_VERIFY_PEER;
    }

    if (!Opt.CaCertFile.empty()) {
        if (SSL_CTX_load_verify_locations(Ctx.Get(), Opt.CaCertFile.c_str(), nullptr) != 1) {
            return false;
        }

        STACK_OF(X509_NAME)* caList = SSL_load_client_CA_file(Opt.CaCertFile.c_str());
        if (caList) SSL_CTX_set_client_CA_list(Ctx.Get(), caList);
    }

    SSL_CTX_set_verify(Ctx.Get(), mode, VerifyCb);
    return true;
}

} // LdapMock
