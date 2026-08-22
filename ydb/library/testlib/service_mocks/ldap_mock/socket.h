#pragma once
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <sys/socket.h>
#include <openssl/ssl.h>

namespace LdapMock {

class TSocket {
private:
    struct TSslDestroy {
        static void Destroy(SSL* ssl) noexcept {
            SSL_free(ssl);
        }

        static void Destroy(X509* cert) noexcept {
            X509_free(cert);
        }
    };

    template <typename T>
    using TSslHolder = THolder<T, TSslDestroy>;

public:
    TSocket(int fd);
    bool isTls() const;
    bool Receive(void* buf, size_t len);
    bool Send(const void* msg, size_t len);
    bool UpgradeToTls(SSL_CTX* ctx);
    TString GetClientCertSubjectName() const;

private:
    bool ReceivePlain(void* buf, size_t len);
    bool SendPlain(const void* msg, size_t len);
    bool ReceiveTls(void* buf, size_t len);
    bool SendTls(const void* msg, size_t len);

private:
    int Fd{0};
    bool UseTls = false;
    TSslHolder<SSL> Ssl = nullptr;
    TString ClientSubjectName;
};

} // LdapMock
