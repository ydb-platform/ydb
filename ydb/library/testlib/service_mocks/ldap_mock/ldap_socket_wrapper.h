#pragma once
#include <util/network/sock.h>

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/tls1.h>

namespace LdapMock {

class TLdapSocketWrapper {
    struct TSslDestroy {
        static void Destroy(SSL_CTX* ctx) noexcept {
            SSL_CTX_free(ctx);
        }

        static void Destroy(SSL* ssl) noexcept {
            SSL_free(ssl);
        }

        static void Destroy(X509* cert) noexcept {
            X509_free(cert);
        }

        static void Destroy(EVP_PKEY* pkey) noexcept {
            EVP_PKEY_free(pkey);
        }

        static void Destroy(BIO* bio) noexcept {
            BIO_free(bio);
        }
    };

    template <typename T>
    using TSslHolder = THolder<T, TSslDestroy>;

public:
    TLdapSocketWrapper(TAtomicSharedPtr<TInetStreamSocket> listenSocket, bool isSecureConnection = false);

    void Close();
    void Receive(void* buf, size_t len);
    void Send(const void* msg, size_t len);
    void OnAccept();
    void EnableSecureConnection();
    bool IsSecure() const;

private:
    ssize_t InsecureReceive(void* buf, size_t len);
    ssize_t InsecureSend(const void* msg, size_t len);
    ssize_t SecureReceive(void* buf, size_t len);
    ssize_t SecureSend(const void* msg, size_t len);
    void SetupCerts();

    static TSslHolder<SSL_CTX> CreateSslContext();

private:
    TAtomicSharedPtr<TInetStreamSocket> ListenSocket;
    TStreamSocket Socket;
    TSslHolder<SSL_CTX> Ctx;
    TSslHolder<SSL> Ssl;
    TSslHolder<EVP_PKEY> Key;
    TSslHolder<X509> X509;
    bool IsSecureConnection = false;

    std::function<ssize_t(TLdapSocketWrapper&, void*, size_t)> ReceiveMsg;
    std::function<ssize_t(TLdapSocketWrapper&, const void*, size_t)> SendMsg;
};

} // namespace LdapMock
