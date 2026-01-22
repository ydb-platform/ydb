#pragma once
#include "ldap_defines.h"

#include <util/system/types.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <openssl/ssl.h>

#include <atomic>
#include <thread>
#include <mutex>

namespace LdapMock {

struct TLdapMockResponses;

class TSimpleServer {
private:
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
    struct TOptions {
        ui16 Port;
        TString CaCert;
        TString Cert;
        TString Key;
        bool RequireClientCert = false;
        bool UseTls = false;
    };

    TSimpleServer(const TOptions& options, TLdapMockResponses responses);

    bool Start();
    void Stop();
    ui16 GetPort() const;
    void ReplaceResponses(TLdapMockResponses&& responses);

private:
    void ThreadMain();
    void HandleClient_(int fd);
    bool InitListenSocket();
    bool InitTlsCtx();

    static void InitOpenSsl();
    static int VerifyCb(int ok, X509_STORE_CTX* store);

private:
    TOptions Opt;
    ui16 Port{0};
    int ListenSocket{-1};
    std::atomic<bool> Running{false};
    std::thread Worker;
    TSslHolder<SSL_CTX> Ctx{nullptr};
    std::mutex Mutex;
    std::shared_ptr<const TLdapMockResponses> Responses;
};

} // LdapMock
