#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/tls1.h>

namespace NHttp {

struct TSslHelpers {
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

    static TSslHolder<SSL_CTX> CreateSslCtx(const SSL_METHOD* method) {
        TSslHolder<SSL_CTX> ctx(SSL_CTX_new(method));

        if (ctx) {
            SSL_CTX_set_options(ctx.Get(), SSL_OP_NO_SSLv2);
            SSL_CTX_set_options(ctx.Get(), SSL_OP_NO_SSLv3);
            SSL_CTX_set_options(ctx.Get(), SSL_OP_NO_TLSv1);
            SSL_CTX_set_options(ctx.Get(), SSL_OP_NO_TLSv1_1);
            SSL_CTX_set_options(ctx.Get(), SSL_OP_MICROSOFT_SESS_ID_BUG);
            SSL_CTX_set_options(ctx.Get(), SSL_OP_NETSCAPE_CHALLENGE_BUG);
        }

        return ctx;
    }

    static TSslHolder<SSL_CTX> CreateClientContext() {
        return CreateSslCtx(SSLv23_client_method());
    }

    static TSslHolder<SSL_CTX> CreateServerContext(const TString& certificate, const TString& key) {
        TSslHolder<SSL_CTX> ctx = CreateSslCtx(SSLv23_server_method());
        SSL_CTX_set_ecdh_auto(ctx.Get(), 1);
        int res;
        res = SSL_CTX_use_certificate_chain_file(ctx.Get(), certificate.c_str());
        if (res < 0) {
            // TODO(xenoxeno): more diagnostics?
            return nullptr;
        }
        res = SSL_CTX_use_PrivateKey_file(ctx.Get(), key.c_str(), SSL_FILETYPE_PEM);
        if (res < 0) {
            // TODO(xenoxeno): more diagnostics?
            return nullptr;
        }
        return ctx;
    }

    static bool LoadX509Chain(TSslHolder<SSL_CTX>& ctx, const TString& pem) {
        TSslHolder<BIO> bio(BIO_new_mem_buf(pem.c_str(), pem.size()));
        if (bio == nullptr) {
            return false;
        }
        TSslHolder<X509> cert(PEM_read_bio_X509_AUX(bio.Get(), nullptr, nullptr, nullptr));
        if (cert == nullptr) {
            return false;
        }
        if (SSL_CTX_use_certificate(ctx.Get(), cert.Release()) <= 0) {
            return false;
        }
        SSL_CTX_clear_chain_certs(ctx.Get());
        while (true) {
            TSslHolder<X509> ca(PEM_read_bio_X509(bio.Get(), nullptr, nullptr, nullptr));
            if (ca == nullptr) {
                break;
            }
            if (!SSL_CTX_add0_chain_cert(ctx.Get(), ca.Release())) {
                return false;
            }
        }
        return true;
    }

    static bool LoadPrivateKey(TSslHolder<SSL_CTX>& ctx, const TString& pem) {
        TSslHolder<BIO> bio(BIO_new_mem_buf(pem.c_str(), pem.size()));
        if (bio == nullptr) {
            return false;
        }
        TSslHolder<EVP_PKEY> pkey(PEM_read_bio_PrivateKey(bio.Get(), nullptr, nullptr, nullptr));
        if (SSL_CTX_use_PrivateKey(ctx.Get(), pkey.Release()) <= 0) {
            return false;
        }
        return true;
    }

    static TSslHolder<SSL_CTX> CreateServerContext(const TString& pem) {
        TSslHolder<SSL_CTX> ctx = CreateSslCtx(SSLv23_server_method());
        SSL_CTX_set_ecdh_auto(ctx.Get(), 1);
        if (!LoadX509Chain(ctx, pem)) {
            return nullptr;
        }
        if (!LoadPrivateKey(ctx, pem)) {
            return nullptr;
        }
        return ctx;
    }

    static TSslHolder<SSL> ConstructSsl(SSL_CTX* ctx, BIO* bio) {
        TSslHolder<SSL> ssl(SSL_new(ctx));

        if (ssl) {
            BIO_up_ref(bio); // SSL_set_bio consumes only one reference if rbio and wbio are the same
            SSL_set_bio(ssl.Get(), bio, bio);
        }

        return ssl;
    }
};

}
