#pragma once

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/tls1.h>
#include <memory>
#include <util/generic/string.h>

namespace NKikimr::NRawSocket {

struct TSslHelpers {
    struct TSslDeleter {
        void operator()(SSL_CTX* ctx) const noexcept {
            SSL_CTX_free(ctx);
        }

        void operator()(SSL* ssl) const noexcept {
            SSL_free(ssl);
        }

        void operator()(X509* cert) const noexcept {
            X509_free(cert);
        }

        void operator()(EVP_PKEY* pkey) const noexcept {
            EVP_PKEY_free(pkey);
        }

        void operator()(BIO* bio) const noexcept {
            BIO_free(bio);
        }
    };

    template <typename T>
    using TSslHolder = std::unique_ptr<T, TSslDeleter>;

    static TSslHolder<SSL_CTX> CreateSslCtx(const SSL_METHOD* method) {
        TSslHolder<SSL_CTX> ctx(SSL_CTX_new(method));

        if (ctx) {
            SSL_CTX_set_options(ctx.get(), SSL_OP_NO_SSLv2);
            SSL_CTX_set_options(ctx.get(), SSL_OP_NO_SSLv3);
            SSL_CTX_set_options(ctx.get(), SSL_OP_MICROSOFT_SESS_ID_BUG);
            SSL_CTX_set_options(ctx.get(), SSL_OP_NETSCAPE_CHALLENGE_BUG);
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
        res = SSL_CTX_use_certificate_chain_file(ctx.get(), certificate.c_str());
        if (res < 0) {
            // TODO(xenoxeno): more diagnostics?
            return nullptr;
        }
        res = SSL_CTX_use_PrivateKey_file(ctx.get(), key.c_str(), SSL_FILETYPE_PEM);
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
        TSslHolder<X509> cert(PEM_read_bio_X509_AUX(bio.get(), nullptr, nullptr, nullptr));
        if (cert == nullptr) {
            return false;
        }
        if (SSL_CTX_use_certificate(ctx.get(), cert.release()) <= 0) {
            return false;
        }
        SSL_CTX_clear_chain_certs(ctx.get());
        while (true) {
            TSslHolder<X509> ca(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
            if (ca == nullptr) {
                break;
            }
            if (!SSL_CTX_add0_chain_cert(ctx.get(), ca.release())) {
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
        TSslHolder<EVP_PKEY> pkey(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr));
        if (SSL_CTX_use_PrivateKey(ctx.get(), pkey.release()) <= 0) {
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
            SSL_set_bio(ssl.get(), bio, bio);
        }

        return ssl;
    }
};

template<typename ImplType>
struct TSslLayer : TSslHelpers {
    static ImplType* IO(BIO* bio) noexcept {
        return static_cast<ImplType*>(BIO_get_data(bio));
    }

    static int IoWrite(BIO* bio, const char* data, int dlen) noexcept {
        BIO_clear_retry_flags(bio);
        int res = IO(bio)->Send(data, dlen);
        if (-res == EAGAIN) {
            BIO_set_retry_write(bio);
        }
        return res;
    }

    static int IoRead(BIO* bio, char* data, int dlen) noexcept {
        BIO_clear_retry_flags(bio);
        int res = IO(bio)->Recv(data, dlen);
        if (-res == EAGAIN) {
            BIO_set_retry_read(bio);
        }
        return res;
    }

    static int IoPuts(BIO* bio, const char* buf) noexcept {
        Y_UNUSED(bio);
        Y_UNUSED(buf);
        return -2;
    }

    static int IoGets(BIO* bio, char* buf, int size) noexcept {
        Y_UNUSED(bio);
        Y_UNUSED(buf);
        Y_UNUSED(size);
        return -2;
    }

    static long IoCtrl(BIO* bio, int cmd, long larg, void* parg) noexcept {
        Y_UNUSED(larg);
        Y_UNUSED(parg);

        if (cmd == BIO_CTRL_FLUSH) {
            Y_UNUSED(bio);
            //IO(bio)->Flush();
            return 1;
        }

        return -2;
    }

    static int IoCreate(BIO* bio) noexcept {
        BIO_set_data(bio, nullptr);
        BIO_set_init(bio, 1);
        return 1;
    }

    static int IoDestroy(BIO* bio) noexcept {
        BIO_set_data(bio, nullptr);
        BIO_set_init(bio, 0);
        return 1;
    }

    static BIO_METHOD* CreateIoMethod() {
        BIO_METHOD* method = BIO_meth_new(BIO_get_new_index() | BIO_TYPE_SOURCE_SINK, "TSslLayer");
        BIO_meth_set_write(method, IoWrite);
        BIO_meth_set_read(method, IoRead);
        BIO_meth_set_puts(method, IoPuts);
        BIO_meth_set_gets(method, IoGets);
        BIO_meth_set_ctrl(method, IoCtrl);
        BIO_meth_set_create(method, IoCreate);
        BIO_meth_set_destroy(method, IoDestroy);
        return method;
    }

    static BIO_METHOD* IoMethod() {
        static BIO_METHOD* method = CreateIoMethod();
        return method;
    }
};

} // namespace NKikimr::NRawSocket
