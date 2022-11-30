#pragma once

#include <library/cpp/http/fetch/sockhandler.h>
#include <library/cpp/openssl/method/io.h>

#include <util/generic/maybe.h>
#include <util/network/ip.h>
#include <util/network/socket.h>
#include <util/system/yassert.h>

#include <contrib/libs/openssl/include/openssl/ssl.h>
#include <cerrno>
#include <util/generic/noncopyable.h>

namespace NHttpFetcher {
    class TSslSocketBase {
    public:
        enum ECertErrors {
            SSL_CERT_VALIDATION_FAILED = 0x01,
            SSL_CERT_HOSTNAME_MISMATCH = 0x02,
        };

        struct TSessIdDestructor {
            static void Destroy(SSL_SESSION* session) {
                SSL_SESSION_free(session);
            }
        };

        static void LoadCaCerts(const TString& caFile, const TString& caPath);

    protected:
        class TSocketCtx {
        public:
            ui16 SslError;
            ui16 CertErrors;
            const char* Host;
            size_t HostLen;

        public:
            TSocketCtx()
                : SslError(0)
                , CertErrors(0)
                , Host(nullptr)
                , HostLen(0)
            {
            }

            void AllocBuffers() {
            }

            void FreeBuffers() {
            }
        };

    protected:
        static SSL_CTX* SslCtx();

        static TString GetSslErrors();

        static bool CheckCertHostname(X509* cert, TStringBuf hostname);

    private:
        class TSslCtx;

    public:
        class TFakeLogger {
        public:
            static void Write(const char* /*format*/, ...) {
            }
        };
    };

    namespace NPrivate {
        template <typename TSocketHandler>
        class TSocketHandlerIO : public NOpenSSL::TAbstractIO {
        private:
            TSocketHandler& Sock;

        public:
            TSocketHandlerIO(TSocketHandler& sock)
                : Sock(sock)
            {
            }

            int Write(const char* data, size_t dlen, size_t* written) override {
                int ret = Sock.send(data, dlen);

                if (ret <= 0) {
                    *written = 0;
                    return ret;
                }

                *written = dlen; // send returns only 0 or 1
                return 1;
            }

            int Read(char* data, size_t dlen, size_t* readbytes) override {
                ssize_t ret = Sock.read(data, dlen);

                if (ret <= 0) {
                    *readbytes = 0;
                    return ret;
                }

                *readbytes = ret;
                return 1;
            }

            int Puts(const char* buf) override {
                TStringBuf sb(buf);
                size_t written = 0;

                int ret = Write(sb.data(), sb.size(), &written);

                if (ret <= 0) {
                    return ret;
                }

                return written;
            }

            int Gets(char* buf, int size) override {
                Y_UNUSED(buf);
                Y_UNUSED(size);
                return -1;
            }

            void Flush() override {
            }
        };

        class TDestroyBio {
        public:
            static void Destroy(BIO* bio) {
                if (BIO_free(bio) != 1) {
                    Y_FAIL("BIO_free failed");
                }
            }
        };

        class TDestroyCert {
        public:
            static void Destroy(X509* cert) {
                X509_free(cert);
            }
        };

        class TErrnoRestore {
        private:
            int Value;

        public:
            TErrnoRestore()
                : Value(errno)
            {
            }

            ~TErrnoRestore() {
                errno = Value;
            }
        };
    }

    template <class TSocketHandler, class TErrorLogger = TSslSocketBase::TFakeLogger>
    class TSslSocketHandler
       : public TSslSocketBase,
          protected TSocketHandler,
          TNonCopyable {
    private:
    public:
        struct TSocketCtx: public TSslSocketBase::TSocketCtx {
            THolder<SSL_SESSION, TSessIdDestructor> CachedSession;
            TMaybe<char> PeekedByte;
        };

        TSslSocketHandler()
            : TSocketHandler()
        {
        }

        virtual ~TSslSocketHandler() {
            Disconnect();
        }

        int Good() const {
            return TSocketHandler::Good();
        }
        bool HasSsl() const {
            return !!SslBio;
        }

        // set reconnect "true" to try to recover from cached session id
        int Connect(TSocketCtx* ctx, const TAddrList& addrs, TDuration timeout, bool isHttps, bool reconnect = false);

        // for debug "file" socket
        bool open(const char* name) {
            Disconnect();
            return TSocketHandler::open(name);
        }

        void Disconnect(TSocketCtx* ctx = nullptr) { // if ctx is non-NULL, cache session id in it.
            if (!!SslBio) {
                if (ctx) {
                    SSL* ssl;
                    if (!BIO_get_ssl(SslBio.Get(), &ssl)) {
                        Y_FAIL("BIO_get_ssl failed");
                    }
                    SSL_SESSION* sess = SSL_get1_session(ssl);
                    if (!sess) {
                        TErrorLogger::Write("TSslSocketHandler::Disconnect: failed to create session id for host %s\n", TString(ctx->Host, ctx->HostLen).data());
                    }
                    ctx->CachedSession.Reset(sess);
                }
                BIO_ssl_shutdown(SslBio.Get());
                SslBio.Destroy();
                SocketBio.Destroy();
            }
            TSocketHandler::Disconnect();
        }

        void shutdown() {
            TSocketHandler::shutdown();
        }

        ssize_t send(TSocketCtx* ctx, const void* message, size_t messlen) {
            Y_ASSERT(TSocketHandler::Good());
            if (!SslBio)
                return TSocketHandler::send(message, messlen);
            int rc = SslWrite(ctx, static_cast<const char*>(message), (int)messlen);
            if (rc < 0) {
                NPrivate::TErrnoRestore errnoRest;
                Disconnect();
                return false;
            }
            Y_ASSERT((size_t)rc == messlen);
            return true;
        }

        bool peek(TSocketCtx* ctx) {
            if (!SslBio)
                return TSocketHandler::peek();
            int rc;
            rc = SslRead(ctx, nullptr, 0);
            if (rc < 0) {
                NPrivate::TErrnoRestore errnoRest;
                Disconnect();
                return false;
            } else {
                return (rc > 0);
            }
        }

        ssize_t read(TSocketCtx* ctx, void* message, size_t messlen) {
            if (!SslBio)
                return TSocketHandler::read(message, messlen);
            int rc;
            if (!messlen)
                return 0;
            rc = SslRead(ctx, static_cast<char*>(message), (int)messlen);
            if (rc < 0) {
                NPrivate::TErrnoRestore errnoRest;
                Disconnect();
            }
            return rc;
        }

    private:
        int SslRead(TSocketCtx* ctx, char* buf, int buflen);
        int SslWrite(TSocketCtx* ctx, const char* buf, int len);

        THolder<BIO, NPrivate::TDestroyBio> SslBio;
        THolder<NPrivate::TSocketHandlerIO<TSocketHandler>> SocketBio;
    };

    template <typename TSocketHandler, typename TErrorLogger>
    int TSslSocketHandler<TSocketHandler, TErrorLogger>::Connect(TSocketCtx* ctx, const TAddrList& addrs, TDuration timeout, bool isHttps, bool reconnect) {
        ctx->SslError = 0;
        ctx->CertErrors = 0;
        Disconnect();
        int res = TSocketHandler::Connect(addrs, timeout);
        if (!isHttps || res != 0) {
            ctx->CachedSession.Destroy();
            return res;
        }

        // create ssl session
        SslBio.Reset(BIO_new_ssl(SslCtx(), /*client =*/1));
        if (!SslBio) {
            ctx->SslError = 1;
            NPrivate::TErrnoRestore errnoRest;
            Disconnect();
            return -1;
        }

        SSL* ssl;
        if (BIO_get_ssl(SslBio.Get(), &ssl) != 1) {
            Y_FAIL("BIO_get_ssl failed");
        }

        SSL_set_ex_data(ssl, /*index =*/0, ctx);

        if (reconnect) {
            SSL_set_session(ssl, ctx->CachedSession.Get());
        }

        TString host(ctx->Host, ctx->HostLen);
        host = host.substr(0, host.rfind(':'));
        if (SSL_set_tlsext_host_name(ssl, host.data()) != 1) {
            ctx->SslError = 1;
            return -1;
        }

        SocketBio.Reset(new NPrivate::TSocketHandlerIO<TSocketHandler>(*this));

        BIO_push(SslBio.Get(), *SocketBio);

        ctx->CachedSession.Destroy();

        // now it's time to perform handshake
        if (BIO_do_handshake(SslBio.Get()) != 1) {
            long verify_err = SSL_get_verify_result(ssl);
            if (verify_err != X509_V_OK) {
                // It failed because the certificate chain validation failed
                TErrorLogger::Write("SSL Handshake failed: %s", GetSslErrors().data());
                ctx->CertErrors |= SSL_CERT_VALIDATION_FAILED;
            }
            ctx->SslError = 1;
            return -1;
        }

        THolder<X509, NPrivate::TDestroyCert> cert(SSL_get_peer_certificate(ssl));
        if (!cert) {
            // The handshake was successful although the server did not provide a certificate
            // Most likely using an insecure anonymous cipher suite... get out!
            TErrorLogger::Write("No SSL certificate");
            ctx->SslError = 1;
            return -1;
        }

        if (!CheckCertHostname(cert.Get(), host)) {
            ctx->SslError = 1;
            ctx->CertErrors |= SSL_CERT_HOSTNAME_MISMATCH;
            return -1;
        }

        return 0;
    }

    template <typename TSocketHandler, typename TErrorLogger>
    int TSslSocketHandler<TSocketHandler, TErrorLogger>::SslRead(TSocketCtx* ctx, char* buf, int buflen) {
        Y_ASSERT(SslCtx());

        if (!SslBio || buflen < 0)
            return -1;

        if (!buf) {
            // peek
            char byte;
            int res = BIO_read(SslBio.Get(), &byte, 1);
            if (res < 0) {
                ctx->SslError = 1;
                return -1;
            }
            if (res == 0) {
                return 0;
            }
            Y_VERIFY(res == 1);
            ctx->PeekedByte = byte;
            return 1;
        }

        if (buflen) {
            size_t read = 0;
            if (!!ctx->PeekedByte) {
                *buf = *(ctx->PeekedByte);
                ++buf;
                --buflen;
                ctx->PeekedByte.Clear();
                read = 1;
            }
            int res = BIO_read(SslBio.Get(), buf, buflen);
            if (res < 0) {
                ctx->SslError = 1;
                return -1;
            }
            read += res;
            return read;
        }

        return 0;
    }

    template <typename TSocketHandler, typename TErrorLogger>
    int TSslSocketHandler<TSocketHandler, TErrorLogger>::SslWrite(TSocketCtx* ctx, const char* buf, int len) {
        if (len <= 0)
            return len ? -1 : 0;

        size_t remaining = len;
        while (remaining) {
            int res = BIO_write(SslBio.Get(), buf, len);
            if (res < 0) {
                ctx->SslError = 1;
                return -1;
            }
            remaining -= res;
            buf += res;
        }
        if (BIO_flush(SslBio.Get()) != 1) {
            ctx->SslError = 1;
            return -1;
        }
        return len;
    }
}
