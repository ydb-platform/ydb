#include "tls.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/listener.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/openssl/io/stream.h>

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

namespace NYT::NCrypto {

using namespace NNet;
using namespace NConcurrency;
using namespace NLogging;

static const TLogger Logger{"Tls"};

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorAttribute GetOpenSSLErrors()
{
    TString errorStr;
    ERR_print_errors_cb([](const char* str, size_t len, void* ctx) {
        TString& out = *reinterpret_cast<TString*>(ctx);
        if (!out.empty()) {
            out += ", ";
        }
        out.append(str, len);
        return 1;
    }, &errorStr);
    return TErrorAttribute("ssl_error", errorStr);
}

constexpr auto TlsBufferSize = 1_MB;

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TSslContextImpl
    : public TRefCounted
{
    SSL_CTX* Ctx = nullptr;

    TSslContextImpl()
    {
        Reset();
    }

    ~TSslContextImpl()
    {
        if (Ctx) {
            SSL_CTX_free(Ctx);
        }
        if (ActiveCtx_) {
            SSL_CTX_free(ActiveCtx_);
        }
    }

    void Reset()
    {
        if (Ctx) {
            SSL_CTX_free(Ctx);
        }
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        Ctx = SSL_CTX_new(TLS_method());
        if (!Ctx) {
            THROW_ERROR_EXCEPTION("SSL_CTX_new(TLS_method()) failed")
                << GetOpenSSLErrors();
        }
        if (SSL_CTX_set_min_proto_version(Ctx, TLS1_2_VERSION) == 0) {
            THROW_ERROR_EXCEPTION("SSL_CTX_set_min_proto_version failed")
                << GetOpenSSLErrors();
        }
        if (SSL_CTX_set_max_proto_version(Ctx, TLS1_2_VERSION) == 0) {
            THROW_ERROR_EXCEPTION("SSL_CTX_set_max_proto_version failed")
                << GetOpenSSLErrors();
        }
#else
        Ctx = SSL_CTX_new(TLSv1_2_method());
        if (!Ctx) {
            THROW_ERROR_EXCEPTION("SSL_CTX_new(TLSv1_2_method()) failed")
                << GetOpenSSLErrors();
        }
#endif
    }

    void Commit()
    {
        SSL_CTX* oldCtx;
        YT_ASSERT(Ctx);
        {
            auto guard = WriterGuard(Lock_);
            oldCtx = ActiveCtx_;
            ActiveCtx_ = Ctx;
            Ctx = nullptr;
        }
        if (oldCtx) {
            SSL_CTX_free(oldCtx);
        }
    }

    SSL* NewSsl()
    {
        auto guard = ReaderGuard(Lock_);
        YT_ASSERT(ActiveCtx_);
        return SSL_new(ActiveCtx_);
    }

    bool IsActive(const SSL* ssl)
    {
        auto guard = ReaderGuard(Lock_);
        return SSL_get_SSL_CTX(ssl) == ActiveCtx_;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    SSL_CTX* ActiveCtx_ = nullptr;
};

DEFINE_REFCOUNTED_TYPE(TSslContextImpl)

////////////////////////////////////////////////////////////////////////////////

struct TTlsBufferTag
{ };

class TTlsConnection
    : public IConnection
{
public:
    TTlsConnection(
        TSslContextImplPtr ctx,
        IPollerPtr poller,
        IConnectionPtr connection)
        : Ctx_(std::move(ctx))
        , Invoker_(CreateSerializedInvoker(poller->GetInvoker(), "crypto_tls_connection"))
        , Underlying_(std::move(connection))
    {
        Ssl_ = Ctx_->NewSsl();
        if (!Ssl_) {
            THROW_ERROR_EXCEPTION("SSL_new failed")
                << GetOpenSSLErrors();
        }

        InputBIO_ = BIO_new(BIO_s_mem());
        YT_VERIFY(InputBIO_);
        // Makes InputBIO_ non-blocking.

        BIO_set_mem_eof_return(InputBIO_, -1);
        OutputBIO_ = BIO_new(BIO_s_mem());
        YT_VERIFY(OutputBIO_);

        SSL_set_bio(Ssl_, InputBIO_, OutputBIO_);

        InputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
        OutputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
    }

    void SetHost(const TString& host)
    {
        SSL_set_tlsext_host_name(Ssl_, host.c_str());
    }

    ~TTlsConnection()
    {
        SSL_free(Ssl_);
    }

    void StartClient()
    {
        SSL_set_connect_state(Ssl_);
        auto sslResult = SSL_do_handshake(Ssl_);
        sslResult = SSL_get_error(Ssl_, sslResult);
        YT_VERIFY(sslResult == SSL_ERROR_WANT_READ);

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeStrong(this)));
    }

    void StartServer()
    {
        SSL_set_accept_state(Ssl_);

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeStrong(this)));
    }

    int GetHandle() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetReadByteCount() const override
    {
        return Underlying_->GetReadByteCount();
    }

    TConnectionStatistics GetReadStatistics() const override
    {
        return Underlying_->GetReadStatistics();
    }

    i64 GetWriteByteCount() const override
    {
        return Underlying_->GetWriteByteCount();
    }

    const TNetworkAddress& LocalAddress() const override
    {
        return Underlying_->LocalAddress();
    }

    const TNetworkAddress& RemoteAddress() const override
    {
        return Underlying_->RemoteAddress();
    }

    TConnectionStatistics GetWriteStatistics() const override
    {
        return Underlying_->GetWriteStatistics();
    }

    void SetReadDeadline(std::optional<TInstant> deadline) override
    {
        Underlying_->SetReadDeadline(deadline);
    }

    void SetWriteDeadline(std::optional<TInstant> deadline) override
    {
        Underlying_->SetWriteDeadline(deadline);
    }

    bool SetNoDelay() override
    {
        return Underlying_->SetNoDelay();
    }

    bool SetKeepAlive() override
    {
        return Underlying_->SetKeepAlive();
    }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        auto promise = NewPromise<size_t>();
        ++ActiveIOCount_;
        Invoker_->Invoke(BIND([this, this_ = MakeStrong(this), promise, buffer] () {
            ReadBuffer_ = buffer;
            ReadPromise_ = promise;

            YT_VERIFY(!ReadActive_);
            ReadActive_ = true;

            DoRun();
        }));
        return promise.ToFuture();
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return WriteV(TSharedRefArray(buffer));
    }

    TFuture<void> WriteV(const TSharedRefArray& buffer) override
    {
        auto promise = NewPromise<void>();
        ++ActiveIOCount_;
        Invoker_->Invoke(BIND([this, this_ = MakeStrong(this), promise, buffer] () {
            WriteBuffer_ = buffer;
            WritePromise_ = promise;

            YT_VERIFY(!WriteActive_);
            WriteActive_ = true;

            DoRun();
        }));
        return promise.ToFuture();
    }

    TFuture<void> CloseRead() override
    {
        // TLS does not support half-open connection state.
        return Close();
    }

    TFuture<void> CloseWrite() override
    {
        // TLS does not support half-open connection state.
        return Close();
    }

    TFuture<void> Close() override
    {
        ++ActiveIOCount_;
        return BIND([this, this_ = MakeStrong(this)] () {
            CloseRequested_ = true;

            DoRun();
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    bool IsIdle() const override
    {
        return ActiveIOCount_ == 0 && !Failed_;
    }

    TFuture<void> Abort() override
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            if (Error_.IsOK()) {
                Error_ = TError("TLS connection aborted");
                CheckError();
            }
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    void SubscribePeerDisconnect(TCallback<void()> cb) override
    {
        return Underlying_->SubscribePeerDisconnect(std::move(cb));
    }

private:
    const TSslContextImplPtr Ctx_;
    const IInvokerPtr Invoker_;
    const IConnectionPtr Underlying_;

    SSL* Ssl_ = nullptr;
    BIO* InputBIO_ = nullptr;
    BIO* OutputBIO_ = nullptr;

    // This counter gets stuck after streams encounters an error.
    std::atomic<int> ActiveIOCount_ = {0};
    std::atomic<bool> Failed_ = {false};

    // FSM
    TError Error_;
    bool HandshakeInProgress_ = true;
    bool CloseRequested_ = false;
    bool ReadActive_ = false;
    bool WriteActive_ = false;
    bool UnderlyingReadActive_ = false;
    bool UnderlyingWriteActive_ = false;

    TSharedMutableRef InputBuffer_;
    TSharedMutableRef OutputBuffer_;

    // Active read
    TSharedMutableRef ReadBuffer_;
    TPromise<size_t> ReadPromise_;

    // Active write
    TSharedRefArray WriteBuffer_;
    TPromise<void> WritePromise_;


    void CheckError()
    {
        if (Error_.IsOK()) {
            return;
        }

        if (ReadActive_) {
            Failed_ = true;
            ReadPromise_.Set(Error_);
            ReadActive_ = false;
        }

        if (WriteActive_) {
            Failed_ = true;
            WritePromise_.Set(Error_);
            WriteActive_ = false;
        }
    }

    template <class T>
    void HandleUnderlyingIOResult(TFuture<T> future, TCallback<void(const TErrorOr<T>&)> handler)
    {
        future.Subscribe(BIND([handler = std::move(handler), invoker = Invoker_] (const TErrorOr<T>& result) {
            GuardedInvoke(
                std::move(invoker),
                BIND(handler, result),
                BIND([=] {
                    TError error("Poller terminated");
                    handler(error);
                }));
        }));
    }

    void MaybeStartUnderlyingIO(bool sslWantRead)
    {
        if (!UnderlyingReadActive_ && sslWantRead) {
            UnderlyingReadActive_ = true;
            HandleUnderlyingIOResult(
                Underlying_->Read(InputBuffer_),
                BIND([this, this_ = MakeStrong(this)] (const TErrorOr<size_t>& result) {
                    UnderlyingReadActive_ = false;
                    if (result.IsOK()) {
                        if (result.Value() > 0) {
                            int count = BIO_write(InputBIO_, InputBuffer_.Begin(), result.Value());
                            YT_VERIFY(count >= 0);
                            YT_VERIFY(static_cast<size_t>(count) == result.Value());
                        } else {
                            BIO_set_mem_eof_return(InputBIO_, 0);
                        }
                    } else {
                        Error_ = result;
                    }

                    DoRun();
                    MaybeStartUnderlyingIO(false);
                }));
        }

        if (!UnderlyingWriteActive_ && BIO_ctrl_pending(OutputBIO_)) {
            UnderlyingWriteActive_ = true;

            int count = BIO_read(OutputBIO_, OutputBuffer_.Begin(), OutputBuffer_.Size());
            YT_VERIFY(count > 0);

            HandleUnderlyingIOResult(
                Underlying_->Write(OutputBuffer_.Slice(0, count)),
                BIND([this, this_ = MakeStrong(this)] (const TError& result) {
                    UnderlyingWriteActive_ = false;
                    if (result.IsOK()) {
                        // Hooray!
                    } else {
                        Error_ = result;
                    }

                    DoRun();
                }));
        }
    }

    void DoRun()
    {
        CheckError();

        if (CloseRequested_ && !HandshakeInProgress_) {
            SSL_shutdown(Ssl_);
            MaybeStartUnderlyingIO(false);
        }

        // NB: We should check for an error here, because Underylying_ might have failed already, and then
        // we will loop on SSL_ERROR_WANT_READ forever.
        if (HandshakeInProgress_ && Error_.IsOK()) {
            int sslResult = SSL_do_handshake(Ssl_);
            if (sslResult == 1) {
                HandshakeInProgress_ = false;
            } else {
                int sslError = SSL_get_error(Ssl_, sslResult);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = TError("SSL_do_handshake failed")
                        << GetOpenSSLErrors();
                    YT_LOG_DEBUG(Error_, "TLS handshake failed");
                    CheckError();
                    return;
                }
            }
        }

        if (HandshakeInProgress_) {
            return;
        }

        // Second condition acts as a poor-man backpressure.
        if (WriteActive_ && !UnderlyingWriteActive_) {
            for (const auto& ref : WriteBuffer_) {
                int count = SSL_write(Ssl_, ref.Begin(), ref.Size());

                if (count < 0) {
                    Error_ = TError("SSL_write failed")
                        << GetOpenSSLErrors();
                    YT_LOG_DEBUG(Error_, "TLS write failed");
                    CheckError();
                    return;
                }

                YT_VERIFY(count == std::ssize(ref));
            }

            MaybeStartUnderlyingIO(false);

            WriteActive_ = false;
            WriteBuffer_.Reset();
            WritePromise_.Set();
            WritePromise_.Reset();
            --ActiveIOCount_;
        }

        if (ReadActive_) {
            int count = SSL_read(Ssl_, ReadBuffer_.Begin(), ReadBuffer_.Size());
            if (count >= 0) {
                ReadActive_ = false;
                ReadPromise_.Set(count);
                ReadPromise_.Reset();
                ReadBuffer_.Reset();
                --ActiveIOCount_;
            } else {
                int sslError = SSL_get_error(Ssl_, count);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = TError("SSL_read failed")
                        << GetOpenSSLErrors();
                    YT_LOG_DEBUG(Error_, "TLS read failed");
                    CheckError();
                    return;
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTlsConnection)

////////////////////////////////////////////////////////////////////////////////

class TTlsDialer
    : public IDialer
{
public:
    TTlsDialer(
        TSslContextImplPtr ctx,
        IDialerPtr dialer,
        IPollerPtr poller)
        : Ctx_(std::move(ctx))
        , Underlying_(std::move(dialer))
        , Poller_(std::move(poller))
    { }

    TFuture<IConnectionPtr> Dial(const TNetworkAddress& remote, TRemoteContextPtr context) override
    {
        return Underlying_->Dial(remote)
            .Apply(BIND([ctx = Ctx_, poller = Poller_, context = std::move(context)](const IConnectionPtr& underlying) -> IConnectionPtr {
                auto connection = New<TTlsConnection>(ctx, poller, underlying);
                if (context != nullptr && context->Host != std::nullopt) {
                    connection->SetHost(*(context->Host));
                }
                connection->StartClient();
                return connection;
        }));
    }

private:
    const TSslContextImplPtr Ctx_;
    const IDialerPtr Underlying_;
    const IPollerPtr Poller_;
};

////////////////////////////////////////////////////////////////////////////////

class TTlsListener
    : public IListener
{
public:
    TTlsListener(
        TSslContextImplPtr ctx,
        IListenerPtr listener,
        IPollerPtr poller)
        : Ctx_(std::move(ctx))
        , Underlying_(std::move(listener))
        , Poller_(std::move(poller))
    { }

    const TNetworkAddress& GetAddress() const override
    {
        return Underlying_->GetAddress();
    }

    TFuture<IConnectionPtr> Accept() override
    {
        return Underlying_->Accept().Apply(
            BIND([ctx = Ctx_, poller = Poller_] (const IConnectionPtr& underlying) -> IConnectionPtr {
                auto connection = New<TTlsConnection>(ctx, poller, underlying);
                connection->StartServer();
                return connection;
            }));
    }

    void Shutdown() override
    {
        Underlying_->Shutdown();
    }

private:
    const TSslContextImplPtr Ctx_;
    const IListenerPtr Underlying_;
    const IPollerPtr Poller_;
};

////////////////////////////////////////////////////////////////////////////////

TSslContext::TSslContext()
    : Impl_(New<TSslContextImpl>())
{ }

void TSslContext::Reset()
{
    Impl_->Reset();
}

void TSslContext::Commit(TInstant time)
{
    CommitTime_ = time;
    Impl_->Commit();
}

TInstant TSslContext::GetCommitTime()
{
    return CommitTime_;
}

void TSslContext::UseBuiltinOpenSslX509Store()
{
    SSL_CTX_set_cert_store(Impl_->Ctx, GetBuiltinOpenSslX509Store().Release());
}

void TSslContext::SetCipherList(const TString& list)
{
    if (SSL_CTX_set_cipher_list(Impl_->Ctx, list.data()) == 0) {
        THROW_ERROR_EXCEPTION("SSL_CTX_set_cipher_list failed")
            << TErrorAttribute("cipher_list", list)
            << GetOpenSSLErrors();
    }
}

void TSslContext::AddCertificateFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_file(Impl_->Ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate_file failed")
            << TErrorAttribute("path", path)
            << GetOpenSSLErrors();
    }
}

void TSslContext::AddCertificateChainFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_chain_file(Impl_->Ctx, path.c_str()) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate_chain_file failed")
            << TErrorAttribute("path", path)
            << GetOpenSSLErrors();
    }
}

void TSslContext::AddPrivateKeyFromFile(const TString& path)
{
    if (SSL_CTX_use_PrivateKey_file(Impl_->Ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_PrivateKey_file failed")
            << TErrorAttribute("path", path)
            << GetOpenSSLErrors();
    }
}

void TSslContext::AddCertificateChain(const TString& certificateChain)
{
    auto bio = BIO_new_mem_buf(certificateChain.c_str(), certificateChain.size());
    YT_VERIFY(bio);
    auto freeBio = Finally([&] {
        BIO_free(bio);
    });

    auto certificateObject = PEM_read_bio_X509_AUX(bio, nullptr, nullptr, nullptr);
    if (!certificateObject) {
        THROW_ERROR_EXCEPTION("PEM_read_bio_X509_AUX failed")
            << GetOpenSSLErrors();
    }
    auto freeCertificate = Finally([&] {
        X509_free(certificateObject);
    });

    if (SSL_CTX_use_certificate(Impl_->Ctx, certificateObject) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate failed")
            << GetOpenSSLErrors();
    }

    SSL_CTX_clear_chain_certs(Impl_->Ctx);
    while (true) {
        auto chainCertificateObject = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
        if (!chainCertificateObject) {
            int err = ERR_peek_last_error();
            if (ERR_GET_LIB(err) == ERR_LIB_PEM && ERR_GET_REASON(err) == PEM_R_NO_START_LINE) {
                ERR_clear_error();
                break;
            }

            THROW_ERROR_EXCEPTION("PEM_read_bio_X509")
                << GetOpenSSLErrors();
        }

        int result = SSL_CTX_add0_chain_cert(Impl_->Ctx, chainCertificateObject);
        if (!result) {
            X509_free(chainCertificateObject);
            THROW_ERROR_EXCEPTION("SSL_CTX_add0_chain_cert")
                << GetOpenSSLErrors();
        }
    }
}

void TSslContext::AddCertificate(const TString& certificate)
{
    auto bio = BIO_new_mem_buf(certificate.c_str(), certificate.size());
    YT_VERIFY(bio);
    auto freeBio = Finally([&] {
        BIO_free(bio);
    });

    auto certificateObject = PEM_read_bio_X509_AUX(bio, nullptr, nullptr, nullptr);
    if (!certificateObject) {
        THROW_ERROR_EXCEPTION("PEM_read_bio_X509_AUX")
            << GetOpenSSLErrors();
    }
    auto freeCertificate = Finally([&] {
        X509_free(certificateObject);
    });

    if (SSL_CTX_use_certificate(Impl_->Ctx, certificateObject) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate failed")
            << GetOpenSSLErrors();
    }
}

void TSslContext::AddPrivateKey(const TString& privateKey)
{
    auto bio = BIO_new_mem_buf(privateKey.c_str(), privateKey.size());
    YT_VERIFY(bio);
    auto freeBio = Finally([&] {
        BIO_free(bio);
    });

    auto privateKeyObject = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    if (!privateKeyObject) {
        THROW_ERROR_EXCEPTION("PEM_read_bio_PrivateKey failed")
            << GetOpenSSLErrors();
    }
    auto freePrivateKey = Finally([&] {
        EVP_PKEY_free(privateKeyObject);
    });

    if (SSL_CTX_use_PrivateKey(Impl_->Ctx, privateKeyObject) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_PrivateKey failed")
            << GetOpenSSLErrors();
    }
}

IDialerPtr TSslContext::CreateDialer(
    const TDialerConfigPtr& config,
    const IPollerPtr& poller,
    const TLogger& logger)
{
    auto dialer = NNet::CreateDialer(config, poller, logger);
    return New<TTlsDialer>(Impl_, dialer, poller);
}

IListenerPtr TSslContext::CreateListener(
    const TNetworkAddress& at,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    auto listener = NNet::CreateListener(at, poller, acceptor);
    return New<TTlsListener>(Impl_, listener, poller);
}

IListenerPtr TSslContext::CreateListener(
    const IListenerPtr& underlying,
    const IPollerPtr& poller)
{
    return New<TTlsListener>(Impl_, underlying, poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
