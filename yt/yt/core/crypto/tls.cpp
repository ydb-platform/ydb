#include "tls.h"
#include "config.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/poller.h>

#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/dialer.h>
#include <yt/yt/core/net/listener.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>  // For EErrorCode::SslError

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/openssl/io/stream.h>

#include <util/string/hex.h>

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

namespace NYT::NCrypto {

using namespace NNet;
using namespace NConcurrency;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Tls");

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto TlsBufferSize = 1_MB;

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Get all saved SSL errors for current thread.
TError GetLastSslError(TString message)
{
    auto lastSslError = ERR_peek_last_error();
    TStringBuilder errorStr;
    ERR_print_errors_cb([] (const char* str, size_t len, void* ctx) {
        auto& out = *reinterpret_cast<TStringBuilder*>(ctx);
        if (!out.GetLength()) {
            out.AppendString(", ");
        }
        out.AppendString(TStringBuf(str, len));
        return 1;
    }, &errorStr);
    return TError(NRpc::EErrorCode::SslError, std::move(message), TError::DisableFormat)
        << TErrorAttribute("ssl_last_error_code", lastSslError)
        << TErrorAttribute("ssl_error", errorStr.Flush());
}

////////////////////////////////////////////////////////////////////////////////

void TSslDeleter::operator()(BIO* bio) const noexcept
{
    BIO_free(bio);
}

void TSslDeleter::operator()(X509* x509) const noexcept
{
    X509_free(x509);
}

void TSslDeleter::operator()(EVP_PKEY* key) const noexcept
{
    EVP_PKEY_free(key);
}

void TSslDeleter::operator()(SSL_CTX* ctx) const noexcept
{
    SSL_CTX_free(ctx);
}

void TSslDeleter::operator()(SSL* ssl) const noexcept
{
    SSL_free(ssl);
}

////////////////////////////////////////////////////////////////////////////////

TString GetFingerprintSHA256(const TX509Ptr& certificate)
{
    auto md_type = EVP_sha256();
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;
    if (!X509_digest(certificate.get(), md_type, md, &md_len)) {
        THROW_ERROR GetLastSslError("X509_digest() failed");
    }
    return HexEncode(md, md_len);
}

////////////////////////////////////////////////////////////////////////////////

struct TSslContextImpl
    : public TRefCounted
{
    TSslContextImpl()
    {
        Reset();
    }

    ~TSslContextImpl()
    {
    }

    SSL_CTX* GetContext() const
    {
        return Context_.get();
    }

    void Reset()
    {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
        Context_.reset(SSL_CTX_new(TLS_method()));
        if (!Context_) {
            THROW_ERROR GetLastSslError("SSL_CTX_new(TLS_method()) failed");
        }
        if (SSL_CTX_set_min_proto_version(GetContext(), TLS1_2_VERSION) == 0) {
            THROW_ERROR GetLastSslError("SSL_CTX_set_min_proto_version failed");
        }
        if (SSL_CTX_set_max_proto_version(GetContext(), TLS1_3_VERSION) == 0) {
            THROW_ERROR GetLastSslError("SSL_CTX_set_max_proto_version failed");
        }
#else
        Context.reset(SSL_CTX_new(TLSv1_2_method()));
        if (!Context) {
             THROW_ERROR GetLastSslError("SSL_CTX_new(TLSv1_2_method()) failed");
        }
#endif
    }

    void Commit(TInstant time)
    {
        TSslCtxPtr oldContext;
        YT_ASSERT(Context_);
        {
            auto guard = WriterGuard(Lock_);
            oldContext.swap(ActiveContext_);
            ActiveContext_.swap(Context_);
            CommitTime_ = time;
        }
    }

    TInstant GetCommitTime() const
    {
        auto guard = ReaderGuard(Lock_);
        return CommitTime_;
    }

    TSslPtr NewSsl()
    {
        SSL* ssl;
        {
            auto guard = ReaderGuard(Lock_);
            YT_ASSERT(ActiveContext_);
            ssl = SSL_new(ActiveContext_.get());
        }
        if (!ssl) {
            THROW_ERROR GetLastSslError("SSL_new failed");
        }
        return TSslPtr(ssl);
    }

    bool IsActive(const SSL* ssl)
    {
        auto guard = ReaderGuard(Lock_);
        return SSL_get_SSL_CTX(ssl) == ActiveContext_.get();
    }

    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(InsecureSkipVerify, DefaultInsecureSkipVerify);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TSslCtxPtr Context_;
    TSslCtxPtr ActiveContext_;
    TInstant CommitTime_;
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
        TSslContextImplPtr context,
        IPollerPtr poller,
        IConnectionPtr connection)
        : Context_(std::move(context))
        , Invoker_(CreateSerializedInvoker(poller->GetInvoker(), "crypto_tls_connection"))
        , Underlying_(std::move(connection))
    {
        Ssl_ = Context_->NewSsl();

        InputBIO_ = BIO_new(BIO_s_mem());
        YT_VERIFY(InputBIO_);
        // Makes InputBIO_ non-blocking.

        BIO_set_mem_eof_return(InputBIO_, -1);
        OutputBIO_ = BIO_new(BIO_s_mem());
        YT_VERIFY(OutputBIO_);

        SSL_set_bio(Ssl_.get(), InputBIO_, OutputBIO_);

        InputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
        OutputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
    }

    void SetHost(const TString& host)
    {
        // Verify hostname in server certificate.
        SSL_set_hostflags(Ssl_.get(), X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        SSL_set1_host(Ssl_.get(), host.c_str());

        SSL_set_tlsext_host_name(Ssl_.get(), host.c_str());
    }

    ~TTlsConnection()
    {
    }

    void StartClient(bool insecureSkipVerify)
    {
        YT_LOG_WARNING_IF(insecureSkipVerify, "Started insecure TLS client connection");
        if (!insecureSkipVerify) {
            // Require and verify server certificate.
            SSL_set_verify(Ssl_.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
        }

        SSL_set_connect_state(Ssl_.get());
        auto sslResult = SSL_do_handshake(Ssl_.get());
        sslResult = SSL_get_error(Ssl_.get(), sslResult);
        YT_VERIFY(sslResult == SSL_ERROR_WANT_READ);

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeWeak(this)));
    }

    void StartServer()
    {
        SSL_set_accept_state(Ssl_.get());

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeWeak(this)));
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

    TConnectionId GetId() const override
    {
        return Underlying_->GetId();
    }

    const TNetworkAddress& GetLocalAddress() const override
    {
        return Underlying_->GetLocalAddress();
    }

    const TNetworkAddress& GetRemoteAddress() const override
    {
        return Underlying_->GetRemoteAddress();
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
        Invoker_->Invoke(BIND([this, weakThis = MakeWeak(this), promise, buffer] {
            auto thisLocked = weakThis.Lock();
            if (!thisLocked) {
                return;
            }
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
        Invoker_->Invoke(BIND([this, weakThis = MakeWeak(this), promise, buffer] {
            auto thisLocked = weakThis.Lock();
            if (!thisLocked) {
                return;
            }
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
        return BIND([this, this_ = MakeStrong(this)] {
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

    bool IsReusable() const override
    {
        return IsIdle() && Underlying_->IsReusable();
    }

    TFuture<void> Abort() override
    {
        return BIND([this, this_ = MakeStrong(this)] {
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
    const TSslContextImplPtr Context_;
    const IInvokerPtr Invoker_;
    const IConnectionPtr Underlying_;

    TSslPtr Ssl_;
    BIO* InputBIO_ = nullptr;
    BIO* OutputBIO_ = nullptr;

    // This counter gets stuck after streams encounters an error.
    std::atomic<int> ActiveIOCount_ = 0;
    std::atomic<bool> Failed_ = false;

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
                BIND([this, weakThis = MakeWeak(this)] (const TErrorOr<size_t>& result) {
                    auto thisLocked = weakThis.Lock();
                    if (!thisLocked) {
                        return;
                    }
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
                BIND([this, weakThis = MakeWeak(this)] (const TError& result) {
                    auto thisLocked = weakThis.Lock();
                    if (!thisLocked) {
                        return;
                    }
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
            SSL_shutdown(Ssl_.get());
            MaybeStartUnderlyingIO(false);
        }

        // NB: We should check for an error here, because Underylying_ might have failed already, and then
        // we will loop on SSL_ERROR_WANT_READ forever.
        if (HandshakeInProgress_ && Error_.IsOK()) {
            int sslResult = SSL_do_handshake(Ssl_.get());
            if (sslResult == 1) {
                HandshakeInProgress_ = false;
            } else {
                int sslError = SSL_get_error(Ssl_.get(), sslResult);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = GetLastSslError("SSL_do_handshake failed");
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
                int count = SSL_write(Ssl_.get(), ref.Begin(), ref.Size());

                if (count < 0) {
                    Error_ = GetLastSslError("SSL_write failed");
                    YT_LOG_DEBUG(Error_, "TLS write failed");
                    CheckError();
                    return;
                }

                YT_VERIFY(count == std::ssize(ref));
            }

            MaybeStartUnderlyingIO(false);

            WriteActive_ = false;
            WriteBuffer_.Reset();
            --ActiveIOCount_;
            WritePromise_.Set();
            WritePromise_.Reset();
        }

        if (ReadActive_) {
            int count = SSL_read(Ssl_.get(), ReadBuffer_.Begin(), ReadBuffer_.Size());
            if (count >= 0) {
                ReadActive_ = false;
                ReadBuffer_.Reset();
                --ActiveIOCount_;
                ReadPromise_.Set(count);
                ReadPromise_.Reset();
            } else {
                int sslError = SSL_get_error(Ssl_.get(), count);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = GetLastSslError("SSL_read failed");
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
        : Context_(std::move(ctx))
        , Underlying_(std::move(dialer))
        , Poller_(std::move(poller))
    { }

    TFuture<IConnectionPtr> Dial(const TNetworkAddress& remoteAddress, TDialerContextPtr context) override
    {
        return Underlying_->Dial(remoteAddress).Apply(BIND(
            [
                ctx = Context_,
                poller = Poller_,
                context = std::move(context),
                insecureSkipVerify = Context_->IsInsecureSkipVerify()
            ] (const IConnectionPtr& underlying) -> IConnectionPtr {
                auto connection = New<TTlsConnection>(ctx, poller, underlying);
                if (context && context->Host) {
                    connection->SetHost(*context->Host);
                }
                connection->StartClient(insecureSkipVerify);
                return connection;
            }));
    }

private:
    const TSslContextImplPtr Context_;
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
    Impl_->Commit(time);
}

TInstant TSslContext::GetCommitTime() const
{
    return Impl_->GetCommitTime();
}

void TSslContext::ApplyConfig(const TSslContextConfigPtr& config, TCertificatePathResolver pathResolver)
{
    if (!config) {
        return;
    }

    const auto& commands = config->SslConfigurationCommands;
    if (!commands.empty()) {
        std::unique_ptr<SSL_CONF_CTX, decltype(&SSL_CONF_CTX_free)> configContext(SSL_CONF_CTX_new(), SSL_CONF_CTX_free);
        if (!configContext) {
            THROW_ERROR GetLastSslError("SSL_CONF_CTX_new failed");
        }

        SSL_CONF_CTX_set_flags(configContext.get(),
            SSL_CONF_FLAG_FILE |
            SSL_CONF_FLAG_CLIENT |
            SSL_CONF_FLAG_SERVER |
            SSL_CONF_FLAG_CERTIFICATE |
            SSL_CONF_FLAG_SHOW_ERRORS);

        SSL_CONF_CTX_set_ssl_ctx(configContext.get(), Impl_->GetContext());

        for (const auto& command: commands) {
            if (SSL_CONF_cmd(configContext.get(), command->Name.c_str(), command->Value.c_str()) <= 0) {
                THROW_ERROR GetLastSslError("SSL_CONF_cmd failed")
                    << TErrorAttribute("ssl_config_command_name", command->Name);
            }
        }

        if (SSL_CONF_CTX_finish(configContext.get()) != 1) {
            THROW_ERROR GetLastSslError("SSL_CONF_CTX_finish failed");
        }
    }

    AddCertificateAuthority(config->CertificateAuthority, pathResolver);
    AddCertificateChain(config->CertificateChain, pathResolver);
    AddPrivateKey(config->PrivateKey, pathResolver);
    Impl_->SetInsecureSkipVerify(config->InsecureSkipVerify);
}

void TSslContext::UseBuiltinOpenSslX509Store()
{
    SSL_CTX_set_cert_store(Impl_->GetContext(), GetBuiltinOpenSslX509Store().Release());
}

void TSslContext::SetCipherList(const TString& list)
{
    if (SSL_CTX_set_cipher_list(Impl_->GetContext(), list.data()) == 0) {
        THROW_ERROR GetLastSslError("SSL_CTX_set_cipher_list failed")
            << TErrorAttribute("cipher_list", list);
    }
}

void TSslContext::AddCertificateAuthorityFromFile(const TString& path)
{
    if (SSL_CTX_load_verify_locations(Impl_->GetContext(), path.c_str(), nullptr) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_load_verify_locations failed")
            << TErrorAttribute("path", path);
    }
}

void TSslContext::AddCertificateFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_file(Impl_->GetContext(), path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_certificate_file failed")
            << TErrorAttribute("path", path);
    }
}

void TSslContext::AddCertificateChainFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_chain_file(Impl_->GetContext(), path.c_str()) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_certificate_chain_file failed")
            << TErrorAttribute("path", path);
    }
}

void TSslContext::AddPrivateKeyFromFile(const TString& path)
{
    if (SSL_CTX_use_PrivateKey_file(Impl_->GetContext(), path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_PrivateKey_file failed")
            << TErrorAttribute("path", path);
    }
}

void TSslContext::AddCertificateChain(const TString& certificateChain)
{
    TBioPtr bio(BIO_new_mem_buf(certificateChain.c_str(), certificateChain.size()));
    if (!bio) {
        THROW_ERROR GetLastSslError("Failed to allocate memory buffer for certificate chain");
    }

    TX509Ptr certificateObject(PEM_read_bio_X509_AUX(bio.get(), nullptr, nullptr, nullptr));
    if (!certificateObject) {
        THROW_ERROR GetLastSslError("PEM_read_bio_X509_AUX failed");
    }

    if (SSL_CTX_use_certificate(Impl_->GetContext(), certificateObject.get()) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_certificate failed");
    }

    SSL_CTX_clear_chain_certs(Impl_->GetContext());
    while (true) {
        TX509Ptr chainCertificateObject(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
        if (!chainCertificateObject) {
            int err = ERR_peek_last_error();
            if (ERR_GET_LIB(err) == ERR_LIB_PEM && ERR_GET_REASON(err) == PEM_R_NO_START_LINE) {
                ERR_clear_error();
                break;
            }

            THROW_ERROR GetLastSslError("PEM_read_bio_X509");
        }

        if (SSL_CTX_add0_chain_cert(Impl_->GetContext(), chainCertificateObject.get()) != 1) {
            THROW_ERROR GetLastSslError("SSL_CTX_add0_chain_cert");
        }

        // Do not X509_free() if certificate was added by SSL_CTX_add0_chain_cert().
        Y_UNUSED(chainCertificateObject.release());
    }
}

void TSslContext::AddCertificateAuthority(const TString& ca)
{
    TBioPtr bio(BIO_new_mem_buf(ca.data(), ca.size()));
    if (!bio) {
        THROW_ERROR GetLastSslError("Failed to allocate memory buffer for CA certificate");
    }

    auto store = SSL_CTX_get_cert_store(Impl_->GetContext());

    // Load certificate chain.
    TX509Ptr cert;
    while (cert.reset(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr)), cert) {
        if (X509_STORE_add_cert(store, cert.get()) != 1) {
             THROW_ERROR GetLastSslError("Failed to add cert to store");
        }
    }
}

void TSslContext::AddCertificate(const TString& certificate)
{
    TBioPtr bio(BIO_new_mem_buf(certificate.c_str(), certificate.size()));
    if (!bio) {
        THROW_ERROR GetLastSslError("Failed to allocate memory buffer for certificate");
    }

    TX509Ptr certificateObject(PEM_read_bio_X509_AUX(bio.get(), nullptr, nullptr, nullptr));
    if (!certificateObject) {
        THROW_ERROR GetLastSslError("PEM_read_bio_X509_AUX");
    }

    if (SSL_CTX_use_certificate(Impl_->GetContext(), certificateObject.get()) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_certificate failed");
    }
}

void TSslContext::AddPrivateKey(const TString& privateKey)
{
    TBioPtr bio(BIO_new_mem_buf(privateKey.c_str(), privateKey.size()));
    if (!bio) {
        THROW_ERROR GetLastSslError("Failed to allocate memory buffer for private key");
    }

    TEvpPKeyPtr privateKeyObject(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr));
    if (!privateKeyObject) {
        THROW_ERROR GetLastSslError("PEM_read_bio_PrivateKey failed");
    }

    if (SSL_CTX_use_PrivateKey(Impl_->GetContext(), privateKeyObject.get()) != 1) {
        THROW_ERROR GetLastSslError("SSL_CTX_use_PrivateKey failed");
    }
}

void TSslContext::AddCertificateAuthority(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver)
{
    if (pem) {
        if (pem->FileName) {
            auto filePath = resolver ? resolver(*pem->FileName) : *pem->FileName;
            AddCertificateAuthorityFromFile(filePath);
        } else {
            AddCertificateAuthority(pem->LoadBlob());
        }
    }
}

void TSslContext::AddCertificate(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver)
{
    if (pem) {
        if (pem->FileName) {
            auto filePath = resolver ? resolver(*pem->FileName) : *pem->FileName;
            AddCertificateFromFile(filePath);
        } else {
            AddCertificate(pem->LoadBlob());
        }
    }
}

void TSslContext::AddCertificateChain(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver)
{
    if (pem) {
        if (pem->FileName) {
            auto filePath = resolver ? resolver(*pem->FileName) : *pem->FileName;
            AddCertificateChainFromFile(filePath);
        } else {
            AddCertificateChain(pem->LoadBlob());
        }
    }
}

void TSslContext::AddPrivateKey(const TPemBlobConfigPtr& pem, TCertificatePathResolver resolver)
{
    if (pem) {
        if (pem->FileName) {
            auto filePath = resolver ? resolver(*pem->FileName) : *pem->FileName;
            AddPrivateKeyFromFile(filePath);
        } else {
            AddPrivateKey(pem->LoadBlob());
        }
    }
}

TSslPtr TSslContext::NewSsl()
{
    return Impl_->NewSsl();
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
