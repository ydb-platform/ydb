#include "interconnect_stream.h"
#include "interconnect_common.h"
#include "logging.h"
#include "poller_actor.h"
#include <library/cpp/openssl/init/init.h>
#include <util/network/socket.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/pem.h>

#if defined(_win_)
#include <util/system/file.h>
#define SOCK_NONBLOCK 0
#elif defined(_darwin_)
#define SOCK_NONBLOCK 0
#else
#include <sys/un.h>
#include <sys/stat.h>
#endif //_win_

#if !defined(_win_)
#include <sys/ioctl.h>
#endif

#include <cerrno>

namespace NInterconnect {
    namespace {
        inline int
        LastSocketError() {
#if defined(_win_)
            return WSAGetLastError();
#else
            return errno;
#endif
        }
    }

    TSocket::TSocket(SOCKET fd)
        : Descriptor(fd)
    {
    }

    TSocket::~TSocket() {
        if (Descriptor == INVALID_SOCKET) {
            return;
        }

        auto const result = ::closesocket(Descriptor);
        if (result == 0)
            return;
        switch (LastSocketError()) {
            case EBADF:
                Y_ABORT("Close bad descriptor");
            case EINTR:
                break;
            case EIO:
                Y_ABORT("EIO");
            default:
                Y_ABORT("It's something unexpected");
        }
    }

    int TSocket::GetDescriptor() {
        return Descriptor;
    }

    int
    TSocket::Bind(const TAddress& addr) const {
        const auto ret = ::bind(Descriptor, addr.SockAddr(), addr.Size());
        if (ret < 0)
            return -LastSocketError();

        return 0;
    }

    int
    TSocket::Shutdown(int how) const {
        const auto ret = ::shutdown(Descriptor, how);
        if (ret < 0)
            return -LastSocketError();

        return 0;
    }

    int TSocket::GetConnectStatus() const {
        int err = 0;
        socklen_t len = sizeof(err);
        if (getsockopt(Descriptor, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&err), &len) == -1) {
            err = LastSocketError();
        }
        return err;
    }

    /////////////////////////////////////////////////////////////////

    TIntrusivePtr<TStreamSocket> TStreamSocket::Make(int domain, int *error) {
        const SOCKET res = ::socket(domain, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (res == -1) {
            const int err = LastSocketError();
            Y_ABORT_UNLESS(err != EMFILE && err != ENFILE);
            if (error) {
                *error = err;
            }
        }
        return MakeIntrusive<TStreamSocket>(res);
    }

    TStreamSocket::TStreamSocket(SOCKET fd)
        : TSocket(fd)
    {
    }

    ssize_t
    TStreamSocket::Send(const void* msg, size_t len, TString* /*err*/) const {
        const auto ret = ::send(Descriptor, static_cast<const char*>(msg), int(len), 0);
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    ssize_t
    TStreamSocket::Recv(void* buf, size_t len, TString* /*err*/) const {
        const auto ret = ::recv(Descriptor, static_cast<char*>(buf), int(len), 0);
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    ssize_t
    TStreamSocket::WriteV(const struct iovec* iov, int iovcnt) const {
#ifndef _win_
        const auto ret = ::writev(Descriptor, iov, iovcnt);
        if (ret < 0)
            return -LastSocketError();
        return ret;
#else
        Y_ABORT("WriteV() unsupported on Windows");
#endif
    }

    ssize_t
    TStreamSocket::ReadV(const struct iovec* iov, int iovcnt) const {
#ifndef _win_
        const auto ret = ::readv(Descriptor, iov, iovcnt);
        if (ret < 0)
            return -LastSocketError();
        return ret;
#else
        Y_ABORT("ReadV() unsupported on Windows");
#endif
    }

    ssize_t TStreamSocket::GetUnsentQueueSize() const {
        int num = -1;
#ifndef _win_ // we have no means to determine output queue size on Windows
        if (ioctl(Descriptor, TIOCOUTQ, &num) == -1) {
            num = -1;
        }
#endif
        return num;
    }

    int
    TStreamSocket::Connect(const TAddress& addr) const {
        const auto ret = ::connect(Descriptor, addr.SockAddr(), addr.Size());
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    int
    TStreamSocket::Connect(const NAddr::IRemoteAddr* addr) const {
        const auto ret = ::connect(Descriptor, addr->Addr(), addr->Len());
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    int
    TStreamSocket::Listen(int backlog) const {
        const auto ret = ::listen(Descriptor, backlog);
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    int
    TStreamSocket::Accept(TAddress& acceptedAddr) const {
        socklen_t acceptedSize = sizeof(::sockaddr_in6);
        const auto ret = ::accept(Descriptor, acceptedAddr.SockAddr(), &acceptedSize);
        if (ret == INVALID_SOCKET)
            return -LastSocketError();

        return ret;
    }

    void
    TStreamSocket::SetSendBufferSize(i32 len) const {
        (void)SetSockOpt(Descriptor, SOL_SOCKET, SO_SNDBUF, len);
    }

    ui32 TStreamSocket::GetSendBufferSize() const {
        ui32 res = 0;
        CheckedGetSockOpt(Descriptor, SOL_SOCKET, SO_SNDBUF, res, "SO_SNDBUF");
        return res;
    }

    void TStreamSocket::Request(NActors::TPollerToken& token, bool read, bool write) {
        token.Request(read, write);
    }

    bool TStreamSocket::RequestReadNotificationAfterWouldBlock(NActors::TPollerToken& token) {
        return token.RequestReadNotificationAfterWouldBlock();
    }

    bool TStreamSocket::RequestWriteNotificationAfterWouldBlock(NActors::TPollerToken& token) {
        return token.RequestWriteNotificationAfterWouldBlock();
    }

    size_t TStreamSocket::ExpectedWriteLength() const {
        return 0;
    }

    //////////////////////////////////////////////////////

    TDatagramSocket::TPtr TDatagramSocket::Make(int domain) {
        const SOCKET res = ::socket(domain, SOCK_DGRAM, 0);
        if (res == -1) {
            const int err = LastSocketError();
            Y_ABORT_UNLESS(err != EMFILE && err != ENFILE);
        }
        return std::make_shared<TDatagramSocket>(res);
    }

    TDatagramSocket::TDatagramSocket(SOCKET fd)
        : TSocket(fd)
    {
    }

    ssize_t
    TDatagramSocket::SendTo(const void* msg, size_t len, const TAddress& toAddr) const {
        const auto ret = ::sendto(Descriptor, static_cast<const char*>(msg), int(len), 0, toAddr.SockAddr(), toAddr.Size());
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }

    ssize_t
    TDatagramSocket::RecvFrom(void* buf, size_t len, TAddress& fromAddr) const {
        socklen_t fromSize = sizeof(::sockaddr_in6);
        const auto ret = ::recvfrom(Descriptor, static_cast<char*>(buf), int(len), 0, fromAddr.SockAddr(), &fromSize);
        if (ret < 0)
            return -LastSocketError();

        return ret;
    }


    // deleter for SSL objects
    struct TDeleter {
        void operator ()(BIO *bio) const {
            BIO_free(bio);
        }

        void operator ()(X509 *x509) const {
            X509_free(x509);
        }

        void operator ()(RSA *rsa) const {
            RSA_free(rsa);
        }

        void operator ()(EVP_PKEY *pKey) const {
            EVP_PKEY_free(pKey);
        }

        void operator ()(SSL_CTX *ctx) const {
            SSL_CTX_free(ctx);
        }
    };

    class TSecureSocketContext::TImpl {
        std::unique_ptr<SSL_CTX, TDeleter> Ctx;
        TIntrusivePtr<NActors::TInterconnectProxyCommon> Common;

    public:
        TImpl(TIntrusivePtr<NActors::TInterconnectProxyCommon> common)
            : Common(std::move(common))
        {
            int ret;
            InitOpenSSL();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
            Ctx.reset(SSL_CTX_new(TLSv1_2_method()));
            Y_ABORT_UNLESS(Ctx, "SSL_CTX_new() failed");
#else
            Ctx.reset(SSL_CTX_new(TLS_method()));
            Y_ABORT_UNLESS(Ctx, "SSL_CTX_new() failed");
            ret = SSL_CTX_set_min_proto_version(Ctx.get(), TLS1_2_VERSION);
            Y_ABORT_UNLESS(ret == 1, "failed to set min proto version");
            ret = SSL_CTX_set_max_proto_version(Ctx.get(), TLS1_3_VERSION);
            Y_ABORT_UNLESS(ret == 1, "failed to set max proto version");
#endif
            SSL_CTX_set_verify(Ctx.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, &Verify);
            SSL_CTX_set_mode(*this, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

            // apply certificates in SSL context
            if (const TString& certificate = Common->Settings.Certificate) {
                std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(certificate.data(), certificate.size()));
                Y_ABORT_UNLESS(bio);

                // first certificate in the chain is expected to be a leaf
                std::unique_ptr<X509, TDeleter> cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
                Y_ABORT_UNLESS(cert, "failed to parse certificate");
                ret = SSL_CTX_use_certificate(Ctx.get(), cert.get());
                Y_ABORT_UNLESS(ret == 1);

                // loading additional certificates in the chain, if any
                while(true) {
                    X509 *ca = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
                    if (ca == nullptr) {
                        break;
                    }
                    ret = SSL_CTX_add0_chain_cert(Ctx.get(), ca);
                    Y_ABORT_UNLESS(ret == 1);
                    // we must not free memory if certificate was added successfully by SSL_CTX_add0_chain_cert
                }
            }
            if (const TString& privateKey = Common->Settings.PrivateKey) {
                std::unique_ptr<BIO, TDeleter> bio(BIO_new_mem_buf(privateKey.data(), privateKey.size()));
                Y_ABORT_UNLESS(bio);
                std::unique_ptr<EVP_PKEY, TDeleter> pkey(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr));
                Y_ABORT_UNLESS(pkey);
                ret = SSL_CTX_use_PrivateKey(Ctx.get(), pkey.get());
                Y_ABORT_UNLESS(ret == 1);
            }
            if (const TString& caFilePath = Common->Settings.CaFilePath) {
                ret = SSL_CTX_load_verify_locations(Ctx.get(), caFilePath.data(), nullptr);
                Y_ABORT_UNLESS(ret == 1);
            }
            const TString& ciphers = Common->Settings.CipherList;
            int success = SSL_CTX_set_cipher_list(Ctx.get(), ciphers ? ciphers.data() : "AES128-GCM-SHA256");
            Y_ABORT_UNLESS(success, "failed to set cipher list");
        }

        operator SSL_CTX*() const {
            return Ctx.get();
        }

        static int GetExIndex() {
            static int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
            return index;
        }

        static int GetContextIndex() {
            static int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
            return index;
        }

    private:
        static int Verify(int preverify, X509_STORE_CTX *ctx) {
            X509* const current = X509_STORE_CTX_get_current_cert(ctx);
            auto* const ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx()));
            auto* const errp = static_cast<TString*>(SSL_get_ex_data(ssl, GetExIndex()));
            auto* const secureSocketContext = static_cast<TSecureSocketContext*>(SSL_get_ex_data(ssl, GetContextIndex()));

            if (!preverify) {
                int err = X509_STORE_CTX_get_error(ctx);
                int depth = X509_STORE_CTX_get_error_depth(ctx);
                char buffer[1024];
                X509_NAME_oneline(X509_get_subject_name(current), buffer, sizeof(buffer));
                TStringBuilder s;
                s << "Error during certificate validation"
                    << " error# " << X509_verify_cert_error_string(err)
                    << " depth# " << depth
                    << " cert# " << buffer;
                if (err == X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT) {
                    X509_NAME_oneline(X509_get_issuer_name(current), buffer, sizeof(buffer));
                    s << " issuer# " << buffer;
                }
                *errp = s;
            } else if (auto& forbidden = secureSocketContext->Impl->Common->Settings.ForbiddenSignatureAlgorithms) {
                do {
                    int pknid;
                    if (X509_get_signature_info(current, nullptr, &pknid, nullptr, nullptr) != 1) {
                        *errp = TStringBuilder() << "failed to acquire signature info";
                    } else if (const char *ln = OBJ_nid2ln(pknid); ln && forbidden.contains(ln)) {
                        *errp = TStringBuilder() << "forbidden signature algorithm: " << ln;
                    } else if (const char *sn = OBJ_nid2ln(pknid); sn && forbidden.contains(sn)) {
                        *errp = TStringBuilder() << "forbidden signature algorithm: " << sn;
                    } else {
                        break;
                    }
                    X509_STORE_CTX_set_error(ctx, X509_V_ERR_UNSUPPORTED_SIGNATURE_ALGORITHM);
                    return 0;
                } while (false);
            }
            return preverify;
        }
    };

    TSecureSocketContext::TSecureSocketContext(TIntrusivePtr<NActors::TInterconnectProxyCommon> common)
        : Impl(new TImpl(std::move(common)))
    {}

    TSecureSocketContext::~TSecureSocketContext()
    {}

    class TSecureSocket::TImpl {
        SSL *Ssl;
        TString ErrorDescription;
        bool WantRead_ = false;
        bool WantWrite_ = false;

    public:
        TImpl(SSL_CTX *ctx, int fd, TSecureSocketContext *secureSocketContext)
            : Ssl(SSL_new(ctx))
        {
            Y_ABORT_UNLESS(Ssl, "SSL_new() failed");
            SSL_set_fd(Ssl, fd);
            SSL_set_ex_data(Ssl, TSecureSocketContext::TImpl::GetExIndex(), &ErrorDescription);
            SSL_set_ex_data(Ssl, TSecureSocketContext::TImpl::GetContextIndex(), secureSocketContext);
        }

        ~TImpl() {
            SSL_free(Ssl);
        }

        TString GetErrorStack() {
            if (ErrorDescription) {
                return ErrorDescription;
            }
            std::unique_ptr<BIO, int(*)(BIO*)> mem(BIO_new(BIO_s_mem()), BIO_free);
            ERR_print_errors(mem.get());
            char *p = nullptr;
            auto len = BIO_get_mem_data(mem.get(), &p);
            return TString(p, len);
        }

        EStatus ConvertResult(int res, TString& err) {
            switch (res) {
                case SSL_ERROR_NONE:
                    return EStatus::SUCCESS;

                case SSL_ERROR_WANT_READ:
                    return EStatus::WANT_READ;

                case SSL_ERROR_WANT_WRITE:
                    return EStatus::WANT_WRITE;

                case SSL_ERROR_SYSCALL:
                    err = TStringBuilder() << "syscall error: " << strerror(LastSocketError()) << ": " << GetErrorStack();
                    break;

                case SSL_ERROR_ZERO_RETURN:
                    err = "TLS negotiation failed";
                    break;

                case SSL_ERROR_SSL:
                    err = "SSL error: " + GetErrorStack();
                    break;

                default:
                    err = "unknown OpenSSL error";
                    break;
            }
            return EStatus::ERROR;
        }

        enum EConnectState {
            CONNECT,
            SHUTDOWN,
            READ,
        } ConnectState = EConnectState::CONNECT;

        EStatus Establish(bool server, bool authOnly, TString& err) {
            switch (ConnectState) {
                case EConnectState::CONNECT: {
                    auto callback = server ? SSL_accept : SSL_connect;
                    const EStatus status = ConvertResult(SSL_get_error(Ssl, callback(Ssl)), err);
                    if (status != EStatus::SUCCESS || !authOnly) {
                        return status;
                    }
                    ConnectState = EConnectState::SHUTDOWN;
                    [[fallthrough]];
                }

                case EConnectState::SHUTDOWN: {
                    const int res = SSL_shutdown(Ssl);
                    if (res == 1) {
                        return EStatus::SUCCESS;
                    } else if (res != 0) {
                        return ConvertResult(SSL_get_error(Ssl, res), err);
                    }
                    ConnectState = EConnectState::READ;
                    [[fallthrough]];
                }

                case EConnectState::READ: {
                    char data[256];
                    size_t numRead = 0;
                    const int res = SSL_get_error(Ssl, SSL_read_ex(Ssl, data, sizeof(data), &numRead));
                    if (res == SSL_ERROR_ZERO_RETURN) {
                        return EStatus::SUCCESS;
                    } else if (res != SSL_ERROR_NONE) {
                        return ConvertResult(res, err);
                    } else if (numRead) {
                        err = "non-zero return from SSL_read_ex: " + ToString(numRead);
                        return EStatus::ERROR;
                    } else {
                        return EStatus::SUCCESS;
                    }
                }
            }
            Y_ABORT();
        }

        std::optional<std::pair<const void*, size_t>> BlockedSend;

        ssize_t Send(const void* msg, size_t len, TString *err) {
            if (BlockedSend && BlockedSend->first == msg && BlockedSend->second < len) {
                len = BlockedSend->second;
            }
            Y_ABORT_UNLESS(!BlockedSend || *BlockedSend == std::make_pair(msg, len));
            const ssize_t res = Operate(msg, len, &SSL_write_ex, err);
            if (res == -EAGAIN) {
                BlockedSend.emplace(msg, len);
            } else {
                BlockedSend.reset();
            }
            return res;
        }

        size_t ExpectedWriteLength() const {
            return BlockedSend ? BlockedSend->second : 0;
        }

        std::optional<std::pair<void*, size_t>> BlockedReceive;

        ssize_t Recv(void* msg, size_t len, TString *err) {
            if (BlockedReceive && BlockedReceive->first == msg && BlockedReceive->second < len) {
                len = BlockedReceive->second;
            }
            Y_ABORT_UNLESS(!BlockedReceive || *BlockedReceive == std::make_pair(msg, len));
            const ssize_t res = Operate(msg, len, &SSL_read_ex, err);
            if (res == -EAGAIN) {
                BlockedReceive.emplace(msg, len);
            } else {
                BlockedReceive.reset();
            }
            return res;
        }

        TString GetCipherName() const {
            return SSL_get_cipher_name(Ssl);
        }

        int GetCipherBits() const {
            return SSL_get_cipher_bits(Ssl, nullptr);
        }

        TString GetProtocolName() const {
            return SSL_get_cipher_version(Ssl);
        }

        TString GetPeerCommonName() const {
            TString res;
            if (std::unique_ptr<X509, void(*)(X509*)> cert{SSL_get_peer_certificate(Ssl), &X509_free}) {
                char buffer[256];
                memset(buffer, 0, sizeof(buffer));
                if (X509_NAME *name = X509_get_subject_name(cert.get())) {
                    X509_NAME_get_text_by_NID(name, NID_commonName, buffer, sizeof(buffer));
                }
                res = TString(buffer, strnlen(buffer, sizeof(buffer)));
            }
            return res;
        }

        TString GetSignatureAlgorithm() const {
            TString res;
            if (std::unique_ptr<X509, void(*)(X509*)> cert{SSL_get_peer_certificate(Ssl), &X509_free}) {
                int mdnid;
                int pknid;
                int secbits;
                if (X509_get_signature_info(cert.get(), &mdnid, &pknid, &secbits, nullptr) == 1) {
                    res = TStringBuilder()
                        << "md: " << OBJ_nid2ln(mdnid)
                        << " pk: " << OBJ_nid2ln(pknid)
                        << " bits: " << secbits;
                } else {
                    res = "<failed to get signature info>";
                }
            } else {
                res = "<failed to get peer certificate>";
            }
            return res;
        }

        bool WantRead() const {
            return WantRead_;
        }

        bool WantWrite() const {
            return WantWrite_;
        }

    private:
        template<typename TBuffer, typename TOp>
        ssize_t Operate(TBuffer* buffer, size_t len, TOp&& op, TString *err) {
            WantRead_ = WantWrite_ = false;
            size_t processed = 0;
            int ret = op(Ssl, buffer, len, &processed);
            if (ret == 1) {
                return processed;
            }
            switch (const int status = SSL_get_error(Ssl, ret)) {
                case SSL_ERROR_ZERO_RETURN:
                    return 0;

                case SSL_ERROR_WANT_READ:
                    WantRead_ = true;
                    return -EAGAIN;

                case SSL_ERROR_WANT_WRITE:
                    WantWrite_ = true;
                    return -EAGAIN;

                case SSL_ERROR_SYSCALL:
                    return -LastSocketError();

                case SSL_ERROR_SSL:
                    if (err) {
                        *err = GetErrorStack();
                    }
                    return -EPROTO;

                default:
                    Y_ABORT("unexpected SSL_get_error() status# %d", status);
            }
        }
    };

    TSecureSocket::TSecureSocket(TStreamSocket& socket, TSecureSocketContext::TPtr context)
        : TStreamSocket(socket.ReleaseDescriptor())
        , Context(std::move(context))
        , Impl(new TImpl(*Context->Impl, Descriptor, Context.get()))
    {}

    TSecureSocket::~TSecureSocket()
    {}

    TSecureSocket::EStatus TSecureSocket::Establish(bool server, bool authOnly, TString& err) const {
        return Impl->Establish(server, authOnly, err);
    }

    TIntrusivePtr<TStreamSocket> TSecureSocket::Detach() {
        return MakeIntrusive<TStreamSocket>(ReleaseDescriptor());
    }

    ssize_t TSecureSocket::Send(const void* msg, size_t len, TString *err) const {
        return Impl->Send(msg, len, err);
    }

    ssize_t TSecureSocket::Recv(void* msg, size_t len, TString *err) const {
        return Impl->Recv(msg, len, err);
    }

    ssize_t TSecureSocket::WriteV(const struct iovec* /*iov*/, int /*iovcnt*/) const {
        Y_ABORT("unsupported on SSL sockets");
    }

    ssize_t TSecureSocket::ReadV(const struct iovec* /*iov*/, int /*iovcnt*/) const {
        Y_ABORT("unsupported on SSL sockets");
    }

    TString TSecureSocket::GetCipherName() const {
        return Impl->GetCipherName();
    }

    int TSecureSocket::GetCipherBits() const {
        return Impl->GetCipherBits();
    }

    TString TSecureSocket::GetProtocolName() const {
        return Impl->GetProtocolName();
    }

    TString TSecureSocket::GetPeerCommonName() const {
        return Impl->GetPeerCommonName();
    }

    TString TSecureSocket::GetSignatureAlgorithm() const {
        return Impl->GetSignatureAlgorithm();
    }

    bool TSecureSocket::WantRead() const {
        return Impl->WantRead();
    }

    bool TSecureSocket::WantWrite() const {
        return Impl->WantWrite();
    }

    void TSecureSocket::Request(NActors::TPollerToken& token, bool /*read*/, bool /*write*/) {
        token.Request(WantRead(), WantWrite());
    }

    bool TSecureSocket::RequestReadNotificationAfterWouldBlock(NActors::TPollerToken& token) {
        bool result = false;
        if (WantRead()) {
            result |= token.RequestReadNotificationAfterWouldBlock();
        }
        if (WantWrite()) {
            result |= token.RequestWriteNotificationAfterWouldBlock();
        }
        return result;
    }

    bool TSecureSocket::RequestWriteNotificationAfterWouldBlock(NActors::TPollerToken& token) {
        return RequestReadNotificationAfterWouldBlock(token);
    }

    size_t TSecureSocket::ExpectedWriteLength() const {
        return Impl->ExpectedWriteLength();
    }
}
