#pragma once

#include <optional>
#include <util/network/sock.h>
#include <util/stream/file.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/core/base/appdata.h>
#include "sock_settings.h"
#include "sock_ssl.h"

namespace NKikimr::NRawSocket {

class TInet64StreamSocket : public TStreamSocket {
    using TBase = TStreamSocket;
protected:
    TInet64StreamSocket(const TInet64StreamSocket& parent, SOCKET fd)
        : TStreamSocket(fd)
        , AF(parent.AF)
    {
    }

public:
    TInet64StreamSocket(const TSocketSettings& socketSettings = {}) {
        CreateSocket(socketSettings);
    }

    TInet64StreamSocket(TInet64StreamSocket&& socket) = default;
    virtual ~TInet64StreamSocket() = default;

    static bool IsIPv6(const TString& host) {
        if (host.find_first_not_of(":0123456789abcdef") != TString::npos) {
            return false;
        }
        if (std::count(host.begin(), host.end(), ':') < 2) {
            return false;
        }
        return true;
    }

    std::shared_ptr<ISockAddr> MakeAddress(const TString& address, int port) {
        if (!address) {
            if (AF == AF_INET6) {
                return std::make_shared<TSockAddrInet6>("::", port);
            } else {
                return std::make_shared<TSockAddrInet>(INADDR_ANY, port);
            }
        }
        if (IsIPv6(address)) {
            return std::make_shared<TSockAddrInet6>(address.data(), port);
        } else {
            return std::make_shared<TSockAddrInet>(address.data(), port);
        }
    }

    static std::shared_ptr<ISockAddr> MakeAddress(const sockaddr_storage& storage) {
        std::shared_ptr<ISockAddr> addr;
        switch (storage.ss_family) {
            case AF_INET:
                addr = std::make_shared<TSockAddrInet>();
                break;
            case AF_INET6:
                addr = std::make_shared<TSockAddrInet6>();
                break;
        }
        if (addr) {
            memcpy(addr->SockAddr(), &storage, addr->Size());
        }
        return addr;
    }

    std::optional<TInet64StreamSocket> Accept(std::shared_ptr<ISockAddr>& acceptedAddr) {
        sockaddr_storage addrStorage = {};
        socklen_t addrLen = sizeof(addrStorage);
        SOCKET s = accept((SOCKET)*this, reinterpret_cast<sockaddr*>(&addrStorage), &addrLen);
        if (s == INVALID_SOCKET) {
            return {};
        }
        acceptedAddr = MakeAddress(addrStorage);
        return TInet64StreamSocket(*this, s);
    }

    bool WantsToRead = false;
    bool WantsToWrite = false;

    void ResetPollerState() {
        WantsToRead = false;
        WantsToWrite = false;
    }

    void RequestPoller(NActors::TPollerToken::TPtr& pollerToken) {
        if (pollerToken && (WantsToRead || WantsToWrite)) {
            pollerToken->Request(WantsToRead, WantsToWrite);
            ResetPollerState();
        }
    }

    virtual ssize_t Send(const void* msg, size_t len, int flags = 0) {
        ssize_t res = TBase::Send(msg, len, flags);
        if (res < 0) {
            if (-res == EAGAIN || -res == EWOULDBLOCK) {
                WantsToWrite = true;
            }
        }
        return res;
    }

    virtual ssize_t Recv(void* buf, size_t len, int flags = 0) {
        ssize_t res = TBase::Recv(buf, len, flags);
        if (res < 0) {
            if (-res == EAGAIN || -res == EWOULDBLOCK) {
                WantsToRead = true;
            }
        }
        return res;
    }

protected:
    int AF = 0;

    void CreateSocket(const TSocketSettings& socketSettings) {
        SOCKET s;
        if (socketSettings.AF == 0) {
            s = socket(AF = AF_INET6, SOCK_STREAM, 0);
            if (s < 0) {
                s = socket(AF = AF_INET, SOCK_STREAM, 0);
            }
        } else {
            s = socket(AF = socketSettings.AF, SOCK_STREAM, 0);
        }
        SetSockOpt(s, IPPROTO_TCP, TCP_NODELAY, (int)socketSettings.TcpNotDelay);
        if (AF == AF_INET6) {
            SetSockOpt(s, SOL_SOCKET, IPV6_V6ONLY, (int)false);
        }
        SetSockOpt(s, SOL_SOCKET, SO_REUSEADDR, (int)true);
#ifdef SO_REUSEPORT
        SetSockOpt(s, SOL_SOCKET, SO_REUSEPORT, (int)true);
#endif

        TSocketHolder sock(s);
        sock.Swap(*this);
    }
};

class TInet64SecureStreamSocket : public TInet64StreamSocket, TSslLayer<TStreamSocket> {
    template<typename T>
    using TSslHolder = TSslLayer<TStreamSocket>::TSslHolder<T>;

    TSslHolder<BIO> Bio;
    TSslHolder<SSL> Ssl;

public:
    TInet64SecureStreamSocket(const TSocketSettings& socketSettings = {})
        : TInet64StreamSocket(socketSettings)
    {}

    TInet64SecureStreamSocket(TInet64StreamSocket&& socket)
        : TInet64StreamSocket(std::move(socket))
    {}

    bool InitServerSsl(SSL_CTX* ctx) { // мб добавить true или false (успех/неуспех)
        Bio.reset(BIO_new(TSslLayer<TStreamSocket>::IoMethod()));
        BIO_set_data(Bio.get(), static_cast<TStreamSocket*>(this));
        BIO_set_nbio(Bio.get(), 1);

        if (AppData()->KafkaProxyConfig.GetMtlsEnable()) {
            Cerr << TInstant::Now() << "Mtls enabled" << Endl;
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,&TInet64SecureStreamSocket::Verify);
            // SSL_set_fd(Ssl.get(), Fd_);
            // Cout << "SecureAccept. Set fd" << Endl;
            // SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_OFF);
            // SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
            // SSL_CTX_set_timeout(ctx, 0);
            auto readFile = [](std::optional<TString> path, const char *name) {
                if (path) {
                    try {
                        return TFileInput(*path).ReadAll();
                    } catch (const std::exception& ex) {
                        ythrow yexception()
                            << "failed to read " << name << " file '" << *path << "': " << ex.what(); // Это надо поправить!! Не надо бросать эксепшн
                    }
                }
                return TString();
            };
            TString kafkaServerCertPath = NKikimr::AppData()->KafkaProxyConfig.GetCert();
            TString kafkaServerPrivateKeyPath = NKikimr::AppData()->KafkaProxyConfig.GetKey();
            TString kafkaCAFilePath = AppData()->KafkaProxyConfig.GetCA();

            TSslHolder<X509> serverCert = GetServerCert(kafkaServerCertPath, readFile);
            if (!serverCert) { return false; }
            int retServerCert = SSL_CTX_use_certificate(ctx, serverCert.get());
            if (retServerCert != 1) { return false; }
            Cout << TInstant::Now() << "SecureAccept. Load server cert" << Endl;


            TSslHolder<EVP_PKEY> privateKey = GetServerPrivateKey(kafkaServerPrivateKeyPath, readFile);
            if (!privateKey) { return false; }

            int retPrivateKey = SSL_CTX_use_PrivateKey(ctx, privateKey.get());
            if (retPrivateKey != 1) { return false; }
            // Cout << TInstant::Now() << "SecureAccept. Load server private key" << Endl;
            int retCA = SSL_CTX_load_verify_locations(ctx, kafkaCAFilePath.data(), nullptr);
            if (retCA != 1) { return false; }
            // Cout << TInstant::Now() << "SecureAccept. Load CA" << Endl;
            SSL_CTX_set_verify_depth(ctx, 9);
            // Cout << TInstant::Now() << "SecureAccept. SSL_CTX_set_verify_depth" << Endl;
            SSL_CTX_set_info_callback(ctx, ssl_info_cb);
            // Cout << TInstant::Now() << "SecureAccept. SSL_CTX_set_info_callback" << Endl;

            SSL_CTX_set_ciphersuites(ctx, "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256");
            // Cout << TInstant::Now() << "SecureAccept. SSL_CTX_set_ciphersuites" << Endl;

            const char *session_context = "YdbMtlsContext";
            SSL_CTX_set_session_id_context(ctx, (const unsigned char *)session_context, strlen(session_context));
            // Cout << TInstant::Now() << "Set session context id" << Endl;
        }

        Ssl = TSslHelpers::ConstructSsl(ctx, Bio.get());

        SSL_set_accept_state(Ssl.get());
        Cout << "Verify mode: " << SSL_CTX_get_verify_mode(ctx) << "SSL_VERIFY_PEER: " << SSL_VERIFY_PEER << Endl;

        return true;
    }

    TSslHolder<X509> GetServerCert(const TString& certPath, TString (*readFileFunc)(std::optional<TString>, const char *)) {
        TString certificate = readFileFunc(certPath, "certificate");
        TSslHolder<BIO> bio(BIO_new_mem_buf(certificate.data(), certificate.size()));
        if (bio) {
            TSslHolder<X509> cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
            return cert;
        }
        return TSslHolder<X509>();
    }

     TSslHolder<EVP_PKEY> GetServerPrivateKey(const TString& keyPath, TString (*readFileFunc)(std::optional<TString>, const char *)) {
        TString privateKey = readFileFunc(keyPath, "key");
        TSslHolder<BIO> bio(BIO_new_mem_buf(privateKey.data(), privateKey.size()));
        if (bio) {
            TSslHolder<EVP_PKEY> pkey(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr));
            return pkey;
        }
        return TSslHolder<EVP_PKEY>();
    }

    int ProcessResult(int res) {
        if (res <= 0) {
            res = SSL_get_error(Ssl.get(), res);
            switch(res) {
            case SSL_ERROR_WANT_READ:
                WantsToRead = true;
                return -EAGAIN;
            case SSL_ERROR_WANT_WRITE:
                WantsToWrite = true;
                return -EAGAIN;
            default:
                return -EIO;
            }
        }
        return res;
    }

    int SecureAccept(SSL_CTX* ctx) {
        if (!Ssl) {

            if (!InitServerSsl(ctx)) {
                return SSL_AD_NO_CERTIFICATE;
            }
        }
        int res = SSL_accept(Ssl.get());
        Cout << "SecureAccept. SSL_accept" << Endl;
        return ProcessResult(res);
    }

    static char* ConvertX509ToPEMString(X509* cert) {
        BIO* bio = BIO_new(BIO_s_mem());
        char* buffer = NULL;
        long length;

        if (PEM_write_bio_X509(bio, cert)) {
            length = BIO_get_mem_data(bio, &buffer);
            char* result = (char*)malloc(length + 1);
            memcpy(result, buffer, length);
            result[length] = '\0';
            BIO_free(bio);
            return result;
        }

        BIO_free(bio);
        return NULL;
    }

    static void ssl_info_cb(const SSL *ssl, int where, int ret)
    {
        const char *role =
            (where & SSL_ST_CONNECT) ? "client" :
            (where & SSL_ST_ACCEPT)  ? "server" : "unknown";

        if (where & SSL_CB_LOOP) {
            // Переходы по состояниям SSL state machine
            Cout << TInstant::Now() << " SSL_LOG: [" << role << "] state: " << SSL_state_string_long(ssl) << Endl;
            // fprintf(stderr, "SSL_LOG: [%s] state: %s\n", role, SSL_state_string_long(ssl));
        }

        if (where & SSL_CB_HANDSHAKE_START) {
            Cout << TInstant::Now() << " SSL_LOG: [" << role << "] handshake start" << Endl;
            // fprintf(stderr, "SSL_LOG: [%s] handshake start\n", role);
        }

        if (where & SSL_CB_HANDSHAKE_DONE) {
            Cout << TInstant::Now() << " SSL_LOG: [" << role << "] handshake done" << Endl;
            // fprintf(stderr, "SSL_LOG: [%s] handshake done\n", role);
        }

        if (where & SSL_CB_ALERT) {
            // Alerts (warning/fatal) + направление (read/write)
            const char *dir = (where & SSL_CB_READ) ? "read" : "write";
            Cout << TInstant::Now() << " SSL_LOG: [" << role << "] alert" << dir << ": " << SSL_alert_type_string_long(ret) << ": " << SSL_alert_desc_string_long(ret) << Endl;
            // fprintf(stderr, "SSL_LOG: [%s] alert %s: %s: %s\n",
            //         role,
            //         dir,
            //         SSL_alert_type_string_long(ret),
            //         SSL_alert_desc_string_long(ret));
        }

        if (where & SSL_CB_EXIT) {
            // Выход из состояния с ошибкой (часто ret <= 0)
            if (ret == 0) {
                Cout << TInstant::Now() << " SSL_LOG: [" << role << "] failure in: " << SSL_state_string_long(ssl) << Endl;
                // fprintf(stderr, "SSL_LOG: [%s] failure in: %s\n", role, SSL_state_string_long(ssl));
            } else if (ret < 0) {
                Cout << TInstant::Now() << " SSL_LOG: [" << role << "] error in: " << SSL_state_string_long(ssl) << Endl;
                // fprintf(stderr, "SSL_LOG: [%s] error in: %s\n", role, SSL_state_string_long(ssl));
                unsigned long e;
                while ((e = ERR_get_error()) != 0) {
                    char buf[256];
                    ERR_error_string_n(e, buf, sizeof(buf));
                    fprintf(stderr, "OpenSSL error: %s\n", buf);
                }
            }
        }
    }

    static int Verify(int preverify, X509_STORE_CTX *ctx) {
        X509* current = X509_STORE_CTX_get_current_cert(ctx);
        // auto* const ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(ctx, SSL_get_ex_data_X509_STORE_CTX_idx()));
        // auto* const errp = static_cast<TString*>(SSL_get_ex_data(ssl, GetExIndex()));
        // auto* const secureSocketContext = static_cast<TInet64SecureStreamSocket*>(SSL_get_ex_data(ssl, GetContextIndex()));
        if (!preverify) {
            int err = X509_STORE_CTX_get_error(ctx);
            int depth = X509_STORE_CTX_get_error_depth(ctx);
            char buffer[1024];
            X509_NAME_oneline(X509_get_subject_name(current), buffer, sizeof(buffer));
            if (err == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
                // Разрешаем self-signed сертификаты
                TString certificate = ConvertX509ToPEMString(current);
                Cout << "Cert=" << certificate << Endl;
                preverify = 1;
            }
            TStringBuilder s;
            s << "Error during certificate validation"
                << " error# " << X509_verify_cert_error_string(err)
                << " depth# " << depth
                << " cert# " << buffer;
            if (err == X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT) {
                X509_NAME_oneline(X509_get_issuer_name(current), buffer, sizeof(buffer));
                s << " issuer# " << buffer;
            }
            Cerr << s << Endl;
            // *errp = s;
        }
        // else if (auto& forbidden = secureSocketContext->Impl->Common->Settings.ForbiddenSignatureAlgorithms) {
        //     do {
        //         int pknid;
        //         if (X509_get_signature_info(current, nullptr, &pknid, nullptr, nullptr) != 1) {
        //             *errp = TStringBuilder() << "failed to acquire signature info";
        //         } else if (const char *ln = OBJ_nid2ln(pknid); ln && forbidden.contains(ln)) {
        //             *errp = TStringBuilder() << "forbidden signature algorithm: " << ln;
        //         } else if (const char *sn = OBJ_nid2ln(pknid); sn && forbidden.contains(sn)) {
        //             *errp = TStringBuilder() << "forbidden signature algorithm: " << sn;
        //         } else {
        //             break;
        //         }
        //         X509_STORE_CTX_set_error(ctx, X509_V_ERR_UNSUPPORTED_SIGNATURE_ALGORITHM);
        //         return 0;
        //     } while (false);
        // }
        Cout << "Verifying client cert" << (current != nullptr) << Endl;
        return preverify;
    }

    int GetSslHandshakeResult() {
        if (!Ssl) {
            return -1;
        }
        int ret = SSL_do_handshake(Ssl.get());
        Cout << "SSL handshake result: " << ret << Endl;
        Cout << "Verify result:" << SSL_get_verify_result(Ssl.get()) << Endl;
        int ssl_err = SSL_get_error(Ssl.get(), ret);
        switch(ssl_err) {
            case SSL_ERROR_NONE:
                Cout << "Error: SSL_ERROR_NONE " << ssl_err << Endl;
                break;
            case SSL_ERROR_ZERO_RETURN:
                Cout << "Error: SSL_ERROR_ZERO_RETURN " << ssl_err << Endl;
                break;
            case SSL_ERROR_WANT_READ:
                Cout << "Error: SSL_ERROR_WANT_READ " << ssl_err << Endl;
                break;
            case SSL_ERROR_WANT_WRITE:
                Cout << "Error: SSL_ERROR_WANT_WRITE " << ssl_err << Endl;
                break;
            case SSL_ERROR_WANT_CONNECT:
                Cout << "Error: SSL_ERROR_WANT_CONNECT " << ssl_err << Endl;
                break;
            case SSL_ERROR_WANT_ACCEPT:
                Cout << "Error: SSL_ERROR_WANT_ACCEPT " << ssl_err << Endl;
                break;
            default:
                Cout << "Error: " << ssl_err << Endl;
        }
        return ret;
    }

    TSslHolder<X509> GetSslClientCert() {
        if (!Ssl) {
            Cerr << "No SSL!!!" << Endl;
            return nullptr;
        }
        return TSslHolder<X509>(SSL_get_peer_certificate(Ssl.get()));
    }

    void PollClientCertAfterHandshake() {
        int res = SSL_verify_client_post_handshake(Ssl.get());
        Cout << "SSL_verify_client_post_handshake res = " << res << Endl;
    }

    ssize_t Send(const void* msg, size_t len, int flags = 0) override {
        Y_UNUSED(flags);
        int res = SSL_write(Ssl.get(), msg, len);
        return ProcessResult(res);
    }

    ssize_t Recv(void* buf, size_t len, int flags = 0) override {
        Y_UNUSED(flags);
        int res = SSL_read(Ssl.get(), buf, len);
        return ProcessResult(res);
    }
};

} // namespace NKikimr::NRawSocket
