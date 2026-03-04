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

    bool InitServerSsl(SSL_CTX* ctx) {
        Bio.reset(BIO_new(TSslLayer<TStreamSocket>::IoMethod()));
        BIO_set_data(Bio.get(), static_cast<TStreamSocket*>(this));
        BIO_set_nbio(Bio.get(), 1);

        if (AppData()->KafkaProxyConfig.GetMtlsEnable()) {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,&TInet64SecureStreamSocket::Verify);

            auto readFile = [](std::optional<TString> path) {
                if (path) {
                    try {
                        return TFileInput(*path).ReadAll();
                    } catch (const std::exception& ex) {
                        return TString();
                    }
                }
                return TString();
            };
            TString kafkaServerCertPath = NKikimr::AppData()->KafkaProxyConfig.GetCert();
            TString kafkaServerPrivateKeyPath = NKikimr::AppData()->KafkaProxyConfig.GetKey();
            TString kafkaCAFilePath = AppData()->KafkaProxyConfig.GetCA();

            TSslHolder<X509> serverCert = GetServerCert(kafkaServerCertPath, readFile);
            if (!serverCert) {
                Cerr << TInstant::Now() << ": Couldn't open server certificate file path from kafka_proxy configuration." << Endl;
                return false;
            }
            int retServerCert = SSL_CTX_use_certificate(ctx, serverCert.get());
            if (retServerCert != 1) {
                Cerr <<  TInstant::Now() << ": Couldn't open server private key file path from kafka_proxy configuration." << Endl;
                return false;
            }

            TSslHolder<EVP_PKEY> privateKey = GetServerPrivateKey(kafkaServerPrivateKeyPath, readFile);
            if (!privateKey) {
                Cerr <<  TInstant::Now() << ": Couldn't open server private key file path from kafka_proxy configuration." << Endl;
                return false;
            }
            int retPrivateKey = SSL_CTX_use_PrivateKey(ctx, privateKey.get());
            if (retPrivateKey != 1) {
                Cerr <<  TInstant::Now() << ": Couldn't load server private key to SSL context." << Endl;
                return false;
            }

            int retCA = SSL_CTX_load_verify_locations(ctx, kafkaCAFilePath.data(), nullptr);
            if (retCA != 1) {
                Cerr <<  TInstant::Now() << ": Couldn't open CA file path from kafka_proxy configuration or load it to the SSL context." << Endl;
                return false;
            }

            SSL_CTX_set_verify_depth(ctx, 9);
            SSL_CTX_set_ciphersuites(ctx, "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256");

            const char *session_context = "YdbMtlsContext";
            SSL_CTX_set_session_id_context(ctx, (const unsigned char *)session_context, strlen(session_context));
        } else {
            SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
        }

        Ssl = TSslHelpers::ConstructSsl(ctx, Bio.get());
        SSL_set_accept_state(Ssl.get());
        return true;
    }

    TSslHolder<X509> GetServerCert(const TString& certPath, TString (*readFileFunc)(std::optional<TString>)) {
        TString certificate = readFileFunc(certPath);
        TSslHolder<BIO> bio(BIO_new_mem_buf(certificate.data(), certificate.size()));
        if (bio) {
            TSslHolder<X509> cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
            return cert;
        }
        return TSslHolder<X509>();
    }

     TSslHolder<EVP_PKEY> GetServerPrivateKey(const TString& keyPath, TString (*readFileFunc)(std::optional<TString>)) {
        TString privateKey = readFileFunc(keyPath);
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
                return -EIO;
            }
        }
        int res = SSL_accept(Ssl.get());
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

    static int Verify(int preverify, X509_STORE_CTX *ctx) {
        X509* current = X509_STORE_CTX_get_current_cert(ctx);
        if (!preverify) {
            int err = X509_STORE_CTX_get_error(ctx);
            int depth = X509_STORE_CTX_get_error_depth(ctx);
            char buffer[1024];
            X509_NAME_oneline(X509_get_subject_name(current), buffer, sizeof(buffer));
            if (err == X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT) {
                // Разрешаем self-signed сертификаты
                preverify = 1;
            } else {
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
            }
        }
        return preverify;
    }

    int GetSslHandshakeResult() {
        if (!Ssl) {
            return -1;
        }
        int ret = SSL_do_handshake(Ssl.get());
        return ret;
    }

    TSslHolder<X509> GetSslClientCert() {
        if (!Ssl) {
            return nullptr;
        }
        return TSslHolder<X509>(SSL_get_peer_certificate(Ssl.get()));
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
