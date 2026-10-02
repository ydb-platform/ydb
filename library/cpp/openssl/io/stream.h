#pragma once

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

class TOpenSslClientIO: public IInputStream, public IOutputStream {
public:
    struct TOptions {
        struct TVerifyCert {
            // Uses builtin certs.
            // Also uses default CA path /etc/ssl/certs/ - can be provided with debian package: ca-certificates.deb.
            // It can be expanded with ENV: SSL_CERT_DIR.
            TString Hostname_;
        };
        struct TClientCert {
            TString CertificateFile_;
            TString PrivateKeyFile_;
            TString PrivateKeyPassword_;
        };

        TMaybe<TVerifyCert> VerifyCert_;
        TMaybe<TClientCert> ClientCert_;
        // TODO - keys, cyphers, etc
    };

    TOpenSslClientIO(IInputStream* in, IOutputStream* out);
    TOpenSslClientIO(IInputStream* in, IOutputStream* out, const TOptions& options);
    ~TOpenSslClientIO() override;

private:
    void DoWrite(const void* buf, size_t len) override;
    size_t DoRead(void* buf, size_t len) override;

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

struct x509_store_st;

namespace NPrivate {
    struct TSslDestroy {
        static void Destroy(x509_store_st* x509) noexcept;
    };
}

using TOpenSslX509StorePtr = THolder<x509_store_st, NPrivate::TSslDestroy>;

TOpenSslX509StorePtr GetBuiltinOpenSslX509Store();

// Builds default Certificate Authority bundle which includes:
// - builtin "certs/cacert.pem" when SSL_CERT_FILE and SSL_CERT_DIR both unset
// - SSL_CERT_FILE when set or /usr/local/ssl/cert.pem when unset
// - SSL_CERT_DIR when set or /usr/local/ssl/certs when unset
TOpenSslX509StorePtr GetDefaultOpenSslX509Store();
