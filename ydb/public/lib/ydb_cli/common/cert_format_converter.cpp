#include "cert_format_converter.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

#include <util/generic/strbuf.h>
#include <util/stream/output.h>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pkcs12.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

namespace NYdb::NConsoleClient {

namespace {

class TOpenSslObjectFree {
public:
    TOpenSslObjectFree() = default;
    TOpenSslObjectFree(const TOpenSslObjectFree&) = default;
    TOpenSslObjectFree(TOpenSslObjectFree&&) = default;

    TOpenSslObjectFree& operator=(const TOpenSslObjectFree&) = default;
    TOpenSslObjectFree& operator=(TOpenSslObjectFree&&) = default;

    void operator()(X509* x509) const {
        X509_free(x509);
    }

    void operator()(BIO* bio) const {
        BIO_free(bio);
    }

    void operator()(EVP_PKEY* pkey) const {
        EVP_PKEY_free(pkey);
    }

    void operator()(PKCS12* p) const {
        PKCS12_free(p);
    }
};

struct TPasswordProcessor {
    TString Password;
    bool IsInteractiveMode;
    bool PasswordIsUsed = false;

    const TString& GetPassword() {
        if (PasswordIsUsed || Password || !IsInteractiveMode) {
            return Password;
        }
        PasswordIsUsed = true;
        Cerr << "Enter private key password: ";
        return Password = InputPassword();
    }

    static int PasswordCallback(char* buf, int size, int rwflag, void* userdata) {
        Y_UNUSED(rwflag);
        const TString& password = reinterpret_cast<TPasswordProcessor*>(userdata)->GetPassword();
        size_t s = 0;
        if (s = Min<size_t>(password.size(), size)) {
            memcpy(buf, password.c_str(), s);
        }
        return static_cast<int>(s);
    }
};

using TX509Ptr = std::unique_ptr<X509, TOpenSslObjectFree>;
using TBioPtr = std::unique_ptr<BIO, TOpenSslObjectFree>;
using TEvpPKeyPtr = std::unique_ptr<EVP_PKEY, TOpenSslObjectFree>;
using TPkcs12Ptr = std::unique_ptr<PKCS12, TOpenSslObjectFree>;

TString ToString(const TBioPtr& bio) {
    char* buf;
    size_t len = BIO_get_mem_data(bio.get(), &buf);
    return TString(buf, len);
}

TBioPtr ToBio(const TStringBuf& buf) {
    return TBioPtr(BIO_new_mem_buf(buf.data(), buf.size()));
}

TBioPtr CreateMemBio() {
    return TBioPtr(BIO_new(BIO_s_mem()));
}

TString GetLastOpenSslError() {
    TBioPtr bio = CreateMemBio();
    ERR_print_errors(bio.get());
    return ToString(bio);
}

TString GetOpenSslErrorText(int errorCode) {
    TStringBuilder result;
    result << "Code " << errorCode;
    if (TString err = GetLastOpenSslError()) {
        result << ": " << err;
    }
    return std::move(result);
}

TX509Ptr TryReadCertFromPEM(const TString& cert) {
    auto bio = ToBio(cert);
    return TX509Ptr(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr));
}

TEvpPKeyPtr TryReadPrivateKeyFromPEM(const TString& key, TPasswordProcessor& password) {
    auto bio = ToBio(key);
    return TEvpPKeyPtr(PEM_read_bio_PrivateKey(bio.get(), nullptr, &TPasswordProcessor::PasswordCallback, &password));
}

TString EncodeCertToPEM(const TX509Ptr& cert) {
    TBioPtr bio = CreateMemBio();

    if (int err = PEM_write_bio_X509(bio.get(), cert.get()); err <= 0) {
        throw yexception() << "Failed to write certificate: " << GetOpenSslErrorText(err);
    }
    return ToString(bio);
}

TString EncodePrivateKeyToPEM(const TEvpPKeyPtr& key) {
    TBioPtr bio = CreateMemBio();

    if (int err = PEM_write_bio_PrivateKey(bio.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr); err <= 0) {
        throw yexception() << "Failed to write private key: " << GetOpenSslErrorText(err);
    }
    return ToString(bio);
}

TPkcs12Ptr TryReadPkcs12(const TString& cert) {
    auto bio = ToBio(cert);
    TPkcs12Ptr pkcs12(d2i_PKCS12_bio(bio.get(), nullptr));
    return pkcs12;
}

void ParsePkcs12(const TPkcs12Ptr& pkcs12, TX509Ptr& cert, TEvpPKeyPtr& key, TPasswordProcessor& password) {
    X509* certPtr = nullptr;
    EVP_PKEY* keyPtr = nullptr;
    int err = PKCS12_parse(pkcs12.get(), password.Password.c_str(), &keyPtr, &certPtr, nullptr);
    if (err == PKCS12_R_MAC_VERIFY_FAILURE && !password.Password) { // Password in not correct, try to ask password from user (if possible)
        if (password.GetPassword()) { // Got new password
            err = PKCS12_parse(pkcs12.get(), password.Password.c_str(), &keyPtr, &certPtr, nullptr);
        }
    }
    if (err <= 0) {
        return;
    }
    if (certPtr) {
        cert.reset(certPtr);
    }
    if (keyPtr) {
        key.reset(keyPtr);
    }
}

} // anonymous

std::pair<TString, TString> ConvertCertToPEM(
    const TString& certificate,
    const TString& privateKey,
    const TString& privateKeyPassword,
    bool isInteractiveMode
)
{
    TPasswordProcessor password = {
        .Password = privateKeyPassword,
        .IsInteractiveMode = isInteractiveMode,
    };

    TString certResult, privateKeyResult;
    TX509Ptr cert;
    TEvpPKeyPtr key;
    if (certificate) {
        cert = TryReadCertFromPEM(certificate);
    }

    if (!key && privateKey) {
        key = TryReadPrivateKeyFromPEM(privateKey, password);
    }

    if (!key && certificate) { // There may be a concatenated file with both cert and key
        key = TryReadPrivateKeyFromPEM(certificate, password);
    }

    if (TPkcs12Ptr pkcs12 = TryReadPkcs12(certificate)) {
        ParsePkcs12(pkcs12, cert, key, password);
    }

    if (cert && key) {
        return {EncodeCertToPEM(cert), EncodePrivateKeyToPEM(key)};
    }
    throw yexception() << "Failed to parse client certificate and/or private key";
}

} // namespace NYdb::NConsoleClient
