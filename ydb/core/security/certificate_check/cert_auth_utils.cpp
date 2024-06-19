#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>


#include <array>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include "cert_auth_utils.h"

namespace NKikimr {

std::vector<TCertificateAuthorizationParams> GetCertificateAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization &clientCertificateAuth) {
    std::vector<TCertificateAuthorizationParams> certAuthParams;
    certAuthParams.reserve(clientCertificateAuth.ClientCertificateDefinitionsSize());

    for (const auto& clientCertificateDefinition : clientCertificateAuth.GetClientCertificateDefinitions()) {
        TCertificateAuthorizationParams::TDN dn;
        for (const auto& term: clientCertificateDefinition.GetSubjectTerms()) {
            auto rdn = TCertificateAuthorizationParams::TRDN(term.GetShortName());
            for (const auto& value: term.GetValues()) {
                rdn.AddValue(value);
            }
            for (const auto& suffix: term.GetSuffixes()) {
                rdn.AddSuffix(suffix);
            }
            dn.AddRDN(std::move(rdn));
        }
        if (dn) {
            std::vector<TString> groups(clientCertificateDefinition.GetMemberGroups().cbegin(), clientCertificateDefinition.GetMemberGroups().cend());
            certAuthParams.emplace_back(std::move(dn), clientCertificateDefinition.GetRequireSameIssuer(), std::move(groups));
        }
    }

    return certAuthParams;
}

NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm MakeSubjectTerm(const TString& name, const TVector<TString>& values, const TVector<TString>& suffixes) {
    NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm term;
    term.SetShortName(name);
    for (const auto& val: values) {
        *term.MutableValues()->Add() = val;
    }
    for (const auto& suf: suffixes) {
        *term.MutableSuffixes()->Add() = suf;
    }
    return term;
}

namespace {

constexpr size_t maxCertSize = 17 * 1024;
constexpr size_t errStringBufSize = 1024;

#define CHECK(expr, msg) \
    if (!expr) { \
        char errString[errStringBufSize]{0}; \
        ERR_error_string_n(ERR_get_error(), errString, errStringBufSize); \
        std::cerr << msg << std::endl; \
        std::cerr << errString << std::endl; \
        exit (1); \
    }

template <auto fn>
struct deleter_from_fn {
    template <typename T>
    constexpr void operator()(T* arg) const {
        fn(arg);
    }
};

using PKeyPtr = std::unique_ptr<EVP_PKEY, deleter_from_fn<&::EVP_PKEY_free>>;
using X509Ptr = std::unique_ptr<X509, deleter_from_fn<&::X509_free>>;
using RSAPtr = std::unique_ptr<RSA, deleter_from_fn<&::RSA_free>>;
using BNPtr = std::unique_ptr<BIGNUM, deleter_from_fn<&::BN_free>>;
using BIOPtr = std::unique_ptr<BIO, deleter_from_fn<&::BIO_free>>;
using ExtPrt = std::unique_ptr<X509_EXTENSION, deleter_from_fn<&::X509_EXTENSION_free>>;
using X509REQPtr = std::unique_ptr<X509_REQ, deleter_from_fn<&::X509_REQ_free>>;
using X509StorePtr = std::unique_ptr<X509_STORE, deleter_from_fn<&::X509_STORE_free>>;
using X509StoreCtxPtr = std::unique_ptr<X509_STORE_CTX, deleter_from_fn<&::X509_STORE_CTX_free>>;

void FillExtFromProps0(X509V3_CTX* ctx, STACK_OF(X509_EXTENSION) *exts, const TProps& props);
void FillExtFromProps1(X509V3_CTX* ctx, STACK_OF(X509_EXTENSION) *exts, const TProps& props);

void VerifyCert(const X509Ptr& cert, const X509Ptr& cacert) {
     auto store = X509StorePtr(X509_STORE_new());
     X509_STORE_add_cert(store.get(), cacert.get());

     auto ctx = X509StoreCtxPtr(X509_STORE_CTX_new());
     X509_STORE_CTX_init(ctx.get(), store.get(), cert.get(), NULL);

     int result = X509_verify_cert(ctx.get());
     CHECK(result == 1, "VerifyCert failed.");
}

PKeyPtr GenerateKeys() {
  /* EVP_PKEY structure is for storing an algorithm-independent private key in memory. */
  auto pkey = PKeyPtr(EVP_PKEY_new());
  CHECK(pkey, "Unable to create pkey structure.");

  /* Generate a RSA key and assign it to pkey.
   * RSA_generate_key is deprecated.
   */
  auto bne = BNPtr(BN_new());
  CHECK(bne, "Unable to create bignum structure.");

  BN_set_word(bne.get(), RSA_F4);

  auto rsa = RSAPtr(RSA_new());
  CHECK(rsa, "Unable to create rsa structure.");

  RSA_generate_key_ex(rsa.get(), 2048, bne.get(), nullptr);

  EVP_PKEY_assign_RSA(pkey.get(), rsa.get());

  rsa.release(); // mem is grabbed by pkkey

  return std::move(pkey);
}

int FillNameFromProps(X509_NAME* name, const TProps& props) {
    if (!name) {
        return 1;
    }

    if (!props.Coutry.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_countryName, MBSTRING_ASC, (const unsigned char*)props.Coutry.c_str(), -1, -1, 0);
    }

    if (!props.State.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_stateOrProvinceName, MBSTRING_ASC, (const unsigned char*)props.State.c_str(), -1, -1, 0);
    }

    if (!props.Location.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_localityName, MBSTRING_ASC, (const unsigned char*)props.Location.c_str(), -1, -1, 0);
    }

    if (!props.Organization.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_organizationName, MBSTRING_ASC, (const unsigned char*)props.Organization.c_str(), -1, -1, 0);
    }

    if (!props.Unit.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_organizationalUnitName, MBSTRING_ASC, (const unsigned char*)props.Unit.c_str(), -1, -1, 0);
    }

    if (!props.CommonName.empty()) {
        X509_NAME_add_entry_by_txt(name, SN_commonName, MBSTRING_ASC, (const unsigned char*)props.CommonName.c_str(), -1, -1, 0);
    }

    return 0;
}

/* Generates a self-signed x509 certificate. */
X509Ptr GenerateSelfSignedCertificate(PKeyPtr& pkey, const TProps& props) {
    /* Allocate memory for the X509 structure. */
    auto x509 = X509Ptr(X509_new());
    CHECK(x509, "Unable to create X509 structure.");

    X509_set_version(x509.get(), 3);

    /* Set the serial number. */
    constexpr int Serial = 1;
    ASN1_INTEGER_set(X509_get_serialNumber(x509.get()), Serial);

    /* This certificate is valid from now until exactly one year from now. */
    X509_gmtime_adj(X509_get_notBefore(x509.get()), 0);
    X509_gmtime_adj(X509_get_notAfter(x509.get()), props.SecondsValid);

    int errNo = 0;
    /* Set the public key for our certificate. */
    errNo = X509_set_pubkey(x509.get(), pkey.get());
    CHECK(x509,"Error setting public key for X509 structure.");

    /* We want to copy the subject name to the issuer name. */
    X509_NAME* name = X509_get_subject_name(x509.get());
    CHECK(name,"Error getting subject name from X509 structure.");

    /* Set the country code and common name. */
    errNo = FillNameFromProps(name, props);
    CHECK(errNo == 0,"Error setting names.");

    errNo = X509_set_subject_name(x509.get(), name);
    CHECK(errNo, "Error setting subject name.");

    errNo = X509_set_issuer_name(x509.get(), name);
    CHECK(errNo, "Error setting issier name.");

    { // fill ext properties witch don't requere filled issuer indentity
        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, x509.get(), x509.get(), nullptr, nullptr, 0);

        STACK_OF (X509_EXTENSION)* exts = sk_X509_EXTENSION_new_null();
        CHECK(exts, "Unable to create STACK_OF X509_EXTENSION structure.");

        FillExtFromProps0(&ctx, exts, props);

        int count = X509v3_get_ext_count(exts);
        for (int i = 0; i < count; ++i) {
            auto tmpExt = X509v3_get_ext(exts, i);
            errNo = X509_add_ext(x509.get(), tmpExt, -1);
            CHECK(errNo, "Error adding ext value");
            tmpExt = nullptr;
        }

        sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
    }

    { // fill ext properties witch requere filled issuer indentity, NID_authority_key_identifier
        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, x509.get(), x509.get(), nullptr, nullptr, 0);

        STACK_OF (X509_EXTENSION)* exts = sk_X509_EXTENSION_new_null();
        CHECK(exts, "Unable to create STACK_OF X509_EXTENSION structure.");

        FillExtFromProps1(&ctx, exts, props);

        int count = X509v3_get_ext_count(exts);
        for (int i = 0; i < count; ++i) {
            auto tmpExt = X509v3_get_ext(exts, i);
            errNo = X509_add_ext(x509.get(), tmpExt, -1);
            CHECK(errNo, "Error adding ext value");
            tmpExt = nullptr;
        }

        sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
    }


    /* Actually sign the certificate with our key. */
    errNo = X509_sign(x509.get(), pkey.get(), EVP_sha1());
    CHECK(errNo, "Error signing certificate.");

    return std::move(x509);
}

std::string WriteAsPEM(PKeyPtr& pkey) {
    std::array<char, maxCertSize> buf{0};

    auto bio = BIOPtr(BIO_new(BIO_s_mem()));
    CHECK(bio, "Unable to create BIO structure.");

    PEM_write_bio_RSAPrivateKey(bio.get(), EVP_PKEY_get0_RSA(pkey.get()), nullptr, nullptr, 0, nullptr, nullptr);

    BIO_read(bio.get(), buf.data(), maxCertSize - 1);

    return std::string(buf.data());
}

std::string WriteAsPEM(X509Ptr& cert) {
    std::array<char, maxCertSize> buf{0};

    auto bio = BIOPtr(BIO_new(BIO_s_mem()));
    CHECK(bio, "Unable to create BIO structure.");

    PEM_write_bio_X509(bio.get(), cert.get());

    BIO_read(bio.get(), buf.data(), maxCertSize - 1);

    return std::string(buf.data());
}

X509Ptr ReadCertAsPEM(const std::string& cert) {
    auto bio = BIOPtr(BIO_new_mem_buf(cert.data(), cert.size()));
    CHECK(bio,"Unable to create BIO structure.");

    auto x509 = X509Ptr(PEM_read_bio_X509(bio.get(), NULL, NULL, NULL));
    CHECK(x509, "failed to load certificate");

    return std::move(x509);
}

PKeyPtr ReadPrivateKeyAsPEM(const std::string& key) {
    auto bio = BIOPtr(BIO_new_mem_buf(key.data(), key.size()));
    CHECK(bio,"Unable to create BIO structure.");

    auto pkey = PKeyPtr(PEM_read_bio_PrivateKey(bio.get(), NULL, NULL, NULL));
    CHECK(pkey, "failed to private key certificate");

    return std::move(pkey);
}

void add_ext(X509V3_CTX* ctx, STACK_OF(X509_EXTENSION)* exts, int nid, const char *value) {
  X509_EXTENSION *ex;
  ex = X509V3_EXT_conf_nid(NULL, ctx, nid, value);
  CHECK(ex, "failed to add ext value " << value);
  sk_X509_EXTENSION_push(exts, ex);
}

void FillExtFromProps0(X509V3_CTX* ctx, STACK_OF(X509_EXTENSION) *exts, const TProps& props) {
    CHECK(ctx, "no context is provided");

    if (!props.AltNames.empty()) {
        bool first = true;
        std::string concat;
        for (const auto& an: props.AltNames) {
            if (an.empty()) {
                continue;
            }
            if (!first) {
                concat += ",";
            } else {
                first = false;
            }

            concat += an;
        }
        add_ext(ctx, exts, NID_subject_alt_name, concat.c_str());
    }

    if (!props.BasicConstraints.empty()) {
        add_ext(ctx, exts, NID_basic_constraints, props.BasicConstraints.c_str());
    }

    if (!props.KeyUsage.empty()) {
        add_ext(ctx, exts, NID_key_usage, props.KeyUsage.c_str());
    }

    if (!props.ExtKeyUsage.empty()) {
        add_ext(ctx, exts, NID_ext_key_usage, props.ExtKeyUsage.c_str());
    }

    if (!props.SubjectKeyIdentifier.empty()) {
        add_ext(ctx, exts, NID_subject_key_identifier, props.SubjectKeyIdentifier.c_str());
    }

    if (!props.NsComment.empty()) {
        add_ext(ctx, exts, NID_netscape_comment, props.NsComment.c_str());
    }
}

void FillExtFromProps1(X509V3_CTX* ctx, STACK_OF(X509_EXTENSION) *exts, const TProps& props) {
    CHECK(ctx, "no context is provided");

    if (!props.AuthorityKeyIdentifier.empty()) {
        add_ext(ctx, exts, NID_authority_key_identifier, props.AuthorityKeyIdentifier.c_str());
    }
}

X509REQPtr GenerateRequest(PKeyPtr& pkey, const TProps& props) {
    auto request = X509REQPtr(X509_REQ_new());
    CHECK(request, "Error creating new X509_REQ structure.");

    int errNo = 0;

    errNo = X509_REQ_set_pubkey(request.get(), pkey.get());
    CHECK(errNo, "Error setting public key for X509_REQ structure.");

    X509_NAME* name = X509_REQ_get_subject_name(request.get());
    CHECK(name, "Error setting public key for X509_REQ structure.");

    errNo = FillNameFromProps(name, props);
    CHECK(errNo == 0,"Error setting names.");

    {
        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, nullptr, nullptr, request.get(), nullptr, 0);

        STACK_OF (X509_EXTENSION)* exts = sk_X509_EXTENSION_new_null();
        FillExtFromProps0(&ctx, exts, props);
        X509_REQ_add_extensions(request.get(), exts);
        sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
    }

    errNo = X509_REQ_sign(request.get(), pkey.get(), EVP_md5());
    CHECK(errNo, "Error MD5 signing X509_REQ structure.");

    return std::move(request);
}

X509Ptr SingRequest(X509REQPtr& request, X509Ptr& rootCert, PKeyPtr& rootKey, const TProps& props) {
    auto* pktmp = X509_REQ_get0_pubkey(request.get()); // X509_REQ_get0_pubkey returns the key, that shouldn't freed
    CHECK(pktmp, "Error unpacking public key from request.");

    int errNo = 0;
    errNo = X509_REQ_verify(request.get(), pktmp);
    CHECK(errNo > 0, "Error verification request signature.");

    auto x509 = X509Ptr(X509_new());
    CHECK(x509, "Unable to create X509 structure.");

    X509_set_version(x509.get(), 3);

    /* Set the serial number. */
    constexpr int Serial = 2;
    ASN1_INTEGER_set(X509_get_serialNumber(x509.get()), Serial);

    /* This certificate is valid from now until exactly one year from now. */
    X509_gmtime_adj(X509_get_notBefore(x509.get()), 0);
    X509_gmtime_adj(X509_get_notAfter(x509.get()), props.SecondsValid);

    X509_set_pubkey(x509.get(), pktmp);

    X509_set_subject_name(x509.get(), X509_REQ_get_subject_name(request.get()));
    X509_set_issuer_name(x509.get(), X509_get_subject_name(rootCert.get()));

    {

        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, rootCert.get(), x509.get(), request.get(), nullptr, 0);

        // fill ext properties witch don't requere filled issuer indentity, actually copy them from request
        STACK_OF(X509_EXTENSION)* exts = X509_REQ_get_extensions(request.get());
        CHECK(exts, "Unable to get STACK_OF X509_EXTENSION structure from request.");

        // fill ext properties witch requere filled issuer indentity, NID_authority_key_identifier
        FillExtFromProps1(&ctx, exts, props);

        int count = X509v3_get_ext_count(exts);
        for (int i = 0; i < count; ++i) {
            auto tmpExt = X509v3_get_ext(exts, i);
            errNo = X509_add_ext(x509.get(), tmpExt, -1);
            CHECK(errNo, "Error adding ext value");
            tmpExt = nullptr;
        }

        sk_X509_EXTENSION_pop_free(exts, X509_EXTENSION_free);
    }

    /* Actually sign the certificate with our key. */
    errNo = X509_sign(x509.get(), rootKey.get(), EVP_sha1());
    CHECK(errNo, "Error signing certificate.");

    pktmp = nullptr;

    return std::move(x509);
}

}

TCertAndKey GenerateCA(const TProps& props) {
    auto keys = GenerateKeys();
    auto cert = GenerateSelfSignedCertificate(keys, props);

    TCertAndKey result;
    result.Certificate = WriteAsPEM(cert);
    result.PrivateKey = WriteAsPEM(keys);

    return result;
}

TCertAndKey GenerateSignedCert(const TCertAndKey& rootCA, const TProps& props) {
    auto keys = GenerateKeys();
    auto request = GenerateRequest(keys, props);

    auto rootCert = ReadCertAsPEM(rootCA.Certificate);
    auto rootKey = ReadPrivateKeyAsPEM(rootCA.PrivateKey);
    auto cert = SingRequest(request, rootCert, rootKey, props); // NID_authority_key_identifier must see ca

    TCertAndKey result;
    result.Certificate = WriteAsPEM(cert);
    result.PrivateKey = WriteAsPEM(keys);

    return result;
}

void VerifyCert(const std::string& cert, const std::string& caCert) {
     auto rootCert = ReadCertAsPEM(caCert);
     auto otherCert = ReadCertAsPEM(cert);

     VerifyCert(otherCert, rootCert);
}

TProps TProps::AsCA() {
    TProps props;

    props.SecondsValid = 3*365 * 24 * 60 *60; // 3 years
    props.Coutry = "RU";
    props.State = "MSK";
    props.Location = "MSK";
    props.Organization = "YA";
    props.Unit = "UtTest";
    props.CommonName = "testCA";

    props.AltNames = {"IP:127.0.0.1", "DNS:localhost"};

    props.BasicConstraints = "critical,CA:TRUE";
    props.AuthorityKeyIdentifier = "keyid:always,issuer";
    props.SubjectKeyIdentifier = "hash";
    props.KeyUsage = "critical,keyCertSign,cRLSign";
    props.ExtKeyUsage = "";
    props.NsComment = "Test Generated Certificate for self-signed CA";

    return props;
}

TProps TProps::AsServer() {
    TProps props = AsCA();

    props.CommonName = "localhost";
    props.AltNames.push_back("DNS:*.yandex.ru");

    props.BasicConstraints = "CA:FALSE";
    props.AuthorityKeyIdentifier = "keyid,issuer";
    props.KeyUsage = "digitalSignature,nonRepudiation,keyEncipherment";
    props.ExtKeyUsage = "serverAuth";
    props.NsComment = "Test Generated Certificate for test Server";

    return props;
}

TProps TProps::AsClient() {
    TProps props = AsServer();

    props.ExtKeyUsage = "clientAuth";
    props.NsComment = "Test Generated Certificate for test Client";

    return props;
}

TProps TProps::AsClientServer() {
    TProps props = AsClient();

    props.ExtKeyUsage = "serverAuth,clientAuth";
    props.NsComment = "Test Generated Certificate for test Client/Server";

    return props;
}

TProps& TProps::WithValid(TDuration duration) { SecondsValid = duration.Seconds(); return *this; }

std::string GetCertificateFingerprint(const std::string& certificate) {
    const static std::string defaultFingerprint = "certificate";
    X509CertificateReader::X509Ptr x509Cert = X509CertificateReader::ReadCertAsPEM(certificate);
    if (!x509Cert) {
        return defaultFingerprint;
    }
    std::string fingerprint = X509CertificateReader::GetFingerprint(x509Cert);
    return (fingerprint.empty() ? defaultFingerprint : fingerprint);
}

}  //namespace NKikimr
