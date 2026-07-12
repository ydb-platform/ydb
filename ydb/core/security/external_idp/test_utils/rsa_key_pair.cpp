#include "rsa_key_pair.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <openssl/bio.h>
#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>

namespace NKikimr::NExternalIdpTestUtils {

TRsaKeyPair GenerateRsaKeyPair() {
    EVP_PKEY* pkey = EVP_PKEY_new();
    RSA* rsa = RSA_new();
    BIGNUM* bn = BN_new();
    BN_set_word(bn, RSA_F4);
    RSA_generate_key_ex(rsa, 2048, bn, nullptr);
    EVP_PKEY_assign_RSA(pkey, rsa);
    BN_free(bn);

    auto extractPem = [](EVP_PKEY* k, bool priv) -> TString {
        BIO* bio = BIO_new(BIO_s_mem());
        if (priv) {
            PEM_write_bio_PrivateKey(bio, k, nullptr, nullptr, 0, nullptr, nullptr);
        } else {
            PEM_write_bio_PUBKEY(bio, k);
        }
        char* buf = nullptr;
        long len = BIO_get_mem_data(bio, &buf);
        TString result(buf, len);
        BIO_free(bio);
        return result;
    };
    TString privPem = extractPem(pkey, true);
    TString pubPem = extractPem(pkey, false);

    X509* x509 = X509_new();
    ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
    X509_gmtime_adj(X509_get_notBefore(x509), 0);
    X509_gmtime_adj(X509_get_notAfter(x509), 365*24*3600);
    X509_set_pubkey(x509, pkey);
    X509_NAME* name = X509_get_subject_name(x509);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (unsigned char*)"test", -1, -1, 0);
    X509_set_issuer_name(x509, name);
    X509_sign(x509, pkey, EVP_sha256());
    unsigned char* derBuf = nullptr;
    int derLen = i2d_X509(x509, &derBuf);
    TString x5c = Base64Encode(TStringBuf(reinterpret_cast<char*>(derBuf), derLen));
    OPENSSL_free(derBuf);
    X509_free(x509);

    EVP_PKEY_free(pkey);
    return {privPem, pubPem, x5c};
}

} // namespace NKikimr::NExternalIdpTestUtils
