#include <memory>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <sstream>
#include <string>
#include <vector>

#include "util/stream/output.h"
#include "util/system/yassert.h"

class TOpenSslObjectFree {
public:
    TOpenSslObjectFree() = default;
    TOpenSslObjectFree(const TOpenSslObjectFree&) = default;
    TOpenSslObjectFree(TOpenSslObjectFree&&) = default;

    void operator()(EVP_CIPHER_CTX* ctx) const {
        EVP_CIPHER_CTX_free(ctx);
    }

    void operator()(BIO* bio) const {
        BIO_free(bio);
    }
};

using TCipherCtxPtr = std::unique_ptr<EVP_CIPHER_CTX, TOpenSslObjectFree>;
using TBioPtr = std::unique_ptr<BIO, TOpenSslObjectFree>;

inline void OpensslCheckErrorAndThrow(int callResult, const char* action) {
    if (callResult <= 0) {
        ERR_print_errors_fp(stderr);
        // flush stderr
        fflush(stderr);
        unsigned long errCode = ERR_get_error();
        char errString[120];
        ERR_error_string_n(errCode, errString, sizeof(errString));
        Y_ABORT("%s failed with code %d, error message: %s", action, callResult, errString);
    }
}

class AES128 {

    void InitKeyAndIV(unsigned char*& key, unsigned char*& iv);

    void InitEncrypt();

    void InitDecrypt();

public:

    AES128();

    void SetKey(const std::vector<ui8>& key);

    void SetIV(const std::vector<ui8>& iv);

    size_t Encrypt(const ui8* plaintext, ui8* ciphertext, ui8 tag[16], size_t len);

    void Decrypt(const ui8* ciphertext, ui8* plaintext, ui8 tag[16], size_t len);

private:
    std::vector<ui8> Key;
    std::vector<ui8> IV;
    TCipherCtxPtr EncryptionCtx;
    TCipherCtxPtr DecryptionCtx;
};
