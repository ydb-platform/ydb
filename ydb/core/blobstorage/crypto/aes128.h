#include <memory>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <vector>

#include <util/system/yassert.h>

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

inline void OpensslCheckError(int callResult, const char* action) {
    if (callResult <= 0) {
        unsigned long errCode = ERR_get_error();
        char errString[120];
        ERR_error_string_n(errCode, errString, sizeof(errString));
        Y_ABORT("%s failed with code %d, error message: %s", action, callResult, errString);
    }
}

class AES128 {
public:
    AES128();

    void SetKeyAndIV(const std::vector<ui8>& key, const std::vector<ui8>& iv);

    size_t Encrypt(const ui8* plaintext, ui8* ciphertext, size_t len);

    void Decrypt(const ui8* ciphertext, ui8* plaintext, size_t len);
private:
    void InitKeyAndIV(unsigned char*& key, unsigned char*& iv);

    void InitEncrypt();

    void InitDecrypt();
    
    std::vector<ui8> Key;
    std::vector<ui8> IV;
    TCipherCtxPtr EncryptionCtx;
    TCipherCtxPtr DecryptionCtx;
};
