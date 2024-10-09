#include "aes128.h"

void AES128::InitKeyAndIV(unsigned char*& key, unsigned char*& iv) {
    if (!Key.empty()) {
        key = &Key[0];
    }
    if (!IV.empty()) {
        iv = &IV[0];
    }
}

void AES128::InitEncrypt() {
    unsigned char* key = nullptr;
    unsigned char* iv = nullptr;
    InitKeyAndIV(key, iv);
    OpensslCheckError(EVP_EncryptInit_ex(EncryptionCtx.get(), nullptr, nullptr, key, iv), "EVP_EncryptInit_ex");
}

AES128::AES128()
    : EncryptionCtx(EVP_CIPHER_CTX_new())
    , DecryptionCtx(EVP_CIPHER_CTX_new()) {
    if (!EncryptionCtx || !DecryptionCtx) {
        Y_ABORT("Failed to create EVP_CIPHER_CTX");
    }

    OpensslCheckError(EVP_EncryptInit_ex(EncryptionCtx.get(), EVP_aes_128_ctr(), nullptr, nullptr, nullptr), "EVP_EncryptInit_ex");
    OpensslCheckError(EVP_DecryptInit_ex(DecryptionCtx.get(), EVP_aes_128_ctr(), nullptr, nullptr, nullptr), "EVP_DecryptInit_ex");
}

void AES128::InitDecrypt() {
    unsigned char* key = nullptr;
    unsigned char* iv = nullptr;
    InitKeyAndIV(key, iv);
    OpensslCheckError(EVP_DecryptInit_ex(DecryptionCtx.get(), nullptr, nullptr, key, iv), "EVP_DecryptInit_ex");
}

void AES128::SetKeyAndIV(const std::vector<ui8>& key, const std::vector<ui8>& iv) {
    Key = key;
    IV = iv;
}

size_t AES128::Encrypt(const ui8* plaintext, ui8* ciphertext, size_t len) {
    InitEncrypt();

    size_t sz = 0;
    int outLen = 0;

    OpensslCheckError(EVP_EncryptUpdate(EncryptionCtx.get(), ciphertext, &outLen, plaintext, len), "EVP_EncryptUpdate");
    sz += outLen;

    OpensslCheckError(EVP_EncryptFinal_ex(EncryptionCtx.get(), ciphertext + outLen, &outLen), "EVP_EncryptFinal_ex");
    sz += outLen;

    OpensslCheckError(EVP_CIPHER_CTX_reset(EncryptionCtx.get()), "EVP_CIPHER_CTX_reset");

    return sz;
}

void AES128::Decrypt(const ui8* ciphertext, ui8* plaintext, size_t len) {
    InitDecrypt();

    int outLen = 0;

    OpensslCheckError(EVP_DecryptUpdate(DecryptionCtx.get(), plaintext, &outLen, ciphertext, len), "EVP_DecryptUpdate");

    OpensslCheckError(EVP_DecryptFinal_ex(DecryptionCtx.get(), plaintext + outLen, &outLen), "EVP_DecryptFinal_ex");

    OpensslCheckError(EVP_CIPHER_CTX_reset(DecryptionCtx.get()), "EVP_CIPHER_CTX_reset");
}
