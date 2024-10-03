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
    OpensslCheckErrorAndThrow(EVP_EncryptInit_ex(EncryptionCtx.get(), nullptr, nullptr, key, iv), "EVP_EncryptInit_ex");
}

AES128::AES128()
    : EncryptionCtx(EVP_CIPHER_CTX_new())
    , DecryptionCtx(EVP_CIPHER_CTX_new()) {
    if (!EncryptionCtx || !DecryptionCtx) {
        Y_ABORT("Failed to create EVP_CIPHER_CTX");
    }

    OpensslCheckErrorAndThrow(EVP_EncryptInit_ex(EncryptionCtx.get(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr), "EVP_EncryptInit_ex");
    OpensslCheckErrorAndThrow(EVP_DecryptInit_ex(DecryptionCtx.get(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr), "EVP_DecryptInit_ex");
}

void AES128::InitDecrypt() {
    unsigned char* key = nullptr;
    unsigned char* iv = nullptr;
    InitKeyAndIV(key, iv);
    OpensslCheckErrorAndThrow(EVP_DecryptInit_ex(DecryptionCtx.get(), nullptr, nullptr, key, iv), "EVP_DecryptInit_ex");
}

void AES128::SetKey(const std::vector<ui8>& key) {
    Key = key;
}

void AES128::SetIV(const std::vector<ui8>& iv) {
    IV = iv;
}

size_t AES128::Encrypt(const ui8* plaintext, ui8* ciphertext, ui8 tag[16], size_t len) {
    InitEncrypt();

    size_t sz = 0;
    int outLen = 0;

    OpensslCheckErrorAndThrow(EVP_EncryptUpdate(EncryptionCtx.get(), ciphertext, &outLen, plaintext, len), "EVP_EncryptUpdate");
    sz += outLen;

    OpensslCheckErrorAndThrow(EVP_EncryptFinal_ex(EncryptionCtx.get(), ciphertext + outLen, &outLen), "EVP_EncryptFinal_ex");
    sz += outLen;

    OpensslCheckErrorAndThrow(EVP_CIPHER_CTX_ctrl(EncryptionCtx.get(), EVP_CTRL_AEAD_GET_TAG, 16, tag), "EVP_CIPHER_CTX_ctrl(EVP_CTRL_AEAD_GET_TAG)");

    OpensslCheckErrorAndThrow(EVP_CIPHER_CTX_reset(EncryptionCtx.get()), "EVP_CIPHER_CTX_reset");

    return sz;
}

void AES128::Decrypt(const ui8* ciphertext, ui8* plaintext, ui8 tag[16], size_t len) {
    InitDecrypt();

    int outLen = 0;
    
    OpensslCheckErrorAndThrow(EVP_CIPHER_CTX_ctrl(DecryptionCtx.get(), EVP_CTRL_AEAD_SET_TAG, 16, tag), "EVP_CIPHER_CTX_ctrl");

    OpensslCheckErrorAndThrow(EVP_DecryptUpdate(DecryptionCtx.get(), plaintext, &outLen, ciphertext, len), "EVP_DecryptUpdate");

    OpensslCheckErrorAndThrow(EVP_DecryptFinal_ex(DecryptionCtx.get(), plaintext + outLen, &outLen), "EVP_DecryptFinal_ex");

    OpensslCheckErrorAndThrow(EVP_CIPHER_CTX_reset(DecryptionCtx.get()), "EVP_CIPHER_CTX_reset");
}
