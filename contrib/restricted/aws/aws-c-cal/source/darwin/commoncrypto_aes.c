/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/private/symmetric_cipher_priv.h>

#include <CommonCrypto/CommonCryptor.h>
#include <CommonCrypto/CommonHMAC.h>
#include <CommonCrypto/CommonSymmetricKeywrap.h>

#if !defined(AWS_APPSTORE_SAFE)
/* CommonCrypto does not offer public APIs for doing AES GCM.
 * There are private APIs for doing it (CommonCryptoSPI.h), but App Store
 * submissions that reference these private symbols will be rejected. */

#    define SUPPORT_AES_GCM_VIA_SPI 1
#    include "common_cryptor_spi.h"

#    if (defined(__MAC_OS_X_VERSION_MAX_ALLOWED) && (__MAC_OS_X_VERSION_MAX_ALLOWED >= 101300 /* macOS 10.13 */)) ||   \
        (defined(__IPHONE_OS_VERSION_MAX_ALLOWED) && (__IPHONE_OS_VERSION_MAX_ALLOWED >= 110000 /* iOS v11 */))
#        define USE_LATEST_CRYPTO_API 1
#    endif
#endif

struct cc_aes_cipher {
    struct aws_symmetric_cipher cipher_base;
    struct _CCCryptor *encryptor_handle;
    struct _CCCryptor *decryptor_handle;
    struct aws_byte_buf working_buffer;
};

static int s_encrypt(struct aws_symmetric_cipher *cipher, struct aws_byte_cursor input, struct aws_byte_buf *out) {
    /* allow for a padded block by making sure we have at least a block of padding reserved. */
    size_t required_buffer_space = input.len + cipher->block_size - 1;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, required_buffer_space)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    size_t available_write_space = out->capacity - out->len;
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    size_t len_written = 0;
    CCStatus status = CCCryptorUpdate(
        cc_cipher->encryptor_handle, input.ptr, input.len, out->buffer + out->len, available_write_space, &len_written);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += len_written;
    return AWS_OP_SUCCESS;
}

static int s_decrypt(struct aws_symmetric_cipher *cipher, struct aws_byte_cursor input, struct aws_byte_buf *out) {
    /* allow for a padded block by making sure we have at least a block of padding reserved. */
    size_t required_buffer_space = input.len + cipher->block_size - 1;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, required_buffer_space)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    size_t available_write_space = out->capacity - out->len;
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    size_t len_written = 0;
    CCStatus status = CCCryptorUpdate(
        cc_cipher->decryptor_handle, input.ptr, input.len, out->buffer + out->len, available_write_space, &len_written);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += len_written;
    return AWS_OP_SUCCESS;
}

static int s_finalize_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    /* in CBC mode, this will pad the final block from the previous encrypt call, or do nothing
     * if we were already on a block boundary. In CTR mode this will do nothing. */
    size_t required_buffer_space = cipher->block_size;
    size_t len_written = 0;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, required_buffer_space)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    size_t available_write_space = out->capacity - out->len;
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    CCStatus status =
        CCCryptorFinal(cc_cipher->encryptor_handle, out->buffer + out->len, available_write_space, &len_written);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += len_written;
    return AWS_OP_SUCCESS;
}

static int s_finalize_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    /* in CBC mode, this will pad the final block from the previous encrypt call, or do nothing
     * if we were already on a block boundary. In CTR mode this will do nothing. */
    size_t required_buffer_space = cipher->block_size;
    size_t len_written = 0;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, required_buffer_space)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    size_t available_write_space = out->capacity - out->len;
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    CCStatus status =
        CCCryptorFinal(cc_cipher->decryptor_handle, out->buffer + out->len, available_write_space, &len_written);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += len_written;
    return AWS_OP_SUCCESS;
}

static int s_initialize_cbc_cipher_materials(
    struct cc_aes_cipher *cc_cipher,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {
    if (!cc_cipher->cipher_base.key.len) {
        if (key) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, *key);
        } else {
            aws_byte_buf_init(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, AWS_AES_256_KEY_BYTE_LEN);
            aws_symmetric_cipher_generate_key(AWS_AES_256_KEY_BYTE_LEN, &cc_cipher->cipher_base.key);
        }
    }

    if (!cc_cipher->cipher_base.iv.len) {
        if (iv) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, *iv);
        } else {
            aws_byte_buf_init(
                &cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
            aws_symmetric_cipher_generate_initialization_vector(
                AWS_AES_256_CIPHER_BLOCK_SIZE, false, &cc_cipher->cipher_base.iv);
        }
    }

    CCCryptorStatus status = CCCryptorCreateWithMode(
        kCCEncrypt,
        kCCModeCBC,
        kCCAlgorithmAES,
        ccPKCS7Padding,
        cc_cipher->cipher_base.iv.buffer,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        0,
        &cc_cipher->encryptor_handle);

    status |= CCCryptorCreateWithMode(
        kCCDecrypt,
        kCCModeCBC,
        kCCAlgorithmAES,
        ccPKCS7Padding,
        cc_cipher->cipher_base.iv.buffer,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        0,
        &cc_cipher->decryptor_handle);

    return status == kCCSuccess ? AWS_OP_SUCCESS : aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
}

static int s_reset(struct aws_symmetric_cipher *cipher) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    if (cc_cipher->encryptor_handle) {
        CCCryptorRelease(cc_cipher->encryptor_handle);
        cc_cipher->encryptor_handle = NULL;
    }

    if (cc_cipher->decryptor_handle) {
        CCCryptorRelease(cc_cipher->decryptor_handle);
        cc_cipher->decryptor_handle = NULL;
    }

    aws_byte_buf_secure_zero(&cc_cipher->working_buffer);

    return AWS_OP_SUCCESS;
}

static void s_destroy(struct aws_symmetric_cipher *cipher) {
    aws_byte_buf_clean_up_secure(&cipher->key);
    aws_byte_buf_clean_up_secure(&cipher->iv);
    aws_byte_buf_clean_up_secure(&cipher->tag);
    aws_byte_buf_clean_up_secure(&cipher->aad);

    s_reset(cipher);

    struct cc_aes_cipher *cc_cipher = cipher->impl;
    aws_byte_buf_clean_up_secure(&cc_cipher->working_buffer);

    aws_mem_release(cipher->allocator, cc_cipher);
}

static int s_cbc_reset(struct aws_symmetric_cipher *cipher) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    int ret_val = s_reset(cipher);

    if (ret_val == AWS_OP_SUCCESS) {
        ret_val = s_initialize_cbc_cipher_materials(cc_cipher, NULL, NULL);
    }

    return ret_val;
}

static struct aws_symmetric_cipher_vtable s_aes_cbc_vtable = {
    .finalize_decryption = s_finalize_decryption,
    .finalize_encryption = s_finalize_encryption,
    .decrypt = s_decrypt,
    .encrypt = s_encrypt,
    .provider = "CommonCrypto",
    .alg_name = "AES-CBC 256",
    .destroy = s_destroy,
    .reset = s_cbc_reset,
};

struct aws_symmetric_cipher *aws_aes_cbc_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {
    struct cc_aes_cipher *cc_cipher = aws_mem_calloc(allocator, 1, sizeof(struct cc_aes_cipher));
    cc_cipher->cipher_base.allocator = allocator;
    cc_cipher->cipher_base.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cc_cipher->cipher_base.key_length_bits = AWS_AES_256_KEY_BIT_LEN;
    cc_cipher->cipher_base.impl = cc_cipher;
    cc_cipher->cipher_base.vtable = &s_aes_cbc_vtable;

    if (s_initialize_cbc_cipher_materials(cc_cipher, key, iv) != AWS_OP_SUCCESS) {
        s_destroy(&cc_cipher->cipher_base);
        return NULL;
    }

    cc_cipher->cipher_base.good = true;
    cc_cipher->cipher_base.key_length_bits = AWS_AES_256_KEY_BIT_LEN;

    return &cc_cipher->cipher_base;
}

static int s_initialize_ctr_cipher_materials(
    struct cc_aes_cipher *cc_cipher,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {
    if (!cc_cipher->cipher_base.key.len) {
        if (key) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, *key);
        } else {
            aws_byte_buf_init(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, AWS_AES_256_KEY_BYTE_LEN);
            aws_symmetric_cipher_generate_key(AWS_AES_256_KEY_BYTE_LEN, &cc_cipher->cipher_base.key);
        }
    }

    if (!cc_cipher->cipher_base.iv.len) {
        if (iv) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, *iv);
        } else {
            aws_byte_buf_init(
                &cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
            aws_symmetric_cipher_generate_initialization_vector(
                AWS_AES_256_CIPHER_BLOCK_SIZE, true, &cc_cipher->cipher_base.iv);
        }
    }

    CCCryptorStatus status = CCCryptorCreateWithMode(
        kCCEncrypt,
        kCCModeCTR,
        kCCAlgorithmAES,
        ccNoPadding,
        cc_cipher->cipher_base.iv.buffer,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        kCCModeOptionCTR_BE,
        &cc_cipher->encryptor_handle);

    status |= CCCryptorCreateWithMode(
        kCCDecrypt,
        kCCModeCTR,
        kCCAlgorithmAES,
        ccNoPadding,
        cc_cipher->cipher_base.iv.buffer,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        kCCModeOptionCTR_BE,
        &cc_cipher->decryptor_handle);

    return status == kCCSuccess ? AWS_OP_SUCCESS : aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
}

static int s_ctr_reset(struct aws_symmetric_cipher *cipher) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    int ret_val = s_reset(cipher);

    if (ret_val == AWS_OP_SUCCESS) {
        ret_val = s_initialize_ctr_cipher_materials(cc_cipher, NULL, NULL);
    }

    return ret_val;
}

static struct aws_symmetric_cipher_vtable s_aes_ctr_vtable = {
    .finalize_decryption = s_finalize_decryption,
    .finalize_encryption = s_finalize_encryption,
    .decrypt = s_decrypt,
    .encrypt = s_encrypt,
    .provider = "CommonCrypto",
    .alg_name = "AES-CTR 256",
    .destroy = s_destroy,
    .reset = s_ctr_reset,
};

struct aws_symmetric_cipher *aws_aes_ctr_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {
    struct cc_aes_cipher *cc_cipher = aws_mem_calloc(allocator, 1, sizeof(struct cc_aes_cipher));
    cc_cipher->cipher_base.allocator = allocator;
    cc_cipher->cipher_base.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cc_cipher->cipher_base.impl = cc_cipher;
    cc_cipher->cipher_base.vtable = &s_aes_ctr_vtable;

    if (s_initialize_ctr_cipher_materials(cc_cipher, key, iv) != AWS_OP_SUCCESS) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        s_destroy(&cc_cipher->cipher_base);
        return NULL;
    }

    cc_cipher->cipher_base.good = true;
    cc_cipher->cipher_base.key_length_bits = AWS_AES_256_KEY_BIT_LEN;

    return &cc_cipher->cipher_base;
}

#ifdef SUPPORT_AES_GCM_VIA_SPI

/*
 * Note that CCCryptorGCMFinal is deprecated in Mac 10.13. It also doesn't compare the tag with expected tag
 * https://opensource.apple.com/source/CommonCrypto/CommonCrypto-60118.1.1/include/CommonCryptorSPI.h.auto.html
 */
static CCStatus s_cc_crypto_gcm_finalize(struct _CCCryptor *encryptor_handle, uint8_t *buffer, size_t tag_length) {
#    ifdef USE_LATEST_CRYPTO_API
    if (__builtin_available(macOS 10.13, iOS 11.0, *)) {
        return CCCryptorGCMFinalize(encryptor_handle, buffer, tag_length);
    } else {
/* We would never hit this branch for newer macOS and iOS versions because of the __builtin_available check, so we can
 * suppress the compiler warning. */
#        pragma clang diagnostic push
#        pragma clang diagnostic ignored "-Wdeprecated-declarations"
        return CCCryptorGCMFinal(encryptor_handle, buffer, &tag_length);
#        pragma clang diagnostic pop
    }
#    else
    return CCCryptorGCMFinal(encryptor_handle, buffer, &tag_length);

#    endif
}

static CCCryptorStatus s_cc_cryptor_gcm_set_iv(struct _CCCryptor *encryptor_handle, uint8_t *buffer, size_t length) {
#    ifdef USE_LATEST_CRYPTO_API
    if (__builtin_available(macOS 10.13, iOS 11.0, *)) {
        return CCCryptorGCMSetIV(encryptor_handle, buffer, length);
    } else {
/* We would never hit this branch for newer macOS and iOS versions because of the __builtin_available check, so we can
 * suppress the compiler warning. */
#        pragma clang diagnostic push
#        pragma clang diagnostic ignored "-Wdeprecated-declarations"
        return CCCryptorGCMAddIV(encryptor_handle, buffer, length);
#        pragma clang diagnostic pop
    }
#    else
    return CCCryptorGCMAddIV(encryptor_handle, buffer, length);
#    endif
}

static int s_finalize_gcm_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    (void)out;

    /* user specification takes precedence. If its wrong its wrong */
    if (!cipher->tag.len) {
        aws_byte_buf_init(&cipher->tag, cipher->allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
    }

    struct cc_aes_cipher *cc_cipher = cipher->impl;

    size_t tag_length = AWS_AES_256_CIPHER_BLOCK_SIZE;
    CCStatus status = s_cc_crypto_gcm_finalize(cc_cipher->encryptor_handle, cipher->tag.buffer, tag_length);
    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    cipher->tag.len = tag_length;
    return AWS_OP_SUCCESS;
}

static int s_finalize_gcm_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    (void)out;

    struct cc_aes_cipher *cc_cipher = cipher->impl;

    size_t tag_length = AWS_AES_256_CIPHER_BLOCK_SIZE;
    CCStatus status = s_cc_crypto_gcm_finalize(cc_cipher->encryptor_handle, cipher->tag.buffer, tag_length);
    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

static int s_initialize_gcm_cipher_materials(
    struct cc_aes_cipher *cc_cipher,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv,
    const struct aws_byte_cursor *aad,
    const struct aws_byte_cursor *tag) {
    if (!cc_cipher->cipher_base.key.len) {
        if (key) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, *key);
        } else {
            aws_byte_buf_init(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, AWS_AES_256_KEY_BYTE_LEN);
            aws_symmetric_cipher_generate_key(AWS_AES_256_KEY_BYTE_LEN, &cc_cipher->cipher_base.key);
        }
    }

    if (!cc_cipher->cipher_base.iv.len) {
        if (iv) {
            aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, *iv);
        } else {
            /* GCM IVs are kind of a hidden implementation detail. 4 are reserved by the system for long running stream
             * blocks. */
            /* This is because there's a GMAC attached to the cipher (that's what tag is for). For that to work, it has
             * to control the actual counter */
            aws_byte_buf_init(
                &cc_cipher->cipher_base.iv, cc_cipher->cipher_base.allocator, AWS_AES_256_CIPHER_BLOCK_SIZE - 4);
            aws_symmetric_cipher_generate_initialization_vector(
                AWS_AES_256_CIPHER_BLOCK_SIZE - 4, false, &cc_cipher->cipher_base.iv);
        }
    }

    if (aad && aad->len) {
        aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.aad, cc_cipher->cipher_base.allocator, *aad);
    }

    if (tag && tag->len) {
        aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.tag, cc_cipher->cipher_base.allocator, *tag);
    }

    CCCryptorStatus status = CCCryptorCreateWithMode(
        kCCEncrypt,
        kCCModeGCM,
        kCCAlgorithmAES,
        ccNoPadding,
        NULL,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        kCCModeOptionCTR_BE,
        &cc_cipher->encryptor_handle);

    if (status != kCCSuccess) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    status = s_cc_cryptor_gcm_set_iv(
        cc_cipher->encryptor_handle, cc_cipher->cipher_base.iv.buffer, cc_cipher->cipher_base.iv.len);

    if (status != kCCSuccess) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (cc_cipher->cipher_base.aad.len) {
        status = CCCryptorGCMAddAAD(
            cc_cipher->encryptor_handle, cc_cipher->cipher_base.aad.buffer, cc_cipher->cipher_base.aad.len);

        if (status != kCCSuccess) {
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    }

    status = CCCryptorCreateWithMode(
        kCCDecrypt,
        kCCModeGCM,
        kCCAlgorithmAES,
        ccNoPadding,
        NULL,
        cc_cipher->cipher_base.key.buffer,
        cc_cipher->cipher_base.key.len,
        NULL,
        0,
        0,
        kCCModeOptionCTR_BE,
        &cc_cipher->decryptor_handle);

    if (status != kCCSuccess) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    status = s_cc_cryptor_gcm_set_iv(
        cc_cipher->decryptor_handle, cc_cipher->cipher_base.iv.buffer, cc_cipher->cipher_base.iv.len);

    if (status != kCCSuccess) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (cc_cipher->cipher_base.aad.len) {
        status = CCCryptorGCMAddAAD(
            cc_cipher->decryptor_handle, cc_cipher->cipher_base.aad.buffer, cc_cipher->cipher_base.aad.len);
    }

    if (status != kCCSuccess) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

static int s_gcm_reset(struct aws_symmetric_cipher *cipher) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    int ret_val = s_reset(cipher);

    if (ret_val == AWS_OP_SUCCESS) {
        ret_val = s_initialize_gcm_cipher_materials(cc_cipher, NULL, NULL, NULL, NULL);
    }

    return ret_val;
}

static struct aws_symmetric_cipher_vtable s_aes_gcm_vtable = {
    .finalize_decryption = s_finalize_gcm_decryption,
    .finalize_encryption = s_finalize_gcm_encryption,
    .decrypt = s_decrypt,
    .encrypt = s_encrypt,
    .provider = "CommonCrypto",
    .alg_name = "AES-GCM 256",
    .destroy = s_destroy,
    .reset = s_gcm_reset,
};

struct aws_symmetric_cipher *aws_aes_gcm_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv,
    const struct aws_byte_cursor *aad,
    const struct aws_byte_cursor *tag) {
    struct cc_aes_cipher *cc_cipher = aws_mem_calloc(allocator, 1, sizeof(struct cc_aes_cipher));
    cc_cipher->cipher_base.allocator = allocator;
    cc_cipher->cipher_base.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cc_cipher->cipher_base.impl = cc_cipher;
    cc_cipher->cipher_base.vtable = &s_aes_gcm_vtable;

    if (s_initialize_gcm_cipher_materials(cc_cipher, key, iv, aad, tag) != AWS_OP_SUCCESS) {
        s_destroy(&cc_cipher->cipher_base);
        return NULL;
    }

    cc_cipher->cipher_base.good = true;
    cc_cipher->cipher_base.key_length_bits = AWS_AES_256_KEY_BIT_LEN;

    return &cc_cipher->cipher_base;
}

#else /* !SUPPORT_AES_GCM_VIA_SPI */

struct aws_symmetric_cipher *aws_aes_gcm_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv,
    const struct aws_byte_cursor *aad,
    const struct aws_byte_cursor *tag) {

    (void)allocator;
    (void)key;
    (void)iv;
    (void)aad;
    (void)tag;
    aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
    return NULL;
}

#endif /* SUPPORT_AES_GCM_VIA_SPI */

static int s_keywrap_encrypt_decrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor input,
    struct aws_byte_buf *out) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;
    return aws_byte_buf_append_dynamic(&cc_cipher->working_buffer, &input);
}

static int s_finalize_keywrap_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    if (cc_cipher->working_buffer.len == 0) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    size_t output_buffer_len = cipher->block_size + cc_cipher->working_buffer.len;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, output_buffer_len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    CCCryptorStatus status = CCSymmetricKeyWrap(
        kCCWRAPAES,
        CCrfc3394_iv,
        CCrfc3394_ivLen,
        cipher->key.buffer,
        cipher->key.len,
        cc_cipher->working_buffer.buffer,
        cc_cipher->working_buffer.len,
        out->buffer,
        &output_buffer_len);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    out->len += output_buffer_len;

    return AWS_OP_SUCCESS;
}

static int s_finalize_keywrap_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct cc_aes_cipher *cc_cipher = cipher->impl;

    if (cc_cipher->working_buffer.len == 0) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    size_t output_buffer_len = cipher->block_size + cc_cipher->working_buffer.len;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, output_buffer_len)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    CCCryptorStatus status = CCSymmetricKeyUnwrap(
        kCCWRAPAES,
        CCrfc3394_iv,
        CCrfc3394_ivLen,
        cipher->key.buffer,
        cipher->key.len,
        cc_cipher->working_buffer.buffer,
        cc_cipher->working_buffer.len,
        out->buffer,
        &output_buffer_len);

    if (status != kCCSuccess) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    out->len += output_buffer_len;

    return AWS_OP_SUCCESS;
}

static struct aws_symmetric_cipher_vtable s_aes_keywrap_vtable = {
    .finalize_decryption = s_finalize_keywrap_decryption,
    .finalize_encryption = s_finalize_keywrap_encryption,
    .decrypt = s_keywrap_encrypt_decrypt,
    .encrypt = s_keywrap_encrypt_decrypt,
    .provider = "CommonCrypto",
    .alg_name = "AES-KEYWRAP 256",
    .destroy = s_destroy,
    .reset = s_reset,
};

struct aws_symmetric_cipher *aws_aes_keywrap_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key) {
    struct cc_aes_cipher *cc_cipher = aws_mem_calloc(allocator, 1, sizeof(struct cc_aes_cipher));
    cc_cipher->cipher_base.allocator = allocator;
    cc_cipher->cipher_base.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE / 2;
    cc_cipher->cipher_base.impl = cc_cipher;
    cc_cipher->cipher_base.vtable = &s_aes_keywrap_vtable;

    if (key) {
        aws_byte_buf_init_copy_from_cursor(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, *key);
    } else {
        aws_byte_buf_init(&cc_cipher->cipher_base.key, cc_cipher->cipher_base.allocator, AWS_AES_256_KEY_BYTE_LEN);
        aws_symmetric_cipher_generate_key(AWS_AES_256_KEY_BYTE_LEN, &cc_cipher->cipher_base.key);
    }

    aws_byte_buf_init(&cc_cipher->working_buffer, allocator, (AWS_AES_256_CIPHER_BLOCK_SIZE * 2) + 8);
    cc_cipher->cipher_base.good = true;
    cc_cipher->cipher_base.key_length_bits = AWS_AES_256_KEY_BIT_LEN;

    return &cc_cipher->cipher_base;
}
