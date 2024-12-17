/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/private/symmetric_cipher_priv.h>

#include <windows.h>

/* keep the space to prevent formatters from reordering this with the Windows.h header. */
#include <bcrypt.h>

#define NT_SUCCESS(status) ((NTSTATUS)status >= 0)

/* handles for AES modes and algorithms we'll be using. These are initialized once and allowed to leak. */
static aws_thread_once s_aes_thread_once = AWS_THREAD_ONCE_STATIC_INIT;
static BCRYPT_ALG_HANDLE s_aes_cbc_algorithm_handle = NULL;
static BCRYPT_ALG_HANDLE s_aes_gcm_algorithm_handle = NULL;
static BCRYPT_ALG_HANDLE s_aes_ctr_algorithm_handle = NULL;
static BCRYPT_ALG_HANDLE s_aes_keywrap_algorithm_handle = NULL;

struct aes_bcrypt_cipher {
    struct aws_symmetric_cipher cipher;
    BCRYPT_ALG_HANDLE alg_handle;
    /* the loaded key handle. */
    BCRYPT_KEY_HANDLE key_handle;
    /* Used for GCM mode to store IV, tag, and aad */
    BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO *auth_info_ptr;
    /* Updated on the fly for things like constant-time CBC padding and GCM hash chaining */
    DWORD cipher_flags;
    /* For things to work, they have to be in 16 byte chunks in several scenarios. Use this
       Buffer for storing excess bytes until we have 16 bytes to operate on. */
    struct aws_byte_buf overflow;
    /* This gets updated as the algorithms run so it isn't the original IV. That's why its separate */
    struct aws_byte_buf working_iv;
    /* A buffer to keep around for the GMAC for GCM. */
    struct aws_byte_buf working_mac_buffer;
};

static void s_load_alg_handles(void *user_data) {
    (void)user_data;

    /* this function is incredibly slow, LET IT LEAK*/
    NTSTATUS status = BCryptOpenAlgorithmProvider(&s_aes_cbc_algorithm_handle, BCRYPT_AES_ALGORITHM, NULL, 0);
    AWS_FATAL_ASSERT(s_aes_cbc_algorithm_handle && "BCryptOpenAlgorithmProvider() failed");

    status = BCryptSetProperty(
        s_aes_cbc_algorithm_handle,
        BCRYPT_CHAINING_MODE,
        (PUCHAR)BCRYPT_CHAIN_MODE_CBC,
        (ULONG)(wcslen(BCRYPT_CHAIN_MODE_CBC) + 1),
        0);

    AWS_FATAL_ASSERT(NT_SUCCESS(status) && "BCryptSetProperty for CBC chaining mode failed");

    /* Set up GCM algorithm */
    status = BCryptOpenAlgorithmProvider(&s_aes_gcm_algorithm_handle, BCRYPT_AES_ALGORITHM, NULL, 0);
    AWS_FATAL_ASSERT(s_aes_gcm_algorithm_handle && "BCryptOpenAlgorithmProvider() failed");

    status = BCryptSetProperty(
        s_aes_gcm_algorithm_handle,
        BCRYPT_CHAINING_MODE,
        (PUCHAR)BCRYPT_CHAIN_MODE_GCM,
        (ULONG)(wcslen(BCRYPT_CHAIN_MODE_GCM) + 1),
        0);

    AWS_FATAL_ASSERT(NT_SUCCESS(status) && "BCryptSetProperty for GCM chaining mode failed");

    /* Setup CTR algorithm */
    status = BCryptOpenAlgorithmProvider(&s_aes_ctr_algorithm_handle, BCRYPT_AES_ALGORITHM, NULL, 0);
    AWS_FATAL_ASSERT(s_aes_ctr_algorithm_handle && "BCryptOpenAlgorithmProvider() failed");

    /* This is ECB because windows doesn't do CTR mode for you.
       Instead we use ECB and XOR the encrypted IV and data to operate on for each block. */
    status = BCryptSetProperty(
        s_aes_ctr_algorithm_handle,
        BCRYPT_CHAINING_MODE,
        (PUCHAR)BCRYPT_CHAIN_MODE_ECB,
        (ULONG)(wcslen(BCRYPT_CHAIN_MODE_ECB) + 1),
        0);

    AWS_FATAL_ASSERT(NT_SUCCESS(status) && "BCryptSetProperty for ECB chaining mode failed");

    /* Setup KEYWRAP algorithm */
    status = BCryptOpenAlgorithmProvider(&s_aes_keywrap_algorithm_handle, BCRYPT_AES_ALGORITHM, NULL, 0);
    AWS_FATAL_ASSERT(s_aes_ctr_algorithm_handle && "BCryptOpenAlgorithmProvider() failed");

    AWS_FATAL_ASSERT(NT_SUCCESS(status) && "BCryptSetProperty for KeyWrap failed");
}

static BCRYPT_KEY_HANDLE s_import_key_blob(
    BCRYPT_ALG_HANDLE algHandle,
    struct aws_allocator *allocator,
    struct aws_byte_buf *key) {
    NTSTATUS status = 0;

    BCRYPT_KEY_DATA_BLOB_HEADER key_data;
    key_data.dwMagic = BCRYPT_KEY_DATA_BLOB_MAGIC;
    key_data.dwVersion = BCRYPT_KEY_DATA_BLOB_VERSION1;
    key_data.cbKeyData = (ULONG)key->len;

    struct aws_byte_buf key_data_buf;
    aws_byte_buf_init(&key_data_buf, allocator, sizeof(key_data) + key->len);
    aws_byte_buf_write(&key_data_buf, (const uint8_t *)&key_data, sizeof(key_data));
    aws_byte_buf_write(&key_data_buf, key->buffer, key->len);

    BCRYPT_KEY_HANDLE key_handle;
    status = BCryptImportKey(
        algHandle, NULL, BCRYPT_KEY_DATA_BLOB, &key_handle, NULL, 0, key_data_buf.buffer, (ULONG)key_data_buf.len, 0);

    aws_byte_buf_clean_up_secure(&key_data_buf);

    if (!NT_SUCCESS(status)) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    return key_handle;
}

static void s_aes_default_destroy(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    aws_byte_buf_clean_up_secure(&cipher->key);
    aws_byte_buf_clean_up_secure(&cipher->iv);
    aws_byte_buf_clean_up_secure(&cipher->tag);
    aws_byte_buf_clean_up_secure(&cipher->aad);

    /* clean_up_secure exists in versions of aws-c-common that don't check that the
       buffer has a buffer and an allocator before freeing the memory. Instead,
       check here. If it's set the buffer was owned and needs to be cleaned up, otherwise
       it can just be dropped as it was an alias.*/
    if (cipher_impl->working_iv.allocator) {
        aws_byte_buf_clean_up_secure(&cipher_impl->working_iv);
    }

    aws_byte_buf_clean_up_secure(&cipher_impl->overflow);
    aws_byte_buf_clean_up_secure(&cipher_impl->working_mac_buffer);

    if (cipher_impl->key_handle) {
        BCryptDestroyKey(cipher_impl->key_handle);
        cipher_impl->key_handle = NULL;
    }

    if (cipher_impl->auth_info_ptr) {
        aws_mem_release(cipher->allocator, cipher_impl->auth_info_ptr);
        cipher_impl->auth_info_ptr = NULL;
    }

    aws_mem_release(cipher->allocator, cipher_impl);
}

/* just a utility function for setting up windows Ciphers and keys etc....
   Handles copying key/iv etc... data to the right buffers and then setting them
   on the windows handles used for the encryption operations. */
static int s_initialize_cipher_materials(
    struct aes_bcrypt_cipher *cipher,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv,
    const struct aws_byte_cursor *tag,
    const struct aws_byte_cursor *aad,
    size_t iv_size,
    bool is_ctr_mode,
    bool is_gcm) {

    if (!cipher->cipher.key.len) {
        if (key) {
            aws_byte_buf_init_copy_from_cursor(&cipher->cipher.key, cipher->cipher.allocator, *key);
        } else {
            aws_byte_buf_init(&cipher->cipher.key, cipher->cipher.allocator, AWS_AES_256_KEY_BYTE_LEN);
            aws_symmetric_cipher_generate_key(AWS_AES_256_KEY_BYTE_LEN, &cipher->cipher.key);
        }
    }

    if (!cipher->cipher.iv.len && iv_size) {
        if (iv) {
            aws_byte_buf_init_copy_from_cursor(&cipher->cipher.iv, cipher->cipher.allocator, *iv);
        } else {
            aws_byte_buf_init(&cipher->cipher.iv, cipher->cipher.allocator, iv_size);
            aws_symmetric_cipher_generate_initialization_vector(iv_size, is_ctr_mode, &cipher->cipher.iv);
        }
    }

    /* these fields are only used in GCM mode. */
    if (is_gcm) {
        if (!cipher->cipher.tag.len) {
            if (tag) {
                aws_byte_buf_init_copy_from_cursor(&cipher->cipher.tag, cipher->cipher.allocator, *tag);
            } else {
                aws_byte_buf_init(&cipher->cipher.tag, cipher->cipher.allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
                aws_byte_buf_secure_zero(&cipher->cipher.tag);
                /* windows handles this, just go ahead and tell the API it's got a length. */
                cipher->cipher.tag.len = AWS_AES_256_CIPHER_BLOCK_SIZE;
            }
        }

        if (!cipher->cipher.aad.len) {
            if (aad) {
                aws_byte_buf_init_copy_from_cursor(&cipher->cipher.aad, cipher->cipher.allocator, *aad);
            }
        }

        if (!cipher->working_mac_buffer.len) {
            aws_byte_buf_init(&cipher->working_mac_buffer, cipher->cipher.allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
            aws_byte_buf_secure_zero(&cipher->working_mac_buffer);
            /* windows handles this, just go ahead and tell the API it's got a length. */
            cipher->working_mac_buffer.len = AWS_AES_256_CIPHER_BLOCK_SIZE;
        }
    }

    cipher->key_handle = s_import_key_blob(cipher->alg_handle, cipher->cipher.allocator, &cipher->cipher.key);

    if (!cipher->key_handle) {
        cipher->cipher.good = false;
        return AWS_OP_ERR;
    }

    cipher->cipher_flags = 0;

    /* In GCM mode, the IV is set on the auth info pointer and a working copy
       is passed to each encryt call. CBC and CTR mode function differently here
       and the IV is set on the key itself. */
    if (!is_gcm && cipher->cipher.iv.len) {
        NTSTATUS status = BCryptSetProperty(
            cipher->key_handle,
            BCRYPT_INITIALIZATION_VECTOR,
            cipher->cipher.iv.buffer,
            (ULONG)cipher->cipher.iv.len,
            0);

        if (!NT_SUCCESS(status)) {
            cipher->cipher.good = false;
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    } else if (is_gcm) {

        cipher->auth_info_ptr =
            aws_mem_acquire(cipher->cipher.allocator, sizeof(BCRYPT_AUTHENTICATED_CIPHER_MODE_INFO));

        /* Create a new authenticated cipher mode info object for GCM mode */
        BCRYPT_INIT_AUTH_MODE_INFO(*cipher->auth_info_ptr);
        cipher->auth_info_ptr->pbNonce = cipher->cipher.iv.buffer;
        cipher->auth_info_ptr->cbNonce = (ULONG)cipher->cipher.iv.len;
        cipher->auth_info_ptr->dwFlags = BCRYPT_AUTH_MODE_CHAIN_CALLS_FLAG;
        cipher->auth_info_ptr->pbTag = cipher->cipher.tag.buffer;
        cipher->auth_info_ptr->cbTag = (ULONG)cipher->cipher.tag.len;
        cipher->auth_info_ptr->pbMacContext = cipher->working_mac_buffer.buffer;
        cipher->auth_info_ptr->cbMacContext = (ULONG)cipher->working_mac_buffer.len;

        if (cipher->cipher.aad.len) {
            cipher->auth_info_ptr->pbAuthData = (PUCHAR)cipher->cipher.aad.buffer;
            cipher->auth_info_ptr->cbAuthData = (ULONG)cipher->cipher.aad.len;
        }
    }

    return AWS_OP_SUCCESS;
}

/* Free up as few resources as possible so we can quickly reuse the cipher. */
static void s_clear_reusable_components(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;
    bool working_iv_optimized = cipher->iv.buffer == cipher_impl->working_iv.buffer;

    if (!working_iv_optimized) {
        aws_byte_buf_secure_zero(&cipher_impl->working_iv);
    }

    /* These can't always be reused in the next operation, so go ahead and destroy it
       and create another. */
    if (cipher_impl->key_handle) {
        BCryptDestroyKey(cipher_impl->key_handle);
        cipher_impl->key_handle = NULL;
    }

    if (cipher_impl->auth_info_ptr) {
        aws_mem_release(cipher->allocator, cipher_impl->auth_info_ptr);
        cipher_impl->auth_info_ptr = NULL;
    }

    aws_byte_buf_secure_zero(&cipher_impl->overflow);
    aws_byte_buf_secure_zero(&cipher_impl->working_mac_buffer);
    /* windows handles this, just go ahead and tell the API it's got a length. */
    cipher_impl->working_mac_buffer.len = AWS_AES_256_CIPHER_BLOCK_SIZE;
}

static int s_reset_cbc_cipher(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    s_clear_reusable_components(cipher);
    return s_initialize_cipher_materials(
        cipher_impl, NULL, NULL, NULL, NULL, AWS_AES_256_CIPHER_BLOCK_SIZE, false, false);
}

static int s_reset_ctr_cipher(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    s_clear_reusable_components(cipher);
    struct aws_byte_cursor iv_cur = aws_byte_cursor_from_buf(&cipher->iv);
    /* reset the working iv back to the original IV. We do this because
       we're manually maintaining the counter. */
    aws_byte_buf_append_dynamic(&cipher_impl->working_iv, &iv_cur);
    return s_initialize_cipher_materials(
        cipher_impl, NULL, NULL, NULL, NULL, AWS_AES_256_CIPHER_BLOCK_SIZE, true, false);
}

static int s_reset_gcm_cipher(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    s_clear_reusable_components(cipher);
    return s_initialize_cipher_materials(
        cipher_impl, NULL, NULL, NULL, NULL, AWS_AES_256_CIPHER_BLOCK_SIZE - 4, false, true);
}

static int s_aes_default_encrypt(
    struct aws_symmetric_cipher *cipher,
    const struct aws_byte_cursor *to_encrypt,
    struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (to_encrypt->len == 0) {
        return AWS_OP_SUCCESS;
    }

    size_t predicted_write_length =
        cipher_impl->cipher_flags & BCRYPT_BLOCK_PADDING
            ? to_encrypt->len + (AWS_AES_256_CIPHER_BLOCK_SIZE - (to_encrypt->len % AWS_AES_256_CIPHER_BLOCK_SIZE))
            : to_encrypt->len;

    ULONG length_written = (ULONG)(predicted_write_length);

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, predicted_write_length)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    PUCHAR iv = NULL;
    ULONG iv_size = 0;

    if (cipher_impl->auth_info_ptr) {
        iv = cipher_impl->working_iv.buffer;
        /* this is looking for buffer size, and the working_iv has only been written to by windows the GCM case.
         * So use capacity rather than length */
        iv_size = (ULONG)cipher_impl->working_iv.capacity;
    }

    /* iv was set on the key itself, so we don't need to pass it here. */
    NTSTATUS status = BCryptEncrypt(
        cipher_impl->key_handle,
        to_encrypt->ptr,
        (ULONG)to_encrypt->len,
        cipher_impl->auth_info_ptr,
        iv,
        iv_size,
        out->buffer + out->len,
        (ULONG)(out->capacity - out->len),
        &length_written,
        cipher_impl->cipher_flags);

    if (!NT_SUCCESS(status)) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += length_written;
    return AWS_OP_SUCCESS;
}

/* manages making sure encryption operations can operate on 16 byte blocks. Stores the excess in the overflow
   buffer and moves stuff around each time to make sure everything is in order. */
static struct aws_byte_buf s_fill_in_overflow(
    struct aws_symmetric_cipher *cipher,
    const struct aws_byte_cursor *to_operate) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    static const size_t RESERVE_SIZE = AWS_AES_256_CIPHER_BLOCK_SIZE * 2;
    cipher_impl->cipher_flags = 0;

    struct aws_byte_buf final_to_operate_on;
    AWS_ZERO_STRUCT(final_to_operate_on);

    if (cipher_impl->overflow.len > 0) {
        aws_byte_buf_init_copy(&final_to_operate_on, cipher->allocator, &cipher_impl->overflow);
        aws_byte_buf_append_dynamic(&final_to_operate_on, to_operate);
        aws_byte_buf_secure_zero(&cipher_impl->overflow);
    } else {
        aws_byte_buf_init_copy_from_cursor(&final_to_operate_on, cipher->allocator, *to_operate);
    }

    size_t overflow = final_to_operate_on.len % RESERVE_SIZE;

    if (final_to_operate_on.len > RESERVE_SIZE) {
        size_t offset = overflow == 0 ? RESERVE_SIZE : overflow;

        struct aws_byte_cursor slice_for_overflow = aws_byte_cursor_from_buf(&final_to_operate_on);
        aws_byte_cursor_advance(&slice_for_overflow, final_to_operate_on.len - offset);
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &slice_for_overflow);
        final_to_operate_on.len -= offset;
    } else {
        struct aws_byte_cursor final_cur = aws_byte_cursor_from_buf(&final_to_operate_on);
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &final_cur);
        aws_byte_buf_clean_up_secure(&final_to_operate_on);
    }

    return final_to_operate_on;
}

static int s_aes_cbc_encrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor to_encrypt,
    struct aws_byte_buf *out) {

    struct aws_byte_buf final_to_encrypt = s_fill_in_overflow(cipher, &to_encrypt);
    struct aws_byte_cursor final_cur = aws_byte_cursor_from_buf(&final_to_encrypt);
    int ret_val = s_aes_default_encrypt(cipher, &final_cur, out);
    aws_byte_buf_clean_up_secure(&final_to_encrypt);

    return ret_val;
}

static int s_aes_cbc_finalize_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (cipher->good && cipher_impl->overflow.len > 0) {
        cipher_impl->cipher_flags = BCRYPT_BLOCK_PADDING;
        /* take the rest of the overflow and turn padding on so the remainder is properly padded
           without timing attack vulnerabilities. */
        struct aws_byte_cursor remaining_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
        int ret_val = s_aes_default_encrypt(cipher, &remaining_cur, out);
        aws_byte_buf_secure_zero(&cipher_impl->overflow);
        return ret_val;
    }

    return AWS_OP_SUCCESS;
}

static int s_default_aes_decrypt(
    struct aws_symmetric_cipher *cipher,
    const struct aws_byte_cursor *to_decrypt,
    struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (to_decrypt->len == 0) {
        return AWS_OP_SUCCESS;
    }

    PUCHAR iv = NULL;
    ULONG iv_size = 0;

    if (cipher_impl->auth_info_ptr) {
        iv = cipher_impl->working_iv.buffer;
        /* this is looking for buffer size, and the working_iv has only been written to by windows the GCM case.
         * So use capacity rather than length */
        iv_size = (ULONG)cipher_impl->working_iv.capacity;
    }

    size_t predicted_write_length = to_decrypt->len;
    ULONG length_written = (ULONG)(predicted_write_length);

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, predicted_write_length)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* iv was set on the key itself, so we don't need to pass it here. */
    NTSTATUS status = BCryptDecrypt(
        cipher_impl->key_handle,
        to_decrypt->ptr,
        (ULONG)to_decrypt->len,
        cipher_impl->auth_info_ptr,
        iv,
        iv_size,
        out->buffer + out->len,
        (ULONG)(out->capacity - out->len),
        &length_written,
        cipher_impl->cipher_flags);

    if (!NT_SUCCESS(status)) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    out->len += length_written;
    return AWS_OP_SUCCESS;
}

static int s_aes_cbc_decrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor to_decrypt,
    struct aws_byte_buf *out) {
    struct aws_byte_buf final_to_decrypt = s_fill_in_overflow(cipher, &to_decrypt);
    struct aws_byte_cursor final_cur = aws_byte_cursor_from_buf(&final_to_decrypt);
    int ret_val = s_default_aes_decrypt(cipher, &final_cur, out);
    aws_byte_buf_clean_up_secure(&final_to_decrypt);

    return ret_val;
}

static int s_aes_cbc_finalize_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (cipher->good && cipher_impl->overflow.len > 0) {
        cipher_impl->cipher_flags = BCRYPT_BLOCK_PADDING;
        /* take the rest of the overflow and turn padding on so the remainder is properly padded
           without timing attack vulnerabilities. */
        struct aws_byte_cursor remaining_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
        int ret_val = s_default_aes_decrypt(cipher, &remaining_cur, out);
        aws_byte_buf_secure_zero(&cipher_impl->overflow);
        return ret_val;
    }

    return AWS_OP_SUCCESS;
}

static struct aws_symmetric_cipher_vtable s_aes_cbc_vtable = {
    .alg_name = "AES-CBC 256",
    .provider = "Windows CNG",
    .decrypt = s_aes_cbc_decrypt,
    .encrypt = s_aes_cbc_encrypt,
    .finalize_encryption = s_aes_cbc_finalize_encryption,
    .finalize_decryption = s_aes_cbc_finalize_decryption,
    .destroy = s_aes_default_destroy,
    .reset = s_reset_cbc_cipher,
};

struct aws_symmetric_cipher *aws_aes_cbc_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {

    aws_thread_call_once(&s_aes_thread_once, s_load_alg_handles, NULL);

    struct aes_bcrypt_cipher *cipher = aws_mem_calloc(allocator, 1, sizeof(struct aes_bcrypt_cipher));

    cipher->cipher.allocator = allocator;
    cipher->cipher.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cipher->cipher.key_length_bits = AWS_AES_256_KEY_BIT_LEN;
    cipher->alg_handle = s_aes_cbc_algorithm_handle;
    cipher->cipher.vtable = &s_aes_cbc_vtable;

    if (s_initialize_cipher_materials(cipher, key, iv, NULL, NULL, AWS_AES_256_CIPHER_BLOCK_SIZE, false, false) !=
        AWS_OP_SUCCESS) {
        goto error;
    }

    aws_byte_buf_init(&cipher->overflow, allocator, AWS_AES_256_CIPHER_BLOCK_SIZE * 2);
    cipher->working_iv = cipher->cipher.iv;
    /* make sure the cleanup doesn't do anything. */
    cipher->working_iv.allocator = NULL;
    cipher->cipher.impl = cipher;
    cipher->cipher.good = true;

    return &cipher->cipher;

error:
    return NULL;
}

/* the buffer management for this mode is a good deal easier because we don't care about padding.
   We do care about keeping the final buffer less than a block size til the finalize call so we can
   turn the auth chaining flag off and compute the GMAC correctly. */
static int s_aes_gcm_encrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor to_encrypt,
    struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (to_encrypt.len == 0) {
        return AWS_OP_SUCCESS;
    }

    struct aws_byte_buf working_buffer;
    AWS_ZERO_STRUCT(working_buffer);

    /* If there's overflow, prepend it to the working buffer, then append the data to encrypt */
    if (cipher_impl->overflow.len) {
        struct aws_byte_cursor overflow_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);

        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, overflow_cur);
        aws_byte_buf_reset(&cipher_impl->overflow, true);
        aws_byte_buf_append_dynamic(&working_buffer, &to_encrypt);
    } else {
        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, to_encrypt);
    }

    int ret_val = AWS_OP_ERR;

    /* whatever is remaining in an incomplete block, copy it to the overflow. If we don't have a full block
       wait til next time or for the finalize call. */
    if (working_buffer.len > AWS_AES_256_CIPHER_BLOCK_SIZE) {
        size_t offset = working_buffer.len % AWS_AES_256_CIPHER_BLOCK_SIZE;
        size_t seek_to = working_buffer.len - (AWS_AES_256_CIPHER_BLOCK_SIZE + offset);
        struct aws_byte_cursor working_buf_cur = aws_byte_cursor_from_buf(&working_buffer);
        struct aws_byte_cursor working_slice = aws_byte_cursor_advance(&working_buf_cur, seek_to);
        /* this is just here to make it obvious. The previous line advanced working_buf_cur to where the
           new overfloew should be. */
        struct aws_byte_cursor new_overflow_cur = working_buf_cur;
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &new_overflow_cur);

        ret_val = s_aes_default_encrypt(cipher, &working_slice, out);
    } else {
        struct aws_byte_cursor working_buffer_cur = aws_byte_cursor_from_buf(&working_buffer);
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &working_buffer_cur);
        ret_val = AWS_OP_SUCCESS;
    }
    aws_byte_buf_clean_up_secure(&working_buffer);
    return ret_val;
}

static int s_aes_gcm_decrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor to_decrypt,
    struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (to_decrypt.len == 0) {
        return AWS_OP_SUCCESS;
    }

    struct aws_byte_buf working_buffer;
    AWS_ZERO_STRUCT(working_buffer);

    /* If there's overflow, prepend it to the working buffer, then append the data to encrypt */
    if (cipher_impl->overflow.len) {
        struct aws_byte_cursor overflow_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);

        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, overflow_cur);
        aws_byte_buf_reset(&cipher_impl->overflow, true);
        aws_byte_buf_append_dynamic(&working_buffer, &to_decrypt);
    } else {
        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, to_decrypt);
    }

    int ret_val = AWS_OP_ERR;

    /* whatever is remaining in an incomplete block, copy it to the overflow. If we don't have a full block
       wait til next time or for the finalize call. */
    if (working_buffer.len > AWS_AES_256_CIPHER_BLOCK_SIZE) {
        size_t offset = working_buffer.len % AWS_AES_256_CIPHER_BLOCK_SIZE;
        size_t seek_to = working_buffer.len - (AWS_AES_256_CIPHER_BLOCK_SIZE + offset);
        struct aws_byte_cursor working_buf_cur = aws_byte_cursor_from_buf(&working_buffer);
        struct aws_byte_cursor working_slice = aws_byte_cursor_advance(&working_buf_cur, seek_to);
        /* this is just here to make it obvious. The previous line advanced working_buf_cur to where the
           new overfloew should be. */
        struct aws_byte_cursor new_overflow_cur = working_buf_cur;
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &new_overflow_cur);

        ret_val = s_default_aes_decrypt(cipher, &working_slice, out);
    } else {
        struct aws_byte_cursor working_buffer_cur = aws_byte_cursor_from_buf(&working_buffer);
        aws_byte_buf_append_dynamic(&cipher_impl->overflow, &working_buffer_cur);
        ret_val = AWS_OP_SUCCESS;
    }
    aws_byte_buf_clean_up_secure(&working_buffer);
    return ret_val;
}

static int s_aes_gcm_finalize_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    cipher_impl->auth_info_ptr->dwFlags &= ~BCRYPT_AUTH_MODE_CHAIN_CALLS_FLAG;
    /* take whatever is remaining, make the final encrypt call with the auth chain flag turned off. */
    struct aws_byte_cursor remaining_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
    int ret_val = s_aes_default_encrypt(cipher, &remaining_cur, out);
    aws_byte_buf_secure_zero(&cipher_impl->overflow);
    aws_byte_buf_secure_zero(&cipher_impl->working_iv);
    return ret_val;
}

static int s_aes_gcm_finalize_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;
    cipher_impl->auth_info_ptr->dwFlags &= ~BCRYPT_AUTH_MODE_CHAIN_CALLS_FLAG;
    /* take whatever is remaining, make the final decrypt call with the auth chain flag turned off. */
    struct aws_byte_cursor remaining_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
    int ret_val = s_default_aes_decrypt(cipher, &remaining_cur, out);
    aws_byte_buf_secure_zero(&cipher_impl->overflow);
    aws_byte_buf_secure_zero(&cipher_impl->working_iv);
    return ret_val;
}

static struct aws_symmetric_cipher_vtable s_aes_gcm_vtable = {
    .alg_name = "AES-GCM 256",
    .provider = "Windows CNG",
    .decrypt = s_aes_gcm_decrypt,
    .encrypt = s_aes_gcm_encrypt,
    .finalize_encryption = s_aes_gcm_finalize_encryption,
    .finalize_decryption = s_aes_gcm_finalize_decryption,
    .destroy = s_aes_default_destroy,
    .reset = s_reset_gcm_cipher,
};

struct aws_symmetric_cipher *aws_aes_gcm_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv,
    const struct aws_byte_cursor *aad,
    const struct aws_byte_cursor *decryption_tag) {

    aws_thread_call_once(&s_aes_thread_once, s_load_alg_handles, NULL);
    struct aes_bcrypt_cipher *cipher = aws_mem_calloc(allocator, 1, sizeof(struct aes_bcrypt_cipher));

    cipher->cipher.allocator = allocator;
    cipher->cipher.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cipher->cipher.key_length_bits = AWS_AES_256_KEY_BIT_LEN;
    cipher->alg_handle = s_aes_gcm_algorithm_handle;
    cipher->cipher.vtable = &s_aes_gcm_vtable;

    /* GCM does the counting under the hood, so we let it handle the final 4 bytes of the IV. */
    if (s_initialize_cipher_materials(
            cipher, key, iv, decryption_tag, aad, AWS_AES_256_CIPHER_BLOCK_SIZE - 4, false, true) != AWS_OP_SUCCESS) {
        goto error;
    }

    aws_byte_buf_init(&cipher->overflow, allocator, AWS_AES_256_CIPHER_BLOCK_SIZE * 2);
    aws_byte_buf_init(&cipher->working_iv, allocator, AWS_AES_256_CIPHER_BLOCK_SIZE);
    aws_byte_buf_secure_zero(&cipher->working_iv);

    cipher->cipher.impl = cipher;
    cipher->cipher.good = true;

    return &cipher->cipher;

error:
    if (cipher != NULL) {
        s_aes_default_destroy(&cipher->cipher);
    }

    return NULL;
}

/* Take a and b, XOR them and store it in dest. Notice the XOR is done up to the length of the smallest input.
   If there's a bug in here, it's being hit inside the finalize call when there's an input stream that isn't an even
   multiple of 16.
 */
static int s_xor_cursors(const struct aws_byte_cursor *a, const struct aws_byte_cursor *b, struct aws_byte_buf *dest) {
    size_t min_size = aws_min_size(b->len, a->len);

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(dest, min_size)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    /* If the profiler is saying this is slow, SIMD the loop below. */
    uint8_t *array_ref = dest->buffer + dest->len;

    for (size_t i = 0; i < min_size; ++i) {
        array_ref[i] = a->ptr[i] ^ b->ptr[i];
    }

    dest->len += min_size;

    return AWS_OP_SUCCESS;
}

/* There is no CTR mode on windows. Instead, we use AES ECB to encrypt the IV a block at a time.
   That value is then XOR'd with the to_encrypt cursor and appended to out. The counter then needs
   to be incremented by 1 for the next call. This has to be done a block at a time, so we slice
   to_encrypt into a cursor per block and do this process for each block. Also notice that CTR mode
   is symmetric for encryption and decryption (encrypt and decrypt are the same thing). */
static int s_aes_ctr_encrypt(
    struct aws_symmetric_cipher *cipher,
    struct aws_byte_cursor to_encrypt,
    struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    if (to_encrypt.len == 0) {
        return AWS_OP_SUCCESS;
    }

    struct aws_byte_buf working_buffer;
    AWS_ZERO_STRUCT(working_buffer);

    /* prepend overflow to the working buffer and then append to_encrypt to it. */
    if (cipher_impl->overflow.len && to_encrypt.ptr != cipher_impl->overflow.buffer) {
        struct aws_byte_cursor overflow_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, overflow_cur);
        aws_byte_buf_reset(&cipher_impl->overflow, true);
        aws_byte_buf_append_dynamic(&working_buffer, &to_encrypt);
    } else {
        aws_byte_buf_init_copy_from_cursor(&working_buffer, cipher->allocator, to_encrypt);
    }

    /* slice working_buffer into a slice per block. */
    struct aws_array_list sliced_buffers;
    aws_array_list_init_dynamic(
        &sliced_buffers,
        cipher->allocator,
        (to_encrypt.len / AWS_AES_256_CIPHER_BLOCK_SIZE) + 1,
        sizeof(struct aws_byte_cursor));

    struct aws_byte_cursor working_buf_cur = aws_byte_cursor_from_buf(&working_buffer);
    while (working_buf_cur.len) {
        struct aws_byte_cursor slice = working_buf_cur;

        if (working_buf_cur.len >= AWS_AES_256_CIPHER_BLOCK_SIZE) {
            slice = aws_byte_cursor_advance(&working_buf_cur, AWS_AES_256_CIPHER_BLOCK_SIZE);
        } else {
            aws_byte_cursor_advance(&working_buf_cur, slice.len);
        }

        aws_array_list_push_back(&sliced_buffers, &slice);
    }

    int ret_val = AWS_OP_ERR;

    size_t sliced_buffers_cnt = aws_array_list_length(&sliced_buffers);

    /* for each slice, if it's a full block, do ECB on the IV, xor it to the slice, and then increment the counter. */
    for (size_t i = 0; i < sliced_buffers_cnt; ++i) {
        struct aws_byte_cursor buffer_cur;
        AWS_ZERO_STRUCT(buffer_cur);

        aws_array_list_get_at(&sliced_buffers, &buffer_cur, i);
        if (buffer_cur.len == AWS_AES_256_CIPHER_BLOCK_SIZE ||
            /* this part of the branch is for handling the finalize call, which does not have to be on an even
               block boundary. */
            (cipher_impl->overflow.len > 0 && sliced_buffers_cnt) == 1) {

            ULONG lengthWritten = (ULONG)AWS_AES_256_CIPHER_BLOCK_SIZE;
            uint8_t temp_buffer[AWS_AES_256_CIPHER_BLOCK_SIZE] = {0};
            struct aws_byte_cursor temp_cur = aws_byte_cursor_from_array(temp_buffer, sizeof(temp_buffer));

            NTSTATUS status = BCryptEncrypt(
                cipher_impl->key_handle,
                cipher_impl->working_iv.buffer,
                (ULONG)cipher_impl->working_iv.len,
                NULL,
                NULL,
                0,
                temp_cur.ptr,
                (ULONG)temp_cur.len,
                &lengthWritten,
                cipher_impl->cipher_flags);

            if (!NT_SUCCESS(status)) {
                cipher->good = false;
                ret_val = aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                goto clean_up;
            }

            /* this does the XOR, after this call the final encrypted output is added to out. */
            if (s_xor_cursors(&buffer_cur, &temp_cur, out)) {
                ret_val = AWS_OP_ERR;
                goto clean_up;
            }

            /* increment the counter. Get the buffers aligned for it first though. */
            size_t counter_offset = AWS_AES_256_CIPHER_BLOCK_SIZE - sizeof(uint32_t);
            struct aws_byte_buf counter_buf = cipher_impl->working_iv;
            /* roll it back 4 so the write works. */
            counter_buf.len = counter_offset;
            struct aws_byte_cursor counter_cur = aws_byte_cursor_from_buf(&cipher_impl->working_iv);
            aws_byte_cursor_advance(&counter_cur, counter_offset);

            /* read current counter value as a Big-endian 32-bit integer*/
            uint32_t counter = 0;
            aws_byte_cursor_read_be32(&counter_cur, &counter);

            /* check for overflow here. */
            if (aws_add_u32_checked(counter, 1, &counter) != AWS_OP_SUCCESS) {
                cipher->good = false;
                ret_val = AWS_OP_ERR;
                goto clean_up;
            }
            /* put the incremented counter back. */
            aws_byte_buf_write_be32(&counter_buf, counter);
        } else {
            /* otherwise dump it into the overflow and wait til the next call */
            aws_byte_buf_append_dynamic(&cipher_impl->overflow, &buffer_cur);
        }

        ret_val = AWS_OP_SUCCESS;
    }

clean_up:
    aws_array_list_clean_up_secure(&sliced_buffers);
    aws_byte_buf_clean_up_secure(&working_buffer);

    return ret_val;
}

static int s_aes_ctr_finalize_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    struct aws_byte_cursor remaining_cur = aws_byte_cursor_from_buf(&cipher_impl->overflow);
    /* take the final overflow, and do the final encrypt call for it. */
    int ret_val = s_aes_ctr_encrypt(cipher, remaining_cur, out);
    aws_byte_buf_secure_zero(&cipher_impl->overflow);
    aws_byte_buf_secure_zero(&cipher_impl->working_iv);
    return ret_val;
}

static struct aws_symmetric_cipher_vtable s_aes_ctr_vtable = {
    .alg_name = "AES-CTR 256",
    .provider = "Windows CNG",
    .decrypt = s_aes_ctr_encrypt,
    .encrypt = s_aes_ctr_encrypt,
    .finalize_encryption = s_aes_ctr_finalize_encryption,
    .finalize_decryption = s_aes_ctr_finalize_encryption,
    .destroy = s_aes_default_destroy,
    .reset = s_reset_ctr_cipher,
};

struct aws_symmetric_cipher *aws_aes_ctr_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key,
    const struct aws_byte_cursor *iv) {

    aws_thread_call_once(&s_aes_thread_once, s_load_alg_handles, NULL);
    struct aes_bcrypt_cipher *cipher = aws_mem_calloc(allocator, 1, sizeof(struct aes_bcrypt_cipher));

    cipher->cipher.allocator = allocator;
    cipher->cipher.block_size = AWS_AES_256_CIPHER_BLOCK_SIZE;
    cipher->cipher.key_length_bits = AWS_AES_256_KEY_BIT_LEN;
    cipher->alg_handle = s_aes_ctr_algorithm_handle;
    cipher->cipher.vtable = &s_aes_ctr_vtable;

    if (s_initialize_cipher_materials(cipher, key, iv, NULL, NULL, AWS_AES_256_CIPHER_BLOCK_SIZE, true, false) !=
        AWS_OP_SUCCESS) {
        goto error;
    }

    aws_byte_buf_init(&cipher->overflow, allocator, AWS_AES_256_CIPHER_BLOCK_SIZE * 2);
    aws_byte_buf_init_copy(&cipher->working_iv, allocator, &cipher->cipher.iv);

    cipher->cipher.impl = cipher;
    cipher->cipher.good = true;

    return &cipher->cipher;

error:
    if (cipher != NULL) {
        s_aes_default_destroy(&cipher->cipher);
    }

    return NULL;
}

/* This is just an encrypted key. Append them to a buffer and on finalize export/import the key using AES keywrap. */
static int s_key_wrap_encrypt_decrypt(
    struct aws_symmetric_cipher *cipher,
    const struct aws_byte_cursor input,
    struct aws_byte_buf *out) {
    (void)out;
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    return aws_byte_buf_append_dynamic(&cipher_impl->overflow, &input);
}

/* Import the buffer we've been appending to as an AES key. Then export it using AES Keywrap format. */
static int s_keywrap_finalize_encryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    BCRYPT_KEY_HANDLE key_handle_to_encrypt =
        s_import_key_blob(s_aes_keywrap_algorithm_handle, cipher->allocator, &cipher_impl->overflow);

    if (!key_handle_to_encrypt) {
        return AWS_OP_ERR;
    }

    NTSTATUS status = 0;

    ULONG output_size = 0;
    /* Call with NULL first to get the required size. */
    status = BCryptExportKey(
        key_handle_to_encrypt, cipher_impl->key_handle, BCRYPT_AES_WRAP_KEY_BLOB, NULL, 0, &output_size, 0);

    if (!NT_SUCCESS(status)) {
        cipher->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    int ret_val = AWS_OP_ERR;

    if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, output_size)) {
        goto clean_up;
    }

    /* now actually export the key */
    ULONG len_written = 0;
    status = BCryptExportKey(
        key_handle_to_encrypt,
        cipher_impl->key_handle,
        BCRYPT_AES_WRAP_KEY_BLOB,
        out->buffer + out->len,
        output_size,
        &len_written,
        0);

    if (!NT_SUCCESS(status)) {
        cipher->good = false;
        goto clean_up;
    }

    out->len += len_written;

    ret_val = AWS_OP_SUCCESS;

clean_up:
    if (key_handle_to_encrypt) {
        BCryptDestroyKey(key_handle_to_encrypt);
    }

    return ret_val;
}

/* Import the buffer we've been appending to as an AES Key Wrapped key. Then export the raw AES key. */

static int s_keywrap_finalize_decryption(struct aws_symmetric_cipher *cipher, struct aws_byte_buf *out) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    BCRYPT_KEY_HANDLE import_key = NULL;

    /* use the cipher key to import the buffer as an AES keywrapped key. */
    NTSTATUS status = BCryptImportKey(
        s_aes_keywrap_algorithm_handle,
        cipher_impl->key_handle,
        BCRYPT_AES_WRAP_KEY_BLOB,
        &import_key,
        NULL,
        0,
        cipher_impl->overflow.buffer,
        (ULONG)cipher_impl->overflow.len,
        0);
    int ret_val = AWS_OP_ERR;

    if (NT_SUCCESS(status) && import_key) {
        ULONG export_size = 0;

        struct aws_byte_buf key_data_blob;
        aws_byte_buf_init(
            &key_data_blob, cipher->allocator, sizeof(BCRYPT_KEY_DATA_BLOB_HEADER) + cipher_impl->overflow.len);

        /* Now just export the key out as a raw AES key. */
        status = BCryptExportKey(
            import_key,
            NULL,
            BCRYPT_KEY_DATA_BLOB,
            key_data_blob.buffer,
            (ULONG)key_data_blob.capacity,
            &export_size,
            0);

        key_data_blob.len += export_size;

        if (NT_SUCCESS(status)) {

            if (aws_symmetric_cipher_try_ensure_sufficient_buffer_space(out, export_size)) {
                goto clean_up;
            }

            BCRYPT_KEY_DATA_BLOB_HEADER *stream_header = (BCRYPT_KEY_DATA_BLOB_HEADER *)key_data_blob.buffer;

            AWS_FATAL_ASSERT(
                aws_byte_buf_write(
                    out, key_data_blob.buffer + sizeof(BCRYPT_KEY_DATA_BLOB_HEADER), stream_header->cbKeyData) &&
                "Copying key data failed but the allocation should have already occured successfully");
            ret_val = AWS_OP_SUCCESS;

        } else {
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            cipher->good = false;
        }

    clean_up:
        aws_byte_buf_clean_up_secure(&key_data_blob);
        BCryptDestroyKey(import_key);

    } else {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        cipher->good = false;
    }

    return ret_val;
}

static int s_reset_keywrap_cipher(struct aws_symmetric_cipher *cipher) {
    struct aes_bcrypt_cipher *cipher_impl = cipher->impl;

    s_clear_reusable_components(cipher);

    return s_initialize_cipher_materials(cipher_impl, NULL, NULL, NULL, NULL, 0, false, false);
}

static struct aws_symmetric_cipher_vtable s_aes_keywrap_vtable = {
    .alg_name = "AES-KEYWRAP 256",
    .provider = "Windows CNG",
    .decrypt = s_key_wrap_encrypt_decrypt,
    .encrypt = s_key_wrap_encrypt_decrypt,
    .finalize_encryption = s_keywrap_finalize_encryption,
    .finalize_decryption = s_keywrap_finalize_decryption,
    .destroy = s_aes_default_destroy,
    .reset = s_reset_keywrap_cipher,
};

struct aws_symmetric_cipher *aws_aes_keywrap_256_new_impl(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *key) {

    aws_thread_call_once(&s_aes_thread_once, s_load_alg_handles, NULL);
    struct aes_bcrypt_cipher *cipher = aws_mem_calloc(allocator, 1, sizeof(struct aes_bcrypt_cipher));

    cipher->cipher.allocator = allocator;
    cipher->cipher.block_size = 8;
    cipher->cipher.key_length_bits = AWS_AES_256_KEY_BIT_LEN;
    cipher->alg_handle = s_aes_keywrap_algorithm_handle;
    cipher->cipher.vtable = &s_aes_keywrap_vtable;

    if (s_initialize_cipher_materials(cipher, key, NULL, NULL, NULL, 0, false, false) != AWS_OP_SUCCESS) {
        goto error;
    }

    aws_byte_buf_init(&cipher->overflow, allocator, (AWS_AES_256_CIPHER_BLOCK_SIZE * 2) + 8);

    cipher->cipher.impl = cipher;
    cipher->cipher.good = true;

    return &cipher->cipher;

error:
    if (cipher != NULL) {
        s_aes_default_destroy(&cipher->cipher);
    }

    return NULL;
}
