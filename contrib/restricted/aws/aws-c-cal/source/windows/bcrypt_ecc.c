/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/private/ecc.h>

#include <aws/cal/cal.h>
#include <aws/cal/private/der.h>

#include <aws/common/thread.h>

#include <windows.h>

#include <bcrypt.h>

#include <winerror.h>

static BCRYPT_ALG_HANDLE s_ecdsa_p256_alg = NULL;
static BCRYPT_ALG_HANDLE s_ecdsa_p384_alg = NULL;

/* size of the P384 curve's signatures. This is the largest we support at the moment.
   Since msvc doesn't support variable length arrays, we need to handle this with a macro. */
#define MAX_SIGNATURE_LENGTH (48 * 2)

static aws_thread_once s_ecdsa_thread_once = AWS_THREAD_ONCE_STATIC_INIT;

static void s_load_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    NTSTATUS status =
        BCryptOpenAlgorithmProvider(&s_ecdsa_p256_alg, BCRYPT_ECDSA_P256_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_ASSERT(s_ecdsa_p256_alg && "BCryptOpenAlgorithmProvider() failed");

    status = BCryptOpenAlgorithmProvider(&s_ecdsa_p384_alg, BCRYPT_ECDSA_P384_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_ASSERT(s_ecdsa_p384_alg && "BCryptOpenAlgorithmProvider() failed");

    (void)status;
}

struct bcrypt_ecc_key_pair {
    struct aws_ecc_key_pair key_pair;
    BCRYPT_KEY_HANDLE key_handle;
};

static BCRYPT_ALG_HANDLE s_key_alg_handle_from_curve_name(enum aws_ecc_curve_name curve_name) {
    switch (curve_name) {
        case AWS_CAL_ECDSA_P256:
            return s_ecdsa_p256_alg;
        case AWS_CAL_ECDSA_P384:
            return s_ecdsa_p384_alg;
        default:
            return 0;
    }
}

static ULONG s_get_magic_from_curve_name(enum aws_ecc_curve_name curve_name, bool private_key) {
    switch (curve_name) {
        case AWS_CAL_ECDSA_P256:
            return private_key ? BCRYPT_ECDSA_PRIVATE_P256_MAGIC : BCRYPT_ECDSA_PUBLIC_P256_MAGIC;
        case AWS_CAL_ECDSA_P384:
            return private_key ? BCRYPT_ECDSA_PRIVATE_P384_MAGIC : BCRYPT_ECDSA_PUBLIC_P384_MAGIC;
        default:
            return 0;
    }
}

static void s_destroy_key(struct aws_ecc_key_pair *key_pair) {
    if (key_pair) {
        struct bcrypt_ecc_key_pair *key_impl = key_pair->impl;

        if (key_impl->key_handle) {
            BCryptDestroyKey(key_impl->key_handle);
        }

        aws_byte_buf_clean_up_secure(&key_pair->key_buf);
        aws_mem_release(key_pair->allocator, key_impl);
    }
}

static size_t s_signature_length(const struct aws_ecc_key_pair *key_pair) {
    static size_t s_der_overhead = 8;
    return s_der_overhead + aws_ecc_key_coordinate_byte_size_from_curve_name(key_pair->curve_name) * 2;
}

static bool s_trim_zeros_predicate(uint8_t value) {
    return value == 0;
}

static int s_sign_message(
    const struct aws_ecc_key_pair *key_pair,
    const struct aws_byte_cursor *message,
    struct aws_byte_buf *signature_output) {
    struct bcrypt_ecc_key_pair *key_impl = key_pair->impl;

    size_t output_buf_space = signature_output->capacity - signature_output->len;

    if (output_buf_space < s_signature_length(key_pair)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    uint8_t temp_signature[MAX_SIGNATURE_LENGTH] = {0};
    struct aws_byte_buf temp_signature_buf = aws_byte_buf_from_empty_array(temp_signature, sizeof(temp_signature));
    size_t signature_length = temp_signature_buf.capacity;

    NTSTATUS status = BCryptSignHash(
        key_impl->key_handle,
        NULL,
        message->ptr,
        (ULONG)message->len,
        temp_signature_buf.buffer,
        (ULONG)signature_length,
        (ULONG *)&signature_length,
        0);

    if (status != 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    temp_signature_buf.len += signature_length;
    size_t coordinate_len = temp_signature_buf.len / 2;

    /* okay. Windows doesn't DER encode this to ASN.1, so we need to do it manually. */
    struct aws_der_encoder *encoder =
        aws_der_encoder_new(key_pair->allocator, signature_output->capacity - signature_output->len);
    if (!encoder) {
        return AWS_OP_ERR;
    }

    aws_der_encoder_begin_sequence(encoder);
    struct aws_byte_cursor integer_cur = aws_byte_cursor_from_array(temp_signature_buf.buffer, coordinate_len);
    /* trim off the leading zero padding for DER encoding */
    integer_cur = aws_byte_cursor_left_trim_pred(&integer_cur, s_trim_zeros_predicate);
    aws_der_encoder_write_unsigned_integer(encoder, integer_cur);
    integer_cur = aws_byte_cursor_from_array(temp_signature_buf.buffer + coordinate_len, coordinate_len);
    /* trim off the leading zero padding for DER encoding */
    integer_cur = aws_byte_cursor_left_trim_pred(&integer_cur, s_trim_zeros_predicate);
    aws_der_encoder_write_unsigned_integer(encoder, integer_cur);
    aws_der_encoder_end_sequence(encoder);

    struct aws_byte_cursor signature_out_cur;
    AWS_ZERO_STRUCT(signature_out_cur);
    aws_der_encoder_get_contents(encoder, &signature_out_cur);
    aws_byte_buf_append(signature_output, &signature_out_cur);
    aws_der_encoder_destroy(encoder);

    return AWS_OP_SUCCESS;
}

static int s_derive_public_key(struct aws_ecc_key_pair *key_pair) {
    struct bcrypt_ecc_key_pair *key_impl = key_pair->impl;

    ULONG result = 0;
    NTSTATUS status = BCryptExportKey(
        key_impl->key_handle,
        NULL,
        BCRYPT_ECCPRIVATE_BLOB,
        key_pair->key_buf.buffer,
        (ULONG)key_pair->key_buf.capacity,
        &result,
        0);
    key_pair->key_buf.len = result;
    (void)result;

    if (status) {
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    return AWS_OP_SUCCESS;
}

static int s_append_coordinate(
    struct aws_byte_buf *buffer,
    struct aws_byte_cursor *coordinate,
    enum aws_ecc_curve_name curve_name) {

    size_t coordinate_size = aws_ecc_key_coordinate_byte_size_from_curve_name(curve_name);
    if (coordinate->len < coordinate_size) {
        size_t leading_zero_count = coordinate_size - coordinate->len;
        AWS_FATAL_ASSERT(leading_zero_count + buffer->len <= buffer->capacity);

        aws_byte_buf_write_u8_n(buffer, 0x0, leading_zero_count);
    }

    return aws_byte_buf_append(buffer, coordinate);
}

static int s_verify_signature(
    const struct aws_ecc_key_pair *key_pair,
    const struct aws_byte_cursor *message,
    const struct aws_byte_cursor *signature) {
    struct bcrypt_ecc_key_pair *key_impl = key_pair->impl;

    /* OKAY Windows doesn't do the whole standard internet formats thing. So we need to manually decode
       the DER encoded ASN.1 format first.*/
    uint8_t temp_signature[MAX_SIGNATURE_LENGTH] = {0};
    struct aws_byte_buf temp_signature_buf = aws_byte_buf_from_empty_array(temp_signature, sizeof(temp_signature));

    struct aws_byte_cursor der_encoded_signature = aws_byte_cursor_from_array(signature->ptr, signature->len);

    struct aws_der_decoder *decoder = aws_der_decoder_new(key_pair->allocator, der_encoded_signature);
    if (!decoder) {
        return AWS_OP_ERR;
    }

    if (!aws_der_decoder_next(decoder) || aws_der_decoder_tlv_type(decoder) != AWS_DER_SEQUENCE) {
        aws_raise_error(AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED);
        goto error;
    }

    if (!aws_der_decoder_next(decoder) || aws_der_decoder_tlv_type(decoder) != AWS_DER_INTEGER) {
        aws_raise_error(AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED);
        goto error;
    }

    /* there will be two coordinates. They need to be concatenated together. */
    struct aws_byte_cursor coordinate;
    AWS_ZERO_STRUCT(coordinate);
    if (aws_der_decoder_tlv_unsigned_integer(decoder, &coordinate)) {
        aws_raise_error(AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED);
        goto error;
    }

    if (s_append_coordinate(&temp_signature_buf, &coordinate, key_pair->curve_name)) {
        goto error;
    }

    if (!aws_der_decoder_next(decoder) || aws_der_decoder_tlv_type(decoder) != AWS_DER_INTEGER) {
        aws_raise_error(AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED);
        goto error;
    }
    AWS_ZERO_STRUCT(coordinate);
    if (aws_der_decoder_tlv_unsigned_integer(decoder, &coordinate)) {
        aws_raise_error(AWS_ERROR_CAL_MALFORMED_ASN1_ENCOUNTERED);
        goto error;
    }

    if (s_append_coordinate(&temp_signature_buf, &coordinate, key_pair->curve_name)) {
        goto error;
    }

    aws_der_decoder_destroy(decoder);

    /* okay, now we've got a windows compatible signature, let's verify it. */
    NTSTATUS status = BCryptVerifySignature(
        key_impl->key_handle,
        NULL,
        message->ptr,
        (ULONG)message->len,
        temp_signature_buf.buffer,
        (ULONG)temp_signature_buf.len,
        0);

    return status == 0 ? AWS_OP_SUCCESS : aws_raise_error(AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED);

error:
    if (decoder) {
        aws_der_decoder_destroy(decoder);
    }
    return AWS_OP_ERR;
}

static struct aws_ecc_key_pair_vtable s_vtable = {
    .destroy = s_destroy_key,
    .derive_pub_key = s_derive_public_key,
    .sign_message = s_sign_message,
    .verify_signature = s_verify_signature,
    .signature_length = s_signature_length,
};

static struct aws_ecc_key_pair *s_alloc_pair_and_init_buffers(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    struct aws_byte_cursor pub_x,
    struct aws_byte_cursor pub_y,
    struct aws_byte_cursor priv_key) {

    aws_thread_call_once(&s_ecdsa_thread_once, s_load_alg_handle, NULL);

    struct bcrypt_ecc_key_pair *key_impl = aws_mem_calloc(allocator, 1, sizeof(struct bcrypt_ecc_key_pair));

    if (!key_impl) {
        return NULL;
    }

    key_impl->key_pair.allocator = allocator;
    key_impl->key_pair.curve_name = curve_name;
    key_impl->key_pair.impl = key_impl;
    key_impl->key_pair.vtable = &s_vtable;
    aws_atomic_init_int(&key_impl->key_pair.ref_count, 1);

    size_t s_key_coordinate_size = aws_ecc_key_coordinate_byte_size_from_curve_name(curve_name);

    if (!s_key_coordinate_size) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    if ((pub_x.ptr && pub_x.len != s_key_coordinate_size) || (pub_y.ptr && pub_y.len != s_key_coordinate_size) ||
        (priv_key.ptr && priv_key.len != s_key_coordinate_size)) {
        aws_raise_error(AWS_ERROR_CAL_INVALID_KEY_LENGTH_FOR_ALGORITHM);
        goto error;
    }

    size_t total_buffer_size = s_key_coordinate_size * 3 + sizeof(BCRYPT_ECCKEY_BLOB);

    if (aws_byte_buf_init(&key_impl->key_pair.key_buf, allocator, total_buffer_size)) {
        goto error;
    }

    aws_byte_buf_secure_zero(&key_impl->key_pair.key_buf);

    BCRYPT_ECCKEY_BLOB key_blob;
    AWS_ZERO_STRUCT(key_blob);
    key_blob.dwMagic = s_get_magic_from_curve_name(curve_name, priv_key.ptr && priv_key.len);
    key_blob.cbKey = (ULONG)s_key_coordinate_size;

    struct aws_byte_cursor header = aws_byte_cursor_from_array(&key_blob, sizeof(key_blob));
    aws_byte_buf_append(&key_impl->key_pair.key_buf, &header);

    LPCWSTR blob_type = BCRYPT_ECCPUBLIC_BLOB;
    ULONG flags = 0;
    if (pub_x.ptr && pub_y.ptr) {
        aws_byte_buf_append(&key_impl->key_pair.key_buf, &pub_x);
        aws_byte_buf_append(&key_impl->key_pair.key_buf, &pub_y);
    } else {
        key_impl->key_pair.key_buf.len += s_key_coordinate_size * 2;
        flags = BCRYPT_NO_KEY_VALIDATION;
    }

    if (priv_key.ptr) {
        blob_type = BCRYPT_ECCPRIVATE_BLOB;
        aws_byte_buf_append(&key_impl->key_pair.key_buf, &priv_key);
    }

    key_impl->key_pair.pub_x =
        aws_byte_buf_from_array(key_impl->key_pair.key_buf.buffer + sizeof(key_blob), s_key_coordinate_size);

    key_impl->key_pair.pub_y =
        aws_byte_buf_from_array(key_impl->key_pair.pub_x.buffer + s_key_coordinate_size, s_key_coordinate_size);

    key_impl->key_pair.priv_d =
        aws_byte_buf_from_array(key_impl->key_pair.pub_y.buffer + s_key_coordinate_size, s_key_coordinate_size);

    BCRYPT_ALG_HANDLE alg_handle = s_key_alg_handle_from_curve_name(curve_name);
    NTSTATUS status = BCryptImportKeyPair(
        alg_handle,
        NULL,
        blob_type,
        &key_impl->key_handle,
        key_impl->key_pair.key_buf.buffer,
        (ULONG)key_impl->key_pair.key_buf.len,
        flags);

    if (status) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    return &key_impl->key_pair;

error:
    s_destroy_key(&key_impl->key_pair);
    return NULL;
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_private_key_impl(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    const struct aws_byte_cursor *priv_key) {

    struct aws_byte_cursor empty;
    AWS_ZERO_STRUCT(empty);
    return s_alloc_pair_and_init_buffers(allocator, curve_name, empty, empty, *priv_key);
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_public_key_impl(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    const struct aws_byte_cursor *public_key_x,
    const struct aws_byte_cursor *public_key_y) {

    struct aws_byte_cursor empty;
    AWS_ZERO_STRUCT(empty);
    return s_alloc_pair_and_init_buffers(allocator, curve_name, *public_key_x, *public_key_y, empty);
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_generate_random(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name) {
    aws_thread_call_once(&s_ecdsa_thread_once, s_load_alg_handle, NULL);

    struct bcrypt_ecc_key_pair *key_impl = aws_mem_calloc(allocator, 1, sizeof(struct bcrypt_ecc_key_pair));

    if (!key_impl) {
        return NULL;
    }

    key_impl->key_pair.allocator = allocator;
    key_impl->key_pair.curve_name = curve_name;
    key_impl->key_pair.impl = key_impl;
    key_impl->key_pair.vtable = &s_vtable;
    aws_atomic_init_int(&key_impl->key_pair.ref_count, 1);

    size_t key_coordinate_size = aws_ecc_key_coordinate_byte_size_from_curve_name(curve_name);

    if (!key_coordinate_size) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    BCRYPT_ALG_HANDLE alg_handle = s_key_alg_handle_from_curve_name(curve_name);

    ULONG key_bit_length = (ULONG)key_coordinate_size * 8;
    NTSTATUS status = BCryptGenerateKeyPair(alg_handle, &key_impl->key_handle, key_bit_length, 0);

    if (status) {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        goto error;
    }

    status = BCryptFinalizeKeyPair(key_impl->key_handle, 0);

    if (status) {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        goto error;
    }

    size_t total_buffer_size = key_coordinate_size * 3 + sizeof(BCRYPT_ECCKEY_BLOB);

    if (aws_byte_buf_init(&key_impl->key_pair.key_buf, allocator, total_buffer_size)) {
        goto error;
    }

    aws_byte_buf_secure_zero(&key_impl->key_pair.key_buf);

    key_impl->key_pair.pub_x =
        aws_byte_buf_from_array(key_impl->key_pair.key_buf.buffer + sizeof(BCRYPT_ECCKEY_BLOB), key_coordinate_size);

    key_impl->key_pair.pub_y =
        aws_byte_buf_from_array(key_impl->key_pair.pub_x.buffer + key_coordinate_size, key_coordinate_size);

    key_impl->key_pair.priv_d =
        aws_byte_buf_from_array(key_impl->key_pair.pub_y.buffer + key_coordinate_size, key_coordinate_size);

    if (s_derive_public_key(&key_impl->key_pair)) {
        goto error;
    }

    return &key_impl->key_pair;

error:
    s_destroy_key(&key_impl->key_pair);
    return NULL;
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_asn1(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *encoded_keys) {
    struct aws_der_decoder *decoder = aws_der_decoder_new(allocator, *encoded_keys);

    /* we could have private key or a public key, or a full pair. */
    struct aws_byte_cursor pub_x;
    AWS_ZERO_STRUCT(pub_x);
    struct aws_byte_cursor pub_y;
    AWS_ZERO_STRUCT(pub_y);
    struct aws_byte_cursor priv_d;
    AWS_ZERO_STRUCT(priv_d);

    enum aws_ecc_curve_name curve_name;
    if (aws_der_decoder_load_ecc_key_pair(decoder, &pub_x, &pub_y, &priv_d, &curve_name)) {
        goto error;
    }

    /* now that we have the buffers, we can just use the normal code path. */
    struct aws_ecc_key_pair *key_pair = s_alloc_pair_and_init_buffers(allocator, curve_name, pub_x, pub_y, priv_d);
    aws_der_decoder_destroy(decoder);

    return key_pair;
error:
    if (decoder) {
        aws_der_decoder_destroy(decoder);
    }
    return NULL;
}
