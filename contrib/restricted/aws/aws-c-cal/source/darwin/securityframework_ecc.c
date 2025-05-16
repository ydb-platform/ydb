/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/private/ecc.h>

#include <aws/cal/cal.h>
#include <aws/cal/private/der.h>

#include <CoreFoundation/CoreFoundation.h>
#include <Security/SecKey.h>
#include <Security/Security.h>

struct commoncrypto_ecc_key_pair {
    struct aws_ecc_key_pair key_pair;
    SecKeyRef priv_key_ref;
    SecKeyRef pub_key_ref;
    CFAllocatorRef cf_allocator;
};

static uint8_t s_preamble = 0x04;

static size_t s_der_overhead = 8;

/* The hard-coded "valid" public keys. Copy/pated from one of our unit test. */
const static uint8_t s_fake_x_ecdsa_p256[] = {
    0xd0, 0x72, 0x0d, 0xc6, 0x91, 0xaa, 0x80, 0x09, 0x6b, 0xa3, 0x2f, 0xed, 0x1c, 0xb9, 0x7c, 0x2b,
    0x62, 0x06, 0x90, 0xd0, 0x6d, 0xe0, 0x31, 0x7b, 0x86, 0x18, 0xd5, 0xce, 0x65, 0xeb, 0x72, 0x8f,
};

const static uint8_t s_fake_y_ecdsa_p256[] = {
    0x96, 0x81, 0xb5, 0x17, 0xb1, 0xcd, 0xa1, 0x7d, 0x0d, 0x83, 0xd3, 0x35, 0xd9, 0xc4, 0xa8, 0xa9,
    0xa9, 0xb0, 0xb1, 0xb3, 0xc7, 0x10, 0x6d, 0x8f, 0x3c, 0x72, 0xbc, 0x50, 0x93, 0xdc, 0x27, 0x5f,
};

const static uint8_t s_fake_x_ecdsa_p384[] = {
    0xfd, 0x3c, 0x84, 0xe5, 0x68, 0x9b, 0xed, 0x27, 0x0e, 0x60, 0x1b, 0x3d, 0x80, 0xf9, 0x0d, 0x67,
    0xa9, 0xae, 0x45, 0x1c, 0xce, 0x89, 0x0f, 0x53, 0xe5, 0x83, 0x22, 0x9a, 0xd0, 0xe2, 0xee, 0x64,
    0x56, 0x11, 0xfa, 0x99, 0x36, 0xdf, 0xa4, 0x53, 0x06, 0xec, 0x18, 0x06, 0x67, 0x74, 0xaa, 0x24,
};

const static uint8_t s_fake_y_ecdsa_p384[] = {
    0xb8, 0x3c, 0xa4, 0x12, 0x6c, 0xfc, 0x4c, 0x4d, 0x1d, 0x18, 0xa4, 0xb6, 0xc2, 0x1c, 0x7f, 0x69,
    0x9d, 0x51, 0x23, 0xdd, 0x9c, 0x24, 0xf6, 0x6f, 0x83, 0x38, 0x46, 0xee, 0xb5, 0x82, 0x96, 0x19,
    0x6b, 0x42, 0xec, 0x06, 0x42, 0x5d, 0xb5, 0xb7, 0x0a, 0x4b, 0x81, 0xb7, 0xfc, 0xf7, 0x05, 0xa0,
};

static int s_sign_message(
    const struct aws_ecc_key_pair *key_pair,
    const struct aws_byte_cursor *message,
    struct aws_byte_buf *signature_output) {
    struct commoncrypto_ecc_key_pair *cc_key = key_pair->impl;

    if (!cc_key->priv_key_ref) {
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    CFDataRef hash_ref = CFDataCreateWithBytesNoCopy(NULL, message->ptr, message->len, kCFAllocatorNull);
    AWS_FATAL_ASSERT(hash_ref && "No allocations should have happened here, this function shouldn't be able to fail.");

    CFErrorRef error = NULL;
    CFDataRef signature =
        SecKeyCreateSignature(cc_key->priv_key_ref, kSecKeyAlgorithmECDSASignatureDigestX962, hash_ref, &error);

    if (error) {
        CFRelease(hash_ref);
        return aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
    }

    struct aws_byte_cursor to_write =
        aws_byte_cursor_from_array(CFDataGetBytePtr(signature), CFDataGetLength(signature));

    if (aws_byte_buf_append(signature_output, &to_write)) {
        CFRelease(signature);
        CFRelease(hash_ref);
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    CFRelease(signature);
    CFRelease(hash_ref);

    return AWS_OP_SUCCESS;
}

static size_t s_signature_length(const struct aws_ecc_key_pair *key_pair) {
    return aws_ecc_key_coordinate_byte_size_from_curve_name(key_pair->curve_name) * 2 + s_der_overhead;
}

static int s_verify_signature(
    const struct aws_ecc_key_pair *key_pair,
    const struct aws_byte_cursor *message,
    const struct aws_byte_cursor *signature) {
    struct commoncrypto_ecc_key_pair *cc_key = key_pair->impl;

    if (!cc_key->pub_key_ref) {
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    CFDataRef hash_ref = CFDataCreateWithBytesNoCopy(NULL, message->ptr, message->len, kCFAllocatorNull);
    CFDataRef signature_ref = CFDataCreateWithBytesNoCopy(NULL, signature->ptr, signature->len, kCFAllocatorNull);

    AWS_FATAL_ASSERT(hash_ref && "No allocations should have happened here, this function shouldn't be able to fail.");
    AWS_FATAL_ASSERT(
        signature_ref && "No allocations should have happened here, this function shouldn't be able to fail.");

    CFErrorRef error = NULL;

    bool verified = SecKeyVerifySignature(
        cc_key->pub_key_ref, kSecKeyAlgorithmECDSASignatureDigestX962, hash_ref, signature_ref, &error);

    CFRelease(signature_ref);
    CFRelease(hash_ref);

    return verified ? AWS_OP_SUCCESS : aws_raise_error(AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED);
}

static int s_derive_public_key(struct aws_ecc_key_pair *key_pair) {
    /* we already have a public key, just lie and tell them we succeeded */
    if (key_pair->pub_x.buffer && key_pair->pub_x.len) {
        return AWS_OP_SUCCESS;
    }

    return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
}

static void s_destroy_key(struct aws_ecc_key_pair *key_pair) {
    if (key_pair) {
        struct commoncrypto_ecc_key_pair *cc_key = key_pair->impl;

        if (cc_key->pub_key_ref) {
            CFRelease(cc_key->pub_key_ref);
        }

        if (cc_key->priv_key_ref) {
            CFRelease(cc_key->priv_key_ref);
        }

        if (cc_key->cf_allocator) {
            aws_wrapped_cf_allocator_destroy(cc_key->cf_allocator);
        }

        aws_byte_buf_clean_up_secure(&key_pair->key_buf);
        aws_mem_release(key_pair->allocator, cc_key);
    }
}

static struct aws_ecc_key_pair_vtable s_key_pair_vtable = {
    .sign_message = s_sign_message,
    .signature_length = s_signature_length,
    .verify_signature = s_verify_signature,
    .derive_pub_key = s_derive_public_key,
    .destroy = s_destroy_key,
};

static struct commoncrypto_ecc_key_pair *s_alloc_pair_and_init_buffers(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    struct aws_byte_cursor pub_x,
    struct aws_byte_cursor pub_y,
    struct aws_byte_cursor priv_key) {
    struct commoncrypto_ecc_key_pair *cc_key_pair =
        aws_mem_calloc(allocator, 1, sizeof(struct commoncrypto_ecc_key_pair));

    if (!cc_key_pair) {
        return NULL;
    }

    aws_atomic_init_int(&cc_key_pair->key_pair.ref_count, 1);
    cc_key_pair->key_pair.impl = cc_key_pair;
    cc_key_pair->key_pair.allocator = allocator;
    cc_key_pair->cf_allocator = aws_wrapped_cf_allocator_new(allocator);

    if (!cc_key_pair->cf_allocator) {
        goto error;
    }

    size_t s_key_coordinate_size = aws_ecc_key_coordinate_byte_size_from_curve_name(curve_name);

    if (!s_key_coordinate_size) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    if ((pub_x.ptr && pub_x.len != s_key_coordinate_size) || (pub_y.ptr && pub_y.len != s_key_coordinate_size) ||
        (priv_key.ptr && priv_key.len != s_key_coordinate_size)) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    size_t total_buffer_size = s_key_coordinate_size * 3 + 1;

    if (aws_byte_buf_init(&cc_key_pair->key_pair.key_buf, allocator, total_buffer_size)) {
        goto error;
    }

    aws_byte_buf_write_u8(&cc_key_pair->key_pair.key_buf, s_preamble);

    if (pub_x.ptr && pub_y.ptr) {
        aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &pub_x);
        aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &pub_y);
    } else {
        aws_byte_buf_write_u8_n(&cc_key_pair->key_pair.key_buf, 0x0, s_key_coordinate_size * 2);
    }

    if (priv_key.ptr) {
        aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &priv_key);
    }

    if (pub_x.ptr) {
        cc_key_pair->key_pair.pub_x =
            aws_byte_buf_from_array(cc_key_pair->key_pair.key_buf.buffer + 1, s_key_coordinate_size);

        cc_key_pair->key_pair.pub_y =
            aws_byte_buf_from_array(cc_key_pair->key_pair.pub_x.buffer + s_key_coordinate_size, s_key_coordinate_size);
    }

    cc_key_pair->key_pair.priv_d = aws_byte_buf_from_array(
        cc_key_pair->key_pair.key_buf.buffer + 1 + (s_key_coordinate_size * 2), s_key_coordinate_size);
    cc_key_pair->key_pair.vtable = &s_key_pair_vtable;
    cc_key_pair->key_pair.curve_name = curve_name;

    return cc_key_pair;

error:
    s_destroy_key(&cc_key_pair->key_pair);
    return NULL;
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_private_key_impl(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    const struct aws_byte_cursor *priv_key) {

    /**
     * We use SecCreateKeyWithData to create ECC key. Expected format for the key passed to that api is a byte buffer
     * consisting of "0x04 | x | y | p", where x,y is public pair and p is private key.
     *
     * In this case we only have private key and we need to construct SecKey from that.
     *
     * We used to just pass 0,0 point for x,y, i.e. "0x04 | 0 | 0 | p".
     *
     * This used to work on Macs before 14, but in 14+ SecCreateKeyWithData returns error,
     * which is reasonable since 0,0 is not a valid public point.
     *
     * To get around the issue, we use a fake public key, which is a valid public point, but not matching the private
     * key as a quick workaround.
     */
    struct aws_byte_cursor fake_pub_x;
    AWS_ZERO_STRUCT(fake_pub_x);
    struct aws_byte_cursor fake_pub_y;
    AWS_ZERO_STRUCT(fake_pub_y);
    switch (curve_name) {
        case AWS_CAL_ECDSA_P256:
            fake_pub_x = aws_byte_cursor_from_array(s_fake_x_ecdsa_p256, AWS_ARRAY_SIZE(s_fake_x_ecdsa_p256));
            fake_pub_y = aws_byte_cursor_from_array(s_fake_y_ecdsa_p256, AWS_ARRAY_SIZE(s_fake_y_ecdsa_p256));
            break;
        case AWS_CAL_ECDSA_P384:
            fake_pub_x = aws_byte_cursor_from_array(s_fake_x_ecdsa_p384, AWS_ARRAY_SIZE(s_fake_x_ecdsa_p384));
            fake_pub_y = aws_byte_cursor_from_array(s_fake_y_ecdsa_p384, AWS_ARRAY_SIZE(s_fake_y_ecdsa_p384));
            break;
        default:
            aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
            return NULL;
    }

    struct commoncrypto_ecc_key_pair *cc_key_pair =
        s_alloc_pair_and_init_buffers(allocator, curve_name, fake_pub_x, fake_pub_y, *priv_key);

    if (!cc_key_pair) {
        return NULL;
    }

    CFMutableDictionaryRef key_attributes = NULL;
    CFDataRef private_key_data = CFDataCreate(
        cc_key_pair->cf_allocator, cc_key_pair->key_pair.key_buf.buffer, cc_key_pair->key_pair.key_buf.len);

    if (!private_key_data) {
        goto error;
    }

    key_attributes = CFDictionaryCreateMutable(cc_key_pair->cf_allocator, 6, NULL, NULL);

    if (!key_attributes) {
        goto error;
    }

    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPrivate);
    CFIndex key_size_bits = cc_key_pair->key_pair.priv_d.len * 8;
    CFDictionaryAddValue(key_attributes, kSecAttrKeySizeInBits, &key_size_bits);
    CFDictionaryAddValue(key_attributes, kSecAttrCanSign, kCFBooleanTrue);
    CFDictionaryAddValue(key_attributes, kSecAttrCanVerify, kCFBooleanFalse);
    CFDictionaryAddValue(key_attributes, kSecAttrCanDerive, kCFBooleanTrue);

    CFErrorRef error = NULL;

    cc_key_pair->priv_key_ref = SecKeyCreateWithData(private_key_data, key_attributes, &error);

    if (error) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        CFRelease(error);
        goto error;
    }

    /* Zero out the fake public keys in the key pair */
    aws_byte_buf_secure_zero(&cc_key_pair->key_pair.pub_x);
    aws_byte_buf_secure_zero(&cc_key_pair->key_pair.pub_y);

    CFRelease(key_attributes);
    CFRelease(private_key_data);

    return &cc_key_pair->key_pair;

error:
    if (private_key_data) {
        CFRelease(private_key_data);
    }

    if (key_attributes) {
        CFRelease(key_attributes);
    }
    s_destroy_key(&cc_key_pair->key_pair);
    return NULL;
}

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_public_key_impl(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name,
    const struct aws_byte_cursor *public_key_x,
    const struct aws_byte_cursor *public_key_y) {

    struct aws_byte_cursor empty_cur;
    AWS_ZERO_STRUCT(empty_cur);
    struct commoncrypto_ecc_key_pair *cc_key_pair =
        s_alloc_pair_and_init_buffers(allocator, curve_name, *public_key_x, *public_key_y, empty_cur);

    if (!cc_key_pair) {
        return NULL;
    }

    CFMutableDictionaryRef key_attributes = NULL;
    CFDataRef pub_key_data = CFDataCreate(
        cc_key_pair->cf_allocator, cc_key_pair->key_pair.key_buf.buffer, cc_key_pair->key_pair.key_buf.len);

    if (!pub_key_data) {
        goto error;
    }

    key_attributes = CFDictionaryCreateMutable(cc_key_pair->cf_allocator, 6, NULL, NULL);

    if (!key_attributes) {
        goto error;
    }

    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPublic);
    CFIndex key_size_bits = cc_key_pair->key_pair.pub_x.len * 8;
    CFDictionaryAddValue(key_attributes, kSecAttrKeySizeInBits, &key_size_bits);
    CFDictionaryAddValue(key_attributes, kSecAttrCanSign, kCFBooleanFalse);
    CFDictionaryAddValue(key_attributes, kSecAttrCanVerify, kCFBooleanTrue);
    CFDictionaryAddValue(key_attributes, kSecAttrCanDerive, kCFBooleanFalse);

    CFErrorRef error = NULL;

    cc_key_pair->pub_key_ref = SecKeyCreateWithData(pub_key_data, key_attributes, &error);

    if (error) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        CFRelease(error);
        goto error;
    }

    CFRelease(key_attributes);
    CFRelease(pub_key_data);

    return &cc_key_pair->key_pair;

error:
    if (key_attributes) {
        CFRelease(key_attributes);
    }

    if (pub_key_data) {
        CFRelease(pub_key_data);
    }

    s_destroy_key(&cc_key_pair->key_pair);
    return NULL;
}

#if defined(AWS_OS_MACOS)
struct aws_ecc_key_pair *aws_ecc_key_pair_new_generate_random(
    struct aws_allocator *allocator,
    enum aws_ecc_curve_name curve_name) {
    struct commoncrypto_ecc_key_pair *cc_key_pair =
        aws_mem_calloc(allocator, 1, sizeof(struct commoncrypto_ecc_key_pair));

    if (!cc_key_pair) {
        return NULL;
    }

    CFDataRef sec_key_export_data = NULL;
    CFStringRef key_size_cf_str = NULL;
    CFMutableDictionaryRef key_attributes = NULL;
    struct aws_der_decoder *decoder = NULL;

    aws_atomic_init_int(&cc_key_pair->key_pair.ref_count, 1);
    cc_key_pair->key_pair.impl = cc_key_pair;
    cc_key_pair->key_pair.allocator = allocator;
    cc_key_pair->cf_allocator = aws_wrapped_cf_allocator_new(allocator);

    if (!cc_key_pair->cf_allocator) {
        goto error;
    }

    size_t key_coordinate_size = aws_ecc_key_coordinate_byte_size_from_curve_name(curve_name);

    if (!key_coordinate_size) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    key_attributes = CFDictionaryCreateMutable(cc_key_pair->cf_allocator, 6, NULL, NULL);

    if (!key_attributes) {
        goto error;
    }

    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPrivate);
    CFIndex key_size_bits = key_coordinate_size * 8;
    char key_size_str[32] = {0};
    snprintf(key_size_str, sizeof(key_size_str), "%d", (int)key_size_bits);
    key_size_cf_str = CFStringCreateWithCString(cc_key_pair->cf_allocator, key_size_str, kCFStringEncodingASCII);

    if (!key_size_cf_str) {
        goto error;
    }

    CFDictionaryAddValue(key_attributes, kSecAttrKeySizeInBits, key_size_cf_str);

    CFErrorRef error = NULL;

    cc_key_pair->priv_key_ref = SecKeyCreateRandomKey(key_attributes, &error);

    if (error) {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        CFRelease(error);
        goto error;
    }

    cc_key_pair->pub_key_ref = SecKeyCopyPublicKey(cc_key_pair->priv_key_ref);

    /* OKAY up to here was incredibly reasonable, after this we get attacked by the bad API design
     * dragons.
     *
     * Summary: Apple assumed we'd never need the raw key data. Apple was wrong. So we have to export each component
     * into the OpenSSL format (just fancy words for DER), but the public key and private key are exported separately
     * for some reason. Anyways, we export the keys, use our handy dandy DER decoder and grab the raw key data out. */
    OSStatus ret_code = SecItemExport(cc_key_pair->priv_key_ref, kSecFormatOpenSSL, 0, NULL, &sec_key_export_data);

    if (ret_code != errSecSuccess) {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
        goto error;
    }

    /* now we need to DER decode data */
    struct aws_byte_cursor key_cur =
        aws_byte_cursor_from_array(CFDataGetBytePtr(sec_key_export_data), CFDataGetLength(sec_key_export_data));

    decoder = aws_der_decoder_new(allocator, key_cur);

    if (!decoder) {
        goto error;
    }

    struct aws_byte_cursor pub_x;
    AWS_ZERO_STRUCT(pub_x);
    struct aws_byte_cursor pub_y;
    AWS_ZERO_STRUCT(pub_y);
    struct aws_byte_cursor priv_d;
    AWS_ZERO_STRUCT(priv_d);

    if (aws_der_decoder_load_ecc_key_pair(decoder, &pub_x, &pub_y, &priv_d, &curve_name)) {
        goto error;
    }

    AWS_ASSERT(
        priv_d.len == key_coordinate_size && pub_x.len == key_coordinate_size && pub_y.len == key_coordinate_size &&
        "Apple Security Framework had better have exported the full pair.");

    size_t total_buffer_size = key_coordinate_size * 3 + 1;

    if (aws_byte_buf_init(&cc_key_pair->key_pair.key_buf, allocator, total_buffer_size)) {
        goto error;
    }

    aws_byte_buf_write_u8(&cc_key_pair->key_pair.key_buf, s_preamble);
    aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &pub_x);
    aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &pub_y);
    aws_byte_buf_append(&cc_key_pair->key_pair.key_buf, &priv_d);

    /* cc_key_pair->key_pair.key_buf is contiguous memory, so just load up the offsets. */
    cc_key_pair->key_pair.pub_x =
        aws_byte_buf_from_array(cc_key_pair->key_pair.key_buf.buffer + 1, key_coordinate_size);
    cc_key_pair->key_pair.pub_y =
        aws_byte_buf_from_array(cc_key_pair->key_pair.pub_x.buffer + key_coordinate_size, key_coordinate_size);
    cc_key_pair->key_pair.priv_d =
        aws_byte_buf_from_array(cc_key_pair->key_pair.pub_y.buffer + key_coordinate_size, key_coordinate_size);

    cc_key_pair->key_pair.curve_name = curve_name;
    cc_key_pair->key_pair.vtable = &s_key_pair_vtable;

    CFRelease(sec_key_export_data);
    CFRelease(key_size_cf_str);
    CFRelease(key_attributes);
    aws_der_decoder_destroy(decoder);

    return &cc_key_pair->key_pair;

error:
    if (decoder) {
        aws_der_decoder_destroy(decoder);
    }

    if (key_attributes) {
        CFRelease(key_attributes);
    }

    if (sec_key_export_data) {
        CFRelease(sec_key_export_data);
    }

    if (key_size_cf_str) {
        CFRelease(key_size_cf_str);
    }

    s_destroy_key(&cc_key_pair->key_pair);
    return NULL;
}
#endif /* AWS_OS_MACOS */

struct aws_ecc_key_pair *aws_ecc_key_pair_new_from_asn1(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *encoded_keys) {

    struct aws_ecc_key_pair *key_pair = NULL;
    struct aws_der_decoder *decoder = aws_der_decoder_new(allocator, *encoded_keys);
    CFMutableDictionaryRef key_attributes = NULL;
    CFDataRef key_data = NULL;

    if (!decoder) {
        return NULL;
    }

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

    if (!pub_x.ptr && !priv_d.ptr) {
        aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
        goto error;
    }

    struct commoncrypto_ecc_key_pair *cc_key_pair =
        s_alloc_pair_and_init_buffers(allocator, curve_name, pub_x, pub_y, priv_d);

    if (!cc_key_pair) {
        goto error;
    }

    key_pair = &cc_key_pair->key_pair;

    key_data = CFDataCreate(
        cc_key_pair->cf_allocator, cc_key_pair->key_pair.key_buf.buffer, cc_key_pair->key_pair.key_buf.len);

    if (!key_data) {
        goto error;
    }

    key_attributes = CFDictionaryCreateMutable(cc_key_pair->cf_allocator, 6, NULL, NULL);

    if (!key_attributes) {
        goto error;
    }

    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeECSECPrimeRandom);

    if (priv_d.ptr) {
        CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPrivate);

        CFDictionaryAddValue(key_attributes, kSecAttrCanSign, kCFBooleanTrue);
        CFDictionaryAddValue(key_attributes, kSecAttrCanDerive, kCFBooleanTrue);

        if (pub_x.ptr) {
            CFDictionaryAddValue(key_attributes, kSecAttrCanVerify, kCFBooleanTrue);
        }
    } else if (pub_x.ptr) {
        CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPublic);
        CFDictionaryAddValue(key_attributes, kSecAttrCanSign, kCFBooleanFalse);
        CFDictionaryAddValue(key_attributes, kSecAttrCanVerify, kCFBooleanTrue);
    }

    CFErrorRef error = NULL;

    cc_key_pair->priv_key_ref = SecKeyCreateWithData(key_data, key_attributes, &error);

    if (error) {
        aws_raise_error(AWS_ERROR_SYS_CALL_FAILURE);
    }

    if (pub_x.ptr) {
        cc_key_pair->pub_key_ref = SecKeyCopyPublicKey(cc_key_pair->priv_key_ref);
    }

    CFRelease(key_attributes);
    CFRelease(key_data);
    aws_der_decoder_destroy(decoder);

    return key_pair;

error:
    if (decoder) {
        aws_der_decoder_destroy(decoder);
    }

    if (key_attributes) {
        CFRelease(key_attributes);
    }

    if (key_data) {
        CFRelease(key_data);
    }

    if (key_pair) {
        s_destroy_key(key_pair);
    }
    return NULL;
}
