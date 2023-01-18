#ifndef AWS_COMMON_ENCODING_H
#define AWS_COMMON_ENCODING_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/byte_order.h>
#include <aws/common/common.h>

#include <memory.h>

AWS_EXTERN_C_BEGIN

/*
 * computes the length necessary to store the result of aws_hex_encode().
 * returns -1 on failure, and 0 on success. encoded_length will be set on
 * success.
 */
AWS_COMMON_API
int aws_hex_compute_encoded_len(size_t to_encode_len, size_t *encoded_length);

/*
 * Base 16 (hex) encodes the contents of to_encode and stores the result in
 * output.  0 terminates the result.  Assumes the buffer is empty and does not resize on
 * insufficient capacity.
 */
AWS_COMMON_API
int aws_hex_encode(const struct aws_byte_cursor *AWS_RESTRICT to_encode, struct aws_byte_buf *AWS_RESTRICT output);

/*
 * Base 16 (hex) encodes the contents of to_encode and appends the result in
 * output.  Does not 0-terminate.  Grows the destination buffer dynamically if necessary.
 */
AWS_COMMON_API
int aws_hex_encode_append_dynamic(
    const struct aws_byte_cursor *AWS_RESTRICT to_encode,
    struct aws_byte_buf *AWS_RESTRICT output);

/*
 * computes the length necessary to store the result of aws_hex_decode().
 * returns -1 on failure, and 0 on success. decoded_len will be set on success.
 */
AWS_COMMON_API
int aws_hex_compute_decoded_len(size_t to_decode_len, size_t *decoded_len);

/*
 * Base 16 (hex) decodes the contents of to_decode and stores the result in
 * output. If output is NULL, output_size will be set to what the output_size
 * should be.
 */
AWS_COMMON_API
int aws_hex_decode(const struct aws_byte_cursor *AWS_RESTRICT to_decode, struct aws_byte_buf *AWS_RESTRICT output);

/*
 * Computes the length necessary to store the output of aws_base64_encode call.
 * returns -1 on failure, and 0 on success. encoded_length will be set on
 * success.
 */
AWS_COMMON_API
int aws_base64_compute_encoded_len(size_t to_encode_len, size_t *encoded_len);

/*
 * Base 64 encodes the contents of to_encode and stores the result in output.
 */
AWS_COMMON_API
int aws_base64_encode(const struct aws_byte_cursor *AWS_RESTRICT to_encode, struct aws_byte_buf *AWS_RESTRICT output);

/*
 * Computes the length necessary to store the output of aws_base64_decode call.
 * returns -1 on failure, and 0 on success. decoded_len will be set on success.
 */
AWS_COMMON_API
int aws_base64_compute_decoded_len(const struct aws_byte_cursor *AWS_RESTRICT to_decode, size_t *decoded_len);

/*
 * Base 64 decodes the contents of to_decode and stores the result in output.
 */
AWS_COMMON_API
int aws_base64_decode(const struct aws_byte_cursor *AWS_RESTRICT to_decode, struct aws_byte_buf *AWS_RESTRICT output);

/* Add a 64 bit unsigned integer to the buffer, ensuring network - byte order
 * Assumes the buffer size is at least 8 bytes.
 */
AWS_STATIC_IMPL void aws_write_u64(uint64_t value, uint8_t *buffer);

/*
 * Extracts a 64 bit unsigned integer from buffer. Ensures conversion from
 * network byte order to host byte order. Assumes buffer size is at least 8
 * bytes.
 */
AWS_STATIC_IMPL uint64_t aws_read_u64(const uint8_t *buffer);

/* Add a 32 bit unsigned integer to the buffer, ensuring network - byte order
 * Assumes the buffer size is at least 4 bytes.
 */
AWS_STATIC_IMPL void aws_write_u32(uint32_t value, uint8_t *buffer);

/*
 * Extracts a 32 bit unsigned integer from buffer. Ensures conversion from
 * network byte order to host byte order. Assumes the buffer size is at least 4
 * bytes.
 */
AWS_STATIC_IMPL uint32_t aws_read_u32(const uint8_t *buffer);

/* Add a 24 bit unsigned integer to the buffer, ensuring network - byte order
 * return the new position in the buffer for the next operation.
 * Note, since this uses uint32_t for storage, the 3 least significant bytes
 * will be used. Assumes buffer is at least 3 bytes long.
 */
AWS_STATIC_IMPL void aws_write_u24(uint32_t value, uint8_t *buffer);
/*
 * Extracts a 24 bit unsigned integer from buffer. Ensures conversion from
 * network byte order to host byte order. Assumes buffer is at least 3 bytes
 * long.
 */
AWS_STATIC_IMPL uint32_t aws_read_u24(const uint8_t *buffer);

/* Add a 16 bit unsigned integer to the buffer, ensuring network-byte order
 * return the new position in the buffer for the next operation.
 * Assumes buffer is at least 2 bytes long.
 */
AWS_STATIC_IMPL void aws_write_u16(uint16_t value, uint8_t *buffer);
/*
 * Extracts a 16 bit unsigned integer from buffer. Ensures conversion from
 * network byte order to host byte order. Assumes buffer is at least 2 bytes
 * long.
 */
AWS_STATIC_IMPL uint16_t aws_read_u16(const uint8_t *buffer);

enum aws_text_encoding {
    AWS_TEXT_UNKNOWN,
    AWS_TEXT_UTF8,
    AWS_TEXT_UTF16,
    AWS_TEXT_UTF32,
    AWS_TEXT_ASCII,
};

/* Checks the BOM in the buffer to see if encoding can be determined. If there is no BOM or
 * it is unrecognizable, then AWS_TEXT_UNKNOWN will be returned.
 */
AWS_STATIC_IMPL enum aws_text_encoding aws_text_detect_encoding(const uint8_t *bytes, size_t size);

/*
 * Returns true if aws_text_detect_encoding() determines the text is UTF8 or ASCII.
 * Note that this immediately returns true if the UTF8 BOM is seen.
 * To fully validate every byte, use aws_text_is_valid_utf8().
 */
AWS_STATIC_IMPL bool aws_text_is_utf8(const uint8_t *bytes, size_t size);

/**
 * Scans every byte, and returns true if it is valid UTF8/ASCII as defined in RFC-3629.
 * The text does not need to begin with a UTF8 BOM.
 */
AWS_COMMON_API bool aws_text_is_valid_utf8(struct aws_byte_cursor bytes);

/**
 * A UTF8 validator scans every byte of text, incrementally,
 * and raises AWS_ERROR_INVALID_UTF8 if isn't valid UTF8/ASCII as defined in RFC-3629.
 * The text does not need to begin with a UTF8 BOM.
 * To validate text all at once, simply use aws_text_is_valid_utf8().
 */
struct aws_utf8_validator;

AWS_COMMON_API struct aws_utf8_validator *aws_utf8_validator_new(struct aws_allocator *allocator);
AWS_COMMON_API void aws_utf8_validator_destroy(struct aws_utf8_validator *validator);
AWS_COMMON_API void aws_utf8_validator_reset(struct aws_utf8_validator *validator);

/**
 * Update the validator with more bytes of text.
 * Raises AWS_ERROR_INVALID_UTF8 if invalid UTF8 is encountered.
 */
AWS_COMMON_API int aws_utf8_validator_update(struct aws_utf8_validator *validator, struct aws_byte_cursor bytes);

/**
 * Tell the validator that you've reached the end of your text.
 * Raises AWS_ERROR_INVALID_UTF8 if the text did not end with a complete UTF8 codepoint.
 * This also resets the validator.
 */
AWS_COMMON_API int aws_utf8_validator_finalize(struct aws_utf8_validator *validator);

#ifndef AWS_NO_STATIC_IMPL
#    include <aws/common/encoding.inl>
#endif /* AWS_NO_STATIC_IMPL */

AWS_EXTERN_C_END

#endif /* AWS_COMMON_ENCODING_H */
