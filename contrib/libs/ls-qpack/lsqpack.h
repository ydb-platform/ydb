/*
 * lsqpack.h - QPACK library
 */

/*
MIT License

Copyright (c) 2018 - 2023 LiteSpeed Technologies Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef LSQPACK_H
#define LSQPACK_H 1

#include <limits.h>
#include <stdint.h>
#include <stdio.h>

#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define LSQPACK_MAJOR_VERSION 2
#define LSQPACK_MINOR_VERSION 6
#define LSQPACK_PATCH_VERSION 2

/** Let's start with four billion for now */
typedef unsigned lsqpack_abs_id_t;

#define LSQPACK_MAX_ABS_ID (~((lsqpack_abs_id_t) 0))

#define LSQPACK_DEF_DYN_TABLE_SIZE      0
#define LSQPACK_DEF_MAX_RISKED_STREAMS  0

struct lsqpack_enc;
struct lsqpack_dec;
struct lsxpack_header;

enum lsqpack_enc_opts
{
    /**
     * Client and server follow different heuristics.  The encoder is either
     * in one or the other mode.
     *
     * At the moment this option is a no-op.  This is a potential future
     * work item where some heuristics may be added to the library.
     */
    LSQPACK_ENC_OPT_SERVER  = 1 << 0,

    /**
     * The encoder was pre-initialized using @ref lsqpack_enc_preinit() and
     * so some initialization steps can be skipped.
     */
    LSQPACK_ENC_OPT_STAGE_2 = 1 << 1,

    /* The options below are advanced.  The author only uses them for debugging
     * or testing.
     */

    /**
     * Disable emitting dup instructions.
     *
     * Disabling dup instructions usually makes compression performance
     * significanly worse.  Do not use unless you know what you are doing.
     */
    LSQPACK_ENC_OPT_NO_DUP  = 1 << 2,

    /**
     * Index aggressively: ignore history
     *
     * Ignoring history usually makes compression performance significanly
     * worse.  Do not use unless you know what you are doing.
     */
    LSQPACK_ENC_OPT_IX_AGGR = 1 << 3,

    /**
     * Turn off memory guard: keep on allocating state tracking oustanding
     * headers even if they never get acknowledged.
     *
     * This is useful for some forms of testing.
     */
    LSQPACK_ENC_OPT_NO_MEM_GUARD = 1 << 4,
};


/**
 * Initialize the encoder so that it can be used without using the
 * dynamic table.  Once peer's settings are known, call
 * @ref lsqpack_enc_init().
 *
 * `logger_ctx' can be set to NULL if no special logging is set up.
 */
void
lsqpack_enc_preinit (struct lsqpack_enc *, void *logger_ctx);

/**
 * Number of bytes required to encode the longest possible Set Dynamic Table
 * Capacity instruction.  This is a theoretical limit based on the integral
 * type (unsigned int) used by this library to store the capacity value.  If
 * the encoder is initialized with a smaller maximum table capacity, it is
 * safe to use fewer bytes.
 *
 * SDTC instructtion can be produced by @ref lsqpack_enc_init() and
 * @ref lsqpack_enc_set_max_capacity().
 */
#if UINT_MAX == 65535
#define LSQPACK_LONGEST_SDTC 4
#elif UINT_MAX == 4294967295
#define LSQPACK_LONGEST_SDTC 6
#elif UINT_MAX == 18446744073709551615ULL
#define LSQPACK_LONGEST_SDTC 11
#else
#error unexpected sizeof(unsigned)
#endif

int
lsqpack_enc_init (struct lsqpack_enc *,
    /** `logger_ctx' can be set to NULL if no special logging is set up. */
    void *logger_ctx,
    /**
     * As specified by the decoder.  This value is used to calculate
     * MaxEntries.
     */
    unsigned max_table_size,
    /**
     * Actual dynamic table size to use.
     */
    unsigned dyn_table_size,
    unsigned max_risked_streams, enum lsqpack_enc_opts,
    /**
     * If `dyn_table_size' is not zero, Set Dynamic Table Capacity (SDTC)
     * instruction is generated and placed into `sdtc_buf'.  `sdtc_buf_sz'
     * parameter is used both for input and output.
     *
     * If `dyn_table_size' is zero, `sdtc_buf' and `sdtc_buf_sz' are optional
     * and can be set to NULL.
     */
    unsigned char *sdtc_buf, size_t *sdtc_buf_sz);

/**
 * Set table size to `capacity'.  If necessary, Set Dynamic Table Capacity
 * (SDTC) instruction is generated and placed into `tsu_buf'.  If `capacity'
 * is larger than the maximum table size specified during initialization, an
 * error is returned.
 */
int
lsqpack_enc_set_max_capacity (struct lsqpack_enc *enc, unsigned capacity,
                                    unsigned char *sdtc_buf, size_t *sdtc_buf_sz);

/** Start a new header block.  Return 0 on success or -1 on error. */
int
lsqpack_enc_start_header (struct lsqpack_enc *, uint64_t stream_id,
                            unsigned seqno);

/** Status returned by @ref lsqpack_enc_encode() */
enum lsqpack_enc_status
{
    /** Header field encoded successfully */
    LQES_OK,
    /** There was not enough room in the encoder stream buffer */
    LQES_NOBUF_ENC,
    /** There was not enough room in the header block buffer */
    LQES_NOBUF_HEAD,
};

enum lsqpack_enc_flags
{
    /**
     * Do not index this header field.  No output to the encoder stream
     * will be produced.
     */
    LQEF_NO_INDEX    = 1 << 0,
    /**
     * Never index this field.  This will set the 'N' bit on Literal Header
     * Field With Name Reference, Literal Header Field With Post-Base Name
     * Reference, and Literal Header Field Without Name Reference instructions
     * in the header block.  Implies LQEF_NO_INDEX.
     */
    LQEF_NEVER_INDEX = 1 << 1,
    /**
     * Do not update history.
     */
    LQEF_NO_HIST_UPD = 1 << 2,
    /**
     * Do not use the dynamic table.  This is stricter than LQEF_NO_INDEX:
     * this means that the dynamic table will be neither referenced nor
     * modified.
     */
    LQEF_NO_DYN      = 1 << 3,
};

/**
 * Encode header field into current header block.
 *
 * See @ref lsqpack_enc_status for explanation of the return values.
 *
 * enc_sz and header_sz parameters are used for both input and output.  If
 * the return value is LQES_OK, they contain number of bytes written to
 * enc_buf and header_buf, respectively.  enc_buf contains the bytes that
 * must be written to the encoder stream; header_buf contains bytes that
 * must be written to the header block.
 *
 * Note that even though this function may allocate memory, it falls back to
 * not using the dynamic table should memory allocation fail.  Thus, failures
 * to encode due to not enough memory do not exist.
 */
enum lsqpack_enc_status
lsqpack_enc_encode (struct lsqpack_enc *,
    unsigned char *enc_buf, size_t *enc_sz,
    unsigned char *header_buf, size_t *header_sz,
    const struct lsxpack_header *,
    enum lsqpack_enc_flags flags);

/**
 * Cancel current header block. Cancellation is only allowed if the dynamic 
 * table is not used. Returns 0 on success, -1 on failure.
 */
int 
lsqpack_enc_cancel_header (struct lsqpack_enc *);


/**
 * Properties of the current header block
 */
enum lsqpack_enc_header_flags
{
    /** Set if there are at-risk references in this header block */
    LSQECH_REF_AT_RISK      = 1 << 0,
    /** Set if the header block references newly inserted entries */
    LSQECH_REF_NEW_ENTRIES  = 1 << 1,
    /** Set if min-reffed value is cached */
    LSQECH_MINREF_CACHED    = 1 << 2,
};


/**
 * End current header block.  The Header Block Prefix is written to `buf'.
 *
 * `buf' must be at least two bytes.  11 bytes are necessary to encode
 * UINT64_MAX using 7- or 8-bit prefix.  Therefore, 22 bytes is the
 * theoretical maximum for this library.
 *
 * Use @ref lsqpack_enc_header_block_prefix_size() if you require better
 * precision.
 *
 * Returns:
 *  -   A positive value indicates success and is the number of bytes
 *      written to `buf'.
 *  -   Zero means that there is not enough room in `buf' to write out the
 *      full prefix.
 *  -   A negative value means an error.  This is returned if there is no
 *      started header to end.
 *
 * If header was ended successfully and @ref flags is not NULL, it is
 * assigned properties of the header block.
 */
ssize_t
lsqpack_enc_end_header (struct lsqpack_enc *, unsigned char *buf, size_t,
    enum lsqpack_enc_header_flags *flags /* Optional */);

/**
 * Process next chunk of bytes from the decoder stream.  Returns 0 on success,
 * -1 on failure.  The failure should be treated as fatal.
 */
int
lsqpack_enc_decoder_in (struct lsqpack_enc *, const unsigned char *, size_t);

/**
 * Return estimated compression ratio until this point.  Compression ratio
 * is defined as size of the output divided by the size of the input, where
 * output includes both header blocks and instructions sent on the encoder
 * stream.
 */
float
lsqpack_enc_ratio (const struct lsqpack_enc *);

/**
 * Return maximum size needed to encode Header Block Prefix
 */
size_t
lsqpack_enc_header_block_prefix_size (const struct lsqpack_enc *);

void
lsqpack_enc_cleanup (struct lsqpack_enc *);

/** Decoder header set interface */
struct lsqpack_dec_hset_if
{
    void    (*dhi_unblocked)(void *hblock_ctx);
    struct lsxpack_header *
            (*dhi_prepare_decode)(void *hblock_ctx,
                                  struct lsxpack_header *, size_t space);
    int     (*dhi_process_header)(void *hblock_ctx, struct lsxpack_header *);
};

enum lsqpack_dec_opts
{
    /**
     * In this mode, returned lsxpack_header will contain ": " between
     * name and value strings and have "\r\n" after the value.
     */
    LSQPACK_DEC_OPT_HTTP1X          = 1 << 0,
    /** Include name hash into lsxpack_header */
    LSQPACK_DEC_OPT_HASH_NAME       = 1 << 1,
    /** Include nameval hash into lsxpack_header */
    LSQPACK_DEC_OPT_HASH_NAMEVAL    = 1 << 2,
};

void
lsqpack_dec_init (struct lsqpack_dec *, void *logger_ctx,
    unsigned dyn_table_size, unsigned max_risked_streams,
    const struct lsqpack_dec_hset_if *, enum lsqpack_dec_opts);

/**
 * Values returned by @ref lsqpack_dec_header_in() and
 * @ref lsqpack_dec_header_read()
 */
enum lsqpack_read_header_status
{
    /**
     * The decoder decodes the header and placed it in `buf'.
     * It is caller's responsibility to manage provided `buf'.
     */
    LQRHS_DONE,
    /**
     * The decoder cannot decode the header block until more dynamic table
     * entries become available.  `buf' is advanced.  When the header block
     * becomes unblocked, the decoder will call hblock_unblocked() callback
     * specified in the constructor.  See @ref lsqpack_dec_init().
     *
     * Once a header block is unblocked, it cannot get blocked again.  In
     * other words, this status can only be returned once per header block.
     */
    LQRHS_BLOCKED,
    /**
     * The decoder needs more bytes from the header block to proceed.  When
     * they become available, call @ref lsqpack_dec_header_read().  `buf' is
     * advanced.
     */
    LQRHS_NEED,
    /**
     * An error has occurred.  This can be any error: decoding error, memory
     * allocation failure, or some internal error.
     */
    LQRHS_ERROR,
};

/**
 * Number of bytes needed to encode the longest Header Acknowledgement
 * instruction.
 */
#define LSQPACK_LONGEST_HEADER_ACK 10


/**
 * Call this function when the header blocks is first read.  The decoder
 * will try to decode the header block.  The decoder can process header
 * blocks in a streaming fashion, which means that there is no need to
 * buffer the header block.  As soon as header block bytes become available,
 * they can be fed to this function or @ref lsqpack_dec_header_read().
 *
 * See @ref lsqpack_read_header_status for explanation of the return codes.
 *
 * If the decoder returns LQRHS_NEED or LQRHS_BLOCKED, it keeps a reference to
 * the user-provided header block context `hblock_ctx'.  It uses this value for
 * two purposes:
 *  1. to use as argument to hblock_unblocked(); and
 *  2. to locate header block state when @ref lsqpack_dec_header_read() is
 *     called.
 *
 * If the decoder returns LQRHS_DONE or LQRHS_ERROR, it means that it no
 * longer has a reference to the header block.
 */
enum lsqpack_read_header_status
lsqpack_dec_header_in (struct lsqpack_dec *, void *hblock_ctx,
                       uint64_t stream_id, size_t header_block_size,
                       const unsigned char **buf, size_t bufsz,
                       unsigned char *dec_buf, size_t *dec_buf_sz);

/**
 * Call this function when more header block bytes are become available
 * after this function or @ref lsqpack_dec_header_in() returned LQRHS_NEED
 * or hblock_unblocked() callback has been called.  This function behaves
 * similarly to @ref lsqpack_dec_header_in(): see its comments for more
 * information.
 */
enum lsqpack_read_header_status
lsqpack_dec_header_read (struct lsqpack_dec *dec, void *hblock_ctx,
                         const unsigned char **buf, size_t bufsz,
                         unsigned char *dec_buf, size_t *dec_buf_sz);

/**
 * Feed encoder stream data to the decoder.  Zero is returned on success,
 * negative value on error.
 */
int
lsqpack_dec_enc_in (struct lsqpack_dec *, const unsigned char *, size_t);

/**
 * Returns true if Insert Count Increment (ICI) instruction is pending.
 */
int
lsqpack_dec_ici_pending (const struct lsqpack_dec *dec);

/**
 * Number of bytes required to encode the longest Insert Count Increment (ICI)
 * instruction.
 */
#define LSQPACK_LONGEST_ICI 6

ssize_t
lsqpack_dec_write_ici (struct lsqpack_dec *, unsigned char *, size_t);

/** Number of bytes required to encode the longest cancel instruction */
#define LSQPACK_LONGEST_CANCEL 6

/**
 * Cancel stream associated with the header block context `hblock_ctx' and
 * write cancellation instruction to `buf'.  `buf' must be at least
 * @ref LSQPACK_LONGEST_CANCEL bytes long.
 *
 * Number of bytes written to `buf' is returned.  If stream `stream_id'
 * could not be found, zero is returned.  If `buf' is too short, -1 is
 * returned.
 */
ssize_t
lsqpack_dec_cancel_stream (struct lsqpack_dec *, void *hblock_ctx,
                                unsigned char *buf, size_t buf_sz);

/**
 * Generate Cancel Stream instruction for stream `stream_id'.  Call when
 * abandoning stream (see [draft-ietf-quic-qpack-14] Section 2.2.2.2).
 *
 * Return values:
 *  -1  error (`buf' is too short)
 *   0  Emitting Cancel Stream instruction is unnecessary
 *  >0  Size of Cancel Stream instruction written to `buf'.
 */
ssize_t
lsqpack_dec_cancel_stream_id (struct lsqpack_dec *dec, uint64_t stream_id,
                                        unsigned char *buf, size_t buf_sz);

/**
 * Delete reference to the header block context `hblock_ctx'.  Use this
 * instead of @ref lsqpack_dec_cancel_stream() when producing a Cancel Stream
 * instruction is not necessary.
 */
int
lsqpack_dec_unref_stream (struct lsqpack_dec *, void *hblock_ctx);

/**
 * Return estimated compression ratio until this point.  Compression ratio
 * is defined as size of the input divided by the size of the output, where
 * input includes both header blocks and instructions received on the encoder
 * stream.
 */
float
lsqpack_dec_ratio (const struct lsqpack_dec *);

/**
 * Clean up the decoder.  If any there are any blocked header blocks,
 * references to them will be discarded.
 */
void
lsqpack_dec_cleanup (struct lsqpack_dec *);

/**
 * Print human-readable decoder table.
 */
void
lsqpack_dec_print_table (const struct lsqpack_dec *, FILE *out);


struct lsqpack_dec_err
{
    enum {
        LSQPACK_DEC_ERR_LOC_HEADER_BLOCK,
        LSQPACK_DEC_ERR_LOC_ENC_STREAM,
    }           type;
    int         line;       /* In the source file */
    uint64_t    off;        /* Offset in header block or on encoder stream */
    uint64_t    stream_id;  /* Only applicable to header block */
};


const struct lsqpack_dec_err *
lsqpack_dec_get_err_info (const struct lsqpack_dec *);

/**
 * Enum for name/value entries in the static table.  Use it to speed up
 * encoding by setting xhdr->qpack_index and LSXPACK_QPACK_IDX flag.  If
 * it's a full match, set LSXPACK_VAL_MATCHED flag as well.
 */
enum lsqpack_tnv
{
    LSQPACK_TNV_AUTHORITY = 0, /* ":authority" "" */
    LSQPACK_TNV_PATH = 1, /* ":path" "/" */
    LSQPACK_TNV_AGE_0 = 2, /* "age" "0" */
    LSQPACK_TNV_CONTENT_DISPOSITION = 3, /* "content-disposition" "" */
    LSQPACK_TNV_CONTENT_LENGTH_0 = 4, /* "content-length" "0" */
    LSQPACK_TNV_COOKIE = 5, /* "cookie" "" */
    LSQPACK_TNV_DATE = 6, /* "date" "" */
    LSQPACK_TNV_ETAG = 7, /* "etag" "" */
    LSQPACK_TNV_IF_MODIFIED_SINCE = 8, /* "if-modified-since" "" */
    LSQPACK_TNV_IF_NONE_MATCH = 9, /* "if-none-match" "" */
    LSQPACK_TNV_LAST_MODIFIED = 10, /* "last-modified" "" */
    LSQPACK_TNV_LINK = 11, /* "link" "" */
    LSQPACK_TNV_LOCATION = 12, /* "location" "" */
    LSQPACK_TNV_REFERER = 13, /* "referer" "" */
    LSQPACK_TNV_SET_COOKIE = 14, /* "set-cookie" "" */
    LSQPACK_TNV_METHOD_CONNECT = 15, /* ":method" "CONNECT" */
    LSQPACK_TNV_METHOD_DELETE = 16, /* ":method" "DELETE" */
    LSQPACK_TNV_METHOD_GET = 17, /* ":method" "GET" */
    LSQPACK_TNV_METHOD_HEAD = 18, /* ":method" "HEAD" */
    LSQPACK_TNV_METHOD_OPTIONS = 19, /* ":method" "OPTIONS" */
    LSQPACK_TNV_METHOD_POST = 20, /* ":method" "POST" */
    LSQPACK_TNV_METHOD_PUT = 21, /* ":method" "PUT" */
    LSQPACK_TNV_SCHEME_HTTP = 22, /* ":scheme" "http" */
    LSQPACK_TNV_SCHEME_HTTPS = 23, /* ":scheme" "https" */
    LSQPACK_TNV_STATUS_103 = 24, /* ":status" "103" */
    LSQPACK_TNV_STATUS_200 = 25, /* ":status" "200" */
    LSQPACK_TNV_STATUS_304 = 26, /* ":status" "304" */
    LSQPACK_TNV_STATUS_404 = 27, /* ":status" "404" */
    LSQPACK_TNV_STATUS_503 = 28, /* ":status" "503" */
    LSQPACK_TNV_ACCEPT = 29, /* "accept" star slash star */
    LSQPACK_TNV_ACCEPT_APPLICATION_DNS_MESSAGE = 30, /* "accept" "application/dns-message" */
    LSQPACK_TNV_ACCEPT_ENCODING_GZIP_DEFLATE_BR = 31, /* "accept-encoding" "gzip, deflate, br" */
    LSQPACK_TNV_ACCEPT_RANGES_BYTES = 32, /* "accept-ranges" "bytes" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_HEADERS_CACHE_CONTROL = 33, /* "access-control-allow-headers" "cache-control" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_HEADERS_CONTENT_TYPE = 34, /* "access-control-allow-headers" "content-type" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_ORIGIN = 35, /* "access-control-allow-origin" "*" */
    LSQPACK_TNV_CACHE_CONTROL_MAX_AGE_0 = 36, /* "cache-control" "max-age=0" */
    LSQPACK_TNV_CACHE_CONTROL_MAX_AGE_2592000 = 37, /* "cache-control" "max-age=2592000" */
    LSQPACK_TNV_CACHE_CONTROL_MAX_AGE_604800 = 38, /* "cache-control" "max-age=604800" */
    LSQPACK_TNV_CACHE_CONTROL_NO_CACHE = 39, /* "cache-control" "no-cache" */
    LSQPACK_TNV_CACHE_CONTROL_NO_STORE = 40, /* "cache-control" "no-store" */
    LSQPACK_TNV_CACHE_CONTROL_PUBLIC_MAX_AGE_31536000 = 41, /* "cache-control" "public, max-age=31536000" */
    LSQPACK_TNV_CONTENT_ENCODING_BR = 42, /* "content-encoding" "br" */
    LSQPACK_TNV_CONTENT_ENCODING_GZIP = 43, /* "content-encoding" "gzip" */
    LSQPACK_TNV_CONTENT_TYPE_APPLICATION_DNS_MESSAGE = 44, /* "content-type" "application/dns-message" */
    LSQPACK_TNV_CONTENT_TYPE_APPLICATION_JAVASCRIPT = 45, /* "content-type" "application/javascript" */
    LSQPACK_TNV_CONTENT_TYPE_APPLICATION_JSON = 46, /* "content-type" "application/json" */
    LSQPACK_TNV_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = 47, /* "content-type" "application/x-www-form-urlencoded" */
    LSQPACK_TNV_CONTENT_TYPE_IMAGE_GIF = 48, /* "content-type" "image/gif" */
    LSQPACK_TNV_CONTENT_TYPE_IMAGE_JPEG = 49, /* "content-type" "image/jpeg" */
    LSQPACK_TNV_CONTENT_TYPE_IMAGE_PNG = 50, /* "content-type" "image/png" */
    LSQPACK_TNV_CONTENT_TYPE_TEXT_CSS = 51, /* "content-type" "text/css" */
    LSQPACK_TNV_CONTENT_TYPE_TEXT_HTML_CHARSET_UTF_8 = 52, /* "content-type" "text/html; charset=utf-8" */
    LSQPACK_TNV_CONTENT_TYPE_TEXT_PLAIN = 53, /* "content-type" "text/plain" */
    LSQPACK_TNV_CONTENT_TYPE_TEXT_PLAIN_CHARSET_UTF_8 = 54, /* "content-type" "text/plain;charset=utf-8" */
    LSQPACK_TNV_RANGE_BYTES_0 = 55, /* "range" "bytes=0-" */
    LSQPACK_TNV_STRICT_TRANSPORT_SECURITY_MAX_AGE_31536000 = 56, /* "strict-transport-security" "max-age=31536000" */
    LSQPACK_TNV_STRICT_TRANSPORT_SECURITY_MAX_AGE_31536000_INCLUDESUBDOMAINS = 57, /* "strict-transport-security" "max-age=31536000; includesubdomains" */
    LSQPACK_TNV_STRICT_TRANSPORT_SECURITY_MAX_AGE_31536000_INCLUDESUBDOMAINS_PRELOAD = 58, /* "strict-transport-security" "max-age=31536000; includesubdomains; preload" */
    LSQPACK_TNV_VARY_ACCEPT_ENCODING = 59, /* "vary" "accept-encoding" */
    LSQPACK_TNV_VARY_ORIGIN = 60, /* "vary" "origin" */
    LSQPACK_TNV_X_CONTENT_TYPE_OPTIONS_NOSNIFF = 61, /* "x-content-type-options" "nosniff" */
    LSQPACK_TNV_X_XSS_PROTECTION_1_MODE_BLOCK = 62, /* "x-xss-protection" "1; mode=block" */
    LSQPACK_TNV_STATUS_100 = 63, /* ":status" "100" */
    LSQPACK_TNV_STATUS_204 = 64, /* ":status" "204" */
    LSQPACK_TNV_STATUS_206 = 65, /* ":status" "206" */
    LSQPACK_TNV_STATUS_302 = 66, /* ":status" "302" */
    LSQPACK_TNV_STATUS_400 = 67, /* ":status" "400" */
    LSQPACK_TNV_STATUS_403 = 68, /* ":status" "403" */
    LSQPACK_TNV_STATUS_421 = 69, /* ":status" "421" */
    LSQPACK_TNV_STATUS_425 = 70, /* ":status" "425" */
    LSQPACK_TNV_STATUS_500 = 71, /* ":status" "500" */
    LSQPACK_TNV_ACCEPT_LANGUAGE = 72, /* "accept-language" "" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_CREDENTIALS_FALSE = 73, /* "access-control-allow-credentials" "FALSE" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_CREDENTIALS_TRUE = 74, /* "access-control-allow-credentials" "TRUE" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_HEADERS = 75, /* "access-control-allow-headers" "*" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_METHODS_GET = 76, /* "access-control-allow-methods" "get" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_METHODS_GET_POST_OPTIONS = 77, /* "access-control-allow-methods" "get, post, options" */
    LSQPACK_TNV_ACCESS_CONTROL_ALLOW_METHODS_OPTIONS = 78, /* "access-control-allow-methods" "options" */
    LSQPACK_TNV_ACCESS_CONTROL_EXPOSE_HEADERS_CONTENT_LENGTH = 79, /* "access-control-expose-headers" "content-length" */
    LSQPACK_TNV_ACCESS_CONTROL_REQUEST_HEADERS_CONTENT_TYPE = 80, /* "access-control-request-headers" "content-type" */
    LSQPACK_TNV_ACCESS_CONTROL_REQUEST_METHOD_GET = 81, /* "access-control-request-method" "get" */
    LSQPACK_TNV_ACCESS_CONTROL_REQUEST_METHOD_POST = 82, /* "access-control-request-method" "post" */
    LSQPACK_TNV_ALT_SVC_CLEAR = 83, /* "alt-svc" "clear" */
    LSQPACK_TNV_AUTHORIZATION = 84, /* "authorization" "" */
    LSQPACK_TNV_CONTENT_SECURITY_POLICY_SCRIPT_SRC_NONE_OBJECT_SRC_NONE_BASE_URI_NONE = 85, /* "content-security-policy" "script-src 'none'; object-src 'none'; base-uri 'none'" */
    LSQPACK_TNV_EARLY_DATA_1 = 86, /* "early-data" "1" */
    LSQPACK_TNV_EXPECT_CT = 87, /* "expect-ct" "" */
    LSQPACK_TNV_FORWARDED = 88, /* "forwarded" "" */
    LSQPACK_TNV_IF_RANGE = 89, /* "if-range" "" */
    LSQPACK_TNV_ORIGIN = 90, /* "origin" "" */
    LSQPACK_TNV_PURPOSE_PREFETCH = 91, /* "purpose" "prefetch" */
    LSQPACK_TNV_SERVER = 92, /* "server" "" */
    LSQPACK_TNV_TIMING_ALLOW_ORIGIN = 93, /* "timing-allow-origin" "*" */
    LSQPACK_TNV_UPGRADE_INSECURE_REQUESTS_1 = 94, /* "upgrade-insecure-requests" "1" */
    LSQPACK_TNV_USER_AGENT = 95, /* "user-agent" "" */
    LSQPACK_TNV_X_FORWARDED_FOR = 96, /* "x-forwarded-for" "" */
    LSQPACK_TNV_X_FRAME_OPTIONS_DENY = 97, /* "x-frame-options" "deny" */
    LSQPACK_TNV_X_FRAME_OPTIONS_SAMEORIGIN = 98, /* "x-frame-options" "sameorigin" */
};

#ifdef __cplusplus
}
#endif


/*
 * Internals follow.  The internals are subject to change without notice.
 */

#include <sys/queue.h>

#ifdef __cplusplus
extern "C" {
#endif

/* It takes 11 bytes to encode UINT64_MAX as HPACK integer */
#define LSQPACK_UINT64_ENC_SZ 11u

struct lsqpack_enc_table_entry;

STAILQ_HEAD(lsqpack_enc_head, lsqpack_enc_table_entry);
struct lsqpack_double_enc_head;

struct lsqpack_header_info_arr;

struct lsqpack_dec_int_state
{
    int         resume;
    unsigned    M, nread;
    uint64_t    val;
};

struct lsqpack_enc
{
    /* The number of all the entries in the dynamic table that have been
     * created so far.  This is used to calculate the Absolute Index.
     */
    lsqpack_abs_id_t            qpe_ins_count;
    lsqpack_abs_id_t            qpe_max_acked_id;
    lsqpack_abs_id_t            qpe_last_ici;

    enum {
        LSQPACK_ENC_HEADER  = 1 << 0,
        LSQPACK_ENC_USE_DUP = 1 << 1,
        LSQPACK_ENC_NO_MEM_GUARD    = 1 << 2,
    }                           qpe_flags;

    unsigned                    qpe_cur_bytes_used;
    unsigned                    qpe_cur_max_capacity;
    unsigned                    qpe_real_max_capacity;
    unsigned                    qpe_max_entries;
    /* Sum of all dropped entries.  OK if it overflows. */
    unsigned                    qpe_dropped;

    /* The maximum risked streams is the SETTINGS_QPACK_BLOCKED_STREAMS
     * setting.  Note that streams must be differentiated from headers.
     */
    unsigned                    qpe_max_risked_streams;
    unsigned                    qpe_cur_streams_at_risk;

    /* Number of used entries in qpe_hinfo_arrs */
    unsigned                    qpe_hinfo_arrs_count;

    /* Dynamic table entries (struct enc_table_entry) live in two hash
     * tables: name/value hash table and name hash table.  These tables
     * are the same size.
     */
    unsigned                    qpe_nelem;
    unsigned                    qpe_nbits;
    struct lsqpack_enc_head     qpe_all_entries;
    struct lsqpack_double_enc_head
                               *qpe_buckets;

    STAILQ_HEAD(, lsqpack_header_info_arr)
                                qpe_hinfo_arrs;
    TAILQ_HEAD(, lsqpack_header_info)
                                qpe_all_hinfos;
    TAILQ_HEAD(, lsqpack_header_info)
                                qpe_risked_hinfos;

    /* Current header state */
    struct {
        struct lsqpack_header_info  *hinfo, *other_at_risk;

        /* Number of headers in this header list added to the history */
        unsigned            n_hdr_added_to_hist;
        lsqpack_abs_id_t    min_reffed;
        enum lsqpack_enc_header_flags
                            flags;
        lsqpack_abs_id_t    base_idx;
    }                           qpe_cur_header;

    struct {
        struct lsqpack_dec_int_state dec_int_state;
        int   (*handler)(struct lsqpack_enc *, uint64_t);
    }                           qpe_dec_stream_state;

    /* Used to calculate estimated compression ratio.  Note that the `out'
     * part contains bytes sent on the decoder stream, as it also counts
     * toward the overhead.
     */
    unsigned                    qpe_bytes_in;
    unsigned                    qpe_bytes_out;
    void                       *qpe_logger_ctx;

    /* Exponential moving averages (EMAs) of the number of elements in the
     * dynamic table and the number of header fields in a single header list.
     * These values are used to adjust history size.
     */
    float                       qpe_table_nelem_ema;
    float                       qpe_header_count_ema;

    struct lsqpack_hist_el     *qpe_hist_els;
    unsigned                    qpe_hist_idx;
    unsigned                    qpe_hist_nels;
    int                         qpe_hist_wrapped;
};

struct lsqpack_ringbuf
{
    unsigned        rb_nalloc, rb_head, rb_tail;
    void          **rb_els;
};

TAILQ_HEAD(lsqpack_header_sets, lsqpack_header_set_elem);

struct lsqpack_header_block;

struct lsqpack_decode_status
{
    uint8_t state;
    uint8_t eos;
};

struct lsqpack_huff_decode_state
{
    int                             resume;
    struct lsqpack_decode_status    status;
};

struct lsqpack_dec_inst;

struct lsqpack_dec
{
    enum lsqpack_dec_opts   qpd_opts;
    /** This is the hard limit set at initialization */
    unsigned                qpd_max_capacity;
    /** The current maximum capacity can be adjusted at run-time */
    unsigned                qpd_cur_max_capacity;
    unsigned                qpd_cur_capacity;
    unsigned                qpd_max_risked_streams;
    unsigned                qpd_max_entries;
    /* Used to calculate estimated compression ratio.  Note that the `in'
     * part contains bytes sent on the decoder stream, as it also counts
     * toward the overhead.
     */
    unsigned                qpd_bytes_in;
    unsigned                qpd_bytes_out;
    /** ID of the last dynamic table entry.  Has the range
     * [0, qpd_max_entries * 2 - 1 ]
     */
    lsqpack_abs_id_t        qpd_last_id;
    /** TODO: describe the mechanism */
    lsqpack_abs_id_t        qpd_largest_known_id;
    const struct lsqpack_dec_hset_if
                           *qpd_dh_if;

    void                   *qpd_logger_ctx;

    /** This is the dynamic table */
    struct lsqpack_ringbuf  qpd_dyn_table;

    TAILQ_HEAD(, header_block_read_ctx)
                            qpd_hbrcs;

    /** Blocked headers are kept in a small hash */
#define LSQPACK_DEC_BLOCKED_BITS 3
    TAILQ_HEAD(, header_block_read_ctx)
                            qpd_blocked_headers[1 << LSQPACK_DEC_BLOCKED_BITS];
    /** Number of blocked streams (in qpd_blocked_headers) */
    unsigned                qpd_n_blocked;

    /** Average number of header fields in header list */
    float                   qpd_hlist_size_ema;

    /** Reading the encoder stream */
    struct {
        int                                                 resume;
        union {
            /* State for reading in the Insert With Named Reference
             * instruction.
             */
            struct {
                struct lsqpack_dec_int_state        dec_int_state;
                struct lsqpack_huff_decode_state    dec_huff_state;
                unsigned                            name_idx;
                unsigned                            val_len;
                struct lsqpack_dec_table_entry     *reffed_entry;
                struct lsqpack_dec_table_entry     *entry;
                const char                         *name;
                unsigned                            alloced_val_len;
                unsigned                            val_off;
                unsigned                            nread;
                unsigned                            name_len;
                signed char                         is_huffman;
                signed char                         is_static;
            }                                               with_namref;

            /* State for reading in the Insert Without Named Reference
             * instruction.
             */
            struct {
                struct lsqpack_dec_int_state        dec_int_state;
                struct lsqpack_huff_decode_state    dec_huff_state;
                unsigned                            str_len;
                struct lsqpack_dec_table_entry     *entry;
                unsigned                            alloced_len;
                unsigned                            str_off;
                unsigned                            nread;
                signed char                         is_huffman;
            }                                               wo_namref;

            struct {
                struct lsqpack_dec_int_state        dec_int_state;
                unsigned                            index;
            }                                               duplicate;

            struct {
                struct lsqpack_dec_int_state        dec_int_state;
                uint64_t                            new_size;
            }                                               sdtc;
        }               ctx_u;
    }                       qpd_enc_state;
    struct lsqpack_dec_err  qpd_err;
};

#ifdef __cplusplus
}
#endif

#endif
