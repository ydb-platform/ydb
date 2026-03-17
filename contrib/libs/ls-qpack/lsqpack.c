/*
 * lsqpack.c -- LiteSpeed QPACK Compression Library: encoder and decoder.
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

#ifndef LS_QPACK_USE_LARGE_TABLES
#define LS_QPACK_USE_LARGE_TABLES 1
#endif

#include <assert.h>
#include <errno.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <inttypes.h>


#if defined(__linux__)
#include <endian.h>

#elif defined(__APPLE__)

#include <machine/endian.h>
#define __BIG_ENDIAN BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BYTE_ORDER BYTE_ORDER

#elif (defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__))

#include <sys/endian.h>
#define __BIG_ENDIAN BIG_ENDIAN
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BYTE_ORDER BYTE_ORDER

#elif WIN32
#endif

#include "lsqpack.h"
#include "lsxpack_header.h"

#ifdef XXH_HEADER_NAME
#error #include XXH_HEADER_NAME
#else
#include <xxhash.h>
#endif

#include "huff-tables.h"

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#ifndef FALL_THROUGH
#  if 201710L < __STDC_VERSION__
#    define FALL_THROUGH [[fallthrough]]
#  elif ((__GNUC__ >= 7 && !defined __clang__)      \
        || (defined __apple_build_version__         \
            ? __apple_build_version__ >= 12000000   \
            : __clang_major__ >= 10))
#    define FALL_THROUGH __attribute__((fallthrough))
#  else
#    define FALL_THROUGH ((void)0)
#  endif
#endif

static unsigned char *
qenc_huffman_enc (const unsigned char *, const unsigned char *const, unsigned char *);

static unsigned
qenc_enc_str_size (const unsigned char *, unsigned);

struct static_table_entry
{
    const char       *name;
    const char       *val;
    unsigned          name_len;
    unsigned          val_len;
};

/* [draft-ietf-quic-qpack-03] Appendix A */
static const struct static_table_entry static_table[] =
{
    {":authority", "", 10, 0,},
    {":path", "/", 5, 1,},
    {"age", "0", 3, 1,},
    {"content-disposition", "", 19, 0,},
    {"content-length", "0", 14, 1,},
    {"cookie", "", 6, 0,},
    {"date", "", 4, 0,},
    {"etag", "", 4, 0,},
    {"if-modified-since", "", 17, 0,},
    {"if-none-match", "", 13, 0,},
    {"last-modified", "", 13, 0,},
    {"link", "", 4, 0,},
    {"location", "", 8, 0,},
    {"referer", "", 7, 0,},
    {"set-cookie", "", 10, 0,},
    {":method", "CONNECT", 7, 7,},
    {":method", "DELETE", 7, 6,},
    {":method", "GET", 7, 3,},
    {":method", "HEAD", 7, 4,},
    {":method", "OPTIONS", 7, 7,},
    {":method", "POST", 7, 4,},
    {":method", "PUT", 7, 3,},
    {":scheme", "http", 7, 4,},
    {":scheme", "https", 7, 5,},
    {":status", "103", 7, 3,},
    {":status", "200", 7, 3,},
    {":status", "304", 7, 3,},
    {":status", "404", 7, 3,},
    {":status", "503", 7, 3,},
    {"accept", "*/*", 6, 3,},
    {"accept", "application/dns-message", 6, 23,},
    {"accept-encoding", "gzip, deflate, br", 15, 17,},
    {"accept-ranges", "bytes", 13, 5,},
    {"access-control-allow-headers", "cache-control", 28, 13,},
    {"access-control-allow-headers", "content-type", 28, 12,},
    {"access-control-allow-origin", "*", 27, 1,},
    {"cache-control", "max-age=0", 13, 9,},
    {"cache-control", "max-age=2592000", 13, 15,},
    {"cache-control", "max-age=604800", 13, 14,},
    {"cache-control", "no-cache", 13, 8,},
    {"cache-control", "no-store", 13, 8,},
    {"cache-control", "public, max-age=31536000", 13, 24,},
    {"content-encoding", "br", 16, 2,},
    {"content-encoding", "gzip", 16, 4,},
    {"content-type", "application/dns-message", 12, 23,},
    {"content-type", "application/javascript", 12, 22,},
    {"content-type", "application/json", 12, 16,},
    {"content-type", "application/x-www-form-urlencoded", 12, 33,},
    {"content-type", "image/gif", 12, 9,},
    {"content-type", "image/jpeg", 12, 10,},
    {"content-type", "image/png", 12, 9,},
    {"content-type", "text/css", 12, 8,},
    {"content-type", "text/html; charset=utf-8", 12, 24,},
    {"content-type", "text/plain", 12, 10,},
    {"content-type", "text/plain;charset=utf-8", 12, 24,},
    {"range", "bytes=0-", 5, 8,},
    {"strict-transport-security", "max-age=31536000", 25, 16,},
    {"strict-transport-security", "max-age=31536000; includesubdomains",
                                                                25, 35,},
    {"strict-transport-security",
                "max-age=31536000; includesubdomains; preload", 25, 44,},
    {"vary", "accept-encoding", 4, 15,},
    {"vary", "origin", 4, 6,},
    {"x-content-type-options", "nosniff", 22, 7,},
    {"x-xss-protection", "1; mode=block", 16, 13,},
    {":status", "100", 7, 3,},
    {":status", "204", 7, 3,},
    {":status", "206", 7, 3,},
    {":status", "302", 7, 3,},
    {":status", "400", 7, 3,},
    {":status", "403", 7, 3,},
    {":status", "421", 7, 3,},
    {":status", "425", 7, 3,},
    {":status", "500", 7, 3,},
    {"accept-language", "", 15, 0,},
    {"access-control-allow-credentials", "FALSE", 32, 5,},
    {"access-control-allow-credentials", "TRUE", 32, 4,},
    {"access-control-allow-headers", "*", 28, 1,},
    {"access-control-allow-methods", "get", 28, 3,},
    {"access-control-allow-methods", "get, post, options", 28, 18,},
    {"access-control-allow-methods", "options", 28, 7,},
    {"access-control-expose-headers", "content-length", 29, 14,},
    {"access-control-request-headers", "content-type", 30, 12,},
    {"access-control-request-method", "get", 29, 3,},
    {"access-control-request-method", "post", 29, 4,},
    {"alt-svc", "clear", 7, 5,},
    {"authorization", "", 13, 0,},
    {"content-security-policy",
            "script-src 'none'; object-src 'none'; base-uri 'none'", 23, 53,},
    {"early-data", "1", 10, 1,},
    {"expect-ct", "", 9, 0,},
    {"forwarded", "", 9, 0,},
    {"if-range", "", 8, 0,},
    {"origin", "", 6, 0,},
    {"purpose", "prefetch", 7, 8,},
    {"server", "", 6, 0,},
    {"timing-allow-origin", "*", 19, 1,},
    {"upgrade-insecure-requests", "1", 25, 1,},
    {"user-agent", "", 10, 0,},
    {"x-forwarded-for", "", 15, 0,},
    {"x-frame-options", "deny", 15, 4,},
    {"x-frame-options", "sameorigin", 15, 10,},
};

#define QPACK_STATIC_TABLE_SIZE (sizeof(static_table) / sizeof(static_table[0]))

/* RFC 7541, Section 4.1:
 *
 * " The size of the dynamic table is the sum of the size of its entries.
 * "
 * " The size of an entry is the sum of its name's length in octets (as
 * " defined in Section 5.2), its value's length in octets, and 32.
 */
#define DYNAMIC_ENTRY_OVERHEAD 32u

/* Initial guess at number of header fields per list: */
#define GUESS_N_HEADER_FIELDS 12

#define MAX_QUIC_STREAM_ID ((1ull << 62) - 1)

#ifdef LSQPACK_ENC_LOGGER_HEADER
#error #include LSQPACK_ENC_LOGGER_HEADER
#else
#define E_LOG(prefix, ...) do {                                         \
    if (enc->qpe_logger_ctx) {                                          \
        fprintf(enc->qpe_logger_ctx, prefix);                           \
        fprintf(enc->qpe_logger_ctx, __VA_ARGS__);                      \
        fprintf(enc->qpe_logger_ctx, "\n");                             \
    }                                                                   \
} while (0)
#define E_DEBUG(...) E_LOG("qenc: debug: ", __VA_ARGS__)
#define E_INFO(...)  E_LOG("qenc: info: ", __VA_ARGS__)
#define E_WARN(...)  E_LOG("qenc: warn: ", __VA_ARGS__)
#define E_ERROR(...) E_LOG("qenc: error: ", __VA_ARGS__)
#endif

/* Entries in the encoder's dynamic table are hashed 1) by name and 2) by
 * name and value.  Instead of having two arrays of buckets, the encoder
 * keeps just one, but each bucket has two heads.
 */
struct lsqpack_double_enc_head
{
    struct lsqpack_enc_head by_name;
    struct lsqpack_enc_head by_nameval;
};

struct lsqpack_enc_table_entry
{
    /* An entry always lives on all three lists */
    STAILQ_ENTRY(lsqpack_enc_table_entry)
                                    ete_next_nameval,
                                    ete_next_name,
                                    ete_next_all;
    lsqpack_abs_id_t                ete_id;
    unsigned                        ete_when_added_used;
    unsigned                        ete_when_added_dropped;
    unsigned                        ete_n_reffd;
    unsigned                        ete_nameval_hash;
    unsigned                        ete_name_hash;
    unsigned                        ete_name_len;
    unsigned                        ete_val_len;
    char                            ete_buf[0];
};

#define ETE_NAME(ete) ((ete)->ete_buf)
#define ETE_VALUE(ete) (&(ete)->ete_buf[(ete)->ete_name_len])
#define ENTRY_COST(name_len, value_len) (DYNAMIC_ENTRY_OVERHEAD + \
                                                        name_len + value_len)
#define ETE_SIZE(ete) ENTRY_COST((ete)->ete_name_len, (ete)->ete_val_len)


#define N_BUCKETS(n_bits) (1U << (n_bits))
#define BUCKNO(n_bits, hash) ((hash) & (N_BUCKETS(n_bits) - 1))

struct lsqpack_header_info
{
    TAILQ_ENTRY(lsqpack_header_info)    qhi_next_all;
    TAILQ_ENTRY(lsqpack_header_info)    qhi_next_risked;
    struct lsqpack_header_info         *qhi_same_stream_id; /* Circular list */
    uint64_t                            qhi_stream_id;
    unsigned                            qhi_seqno;
    unsigned                            qhi_bytes_inserted;
    lsqpack_abs_id_t                    qhi_min_id;
    lsqpack_abs_id_t                    qhi_max_id;
};

/* Absolute index starts with 1.  0 indicates that the value is not set */
#define HINFO_IDS_SET(hinfo) ((hinfo)->qhi_max_id != 0)

/* Header info structures are kept in a list of arrays, which is faster than
 * searching through a linked list whose elements may be all over the place
 * in memory.  This is important because we need to look up header infos by
 * stream ID and also calculate the minimum absolute ID.  If not sequential
 * search, we would need to implement a hybrid of min-heap and a hash (or
 * search tree) and that seems to be an overkill for something that is not
 * likely to have more than a few hundred elements at the most.
 */
struct lsqpack_header_info_arr
{
    STAILQ_ENTRY(lsqpack_header_info_arr)   hia_next;
    uint64_t                                hia_slots;
    struct lsqpack_header_info              hia_hinfos[64];
};

static unsigned
find_free_slot (uint64_t slots)
{
#if __GNUC__
    return __builtin_ffsll(~slots) - 1;
#else
    unsigned n;

    slots =~ slots;
    n = 0;

    if (0 == (slots & ((1ULL << 32) - 1))) { n += 32; slots >>= 32; }
    if (0 == (slots & ((1ULL << 16) - 1))) { n += 16; slots >>= 16; }
    if (0 == (slots & ((1ULL <<  8) - 1))) { n +=  8; slots >>=  8; }
    if (0 == (slots & ((1ULL <<  4) - 1))) { n +=  4; slots >>=  4; }
    if (0 == (slots & ((1ULL <<  2) - 1))) { n +=  2; slots >>=  2; }
    if (0 == (slots & ((1ULL <<  1) - 1))) { n +=  1; slots >>=  1; }
    return n;
#endif
}

static struct lsqpack_header_info *
enc_alloc_hinfo (struct lsqpack_enc *enc)
{
    struct lsqpack_header_info_arr *hiarr;
    struct lsqpack_header_info *hinfo;
    unsigned slot;

    STAILQ_FOREACH(hiarr, &enc->qpe_hinfo_arrs, hia_next)
        if (hiarr->hia_slots != ~0ULL)
            break;

    if (!hiarr)
    {
        if (0 == (enc->qpe_flags & LSQPACK_ENC_NO_MEM_GUARD)
                && enc->qpe_hinfo_arrs_count * sizeof(*hiarr)
                                            >= enc->qpe_cur_max_capacity)
            return NULL;
        hiarr = malloc(sizeof(*hiarr));
        if (!hiarr)
            return NULL;
        hiarr->hia_slots = 0;
        STAILQ_INSERT_TAIL(&enc->qpe_hinfo_arrs, hiarr, hia_next);
        ++enc->qpe_hinfo_arrs_count;
    }

    slot = find_free_slot(hiarr->hia_slots);
    hiarr->hia_slots |= 1ULL << slot;
    hinfo = &hiarr->hia_hinfos[ slot ];
    memset(hinfo, 0, sizeof(*hinfo));
    hinfo->qhi_same_stream_id = hinfo;
    TAILQ_INSERT_TAIL(&enc->qpe_all_hinfos, hinfo, qhi_next_all);
    return hinfo;
}

static void
enc_free_hinfo (struct lsqpack_enc *enc, struct lsqpack_header_info *hinfo)
{
    struct lsqpack_header_info_arr *hiarr;
    unsigned slot;

    STAILQ_FOREACH(hiarr, &enc->qpe_hinfo_arrs, hia_next)
        if (hinfo >= hiarr->hia_hinfos && hinfo < &hiarr->hia_hinfos[64])
        {
            slot = (unsigned)(hinfo - hiarr->hia_hinfos);
            hiarr->hia_slots &= ~(1ULL << slot);
            TAILQ_REMOVE(&enc->qpe_all_hinfos, &hiarr->hia_hinfos[slot], qhi_next_all);
            return;
        }

    assert(0);
}

static int
enc_use_dynamic_table (const struct lsqpack_enc *enc)
{
    return enc->qpe_max_entries > 0
        && enc->qpe_cur_header.hinfo != NULL
        && enc->qpe_cur_header.hinfo->qhi_bytes_inserted
                                                < enc->qpe_cur_max_capacity / 2;
}


enum he { HE_NAME, HE_NAMEVAL, N_HES };


struct lsqpack_hist_el {
    unsigned    he_hashes[N_HES];
};


static void
qenc_hist_update_size (struct lsqpack_enc *enc, unsigned new_size)
{
    struct lsqpack_hist_el *els;
    unsigned first, count, i, j;

    if (new_size == enc->qpe_hist_nels)
        return;

    if (new_size == 0)
    {
        enc->qpe_hist_nels = 0;
        enc->qpe_hist_idx = 0;
        enc->qpe_hist_wrapped = 0;
        return;
    }

    els = malloc(sizeof(els[0]) * (new_size + 1));
    if (!els)
        return;

    E_DEBUG("history size change from %u to %u", enc->qpe_hist_nels, new_size);

    if (enc->qpe_hist_wrapped)
    {
        first = (enc->qpe_hist_idx + 1) % enc->qpe_hist_nels;
        count = enc->qpe_hist_nels;
    }
    else
    {
        first = 0;
        count = enc->qpe_hist_idx;
    }
    for (i = 0, j = 0; count > 0 && j < new_size; ++i, ++j, --count)
        els[j] = enc->qpe_hist_els[ (first + i) % enc->qpe_hist_nels ];
    enc->qpe_hist_nels = new_size;
    enc->qpe_hist_idx = j % new_size;
    enc->qpe_hist_wrapped = enc->qpe_hist_idx == 0;
    free(enc->qpe_hist_els);
    enc->qpe_hist_els = els;
}


static void
qenc_hist_add (struct lsqpack_enc *enc, unsigned name_hash,
                                                    unsigned nameval_hash)
{
    if (enc->qpe_hist_nels)
    {
        enc->qpe_hist_els[ enc->qpe_hist_idx ].he_hashes[HE_NAME] = name_hash;
        enc->qpe_hist_els[ enc->qpe_hist_idx ].he_hashes[HE_NAMEVAL]
                                                                = nameval_hash;
        enc->qpe_hist_idx = (enc->qpe_hist_idx + 1) % enc->qpe_hist_nels;
        enc->qpe_hist_wrapped |= enc->qpe_hist_idx == 0;
    }
}


static int
qenc_hist_seen (struct lsqpack_enc *enc, enum he he, unsigned hash)
{
    const struct lsqpack_hist_el *el;
    unsigned last_idx;

    if (enc->qpe_hist_els)
    {
        if (enc->qpe_hist_wrapped)
            last_idx = enc->qpe_hist_nels;
        else
            last_idx = enc->qpe_hist_idx;
        enc->qpe_hist_els[ last_idx ].he_hashes[he] = hash;
        for (el = enc->qpe_hist_els; el->he_hashes[he] != hash; ++el)
            ;
        return el < &enc->qpe_hist_els[ last_idx ];
    }
    else
        return 1;
}


unsigned char *
lsqpack_enc_int (unsigned char *, unsigned char *const, uint64_t, unsigned);


void
lsqpack_enc_preinit (struct lsqpack_enc *enc, void *logger_ctx)
{
    memset(enc, 0, sizeof(*enc));
    STAILQ_INIT(&enc->qpe_all_entries);
    STAILQ_INIT(&enc->qpe_hinfo_arrs);
    TAILQ_INIT(&enc->qpe_all_hinfos);
    TAILQ_INIT(&enc->qpe_risked_hinfos);
    enc->qpe_logger_ctx        = logger_ctx;
    E_DEBUG("preinitialized");
};


int
lsqpack_enc_init (struct lsqpack_enc *enc, void *logger_ctx,
                  unsigned max_table_size, unsigned dyn_table_size,
                  unsigned max_risked_streams, enum lsqpack_enc_opts enc_opts,
                  unsigned char *tsu_buf, size_t *tsu_buf_sz)
{
    struct lsqpack_double_enc_head *buckets;
    unsigned char *p;
    unsigned nbits = 2;
    unsigned i;

    if (dyn_table_size > max_table_size)
    {
        errno = EINVAL;
        return -1;
    }

    if (!(enc_opts & LSQPACK_ENC_OPT_STAGE_2))
        lsqpack_enc_preinit(enc, logger_ctx);

    if (dyn_table_size != LSQPACK_DEF_DYN_TABLE_SIZE)
    {
        if (!(tsu_buf && tsu_buf_sz && *tsu_buf_sz))
        {
            errno = EINVAL;
            return -1;
        }
        p = tsu_buf;
        *p = 0x20;
        p = lsqpack_enc_int(p, tsu_buf + *tsu_buf_sz, dyn_table_size, 5);
        if (p <= tsu_buf)
        {
            errno = ENOBUFS;
            return -1;
        }
        E_DEBUG("generated TSU=%u instruction %zd byte%.*s in size",
            dyn_table_size, p - tsu_buf, p - tsu_buf != 1, "s");
        *tsu_buf_sz = p - tsu_buf;
    }
    else if (tsu_buf_sz)
        *tsu_buf_sz = 0;

    if (!(enc_opts & LSQPACK_ENC_OPT_IX_AGGR))
    {
        enc->qpe_hist_nels = MAX(
            /* Initial guess at number of entries in dynamic table: */
            dyn_table_size / DYNAMIC_ENTRY_OVERHEAD / 3,
            GUESS_N_HEADER_FIELDS
        );
        enc->qpe_hist_els = malloc(sizeof(enc->qpe_hist_els[0]) * (enc->qpe_hist_nels + 1));
        if (!enc->qpe_hist_els)
            return -1;
    }
    else
    {
        enc->qpe_hist_nels = 0;
        enc->qpe_hist_els = NULL;
    }

    if (max_table_size / DYNAMIC_ENTRY_OVERHEAD)
    {
        nbits = 2;
        buckets = malloc(sizeof(buckets[0]) * N_BUCKETS(nbits));
        if (!buckets)
        {
            free(enc->qpe_hist_els);
            return -1;
        }

        for (i = 0; i < N_BUCKETS(nbits); ++i)
        {
            STAILQ_INIT(&buckets[i].by_name);
            STAILQ_INIT(&buckets[i].by_nameval);
        }
    }
    else
    {
        nbits = 0;
        buckets = NULL;
    }

    enc->qpe_max_entries  = max_table_size / DYNAMIC_ENTRY_OVERHEAD;
    enc->qpe_real_max_capacity = max_table_size;
    enc->qpe_cur_max_capacity = dyn_table_size;
    enc->qpe_max_risked_streams = max_risked_streams;
    enc->qpe_buckets      = buckets;
    enc->qpe_nbits        = nbits;
    enc->qpe_logger_ctx   = logger_ctx;
    if (!(enc_opts & LSQPACK_ENC_OPT_NO_DUP))
        enc->qpe_flags   |= LSQPACK_ENC_USE_DUP;
    if (enc_opts & LSQPACK_ENC_OPT_NO_MEM_GUARD)
        enc->qpe_flags   |= LSQPACK_ENC_NO_MEM_GUARD;
    E_DEBUG("initialized.  opts: 0x%X; max capacity: %u; max risked "
        "streams: %u.", enc_opts, enc->qpe_cur_max_capacity,
        enc->qpe_max_risked_streams);

    return 0;
}


void
lsqpack_enc_cleanup (struct lsqpack_enc *enc)
{
    struct lsqpack_enc_table_entry *entry, *next;
    struct lsqpack_header_info_arr *hiarr, *next_hiarr;

    for (entry = STAILQ_FIRST(&enc->qpe_all_entries); entry; entry = next)
    {
        next = STAILQ_NEXT(entry, ete_next_all);
        free(entry);
    }

    for (hiarr = STAILQ_FIRST(&enc->qpe_hinfo_arrs); hiarr; hiarr = next_hiarr)
    {
        next_hiarr = STAILQ_NEXT(hiarr, hia_next);
        free(hiarr);
    }

    free(enc->qpe_buckets);
    free(enc->qpe_hist_els);
    E_DEBUG("cleaned up");
}


#define LSQPACK_XXH_SEED 39378473
#define XXH_NAME_WIDTH 9
#define XXH_NAME_SHIFT 0
#define XXH_NAMEVAL_WIDTH 9
#define XXH_NAMEVAL_SHIFT 0

static const unsigned char name2id_plus_one[ 1 << XXH_NAME_WIDTH ] =
{
    [347]  =  1,   [397]  =  2,   [64]   =  3,   [144]  =  4,   [25]   =  5,
    [216]  =  6,   [361]  =  7,   [442]  =  8,   [190]  =  9,   [404]  =  10,
    [181]  =  11,  [210]  =  12,  [38]   =  13,  [51]   =  14,  [318]  =  15,
    [484]  =  16,  [81]   =  23,  [83]   =  25,  [248]  =  30,  [169]  =  32,
    [456]  =  33,  [479]  =  34,  [59]   =  36,  [498]  =  37,  [401]  =  43,
    [453]  =  45,  [266]  =  56,  [88]   =  57,  [317]  =  60,  [74]   =  62,
    [189]  =  63,  [211]  =  73,  [334]  =  74,  [365]  =  77,  [382]  =  80,
    [377]  =  81,  [257]  =  82,  [56]   =  84,  [321]  =  85,  [79]   =  86,
    [384]  =  87,  [357]  =  88,  [438]  =  89,  [84]   =  90,  [264]  =  91,
    [146]  =  92,  [225]  =  93,  [490]  =  94,  [305]  =  95,  [362]  =  96,
    [486]  =  97,  [497]  =  98,
};

static const unsigned char nameval2id_plus_one[ 1 << XXH_NAMEVAL_WIDTH ] =
{
    [150]  =  1,   [502]  =  2,   [353]  =  3,   [262]  =  4,   [443]  =  5,
    [164]  =  6,   [463]  =  7,   [84]   =  8,   [205]  =  9,   [228]  =  10,
    [451]  =  11,  [444]  =  12,  [176]  =  13,  [75]   =  14,  [399]  =  15,
    [56]   =  16,  [384]  =  17,  [21]   =  18,  [484]  =  19,  [382]  =  20,
    [439]  =  21,  [329]  =  22,  [360]  =  23,  [67]   =  24,  [105]  =  25,
    [342]  =  26,  [457]  =  27,  [161]  =  28,  [337]  =  29,  [135]  =  30,
    [314]  =  31,  [370]  =  32,  [404]  =  33,  [184]  =  34,  [156]  =  35,
    [139]  =  36,  [339]  =  37,  [508]  =  38,  [267]  =  39,  [375]  =  40,
    [122]  =  41,  [297]  =  42,  [144]  =  43,  [85]   =  44,  [466]  =  45,
    [38]   =  46,  [320]  =  47,  [273]  =  48,  [277]  =  49,  [136]  =  50,
    [454]  =  51,  [477]  =  52,  [91]   =  53,  [227]  =  54,  [301]  =  55,
    [272]  =  56,  [319]  =  57,  [142]  =  58,  [268]  =  59,  [65]   =  60,
    [410]  =  61,  [4]    =  62,  [373]  =  63,  [1]    =  64,  [210]  =  65,
    [224]  =  66,  [423]  =  67,  [222]  =  68,  [386]  =  69,  [12]   =  70,
    [7]    =  71,  [391]  =  72,  [73]   =  73,  [307]  =  74,  [27]   =  75,
    [256]  =  76,  [154]  =  77,  [204]  =  78,  [310]  =  79,  [198]  =  80,
    [162]  =  81,  [334]  =  82,  [438]  =  83,  [69]   =  84,  [188]  =  85,
    [244]  =  86,  [190]  =  87,  [465]  =  88,  [468]  =  89,  [417]  =  90,
    [110]  =  91,  [107]  =  92,  [368]  =  93,  [460]  =  94,  [54]   =  95,
    [492]  =  96,  [402]  =  97,  [196]  =  98,  [383]  =  99,
};


static const uint32_t name_hashes[] =
{
    0x653A915Bu, 0x3513518Du, 0xBEC8E440u, 0x16020A90u, 0x48F5CC19u,
    0x0B486ED8u, 0x1A7AA369u, 0x6DE855BAu, 0xF2BADABEu, 0xD8CA2594u,
    0x6B86C0B5u, 0xC62FECD2u, 0x8DA64A26u, 0x01F10233u, 0x8F7E493Eu,
    0xC7742BE4u, 0xC7742BE4u, 0xC7742BE4u, 0xC7742BE4u, 0xC7742BE4u,
    0xC7742BE4u, 0xC7742BE4u, 0xF49F1451u, 0xF49F1451u, 0x672BDA53u,
    0x672BDA53u, 0x672BDA53u, 0x672BDA53u, 0x672BDA53u, 0x1AB214F8u,
    0x1AB214F8u, 0xF93AD8A9u, 0x1DC691C8u, 0x7C21CFDFu, 0x7C21CFDFu,
    0x7D3B7A3Bu, 0xC25511F2u, 0xC25511F2u, 0xC25511F2u, 0xC25511F2u,
    0xC25511F2u, 0xC25511F2u, 0x48011191u, 0x48011191u, 0x085EF7C5u,
    0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u,
    0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u, 0x085EF7C5u,
    0xB396750Au, 0x85E74C58u, 0x85E74C58u, 0x85E74C58u, 0x1A04DF3Du,
    0x1A04DF3Du, 0x28686A4Au, 0x9F8BCEBDu, 0x672BDA53u, 0x672BDA53u,
    0x672BDA53u, 0x672BDA53u, 0x672BDA53u, 0x672BDA53u, 0x672BDA53u,
    0x672BDA53u, 0x672BDA53u, 0x98BD32D3u, 0x0A829D4Eu, 0x0A829D4Eu,
    0x7C21CFDFu, 0x363F796Du, 0x363F796Du, 0x363F796Du, 0xD8A0B17Eu,
    0xAAF9FD79u, 0x617E4501u, 0x617E4501u, 0x1E6DBE38u, 0x19D88141u,
    0x3392084Fu, 0x5579EF80u, 0x8F3D7765u, 0x7EDC71B6u, 0xFBA64C54u,
    0x3ECDA708u, 0xEBA96E92u, 0x82E1B4E1u, 0x5AD275EAu, 0xDD09E931u,
    0x34C0456Au, 0x5EF889E6u, 0x4B1BB7F1u, 0x4B1BB7F1u,
};


static const uint32_t nameval_hashes[] =
{
    0xF8614896u, 0xC8C267F6u, 0xF4617F61u, 0x8410A906u, 0xC8D109BBu,
    0x51D448A4u, 0x52C167CFu, 0xFB22AA54u, 0x4F5272CDu, 0x9D4170E4u,
    0x4E8C1DC3u, 0x684BDDBCu, 0xE113A2B0u, 0x5010D24Bu, 0xBCA5998Fu,
    0xC8490E38u, 0x19094780u, 0x25D95A15u, 0x342283E4u, 0x15893F7Eu,
    0x33968BB7u, 0x4C856F49u, 0x98573F68u, 0x16DDE443u, 0x813C3469u,
    0x352A6556u, 0xD7988BC9u, 0x65E6ECA1u, 0x7EEE2551u, 0x77EBAE87u,
    0xBDF5A53Au, 0x7F49F172u, 0xC06A7994u, 0xDB2FBCB8u, 0x343EA49Cu,
    0xD143768Bu, 0x3E2D8753u, 0xA2EA09FCu, 0x467B5D0Bu, 0xCEB7F977u,
    0x7119DC7Au, 0xDEFDA129u, 0x3F6EBC90u, 0x14E09A55u, 0x43C8B9D2u,
    0xA707C426u, 0xFE372940u, 0x77591711u, 0xA6410F15u, 0xEACDE488u,
    0x8B2C4DC6u, 0x8C2B11DDu, 0x9703CE5Bu, 0x0FAA28E3u, 0x13CCE32Du,
    0xDCD68310u, 0x416F0B3Fu, 0x3BB4D68Eu, 0xF81F070Cu, 0xBDD89641u,
    0x3915039Au, 0xF609E604u, 0x1C9DBB75u, 0x7ACD6A01u, 0xD4F462D2u,
    0x125E66E0u, 0x0AD44FA7u, 0x4C3C90DEu, 0x27AD6982u, 0x0673640Cu,
    0x65C03607u, 0xB05B7B87u, 0x97E01849u, 0xBA18BD33u, 0xDEF6041Bu,
    0xE227F500u, 0x8A871E9Au, 0xCB120ACCu, 0x4B1B6336u, 0xEBDA42C6u,
    0xFF166CA2u, 0x3A5E054Eu, 0x027207B6u, 0x04E3E645u, 0xAA95A0BCu,
    0x77BFA4F4u, 0x3C95E0BEu, 0xD506A9D1u, 0x443EDFD4u, 0xD4E28BA1u,
    0xA60BF66Eu, 0x46201E6Bu, 0xB2DE5570u, 0xF19F5DCCu, 0x73B6C636u,
    0xDC83E7ECu, 0xAA333392u, 0x4EDB46C4u, 0xF64F937Fu,
};


/* -1 means not found */
static int
find_in_static_full (uint32_t nameval_hash, const char *name,
                        unsigned name_len, const char *val, unsigned val_len)
{
    unsigned id;

    id = nameval2id_plus_one[ (nameval_hash >> XXH_NAMEVAL_SHIFT)
                                    & ((1 << XXH_NAMEVAL_WIDTH) - 1) ];

    if (id == 0)
        return -1;

    --id;
    if (static_table[id].name_len == name_len
            && static_table[id].val_len == val_len
            && memcmp(static_table[id].name, name, name_len) == 0
            && memcmp(static_table[id].val, val, val_len) == 0)
        return id;
    else
        return -1;
}


#ifdef NDEBUG
static
#endif
int
lsqpack_find_in_static_headers (uint32_t name_hash, const char *name,
                                                            unsigned name_len)
{
    unsigned id;

    id = name2id_plus_one[ (name_hash >> XXH_NAME_SHIFT)
                                    & ((1 << XXH_NAME_WIDTH) - 1) ];

    if (id == 0)
        return -1;

    --id;
    if (static_table[id].name_len == name_len
            && memcmp(static_table[id].name, name, name_len) == 0)
        return id;
    else
        return -1;
}


static unsigned
lsqpack_val2len (uint64_t value, unsigned prefix_bits)
{
    uint64_t mask = (1ULL << prefix_bits) - 1;
    return 1
         + (value >=                 mask )
         + (value >= ((1ULL <<  7) + mask))
         + (value >= ((1ULL << 14) + mask))
         + (value >= ((1ULL << 21) + mask))
         + (value >= ((1ULL << 28) + mask))
         + (value >= ((1ULL << 35) + mask))
         + (value >= ((1ULL << 42) + mask))
         + (value >= ((1ULL << 49) + mask))
         + (value >= ((1ULL << 56) + mask))
         + (value >= ((1ULL << 63) + mask))
         ;
}


unsigned char *
lsqpack_enc_int (unsigned char *dst, unsigned char *const end, uint64_t value,
                                                        unsigned prefix_bits)
{
    unsigned char *const dst_orig = dst;

    /* This function assumes that at least one byte is available */
    assert(dst < end);
    if (value < ((uint64_t)1 << prefix_bits) - 1)
        *dst++ |= value;
    else
    {
        *dst++ |= (1 << prefix_bits) - 1;
        value -= (1 << prefix_bits) - 1;
        while (value >= 128)
        {
            if (dst < end)
            {
                *dst++ = 0x80 | (unsigned char) value;
                value >>= 7;
            }
            else
                return dst_orig;
        }
        if (dst < end)
            *dst++ = (unsigned char) value;
        else
            return dst_orig;
    }
    return dst;
}


static void
lsqpack_enc_int_nocheck (unsigned char *dst, uint64_t value,
                                                        unsigned prefix_bits)
{
    if (value < ((uint64_t)1 << prefix_bits) - 1)
        *dst++ |= value;
    else
    {
        *dst++ |= (1 << prefix_bits) - 1;
        value -= (1 << prefix_bits) - 1;
        while (value >= 128)
        {
            *dst++ = 0x80 | (unsigned char) value;
            value >>= 7;
        }
        *dst++ = (unsigned char) value;
    }
}


int
lsqpack_enc_enc_str (unsigned prefix_bits, unsigned char *const dst,
        size_t dst_len, const unsigned char *str, unsigned str_len)
{
    unsigned char *p;
    unsigned enc_size_bytes, len_size;

    enc_size_bytes = qenc_enc_str_size(str, str_len);

    if (enc_size_bytes < str_len)
    {
        len_size = lsqpack_val2len(enc_size_bytes, prefix_bits);
        if (len_size + enc_size_bytes <= dst_len)
        {
            *dst &= ~((1 << (prefix_bits + 1)) - 1);
            *dst |= 1 << prefix_bits;
            lsqpack_enc_int_nocheck(dst, enc_size_bytes, prefix_bits);
            p = qenc_huffman_enc(str, str + str_len, dst + len_size);
            assert((unsigned) (p - dst) == len_size + enc_size_bytes);
            return (int)(p - dst);
        }
        else
            return -1;
    }
    else
    {
        len_size = lsqpack_val2len(str_len, prefix_bits);
        if (len_size + str_len <= dst_len)
        {
            *dst &= ~((1 << (prefix_bits + 1)) - 1);
            lsqpack_enc_int_nocheck(dst, str_len, prefix_bits);
            memcpy(dst + len_size, str, str_len);
            return len_size + str_len;
        }
        else
            return -1;
    }
}


static void
qenc_drop_oldest_entry (struct lsqpack_enc *enc)
{
    struct lsqpack_enc_table_entry *entry;
    unsigned buckno;

    entry = STAILQ_FIRST(&enc->qpe_all_entries);
    assert(entry);
    E_DEBUG("drop entry %u (`%.*s': `%.*s'), nelem: %u; capacity: %u",
        entry->ete_id, (int) entry->ete_name_len, ETE_NAME(entry),
        (int) entry->ete_val_len, ETE_VALUE(entry), enc->qpe_nelem - 1,
        enc->qpe_cur_bytes_used - ETE_SIZE(entry));
    STAILQ_REMOVE_HEAD(&enc->qpe_all_entries, ete_next_all);
    buckno = BUCKNO(enc->qpe_nbits, entry->ete_nameval_hash);
    assert(entry == STAILQ_FIRST(&enc->qpe_buckets[buckno].by_nameval));
    STAILQ_REMOVE_HEAD(&enc->qpe_buckets[buckno].by_nameval, ete_next_nameval);
    buckno = BUCKNO(enc->qpe_nbits, entry->ete_name_hash);
    assert(entry == STAILQ_FIRST(&enc->qpe_buckets[buckno].by_name));
    STAILQ_REMOVE_HEAD(&enc->qpe_buckets[buckno].by_name, ete_next_name);

    enc->qpe_dropped += ETE_SIZE(entry);
    enc->qpe_cur_bytes_used -= ETE_SIZE(entry);
    --enc->qpe_nelem;
    free(entry);
}


static float
qenc_effective_fill (const struct lsqpack_enc *enc)
{
    struct lsqpack_enc_table_entry *entry, *dup;
    unsigned dups_size = 0;

    assert(enc->qpe_cur_max_capacity);

    STAILQ_FOREACH(entry, &enc->qpe_all_entries, ete_next_all)
        for (dup = STAILQ_NEXT(entry, ete_next_all); dup;
                                        dup = STAILQ_NEXT(dup, ete_next_all))
            if (dup->ete_name_len == entry->ete_name_len &&
                dup->ete_val_len == entry->ete_val_len &&
                0 == memcmp(ETE_NAME(dup), ETE_NAME(entry),
                                    dup->ete_name_len + dup->ete_val_len))
            {
                dups_size += ETE_SIZE(dup);
                break;
            }

    return (float) (enc->qpe_cur_bytes_used - dups_size)
                                        / (float) enc->qpe_cur_max_capacity;
}


static void
update_ema (float *val, unsigned new)
{
    if (*val)
        *val = (float)((new - *val) * 0.4 + *val);
    else
        *val = (float)new;
}


static void
qenc_sample_table_size (struct lsqpack_enc *enc)
{
    update_ema(&enc->qpe_table_nelem_ema, enc->qpe_nelem);
    E_DEBUG("table size actual: %u; exponential moving average: %.3f",
                                    enc->qpe_nelem, enc->qpe_table_nelem_ema);
}


static void
qenc_sample_header_count (struct lsqpack_enc *enc)
{
    update_ema(&enc->qpe_header_count_ema,
                                    enc->qpe_cur_header.n_hdr_added_to_hist);
    E_DEBUG("header count actual: %u; exponential moving average: %.3f",
        enc->qpe_cur_header.n_hdr_added_to_hist, enc->qpe_header_count_ema);
}


static void
qenc_remove_overflow_entries (struct lsqpack_enc *enc)
{
    int dropped;

    dropped = 0;
    while (enc->qpe_cur_bytes_used > enc->qpe_cur_max_capacity)
    {
        qenc_drop_oldest_entry(enc);
        ++dropped;
    }

    if (enc->qpe_logger_ctx && enc->qpe_cur_max_capacity)
    {
        if (enc->qpe_flags & LSQPACK_ENC_USE_DUP)
            E_DEBUG("fill: %.2f; effective fill: %.2f",
                (float) enc->qpe_cur_bytes_used / (float) enc->qpe_cur_max_capacity,
                qenc_effective_fill(enc));
        else
            E_DEBUG("fill: %.2f",
                (float) enc->qpe_cur_bytes_used / (float) enc->qpe_cur_max_capacity);
    }

    /* It's important to sample only when entries have been dropped: that
     * indicates that the table is being cycled through.
     */
    if (dropped && enc->qpe_hist_els)
        qenc_sample_table_size(enc);
}


static int
qenc_grow_tables (struct lsqpack_enc *enc)
{
    struct lsqpack_double_enc_head *new_buckets, *new[2];
    struct lsqpack_enc_table_entry *entry;
    unsigned n, old_nbits;
    int idx;

    old_nbits = enc->qpe_nbits;
    new_buckets = malloc(sizeof(enc->qpe_buckets[0])
                                                * N_BUCKETS(old_nbits + 1));
    if (!new_buckets)
        return -1;

    for (n = 0; n < N_BUCKETS(old_nbits); ++n)
    {
        new[0] = &new_buckets[n];
        new[1] = &new_buckets[n + N_BUCKETS(old_nbits)];
        STAILQ_INIT(&new[0]->by_name);
        STAILQ_INIT(&new[1]->by_name);
        STAILQ_INIT(&new[0]->by_nameval);
        STAILQ_INIT(&new[1]->by_nameval);
        while (entry = STAILQ_FIRST(&enc->qpe_buckets[n].by_name), entry != NULL)
        {
            STAILQ_REMOVE_HEAD(&enc->qpe_buckets[n].by_name, ete_next_name);
            idx = (BUCKNO(old_nbits + 1, entry->ete_name_hash)
                                                        >> old_nbits) & 1;
            STAILQ_INSERT_TAIL(&new[idx]->by_name, entry, ete_next_name);
        }
        while (entry = STAILQ_FIRST(&enc->qpe_buckets[n].by_nameval), entry != NULL)
        {
            STAILQ_REMOVE_HEAD(&enc->qpe_buckets[n].by_nameval,
                                                        ete_next_nameval);
            idx = (BUCKNO(old_nbits + 1, entry->ete_nameval_hash)
                                                        >> old_nbits) & 1;
            STAILQ_INSERT_TAIL(&new[idx]->by_nameval, entry,
                                                        ete_next_nameval);
        }
    }

    free(enc->qpe_buckets);
    enc->qpe_nbits   = old_nbits + 1;
    enc->qpe_buckets = new_buckets;
    return 0;
}


static struct lsqpack_enc_table_entry *
lsqpack_enc_push_entry (struct lsqpack_enc *enc, uint32_t name_hash,
                uint32_t nameval_hash, const char *name, unsigned name_len,
                const char *value, unsigned value_len)
{
    struct lsqpack_enc_table_entry *entry;
    unsigned buckno;
    size_t size;

    if (enc->qpe_nelem >= N_BUCKETS(enc->qpe_nbits) / 2 &&
                                                0 != qenc_grow_tables(enc))
        return NULL;

    size = sizeof(*entry) + name_len + value_len;
    entry = malloc(size);
    if (!entry)
        return NULL;

    entry->ete_name_hash = name_hash;
    entry->ete_nameval_hash = nameval_hash;
    entry->ete_name_len = name_len;
    entry->ete_val_len = value_len;
    entry->ete_when_added_used = enc->qpe_cur_bytes_used;
    entry->ete_when_added_dropped = enc->qpe_dropped;
    entry->ete_id = 1 + enc->qpe_ins_count++;
    memcpy(ETE_NAME(entry), name, name_len);
    memcpy(ETE_VALUE(entry), value, value_len);

    STAILQ_INSERT_TAIL(&enc->qpe_all_entries, entry, ete_next_all);
    buckno = BUCKNO(enc->qpe_nbits, nameval_hash);
    STAILQ_INSERT_TAIL(&enc->qpe_buckets[buckno].by_nameval, entry,
                                                        ete_next_nameval);
    buckno = BUCKNO(enc->qpe_nbits, name_hash);
    STAILQ_INSERT_TAIL(&enc->qpe_buckets[buckno].by_name, entry,
                                                        ete_next_name);

    enc->qpe_cur_bytes_used += ENTRY_COST(name_len, value_len);
    ++enc->qpe_nelem;
    E_DEBUG("pushed entry %u (`%.*s': `%.*s'), nelem: %u; capacity: %u",
        entry->ete_id, (int) entry->ete_name_len, ETE_NAME(entry),
        (int) entry->ete_val_len, ETE_VALUE(entry), enc->qpe_nelem,
        enc->qpe_cur_bytes_used);
    return entry;
}


int
lsqpack_enc_start_header (struct lsqpack_enc *enc, uint64_t stream_id,
                            unsigned seqno)
{
    struct lsqpack_header_info *hinfo;

    if (enc->qpe_flags & LSQPACK_ENC_HEADER)
        return -1;

    E_DEBUG("Start header for stream %"PRIu64, stream_id);

    enc->qpe_cur_header.hinfo = enc_alloc_hinfo(enc);
    if (enc->qpe_cur_header.hinfo)
    {
        enc->qpe_cur_header.hinfo->qhi_stream_id = stream_id;
        enc->qpe_cur_header.hinfo->qhi_seqno     = seqno;
    }
    else
        E_INFO("could not allocate hinfo for stream %"PRIu64, stream_id);
    enc->qpe_cur_header.flags = 0;
    enc->qpe_cur_header.other_at_risk = NULL;
    enc->qpe_cur_header.n_hdr_added_to_hist = 0;
    enc->qpe_cur_header.base_idx = enc->qpe_ins_count;

    /* Check if there are other header blocks with the same stream ID that
     * are at risk.
     */
    if (seqno && enc->qpe_cur_header.hinfo)
        TAILQ_FOREACH(hinfo, &enc->qpe_risked_hinfos, qhi_next_risked)
            if (hinfo->qhi_stream_id == stream_id)
            {
                enc->qpe_cur_header.other_at_risk = hinfo;
                break;
            }

    enc->qpe_flags |= LSQPACK_ENC_HEADER;

    return 0;
}


/*
 * Each header block is prefixed with two integers.  The Required Insert
 * Count is encoded as an integer with an 8-bit prefix.  The Base is encoded
 * as sign- and-modulus integer, using a single sign bit and a value with a
 * 7-bit prefix.
 *
 *   0   1   2   3   4   5   6   7
 * +---+---+---+---+---+---+---+---+
 * |   Required Insert Count (8+)  |
 * +---+---------------------------+
 * | S |      Delta Base (7+)      |
 * +---+---------------------------+
 * |      Compressed Headers     ...
 * +-------------------------------+
 */
size_t
lsqpack_enc_header_block_prefix_size (const struct lsqpack_enc *enc)
{
    unsigned req_ins_count_len, delta_base_len;

    req_ins_count_len = lsqpack_val2len(2 * enc->qpe_max_entries, 8);
    delta_base_len = lsqpack_val2len(2 * enc->qpe_max_entries, 7);
    return req_ins_count_len + delta_base_len;
}


int
lsqpack_enc_cancel_header (struct lsqpack_enc *enc)
{
    /* No header has been started. */
    if (!(enc->qpe_flags & LSQPACK_ENC_HEADER))
        return -1;

    /* Cancellation is not (yet) allowed if the dynamic table is used since
     * ls-qpack's state is changed when the dynamic table is used.
     */
    if (enc->qpe_cur_header.hinfo && HINFO_IDS_SET(enc->qpe_cur_header.hinfo))
        return -1;

    if (enc->qpe_cur_header.hinfo) {
        enc_free_hinfo(enc, enc->qpe_cur_header.hinfo);
        enc->qpe_cur_header.hinfo = NULL;
    }

    enc->qpe_flags &= ~LSQPACK_ENC_HEADER;

    return 0;
}


static void
qenc_add_to_risked_list (struct lsqpack_enc *enc,
                                            struct lsqpack_header_info *hinfo)
{
    TAILQ_INSERT_TAIL(&enc->qpe_risked_hinfos, hinfo, qhi_next_risked);
    if (enc->qpe_cur_header.other_at_risk)
    {
        hinfo->qhi_same_stream_id
                    = enc->qpe_cur_header.other_at_risk->qhi_same_stream_id;
        enc->qpe_cur_header.other_at_risk->qhi_same_stream_id = hinfo;
    }
    else
    {
        ++enc->qpe_cur_streams_at_risk;
        E_DEBUG("streams at risk: %u", enc->qpe_cur_streams_at_risk);
        assert(enc->qpe_cur_streams_at_risk <= enc->qpe_max_risked_streams);
    }
}


static void
qenc_remove_from_risked_list (struct lsqpack_enc *enc,
                                            struct lsqpack_header_info *hinfo)
{
    struct lsqpack_header_info *prev;
    if (TAILQ_EMPTY(&enc->qpe_risked_hinfos))
    {
        assert(enc->qpe_cur_streams_at_risk == 0);
        return;
    }
    TAILQ_REMOVE(&enc->qpe_risked_hinfos, hinfo, qhi_next_risked);
    if (hinfo->qhi_same_stream_id == hinfo)
    {
        assert(enc->qpe_cur_streams_at_risk > 0);
        --enc->qpe_cur_streams_at_risk;
        E_DEBUG("streams at risk: %u", enc->qpe_cur_streams_at_risk);
    }
    else
    {
        for (prev = hinfo->qhi_same_stream_id;
            prev->qhi_same_stream_id != hinfo; prev = prev->qhi_same_stream_id)
            ;
        prev->qhi_same_stream_id = hinfo->qhi_same_stream_id;
        hinfo->qhi_same_stream_id = hinfo;
    }
}


static int
qenc_hinfo_at_risk (const struct lsqpack_enc *enc,
                                    const struct lsqpack_header_info *hinfo)
{
    return hinfo->qhi_max_id > enc->qpe_max_acked_id;
}


ssize_t
lsqpack_enc_end_header (struct lsqpack_enc *enc, unsigned char *buf, size_t sz,
        enum lsqpack_enc_header_flags *header_flags)
{
    struct lsqpack_header_info *hinfo;
    unsigned char *dst, *end;
    lsqpack_abs_id_t diff, encoded_largest_ref;
    unsigned sign, nelem;
    float count_diff;

    if (sz == 0)
        return -1;

    if (!(enc->qpe_flags & LSQPACK_ENC_HEADER))
        return -1;

    if (enc->qpe_hist_els)
    {
        qenc_sample_header_count(enc);
        if (enc->qpe_table_nelem_ema
            /* History size should not be smaller than the average number of
             * header fields in a header list.
             */
            && enc->qpe_table_nelem_ema > enc->qpe_header_count_ema)
        {
            count_diff = fabsf(enc->qpe_hist_nels - enc->qpe_table_nelem_ema);
            /* If difference is 2 or 10%: */
            if (count_diff >= 1.5
                            || count_diff / enc->qpe_table_nelem_ema >= 0.1)
            {
                nelem = (unsigned) round(enc->qpe_table_nelem_ema);
                qenc_hist_update_size(enc, nelem);
            }
        }
    }

    if (enc->qpe_cur_header.hinfo && HINFO_IDS_SET(enc->qpe_cur_header.hinfo))
    {
        hinfo = enc->qpe_cur_header.hinfo;  /* shorthand */
        end = buf + sz;

        *buf = 0;
        encoded_largest_ref = hinfo->qhi_max_id
                                            % (2 * enc->qpe_max_entries) + 1;
        E_DEBUG("LargestRef for stream %"PRIu64" is encoded as %u",
            hinfo->qhi_stream_id, encoded_largest_ref);
        dst = lsqpack_enc_int(buf, end, encoded_largest_ref, 8);
        if (dst <= buf)
            return 0;

        if (dst >= end)
            return 0;

        buf = dst;
        if (enc->qpe_cur_header.base_idx >= hinfo->qhi_max_id)
        {
            sign = 0;
            diff = enc->qpe_cur_header.base_idx - hinfo->qhi_max_id;
        }
        else
        {
            sign = 1;
            diff = hinfo->qhi_max_id - enc->qpe_cur_header.base_idx - 1;
        }
        *buf = (unsigned char) (sign << 7);
        dst = lsqpack_enc_int(buf, end, diff, 7);
        if (dst <= buf)
            return 0;

        if (qenc_hinfo_at_risk(enc, hinfo))
            qenc_add_to_risked_list(enc, hinfo);

        E_DEBUG("ended header for stream %"PRIu64"; max ref: %u encoded as %u; "
            "risked: %d", hinfo->qhi_stream_id, hinfo->qhi_max_id,
            encoded_largest_ref, qenc_hinfo_at_risk(enc, hinfo));

        enc->qpe_cur_header.hinfo = NULL;
        enc->qpe_flags &= ~LSQPACK_ENC_HEADER;
        if (header_flags)
        {
            *header_flags = enc->qpe_cur_header.flags;
            if (qenc_hinfo_at_risk(enc, hinfo))
                *header_flags |= LSQECH_REF_AT_RISK;
        }
        enc->qpe_bytes_out += (unsigned)(dst - end + sz);
        return dst - end + sz;
    }

    if (sz >= 2)
    {
        memset(buf, 0, 2);
        if (enc->qpe_cur_header.hinfo)
        {
            E_DEBUG("ended header for stream %"PRIu64"; dynamic table not "
                "referenced", enc->qpe_cur_header.hinfo->qhi_stream_id);
            enc_free_hinfo(enc, enc->qpe_cur_header.hinfo);
            enc->qpe_cur_header.hinfo = NULL;
        }
        else
            E_DEBUG("ended header; hinfo absent");
        enc->qpe_flags &= ~LSQPACK_ENC_HEADER;
        if (header_flags)
            *header_flags = enc->qpe_cur_header.flags;
        enc->qpe_bytes_out += 2;
        return 2;
    }
    else
        return 0;
}


struct encode_program
{
    enum enc_stream_action {        /* What to do on encoder stream */
        EEA_NONE,
        EEA_DUP,
        EEA_INS_NAMEREF_STATIC,
        EEA_INS_NAMEREF_DYNAMIC,
        EEA_INS_LIT,
        EEA_INS_LIT_NAME,
    }           ep_enc_action;
    enum hea_block_action {         /* What to output to header block */
        EHA_INDEXED_NEW,
        EHA_INDEXED_STAT,
        EHA_INDEXED_DYN,
        EHA_LIT_WITH_NAME_STAT,
        EHA_LIT_WITH_NAME_DYN,
        EHA_LIT_WITH_NAME_NEW,
        EHA_LIT,
    }           ep_hea_action;
    enum dyn_table_action {         /* Any changes to the dynamic table */
        ETA_NOOP,
        ETA_NEW,
        ETA_NEW_NAME,
    }           ep_tab_action;
    enum ref_flags {                /* Which entries to take references to */
        EPF_REF_FOUND   = 1 << 1,
        EPF_REF_NEW     = 1 << 2,
    }           ep_flags;
};


static const char *const eea2str[] =
{
    [EEA_NONE] = "EEA_NONE",
    [EEA_DUP] = "EEA_DUP",
    [EEA_INS_NAMEREF_STATIC] = "EEA_INS_NAMEREF_STATIC",
    [EEA_INS_NAMEREF_DYNAMIC] = "EEA_INS_NAMEREF_DYNAMIC",
    [EEA_INS_LIT] = "EEA_INS_LIT",
    [EEA_INS_LIT_NAME] = "EEA_INS_LIT_NAME",
};


static const char *const eha2str[] =
{
    [EHA_INDEXED_NEW] = "EHA_INDEXED_NEW",
    [EHA_INDEXED_STAT] = "EHA_INDEXED_STAT",
    [EHA_INDEXED_DYN] = "EHA_INDEXED_DYN",
    [EHA_LIT_WITH_NAME_STAT] = "EHA_LIT_WITH_NAME_STAT",
    [EHA_LIT_WITH_NAME_DYN] = "EHA_LIT_WITH_NAME_DYN",
    [EHA_LIT_WITH_NAME_NEW] = "EHA_LIT_WITH_NAME_NEW",
    [EHA_LIT] = "EHA_LIT",
};


static const char *const eta2str[] =
{
    [ETA_NOOP] = "ETA_NOOP",
    [ETA_NEW] = "ETA_NEW",
    [ETA_NEW_NAME] = "ETA_NEW_NAME",
};


static lsqpack_abs_id_t
qenc_min_reffed_id (struct lsqpack_enc *enc)
{
    const struct lsqpack_header_info *hinfo;
    lsqpack_abs_id_t min_id;

    if (enc->qpe_cur_header.flags & LSQECH_MINREF_CACHED)
        min_id = enc->qpe_cur_header.min_reffed;
    else
    {
        min_id = 0;
        TAILQ_FOREACH(hinfo, &enc->qpe_all_hinfos, qhi_next_all)
            if (min_id == 0 ||
                (hinfo->qhi_min_id != 0 && hinfo->qhi_min_id < min_id))
            {
                min_id = hinfo->qhi_min_id;
            }
        enc->qpe_cur_header.min_reffed = min_id;
        enc->qpe_cur_header.flags |= LSQECH_MINREF_CACHED;
    }

    if (enc->qpe_cur_header.hinfo
            && (min_id == 0 || (enc->qpe_cur_header.hinfo->qhi_min_id != 0
                            && enc->qpe_cur_header.hinfo->qhi_min_id < min_id)))
        min_id = enc->qpe_cur_header.hinfo->qhi_min_id;

    return min_id;
}


static int
qenc_safe_to_dup (const struct lsqpack_enc *enc,
                   const struct lsqpack_enc_table_entry *const pinned_entry)
{
    const struct lsqpack_enc_table_entry *entry;
    unsigned bytes_used;

    bytes_used = enc->qpe_cur_bytes_used + ETE_SIZE(pinned_entry);
    if (bytes_used <= enc->qpe_cur_max_capacity)
        return 1;

    for (entry = STAILQ_FIRST(&enc->qpe_all_entries); entry != pinned_entry;
                                        entry = STAILQ_NEXT(entry, ete_next_all))
    {
        bytes_used -= ETE_SIZE(entry);
        if (bytes_used <= enc->qpe_cur_max_capacity)
            return 1;
    }

    return 0;
}


static int
qenc_has_or_can_evict_at_least (struct lsqpack_enc *enc, size_t new_entry_size)
{
    const struct lsqpack_enc_table_entry *entry;
    lsqpack_abs_id_t min_id;
    size_t avail;

    avail = enc->qpe_cur_max_capacity - enc->qpe_cur_bytes_used;
    if (avail >= new_entry_size)
        return 1;

    min_id = qenc_min_reffed_id(enc);

    STAILQ_FOREACH(entry, &enc->qpe_all_entries, ete_next_all)
        if ((min_id == 0 || entry->ete_id < min_id)
                && entry->ete_id <= enc->qpe_max_acked_id)
        {
            avail += ETE_SIZE(entry);
            if (avail >= new_entry_size)
                return 1;
        }
        else
            break;

    return avail >= new_entry_size;
}


static int
qenc_duplicable_entry (struct lsqpack_enc *enc,
                       const struct lsqpack_enc_table_entry *const entry)
{
    float fill, fraction;
    unsigned off;

    if (!(enc->qpe_flags & LSQPACK_ENC_USE_DUP))
        return 0;

    fill = (float) (enc->qpe_cur_bytes_used + ETE_SIZE(entry))
                                        / (float) enc->qpe_cur_max_capacity;
    if (fill < 0.8)
        return 0;

    off = entry->ete_when_added_used
        - (enc->qpe_dropped - entry->ete_when_added_dropped);
    fraction = (float) off / (float) enc->qpe_cur_max_capacity;
    return fraction < 0.2f
        && qenc_has_or_can_evict_at_least(enc, ETE_SIZE(entry));
}


static void
qenc_maybe_update_hinfo_min_max (struct lsqpack_header_info *hinfo,
                                                    lsqpack_abs_id_t dyn_id)
{
    if (HINFO_IDS_SET(hinfo))
    {
        if (dyn_id > hinfo->qhi_max_id)
            hinfo->qhi_max_id = dyn_id;
        else if (dyn_id < hinfo->qhi_min_id)
            hinfo->qhi_min_id = dyn_id;
    }
    else
    {
        hinfo->qhi_max_id = dyn_id;
        hinfo->qhi_min_id = dyn_id;
    }
}


static int
qenc_entry_is_draining (const struct lsqpack_enc *enc,
                            const struct lsqpack_enc_table_entry *entry)
{
    unsigned dist;

    dist = entry->ete_when_added_used
        - (enc->qpe_dropped - entry->ete_when_added_dropped);
    dist += enc->qpe_cur_max_capacity - enc->qpe_cur_bytes_used;
    return dist < enc->qpe_cur_max_capacity / 4;
}


static int
qenc_can_risk (const struct lsqpack_enc *enc)
{
    return enc->qpe_cur_header.other_at_risk
        || enc->qpe_cur_streams_at_risk < enc->qpe_max_risked_streams
        || (enc->qpe_cur_header.hinfo
                        && qenc_hinfo_at_risk(enc, enc->qpe_cur_header.hinfo))
        ;
}


/* Returns number of bytes written to enc_buf if an entry was duplicated, 0 if
 * it wasn't.
 */
static unsigned
qenc_dup_draining (struct lsqpack_enc *enc, unsigned char *enc_buf,
                                                            size_t enc_buf_sz)
{
    struct lsqpack_enc_table_entry *entry, *candidate, *next;
    unsigned char *dst;

    if (enc_buf_sz == 0
            || !(enc->qpe_flags & LSQPACK_ENC_USE_DUP)
            || enc->qpe_ins_count == LSQPACK_MAX_ABS_ID)
        return 0;
    if ((enc->qpe_table_nelem_ema || qenc_can_risk(enc))
                    && enc->qpe_table_nelem_ema < enc->qpe_header_count_ema)
        return 0;

    candidate = NULL;
    STAILQ_FOREACH(entry, &enc->qpe_all_entries, ete_next_all)
    {
        if (!qenc_entry_is_draining(enc, entry))
            break;
        /*
        if (ETE_SIZE(entry) > enc->qpe_cur_max_capacity / 4)
            continue;
        if (ETE_SIZE(entry) < DYNAMIC_ENTRY_OVERHEAD + 20)
            continue;
            */
        if (candidate && ETE_SIZE(entry) < ETE_SIZE(candidate))
            continue;
        for (next = STAILQ_NEXT(entry, ete_next_nameval); next;
                                    next = STAILQ_NEXT(next, ete_next_nameval))
            if (next->ete_nameval_hash == entry->ete_nameval_hash
                    && next->ete_name_len == entry->ete_name_len
                    && next->ete_val_len == entry->ete_val_len
                    && 0 == memcmp(ETE_NAME(next), ETE_NAME(entry),
                                                        next->ete_name_len)
                    && 0 == memcmp(ETE_VALUE(next), ETE_VALUE(entry),
                                                        next->ete_val_len))
                break;
        if (!next
                && qenc_hist_seen(enc, HE_NAMEVAL, entry->ete_nameval_hash)
                        && qenc_has_or_can_evict_at_least(enc, ETE_SIZE(entry)))
            candidate = entry;
    }

    if (!candidate)
        return 0;

    E_DEBUG("dup draining");

    *enc_buf = 0;
    dst = lsqpack_enc_int(enc_buf, enc_buf + enc_buf_sz,
                                    enc->qpe_ins_count - candidate->ete_id, 5);
    if (dst <= enc_buf)
        return 0;

    entry = lsqpack_enc_push_entry(enc, candidate->ete_name_hash,
                candidate->ete_nameval_hash, ETE_NAME(candidate),
                candidate->ete_name_len, ETE_VALUE(candidate),
                candidate->ete_val_len);
    if (!entry)
        return 0;

    return (unsigned) (dst - enc_buf);
}


/* Clang does not produce incorrect "may be used uninitialized" warnings
 * in the function below, but gcc 5.4.0 does.
 */
#ifdef __clang__
#define USE_USELESS_INITIALIZATION 0
#else
#define USE_USELESS_INITIALIZATION 1
#endif


enum lsqpack_enc_status
lsqpack_enc_encode (struct lsqpack_enc *enc,
        unsigned char *enc_buf, size_t *enc_sz_p,
        unsigned char *hea_buf, size_t *hea_sz_p,
        const struct lsxpack_header *xhdr,
        enum lsqpack_enc_flags flags)
{
    unsigned char *const enc_buf_end = enc_buf + *enc_sz_p;
    unsigned char *const hea_buf_end = hea_buf + *hea_sz_p;
    struct lsqpack_enc_table_entry *entry, *new_entry;
    struct lsqpack_enc_table_entry *candidates[2];
    struct encode_program prog;
    int index, risk, use_dyn_table, static_id, enough_room, seen_nameval;
    int update_hist;
    unsigned name_hash, nameval_hash, buckno;

    size_t enc_sz, hea_sz, sz;
    unsigned char *dst;
    lsqpack_abs_id_t id;
    unsigned n_cand;
    int r;

    const char *const name = lsxpack_header_get_name(xhdr);
    const char *const value = lsxpack_header_get_value(xhdr);
    const unsigned name_len = xhdr->name_len;
    const unsigned value_len = xhdr->val_len;

    E_DEBUG("encode `%.*s': `%.*s'", (int) name_len, name,
                                                (int) value_len, value);

    /* Encoding always outputs at least a byte to the header block.  If
     * no bytes are available, encoding cannot proceed.
     */
    if (hea_buf == hea_buf_end)
        return LQES_NOBUF_HEAD;

    seen_nameval = -1;

    if (xhdr->flags & LSXPACK_NEVER_INDEX)
        flags |= LQEF_NEVER_INDEX;

    /* Look for a full match in the static table */
    if ((xhdr->flags & (LSXPACK_QPACK_IDX|LSXPACK_VAL_MATCHED))
                                != (LSXPACK_QPACK_IDX|LSXPACK_VAL_MATCHED))
    {
        /* Hash calculation is delayed until we really have to do it */
        if (xhdr->flags & LSXPACK_NAME_HASH)
            name_hash = xhdr->name_hash;
        else if (xhdr->flags & LSXPACK_QPACK_IDX)
            name_hash = name_hashes[ xhdr->qpack_index ];
        else
            name_hash = XXH32(name, name_len, LSQPACK_XXH_SEED);
        if (xhdr->flags & LSXPACK_NAMEVAL_HASH)
            nameval_hash = xhdr->nameval_hash;
        else
            nameval_hash = XXH32(value, value_len, name_hash);
        E_DEBUG("name hash: 0x%X; nameval hash: 0x%X", name_hash, nameval_hash);
        static_id = find_in_static_full(nameval_hash, name, name_len, value,
                                                                value_len);
    }
    else
    {
        static_id = xhdr->qpack_index;
        goto static_nameval_match;
    }
    if (static_id >= 0)
    {
  static_nameval_match:
        id = static_id;
        prog = (struct encode_program) {
                    .ep_enc_action = EEA_NONE,
                    .ep_hea_action = EHA_INDEXED_STAT,
                    .ep_tab_action = ETA_NOOP,
                    .ep_flags      = 0,
        };
        update_hist = 0;
#if USE_USELESS_INITIALIZATION
        nameval_hash = 0;
        name_hash = 0;
        use_dyn_table = 0;
        risk = 0;
        entry = NULL;
        index = 0;
#endif
        goto execute_program;
    }
#if USE_USELESS_INITIALIZATION
    else
        id = 0;
#endif

    use_dyn_table = !(flags & LQEF_NO_DYN)
        && enc_use_dynamic_table(enc)
        ;

    index = !(flags & (LQEF_NO_INDEX|LQEF_NEVER_INDEX|LQEF_NO_DYN))
        && use_dyn_table
        && enc->qpe_ins_count < LSQPACK_MAX_ABS_ID
        ;

    risk = qenc_can_risk(enc);

    /* Add header to history if it exists.  Defer updating history until we
     * know the function will return success.
     */
    update_hist = enc->qpe_hist_els != NULL && !(flags & LQEF_NO_HIST_UPD);

  restart:
    /* Look for a full match in the dynamic table */
    if (use_dyn_table)
    {
        buckno = BUCKNO(enc->qpe_nbits, nameval_hash);
        n_cand = 0;
        STAILQ_FOREACH(entry, &enc->qpe_buckets[buckno].by_nameval,
                                                            ete_next_nameval)
            if (nameval_hash == entry->ete_nameval_hash &&
                name_len == entry->ete_name_len &&
                value_len == entry->ete_val_len &&
                0 == memcmp(name, ETE_NAME(entry), name_len) &&
                0 == memcmp(value, ETE_VALUE(entry), value_len))
            {
                candidates[ n_cand++ ] = entry;
                if (n_cand >= sizeof(candidates) / sizeof(candidates[0]))
                    break;
            }

        switch (n_cand)
        {
        case 1:
            entry = candidates[0];
            id = entry->ete_id;
            if ((risk || entry->ete_id <= enc->qpe_max_acked_id) && index && qenc_duplicable_entry(enc, entry))
            {
                if (risk)
                    prog = (struct encode_program) {
                            .ep_enc_action = EEA_DUP,
                            .ep_hea_action = EHA_INDEXED_NEW,
                            .ep_tab_action = ETA_NEW,
                            .ep_flags      = EPF_REF_FOUND | EPF_REF_NEW,
                    };
                else if (!qenc_entry_is_draining(enc, entry))
                {
                    if (qenc_safe_to_dup(enc, entry))
                        prog = (struct encode_program) {
                            .ep_enc_action = EEA_DUP,
                            .ep_hea_action = EHA_INDEXED_DYN,
                            .ep_tab_action = ETA_NEW,
                            .ep_flags      = EPF_REF_FOUND,
                        };
                    else
                        prog = (struct encode_program) {
                            .ep_enc_action = EEA_NONE,
                            .ep_hea_action = EHA_INDEXED_DYN,
                            .ep_tab_action = ETA_NOOP,
                            .ep_flags      = EPF_REF_FOUND,
                        };
                }
                else
                    break;
            }
            else if ((risk || entry->ete_id <= enc->qpe_max_acked_id)
                                    && !qenc_entry_is_draining(enc, entry))
                prog = (struct encode_program) {
                            .ep_enc_action = EEA_NONE,
                            .ep_hea_action = EHA_INDEXED_DYN,
                            .ep_tab_action = ETA_NOOP,
                            .ep_flags      = EPF_REF_FOUND,
                };
            else
                break;
            goto execute_program;
        case 2:
            /* The order holds due to the way hash table is structured: */
            assert(candidates[1]->ete_id > candidates[0]->ete_id);
            if (risk)
                /* TODO: make this smarter?  Perhaps it may be preferable
                 * to use an acknowledged entry if it is not in the "about
                 * to be evicted" range?
                 */
                entry = candidates[1];
            else if (candidates[1]->ete_id <= enc->qpe_max_acked_id)
                entry = candidates[1];
            else if (candidates[0]->ete_id <= enc->qpe_max_acked_id
                            && !qenc_entry_is_draining(enc, candidates[0]))
                entry = candidates[0];
            else
                break;
            id = entry->ete_id;
            prog = (struct encode_program) {
                        .ep_enc_action = EEA_NONE,
                        .ep_hea_action = EHA_INDEXED_DYN,
                        .ep_tab_action = ETA_NOOP,
                        .ep_flags      = EPF_REF_FOUND,
            };
            goto execute_program;
        }
    }
#if USE_USELESS_INITIALIZATION
    else
    {
        entry = NULL;
        n_cand = 0;
    }
#endif

    /* Look for name-only match in the static table */
    if (xhdr->flags & LSXPACK_QPACK_IDX)
    {
        static_id = xhdr->qpack_index;
        goto static_name_match;
    }
    else
        static_id = lsqpack_find_in_static_headers(name_hash, name, name_len);
    if (static_id >= 0)
    {
  static_name_match:
        id = static_id;
        if (index && (enough_room = qenc_has_or_can_evict_at_least(enc,
                                             ENTRY_COST(name_len, value_len)), enough_room != 0))
        {
            static const struct encode_program programs[2][2][2] = {
                [0][0][0] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },
                [0][0][1] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },
                [0][1][0] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },
                [0][1][1] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },
                [1][0][0] = { EEA_INS_NAMEREF_STATIC, EHA_LIT_WITH_NAME_STAT, ETA_NEW,  0, },
                [1][0][1] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },
                [1][1][0] = { EEA_INS_NAMEREF_STATIC, EHA_INDEXED_NEW,        ETA_NEW,  EPF_REF_NEW, },
                [1][1][1] = { EEA_NONE,               EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, },   /* Invalid state */
            };
            seen_nameval = qenc_hist_seen(enc, HE_NAMEVAL, nameval_hash);
            prog = programs[seen_nameval][risk][use_dyn_table && n_cand > 0];
        }
        else
            prog = (struct encode_program) { EEA_NONE, EHA_LIT_WITH_NAME_STAT, ETA_NOOP, 0, };
        goto execute_program;
    }

    /* Look for name-only match in the dynamic table */
    /* TODO We may want to duplicate a dynamic entry whose name matches.
     * In that case, we'd follow similar logic as above: select candidates
     * and pick among them based on some factors.
     */
    enough_room = -1;
    if (use_dyn_table)
    {
        buckno = BUCKNO(enc->qpe_nbits, name_hash);
        STAILQ_FOREACH(entry, &enc->qpe_buckets[buckno].by_name, ete_next_name)
            if (name_hash == entry->ete_name_hash &&
                !qenc_entry_is_draining(enc, entry) &&
                name_len == entry->ete_name_len &&
                (risk || entry->ete_id <= enc->qpe_max_acked_id) &&
                (!index ||
                    (enough_room < 0 ?
                        (enough_room = qenc_has_or_can_evict_at_least(enc,
                                             ENTRY_COST(name_len, value_len)))
                                : enough_room))
                &&
                0 == memcmp(name, ETE_NAME(entry), name_len))
            {
                id = entry->ete_id;
                if (index && enough_room && risk
                    && (seen_nameval < 0 ? (seen_nameval
                        = qenc_hist_seen(enc, HE_NAMEVAL, nameval_hash)) : seen_nameval))
                    prog = (struct encode_program) { EEA_INS_NAMEREF_DYNAMIC,
                                EHA_INDEXED_NEW, ETA_NEW,
                                EPF_REF_NEW|EPF_REF_FOUND, };
                else
                    prog = (struct encode_program) { EEA_NONE,
                            EHA_LIT_WITH_NAME_DYN, ETA_NOOP, EPF_REF_FOUND, };
                goto execute_program;
            }
    }

    /* No matches found */
    if (index
            && (seen_nameval < 0 ? (seen_nameval
                    = qenc_hist_seen(enc, HE_NAMEVAL, nameval_hash)) : seen_nameval)
            && (enough_room < 0 ?
            (enough_room = qenc_has_or_can_evict_at_least(enc,
                            ENTRY_COST(name_len, value_len))) : enough_room))
    {
        static const struct encode_program programs[2][2] = {
            [0][0] = { EEA_INS_LIT,     EHA_LIT,                ETA_NEW,  0, },
            [0][1] = { EEA_NONE,        EHA_LIT,                ETA_NOOP, 0, },
            [1][0] = { EEA_INS_LIT,     EHA_INDEXED_NEW,        ETA_NEW,  EPF_REF_NEW, },
            [1][1] = { EEA_NONE,        EHA_LIT,                ETA_NOOP, 0, },  /* Invalid state */
        };
        prog = programs[risk][use_dyn_table && n_cand > 0];
    }
    else if (index && qenc_hist_seen(enc, HE_NAME, name_hash)
                && qenc_has_or_can_evict_at_least(enc, ENTRY_COST(name_len, 0)))
    {
        static const struct encode_program programs[2] = {
            [0] = { EEA_INS_LIT_NAME, EHA_LIT,               ETA_NEW_NAME, 0, },
            [1] = { EEA_INS_LIT_NAME, EHA_LIT_WITH_NAME_NEW, ETA_NEW_NAME, EPF_REF_NEW, },
        };
        prog = programs[ risk ];
    }
    else
        prog = (struct encode_program) { EEA_NONE, EHA_LIT, ETA_NOOP, 0, };

  execute_program:
    if (((1 << prog.ep_enc_action) &
            ((1 << EEA_INS_NAMEREF_STATIC)  |
             (1 << EEA_INS_NAMEREF_DYNAMIC) |
             (1 << EEA_INS_LIT)             |
             (1 << EEA_INS_LIT_NAME)))
         &&
        ((1 << prog.ep_hea_action) &
            ((1 << EHA_LIT)                 |
             (1 << EHA_LIT_WITH_NAME_STAT)  |
             (1 << EHA_LIT_WITH_NAME_DYN)   |
             (1 << EHA_LIT_WITH_NAME_NEW))))
    {
        unsigned bytes_out, bytes_in;
        bytes_out = enc->qpe_bytes_out
                  + qenc_enc_str_size((unsigned char *) name, name_len)
                  + qenc_enc_str_size((unsigned char *) value, value_len)
                  ;
        bytes_in = enc->qpe_bytes_in + name_len + value_len;
        if ((float) bytes_out / (float) bytes_in > 0.95)
        {
            assert(index);
            index = 0;
            E_DEBUG("double lit would result in ratio > 0.95, reset");
            goto restart;
        }
    }

    E_DEBUG("program: %s; %s; %s; flags: 0x%X",
        eea2str[ prog.ep_enc_action ], eha2str[ prog.ep_hea_action ],
        eta2str[ prog.ep_tab_action ], prog.ep_flags);
    switch (prog.ep_enc_action)
    {
    case EEA_DUP:
        if (enc_buf >= enc_buf_end)
            return LQES_NOBUF_ENC;
        dst = enc_buf;
        *dst = 0;
        dst = lsqpack_enc_int(dst, enc_buf_end, enc->qpe_ins_count - id, 5);
        if (dst <= enc_buf)
            return LQES_NOBUF_ENC;
        enc_sz = dst - enc_buf;
        break;
    case EEA_INS_NAMEREF_STATIC:
        if (enc_buf >= enc_buf_end)
            return LQES_NOBUF_ENC;
        dst = enc_buf;
        *dst = 0x80 | 0x40;
        dst = lsqpack_enc_int(dst, enc_buf_end, id, 6);
        if (dst <= enc_buf)
            return LQES_NOBUF_ENC;
        r = lsqpack_enc_enc_str(7, dst, enc_buf_end - dst,
                                    (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_ENC;
        dst += (unsigned) r;
        enc_sz = dst - enc_buf;
        break;
    case EEA_INS_NAMEREF_DYNAMIC:
        if (enc_buf >= enc_buf_end)
            return LQES_NOBUF_ENC;
        dst = enc_buf;
        *dst = 0x80;
        dst = lsqpack_enc_int(dst, enc_buf_end, enc->qpe_ins_count - id, 6);
        if (dst <= enc_buf)
            return LQES_NOBUF_ENC;
        r = lsqpack_enc_enc_str(7, dst, enc_buf_end - dst,
                                    (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_ENC;
        dst += (unsigned) r;
        enc_sz = dst - enc_buf;
        break;
    case EEA_INS_LIT:
    case EEA_INS_LIT_NAME:
        if (enc_buf >= enc_buf_end)
            return LQES_NOBUF_ENC;
        dst = enc_buf;
        *dst = 0x40;
        r = lsqpack_enc_enc_str(5, dst, enc_buf_end - dst,
                                (const unsigned char *) name, name_len);
        if (r < 0)
            return LQES_NOBUF_ENC;
        dst += r;
        r = lsqpack_enc_enc_str(7, dst, enc_buf_end - dst,
                        (const unsigned char *) value,
                        prog.ep_enc_action == EEA_INS_LIT ? value_len : 0);
        if (r < 0)
            return LQES_NOBUF_ENC;
        dst += r;
        enc_sz = dst - enc_buf;
        break;
    case EEA_NONE: default:
        assert(EEA_NONE == prog.ep_enc_action);
        enc_sz = 0;
        break;
    }

    dst = hea_buf;
    switch (prog.ep_hea_action)
    {
    case EHA_INDEXED_STAT:
        *dst = 0x80 | 0x40;
        dst = lsqpack_enc_int(dst, hea_buf_end, id, 6);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        hea_sz = dst - hea_buf;
        break;
    case EHA_INDEXED_NEW:
        id = enc->qpe_ins_count + 1;
  post_base_idx:
        *dst = 0x10;
        assert(id > enc->qpe_cur_header.base_idx);
        dst = lsqpack_enc_int(dst, hea_buf_end,
                                    id - enc->qpe_cur_header.base_idx - 1, 4);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        hea_sz = dst - hea_buf;
        break;
    case EHA_INDEXED_DYN:
        if (id > enc->qpe_cur_header.base_idx)
            goto post_base_idx;
        *dst = 0x80;
        dst = lsqpack_enc_int(dst, hea_buf_end,
                                        enc->qpe_cur_header.base_idx - id, 6);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        hea_sz = dst - hea_buf;
        break;
    case EHA_LIT:
        *dst = 0x20
               | (((flags & LQEF_NEVER_INDEX) > 0) << 4)
               ;
        r = lsqpack_enc_enc_str(3, dst, hea_buf_end - dst,
                                (const unsigned char *) name, name_len);
        if (r < 0)
            return LQES_NOBUF_HEAD;
        dst += r;
        r = lsqpack_enc_enc_str(7, dst, hea_buf_end - dst,
                                (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_HEAD;
        dst += r;
        hea_sz = dst - hea_buf;
        break;
    case EHA_LIT_WITH_NAME_NEW:
        id = enc->qpe_ins_count + 1;
 post_base_name_ref:
        *dst = (((flags & LQEF_NEVER_INDEX) > 0) << 3);
        assert(id > enc->qpe_cur_header.base_idx);
        dst = lsqpack_enc_int(dst, hea_buf_end,
                                    id - enc->qpe_cur_header.base_idx - 1, 3);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        r = lsqpack_enc_enc_str(7, dst, hea_buf_end - dst,
                                (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_HEAD;
        dst += (unsigned) r;
        hea_sz = dst - hea_buf;
        break;
    case EHA_LIT_WITH_NAME_DYN:
        if (id > enc->qpe_cur_header.base_idx)
            goto post_base_name_ref;
        *dst = 0x40
               | (((flags & LQEF_NEVER_INDEX) > 0) << 5)
               ;
        dst = lsqpack_enc_int(dst, hea_buf_end,
                                        enc->qpe_cur_header.base_idx - id, 4);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        r = lsqpack_enc_enc_str(7, dst, hea_buf_end - dst,
                                (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_HEAD;
        dst += (unsigned) r;
        hea_sz = dst - hea_buf;
        break;
    case EHA_LIT_WITH_NAME_STAT: default:
        assert(prog.ep_hea_action == EHA_LIT_WITH_NAME_STAT);
        *dst = 0x40
               | (((flags & LQEF_NEVER_INDEX) > 0) << 5)
               | 0x10
               ;
        dst = lsqpack_enc_int(dst, hea_buf_end, id, 4);
        if (dst <= hea_buf)
            return LQES_NOBUF_HEAD;
        r = lsqpack_enc_enc_str(7, dst, hea_buf_end - dst,
                                (const unsigned char *) value, value_len);
        if (r < 0)
            return LQES_NOBUF_HEAD;
        dst += (unsigned) r;
        hea_sz = dst - hea_buf;
        break;
    }

    switch (prog.ep_tab_action)
    {
    case ETA_NEW:
    case ETA_NEW_NAME:
        new_entry = lsqpack_enc_push_entry(enc, name_hash, nameval_hash, name,
                name_len, value, prog.ep_tab_action == ETA_NEW ? value_len : 0);
        if (!new_entry)
        {   /* Push can only fail due to inability to allocate memory.
             * In this case, fall back on encoding without indexing.
             */
            index = 0;
            goto restart;
        }
        enc->qpe_cur_header.hinfo->qhi_bytes_inserted += ETE_SIZE(new_entry);
        if (prog.ep_flags & EPF_REF_NEW)
        {
            ++new_entry->ete_n_reffd;
            enc->qpe_cur_header.flags |= LSQECH_REF_NEW_ENTRIES;
            if (HINFO_IDS_SET(enc->qpe_cur_header.hinfo))
                assert(new_entry->ete_id > enc->qpe_cur_header.hinfo->qhi_max_id);
            qenc_maybe_update_hinfo_min_max(enc->qpe_cur_header.hinfo,
                                                            new_entry->ete_id);
        }
        break;
    case ETA_NOOP: default:
        assert(prog.ep_tab_action == ETA_NOOP);
        break;
    }

    if (prog.ep_flags & EPF_REF_FOUND)
    {
        ++entry->ete_n_reffd;
        qenc_maybe_update_hinfo_min_max(enc->qpe_cur_header.hinfo,
                                                            entry->ete_id);
    }

    qenc_remove_overflow_entries(enc);

    if (update_hist)
    {
        assert(enc->qpe_hist_els);
        if (enc->qpe_cur_header.n_hdr_added_to_hist >= enc->qpe_hist_nels)
            qenc_hist_update_size(enc, enc->qpe_hist_nels + 4);
        qenc_hist_add(enc, name_hash, nameval_hash);
        ++enc->qpe_cur_header.n_hdr_added_to_hist;
    }

    while (sz = qenc_dup_draining(enc, enc_buf + enc_sz,
                                    enc_buf_end - enc_buf - enc_sz), sz > 0)
    {
        enc_sz += sz;
        qenc_remove_overflow_entries(enc);
    }

    enc->qpe_bytes_in += name_len + value_len;
    enc->qpe_bytes_out += (unsigned)(enc_sz + hea_sz);
    if (enc->qpe_bytes_out > (1u << (sizeof(enc->qpe_bytes_out) * 8 - 1)))
    {
        enc->qpe_bytes_in = (int)((float) enc->qpe_bytes_in
                                    / (float) enc->qpe_bytes_out * 1000);
        enc->qpe_bytes_out = 1000;
        E_DEBUG("reset bytes in/out counters, ratio: %.3f",
                                                    lsqpack_enc_ratio(enc));
    }

    *enc_sz_p = enc_sz;
    *hea_sz_p = hea_sz;
    return LQES_OK;
}


int
lsqpack_enc_set_max_capacity (struct lsqpack_enc *enc, unsigned capacity,
                                    unsigned char *tsu_buf, size_t *tsu_buf_sz)
{
    unsigned char *p;

    if (capacity > enc->qpe_real_max_capacity)
    {
        errno = EINVAL;
        return -1;
    }

    if (capacity == enc->qpe_cur_max_capacity)
    {
        E_DEBUG("set_capacity: capacity stays unchanged at %u", capacity);
        *tsu_buf_sz = 0;
        return 0;
    }

    if (!(tsu_buf && tsu_buf_sz))
    {
        errno = EINVAL;
        return -1;
    }
    p = tsu_buf;
    *p = 0x20;
    p = lsqpack_enc_int(p, tsu_buf + *tsu_buf_sz, capacity, 5);
    if (p <= tsu_buf)
    {
        errno = ENOBUFS;
        return -1;
    }
    *tsu_buf_sz = p - tsu_buf;

    E_DEBUG("maximum capacity goes from %u to %u", enc->qpe_cur_max_capacity,
                                                                    capacity);
    enc->qpe_cur_max_capacity = capacity;
    qenc_remove_overflow_entries(enc);
    return 0;
}


static void
qenc_update_risked_list (struct lsqpack_enc *enc)
{
    struct lsqpack_header_info *hinfo, *next;

    for (hinfo = TAILQ_FIRST(&enc->qpe_risked_hinfos); hinfo; hinfo = next)
    {
        next = TAILQ_NEXT(hinfo, qhi_next_risked);
        if (!qenc_hinfo_at_risk(enc, hinfo))
            qenc_remove_from_risked_list(enc, hinfo);
    }
}


static int
enc_proc_header_ack (struct lsqpack_enc *enc, uint64_t stream_id)
{
    struct lsqpack_header_info *hinfo;

    E_DEBUG("got Header Ack instruction, stream=%"PRIu64, stream_id);
    if (stream_id > MAX_QUIC_STREAM_ID)
        return -1;

    TAILQ_FOREACH(hinfo, &enc->qpe_all_hinfos, qhi_next_all)
        if (stream_id == hinfo->qhi_stream_id)
            break;

    /*
     * XXX if an ACK comes in while a header is being encoded, it will not
     *     have any effect because the the `qhi_max_id` is 0 until the header
     *     encoding is finished (see enc_end_header()).
     */

    if (!hinfo)
        return -1;

    if (hinfo->qhi_max_id > enc->qpe_max_acked_id)
    {
        qenc_remove_from_risked_list(enc, hinfo);
        enc->qpe_max_acked_id = hinfo->qhi_max_id;
        qenc_update_risked_list(enc);
        E_DEBUG("max acked ID is now %u", enc->qpe_max_acked_id);
    }

    enc_free_hinfo(enc, hinfo);
    return 0;
}


static int
enc_proc_ici (struct lsqpack_enc *enc, uint64_t ins_count)
{
    lsqpack_abs_id_t max_acked;

    E_DEBUG("got ICI instruction, count=%"PRIu64, ins_count);
    if (ins_count == 0)
    {
        E_INFO("ICI=0 is an error");
        return -1;
    }

    if (ins_count > LSQPACK_MAX_ABS_ID)
    {
        /* We never insert this many */
        E_INFO("insertion count too high: %"PRIu64, ins_count);
        return -1;
    }

    max_acked = (lsqpack_abs_id_t) ins_count + enc->qpe_last_ici;
    if (max_acked > enc->qpe_ins_count)
    {
        E_DEBUG("ICI: max_acked %u is larger than number of inserts %u",
            max_acked, enc->qpe_ins_count);
        return -1;
    }

    if (max_acked > enc->qpe_max_acked_id)
    {
        enc->qpe_last_ici = max_acked;
        enc->qpe_max_acked_id = max_acked;
        E_DEBUG("max acked ID is now %u", enc->qpe_max_acked_id);
        qenc_update_risked_list(enc);
    }
    else
    {
        E_DEBUG("duplicate ICI: %u", max_acked);
    }
    return 0;
}


static int
enc_proc_stream_cancel (struct lsqpack_enc *enc, uint64_t stream_id)
{
    struct lsqpack_header_info *hinfo, *next;
    unsigned count;

    E_DEBUG("got Cancel Stream instruction; stream=%"PRIu64, stream_id);

    if (stream_id > MAX_QUIC_STREAM_ID)
    {
        E_INFO("Invalid stream ID %"PRIu64" in Cancel Stream", stream_id);
        return -1;
    }

    count = 0;
    for (hinfo = TAILQ_FIRST(&enc->qpe_all_hinfos); hinfo; hinfo = next)
    {
        next = TAILQ_NEXT(hinfo, qhi_next_all);
        if (hinfo->qhi_stream_id == stream_id)
        {
            E_DEBUG("cancel header block for stream %"PRIu64", seqno %u",
                stream_id, hinfo->qhi_seqno);
            if (qenc_hinfo_at_risk(enc, hinfo))
                qenc_remove_from_risked_list(enc, hinfo);
            enc_free_hinfo(enc, hinfo);
            ++count;
        }
    }

    E_DEBUG("cancelled %u header block%.*s of stream %"PRIu64,
                                        count, count != 1, "s", stream_id);
    return 0;
}


/* Assumption: we have at least one byte to work with */
/* Return value:
 *  0   OK
 *  -1  Out of input
 *  -2  Value cannot be represented as 64-bit integer (overflow)
 */
int
lsqpack_dec_int (const unsigned char **src_p, const unsigned char *src_end,
                   unsigned prefix_bits, uint64_t *value_p,
                   struct lsqpack_dec_int_state *state)
{
    const unsigned char *const orig_src = *src_p;
    const unsigned char *src;
    unsigned char prefix_max;
    unsigned M, nread;
    uint64_t val, B;

    src = *src_p;

    if (state->resume)
    {
        val = state->val;
        M = state->M;
        goto resume;
    }

    prefix_max = (1 << prefix_bits) - 1;
    val = *src++;
    val &= prefix_max;

    if (val < prefix_max)
    {
        *src_p = src;
        *value_p = val;
        return 0;
    }

    M = 0;
    do
    {
        if (src < src_end)
        {
  resume:   B = *src++;
            val = val + ((B & 0x7f) << M);
            M += 7;
        }
        else
        {
            nread = (state->resume ? state->nread : 0) + (unsigned)(src - orig_src);
            if (nread < LSQPACK_UINT64_ENC_SZ)
            {
                state->val = val;
                state->M = M;
                state->nread = nread;
                state->resume = 1;
                return -1;
            }
            else
                return -2;
        }
    }
    while (B & 0x80);

    if (M <= 63 || (M == 70 && src[-1] <= 1 && (val & (1ull << 63))))
    {
        *src_p = src;
        *value_p = val;
        return 0;
    }
    else
        return -2;
}


typedef char unsigned_is_32bits[(sizeof(unsigned) == 4) ? 1 : -1];

/* TODO: rewrite as a standalone function */
int
lsqpack_dec_int24 (const unsigned char **src_p, const unsigned char *src_end,
                   unsigned prefix_bits, unsigned *value_p,
                   struct lsqpack_dec_int_state *state)
{
    uint64_t val;
    int r;

    r = lsqpack_dec_int(src_p, src_end, prefix_bits, &val, state);
    if (r == 0 && val < (1u << 24))
    {
        *value_p = (unsigned int) val;
        return 0;
    }
    else if (r != 0)
        return r;
    else
        return -2;
}


int
lsqpack_enc_decoder_in (struct lsqpack_enc *enc,
                                    const unsigned char *buf, size_t buf_sz)
{
    const unsigned char *const end = buf + buf_sz;
    uint64_t val;
    int r;
    unsigned prefix_bits = ~0u; /* This can be any value in a resumed call
                                 * to the integer decoder -- it is only
                                 * used in the first call.
                                 */
    E_DEBUG("got %zu bytes of decoder stream", buf_sz);

    while (buf < end)
    {
        switch (enc->qpe_dec_stream_state.dec_int_state.resume)
        {
        case 0:
            if (buf[0] & 0x80)              /* Header ACK */
            {
                prefix_bits = 7;
                enc->qpe_dec_stream_state.handler = enc_proc_header_ack;
            }
            else if ((buf[0] & 0xC0) == 0)  /* Insert Count Increment */
            {
                prefix_bits = 6;
                enc->qpe_dec_stream_state.handler = enc_proc_ici;
            }
            else                            /* Stream Cancellation */
            {
                assert((buf[0] & 0xC0) == 0x40);
                prefix_bits = 6;
                enc->qpe_dec_stream_state.handler = enc_proc_stream_cancel;
            }
            FALL_THROUGH;
        case 1:
            r = lsqpack_dec_int(&buf, end, prefix_bits, &val,
                                &enc->qpe_dec_stream_state.dec_int_state);
            if (r == 0)
            {
                r = enc->qpe_dec_stream_state.handler(enc, val);
                if (r != 0)
                    return -1;
                enc->qpe_dec_stream_state.dec_int_state.resume = 0;
            }
            else if (r == -1)
            {
                enc->qpe_dec_stream_state.dec_int_state.resume = 1;
                return 0;
            }
            else
                return -1;
            break;
        }
    }
    enc->qpe_bytes_out += (unsigned)buf_sz;

    return 0;
}


float
lsqpack_enc_ratio (const struct lsqpack_enc *enc)
{
    float ratio;

    if (enc->qpe_bytes_in)
    {
        ratio = (float) enc->qpe_bytes_out / (float) enc->qpe_bytes_in;
        E_DEBUG("bytes out: %u; bytes in: %u, ratio: %.3f",
                            enc->qpe_bytes_out, enc->qpe_bytes_in, ratio);
        return ratio;
    }
    else
        return 0;
}


#ifdef LSQPACK_DEC_LOGGER_HEADER
#error #include LSQPACK_DEC_LOGGER_HEADER
#else
#define D_LOG(prefix, ...) do {                                     \
    if (dec->qpd_logger_ctx) {                                          \
        fprintf(dec->qpd_logger_ctx, prefix);                           \
        fprintf(dec->qpd_logger_ctx, __VA_ARGS__);                      \
        fprintf(dec->qpd_logger_ctx, "\n");                             \
    }                                                                   \
} while (0)
#define D_DEBUG(...) D_LOG("qdec: debug: ", __VA_ARGS__)
#define D_INFO(...)  D_LOG("qdec: info: ", __VA_ARGS__)
#define D_WARN(...)  D_LOG("qdec: warn: ", __VA_ARGS__)
#define D_ERROR(...) D_LOG("qdec: error: ", __VA_ARGS__)
#endif


/* Dynamic table entry: */
struct lsqpack_dec_table_entry
{
    unsigned    dte_name_len;
    unsigned    dte_val_len;
    unsigned    dte_refcnt;
    unsigned    dte_name_hash;
    unsigned    dte_nameval_hash;
    unsigned    dte_name_idx;
    enum {
        DTEF_NAME_HASH      = 1 << 0,
        DTEF_NAMEVAL_HASH   = 1 << 1,
        DTEF_NAME_IDX       = 1 << 2,
    }           dte_flags;
    char        dte_buf[0];     /* Contains both name and value */
};

#define DTE_NAME(dte) ((dte)->dte_buf)
#define DTE_VALUE(dte) (&(dte)->dte_buf[(dte)->dte_name_len])
#define DTE_SIZE(dte) ENTRY_COST((dte)->dte_name_len, (dte)->dte_val_len)

enum
{
    HPACK_HUFFMAN_FLAG_ACCEPTED = 0x01,
    HPACK_HUFFMAN_FLAG_SYM = 0x02,
    HPACK_HUFFMAN_FLAG_FAIL = 0x04,
};


#if LSQPACK_DEVEL_MODE
#   define STATIC
#else
#   define STATIC static
#endif

STATIC unsigned
ringbuf_count (const struct lsqpack_ringbuf *rbuf)
{
    if (rbuf->rb_nalloc)
    {
        if (rbuf->rb_head >= rbuf->rb_tail)
            return rbuf->rb_head - rbuf->rb_tail;
        else
            return rbuf->rb_nalloc - (rbuf->rb_tail - rbuf->rb_head);
    }
    else
        return 0;
}


STATIC int
ringbuf_full (const struct lsqpack_ringbuf *rbuf)
{
    return rbuf->rb_nalloc == 0
        || (rbuf->rb_head + 1) % rbuf->rb_nalloc == rbuf->rb_tail;
}


STATIC int
ringbuf_empty (const struct lsqpack_ringbuf *rbuf)
{
    return rbuf->rb_head == rbuf->rb_tail;
}


struct ringbuf_iter
{
    const struct lsqpack_ringbuf *rbuf;
    unsigned next, end;
};


STATIC void *
ringbuf_iter_next (struct ringbuf_iter *iter)
{
    void *el;

    if (iter->next != iter->rbuf->rb_head)
    {
        el = iter->rbuf->rb_els[ iter->next ];
        iter->next = (iter->next + 1) % iter->rbuf->rb_nalloc;
        return el;
    }
    else
        return NULL;
}


STATIC void *
ringbuf_iter_first (struct ringbuf_iter *iter,
                                        const struct lsqpack_ringbuf *rbuf)
{
    if (!ringbuf_empty(rbuf))
    {
        iter->rbuf = rbuf;
        iter->next = rbuf->rb_tail;
        return ringbuf_iter_next(iter);
    }
    else
        return NULL;
}


static void
ringbuf_cleanup (struct lsqpack_ringbuf *rbuf)
{
    free(rbuf->rb_els);
    memset(rbuf, 0, sizeof(*rbuf));
}


static void *
ringbuf_get_head (const struct lsqpack_ringbuf *rbuf, unsigned off)
{
    unsigned i;

    i = (rbuf->rb_nalloc + rbuf->rb_head - off) % rbuf->rb_nalloc;
    return rbuf->rb_els[i];
}


static void *
ringbuf_advance_tail (struct lsqpack_ringbuf *rbuf)
{
    void *el;

    el = rbuf->rb_els[rbuf->rb_tail];
    rbuf->rb_tail = (rbuf->rb_tail + 1) % rbuf->rb_nalloc;
    return el;
}


static int
ringbuf_add (struct lsqpack_ringbuf *rbuf, void *el)
{
    void **els;
    unsigned count;

    if (!ringbuf_full(rbuf))
    {
  insert:
        rbuf->rb_els[ rbuf->rb_head ] = el;
        rbuf->rb_head = (rbuf->rb_head + 1) % rbuf->rb_nalloc;
        return 0;
    }

    if (rbuf->rb_nalloc)
    {
        els = malloc(rbuf->rb_nalloc * 2 * sizeof(rbuf->rb_els[0]));
        if (els)
        {
            if (rbuf->rb_head >= rbuf->rb_tail)
            {
                count = rbuf->rb_head - rbuf->rb_tail + 1;
                memcpy(els, rbuf->rb_els + rbuf->rb_tail,
                                    count * sizeof(rbuf->rb_els[0]));
                rbuf->rb_tail = 0;
                rbuf->rb_head = count - 1;
            }
            else
            {
                memcpy(els, rbuf->rb_els,
                            (rbuf->rb_head + 1) * sizeof(rbuf->rb_els[0]));
                memcpy(els + rbuf->rb_nalloc + rbuf->rb_tail,
                        rbuf->rb_els + rbuf->rb_tail,
                        (rbuf->rb_nalloc - rbuf->rb_tail)
                                                * sizeof(rbuf->rb_els[0]));
                rbuf->rb_tail += rbuf->rb_nalloc;

            }
            free(rbuf->rb_els);
            rbuf->rb_els = els;
            rbuf->rb_nalloc *= 2;
            goto insert;
        }
        return -1;
    }
    else
    {
        /* First time */
        rbuf->rb_els = malloc(4 * sizeof(rbuf->rb_els[0]));
        if (rbuf->rb_els)
        {
            rbuf->rb_nalloc = 4;
            goto insert;
        }
        return -1;
    }
}


#define ID_MINUS(a, b) ( (dec)->qpd_max_entries ? \
    ((a) + (dec)->qpd_max_entries * 2 - (b)) % ((dec)->qpd_max_entries * 2) : 0)

#define ID_PLUS(a, b) ( (dec)->qpd_max_entries ? \
    ((a) + (b)) % ((dec)->qpd_max_entries * 2) : 0 )

static struct lsqpack_dec_table_entry *
qdec_get_table_entry_rel (const struct lsqpack_dec *dec,
                                            lsqpack_abs_id_t relative_idx)
{
    ++relative_idx;
    if (ringbuf_count(&dec->qpd_dyn_table) >= relative_idx)
        return ringbuf_get_head(&dec->qpd_dyn_table, relative_idx);
    else
        return NULL;
}


static struct lsqpack_dec_table_entry *
qdec_get_table_entry_abs (const struct lsqpack_dec *dec,
                                                lsqpack_abs_id_t abs_idx)
{
    unsigned off;

    off = ID_MINUS(dec->qpd_last_id, abs_idx);
    return qdec_get_table_entry_rel(dec, off);
}


void
lsqpack_dec_init (struct lsqpack_dec *dec, void *logger_ctx,
    unsigned dyn_table_size, unsigned max_risked_streams,
    const struct lsqpack_dec_hset_if *dh_if, enum lsqpack_dec_opts opts)
{
    unsigned i;
    memset(dec, 0, sizeof(*dec));
    dec->qpd_opts = opts;
    dec->qpd_logger_ctx = logger_ctx;
    dec->qpd_max_capacity = dyn_table_size;
    dec->qpd_cur_max_capacity = dyn_table_size;
    dec->qpd_max_entries = dec->qpd_max_capacity / DYNAMIC_ENTRY_OVERHEAD;
    dec->qpd_last_id = dec->qpd_max_entries * 2 - 1;
    dec->qpd_largest_known_id = dec->qpd_max_entries * 2 - 1;
    dec->qpd_max_risked_streams = max_risked_streams;
    dec->qpd_dh_if = dh_if;
    TAILQ_INIT(&dec->qpd_hbrcs);
    for (i = 0; i < (1 << LSQPACK_DEC_BLOCKED_BITS); ++i)
        TAILQ_INIT(&dec->qpd_blocked_headers[i]);
    D_DEBUG("initialized.  max capacity=%u; max risked streams=%u",
        dec->qpd_max_capacity, dec->qpd_max_risked_streams);
}


static void
qdec_decref_entry (struct lsqpack_dec_table_entry *entry)
{
    --entry->dte_refcnt;
    if (0 == entry->dte_refcnt)
        free(entry);
}


enum {
    DEI_NEXT_INST,
    DEI_WINR_READ_NAME_IDX,
    DEI_WINR_BEGIN_READ_VAL_LEN,
    DEI_WINR_READ_VAL_LEN,
    DEI_WINR_READ_VALUE_PLAIN,
    DEI_WINR_READ_VALUE_HUFFMAN,
    DEI_DUP_READ_IDX,
    DEI_SIZE_UPD_READ_IDX,
    DEI_WONR_READ_NAME_LEN,
    DEI_WONR_READ_NAME_HUFFMAN,
    DEI_WONR_READ_NAME_PLAIN,
    DEI_WONR_BEGIN_READ_VAL_LEN,
    DEI_WONR_READ_VAL_LEN,
    DEI_WONR_READ_VALUE_HUFFMAN,
    DEI_WONR_READ_VALUE_PLAIN,
};

struct header_block_read_ctx
{
    TAILQ_ENTRY(header_block_read_ctx)  hbrc_next_all,
                                        hbrc_next_blocked;
    void                               *hbrc_hblock;
    uint64_t                            hbrc_stream_id;
    size_t                              hbrc_orig_size;     /* To report error offset */
    size_t                              hbrc_size;
    lsqpack_abs_id_t                    hbrc_largest_ref;   /* Parsed from prefix */
    lsqpack_abs_id_t                    hbrc_base_index;    /* Parsed from prefix */
    unsigned                            hbrc_header_count;

    struct {
        struct lsxpack_header          *xhdr;               /* Current header */
        enum {
            XOUT_NAME,                                      /* Writing name */
            XOUT_VALUE,                                     /* Writing value */
        }                               state;
        unsigned                        off;                /* How much has been written */
    }                                   hbrc_out;

    /* There are two parsing phases: reading the prefix and reading the
     * instruction stream.
     */
    enum lsqpack_read_header_status           (*hbrc_parse) (struct lsqpack_dec *,
            struct header_block_read_ctx *, const unsigned char *, size_t);

    enum {
        HBRC_LARGEST_REF_READ   = 1 << 0,
        HBRC_LARGEST_REF_SET    = 1 << 1, /* hbrc_largest_ref is an actual ID */
        HBRC_BLOCKED            = 1 << 2,
        HBRC_DINST              = 1 << 3,
        HBRC_ON_LIST            = 1 << 4,
#define LARGEST_USED_SHIFT 5
        HBRC_LARGEST_REF_USED   = 1 << LARGEST_USED_SHIFT,
        HBRC_DYN_USED_IN_ERR    = 1 << 6,
    }                                   hbrc_flags;

    struct hbrc_buf {
        const unsigned char *buf;
        size_t               sz;
        size_t               off;
    }                                   hbrc_buf;

    union {
        struct {
            enum {
                PREFIX_STATE_BEGIN_READING_LARGEST_REF,
                PREFIX_STATE_READ_LARGEST_REF,
                PREFIX_STATE_BEGIN_READING_BASE_IDX,
                PREFIX_STATE_READ_DELTA_BASE_IDX,
            }                                               state;
            union {
                /* Required Insert Count */
                struct {
                    struct lsqpack_dec_int_state    dec_int_state;
                    uint64_t                        value;
                }                                               ric;
                /* Delta Base */
                struct {
                    struct lsqpack_dec_int_state    dec_int_state;
                    uint64_t                        value;
                    int                             sign;
                }                                               delb;
            }                                               u;
        }                                       prefix;
        struct {
            enum {
                DATA_STATE_NEXT_INSTRUCTION,
                DATA_STATE_READ_IHF_IDX,
                DATA_STATE_READ_IPBI_IDX,
                DATA_STATE_READ_LFINR_IDX,
                DATA_STATE_BEGIN_READ_VAL_LEN,
                DATA_STATE_READ_VAL_LEN,
                DATA_STATE_READ_VAL_HUFFMAN,
                DATA_STATE_READ_VAL_PLAIN,
                DATA_STATE_READ_LFONR_NAME_LEN,
                DATA_STATE_READ_NAME_HUFFMAN,
                DATA_STATE_READ_NAME_PLAIN,
                DATA_STATE_READ_LFPBNR_IDX,
                DATA_STATE_BEGIN_READ_LFPBNR_VAL_LEN,
                DATA_STATE_READ_LFPBNR_VAL_LEN,
            }                                               state;

            /* We decode one string at a time, header name or header value. */
            unsigned                                        left;   /* Left to read */
            int                                             is_static;
            int                                             is_never;
            int                                             is_huffman;
            struct lsqpack_dec_int_state                    dec_int_state;
            struct lsqpack_huff_decode_state                dec_huff_state;
        }                                       data;
    }                                   hbrc_parse_ctx_u;
};


static enum lsqpack_read_header_status
parse_header_data (struct lsqpack_dec *,
        struct header_block_read_ctx *, const unsigned char *, size_t);


float
lsqpack_dec_ratio (const struct lsqpack_dec *dec)
{
    float ratio;

    if (dec->qpd_bytes_out)
    {
        ratio = (float) dec->qpd_bytes_in / (float) dec->qpd_bytes_out;
        D_DEBUG("bytes in: %u; bytes out: %u, ratio: %.3f",
                            dec->qpd_bytes_out, dec->qpd_bytes_in, ratio);
        return ratio;
    }
    else
        return 0;
}


void
lsqpack_dec_cleanup (struct lsqpack_dec *dec)
{
    struct lsqpack_dec_table_entry *entry;
    struct header_block_read_ctx *read_ctx, *next_read_ctx;

    for (read_ctx = TAILQ_FIRST(&dec->qpd_hbrcs); read_ctx;
                                                    read_ctx = next_read_ctx)
    {
        next_read_ctx = TAILQ_NEXT(read_ctx, hbrc_next_all);
        free(read_ctx);
    }

    if (dec->qpd_enc_state.resume >= DEI_WINR_READ_NAME_IDX
            && dec->qpd_enc_state.resume <= DEI_WINR_READ_VALUE_HUFFMAN)
    {
        if (dec->qpd_enc_state.ctx_u.with_namref.entry)
            free(dec->qpd_enc_state.ctx_u.with_namref.entry);
    }
    else if (dec->qpd_enc_state.resume >= DEI_WONR_READ_NAME_LEN
            && dec->qpd_enc_state.resume <= DEI_WONR_READ_VALUE_PLAIN)
    {
        if (dec->qpd_enc_state.ctx_u.wo_namref.entry)
            free(dec->qpd_enc_state.ctx_u.wo_namref.entry);
    }

    while (!ringbuf_empty(&dec->qpd_dyn_table))
    {
        entry = ringbuf_advance_tail(&dec->qpd_dyn_table);
        qdec_decref_entry(entry);
    }
    ringbuf_cleanup(&dec->qpd_dyn_table);
    D_DEBUG("cleaned up");
}


static void
qdec_maybe_update_entry_hashes (const struct lsqpack_dec *dec,
                                    struct lsqpack_dec_table_entry *entry)
{
    if ((dec->qpd_opts & (LSQPACK_DEC_OPT_HASH_NAME
                         |LSQPACK_DEC_OPT_HASH_NAMEVAL))
                                && !(entry->dte_flags & DTEF_NAME_HASH))
    {
        entry->dte_flags |= DTEF_NAME_HASH;
        entry->dte_name_hash = XXH32(DTE_NAME(entry), entry->dte_name_len,
                                                            LSQPACK_XXH_SEED);
    }
    if ((dec->qpd_opts & LSQPACK_DEC_OPT_HASH_NAMEVAL)
                                && !(entry->dte_flags & DTEF_NAMEVAL_HASH))
    {
        assert(entry->dte_flags & DTEF_NAME_HASH);
        entry->dte_flags |= DTEF_NAMEVAL_HASH;
        entry->dte_nameval_hash = XXH32(DTE_VALUE(entry), entry->dte_val_len,
                                                        entry->dte_name_hash);
    }
}


static int
header_out_static_entry (struct lsqpack_dec *dec,
                    struct header_block_read_ctx *read_ctx, uint64_t idx)
{
    struct lsxpack_header *xhdr;
    size_t need, http1x;
    char *dst;
    int r;

    if (idx >= QPACK_STATIC_TABLE_SIZE)
        return -1;

    http1x = !!(dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X) << 2; /* 0 or 4 */
    need = static_table[ idx ].name_len + static_table[ idx ].val_len + http1x;
    xhdr = dec->qpd_dh_if->dhi_prepare_decode(read_ctx->hbrc_hblock, NULL,
                                                                        need);
    if (!xhdr)
        return -1;

    xhdr->dec_overhead = (uint8_t)http1x;
    xhdr->qpack_index = (uint8_t)idx;
    xhdr->flags |= LSXPACK_VAL_MATCHED | LSXPACK_QPACK_IDX
                | LSXPACK_NAME_HASH | LSXPACK_NAMEVAL_HASH;
    xhdr->name_len = (lsxpack_strlen_t)static_table[ idx ].name_len;
    xhdr->val_len = (lsxpack_strlen_t)(static_table[ idx ].val_len);
    xhdr->name_hash = name_hashes[ idx ];
    xhdr->nameval_hash = nameval_hashes[ idx ];
    dst = xhdr->buf + xhdr->name_offset;
    memcpy(dst, static_table[ idx ].name, static_table[ idx ].name_len);
    dst += static_table[ idx ].name_len;
    if (http1x)
    {
        memcpy(dst, ": ", 2);
        dst += 2;
    }
    xhdr->val_offset = (lsxpack_offset_t)(dst - xhdr->buf);
    memcpy(dst, static_table[ idx ].val, static_table[ idx ].val_len);
    dst += static_table[ idx ].val_len;
    if (http1x)
        memcpy(dst, "\r\n", 2);
    r = dec->qpd_dh_if->dhi_process_header(read_ctx->hbrc_hblock, xhdr);
    if (r == 0)
        dec->qpd_bytes_out += static_table[ idx ].name_len
                            + static_table[ idx ].val_len;
    return r;
}


static int
header_out_dynamic_entry (struct lsqpack_dec *dec,
                    struct header_block_read_ctx *read_ctx, lsqpack_abs_id_t idx)
{
    struct lsqpack_dec_table_entry *entry;
    struct lsxpack_header *xhdr;
    size_t need, http1x;
    char *dst;
    int r;

    entry = qdec_get_table_entry_abs(dec, idx);
    if (!entry)
        return -1;

    http1x = !!(dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X) << 2; /* 0 or 4 */
    need = entry->dte_name_len + entry->dte_val_len + http1x;
    xhdr = dec->qpd_dh_if->dhi_prepare_decode(read_ctx->hbrc_hblock, NULL,
                                                                        need);
    if (!xhdr)
        return -1;

    qdec_maybe_update_entry_hashes(dec, entry);
    if (entry->dte_flags & DTEF_NAME_HASH)
    {
        xhdr->flags |= LSXPACK_NAME_HASH;
        xhdr->name_hash = entry->dte_name_hash;
    }
    if (entry->dte_flags & DTEF_NAMEVAL_HASH)
    {
        xhdr->flags |= LSXPACK_NAMEVAL_HASH;
        xhdr->nameval_hash = entry->dte_nameval_hash;
    }
    if (entry->dte_flags & DTEF_NAME_IDX)
    {
        xhdr->flags |= LSXPACK_QPACK_IDX;
        xhdr->qpack_index = (uint8_t)entry->dte_name_idx;
    }
    xhdr->dec_overhead = (uint8_t)http1x;
    xhdr->name_len = (lsxpack_strlen_t)entry->dte_name_len;
    xhdr->val_len = (lsxpack_strlen_t)entry->dte_val_len;
    dst = xhdr->buf + xhdr->name_offset;
    memcpy(dst, DTE_NAME(entry), entry->dte_name_len);
    dst += entry->dte_name_len;
    if (http1x)
    {
        memcpy(dst, ": ", 2);
        dst += 2;
    }
    xhdr->val_offset = (lsxpack_offset_t)(dst - xhdr->buf);
    memcpy(dst, DTE_VALUE(entry), entry->dte_val_len);
    dst += entry->dte_val_len;
    if (http1x)
        memcpy(dst, "\r\n", 2);
    r = dec->qpd_dh_if->dhi_process_header(read_ctx->hbrc_hblock, xhdr);
    if (r == 0)
        dec->qpd_bytes_out += entry->dte_name_len + entry->dte_val_len;
    return r;
}


static int
header_out_begin_static_nameref (struct lsqpack_dec *dec,
        struct header_block_read_ctx *read_ctx, unsigned idx, int is_never)
{
    struct lsxpack_header *xhdr;    /* Shorthand */
    size_t need, http1x;
    char *dst;

    assert(!read_ctx->hbrc_out.xhdr);

    if (idx >= QPACK_STATIC_TABLE_SIZE)
        return -1;

    http1x = !!(dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X) << 2; /* 0 or 4 */
    need = static_table[ idx ].name_len + http1x;
    read_ctx->hbrc_out.xhdr = xhdr = dec->qpd_dh_if->dhi_prepare_decode(
                                        read_ctx->hbrc_hblock, NULL, need);
    if (!xhdr)
        return -1;

    xhdr->dec_overhead = (uint8_t)http1x;
    xhdr->qpack_index = (uint8_t)idx;
    xhdr->flags |= LSXPACK_QPACK_IDX | LSXPACK_NAME_HASH;
    xhdr->name_hash = name_hashes[ idx ];
    if (is_never)
        xhdr->flags |= LSXPACK_NEVER_INDEX;
    xhdr->name_len = (lsxpack_strlen_t)(static_table[ idx ].name_len);
    dst = xhdr->buf + xhdr->name_offset;
    memcpy(dst, static_table[ idx ].name, static_table[ idx ].name_len);
    dst += static_table[ idx ].name_len;
    if (http1x)
    {
        memcpy(dst, ": ", 2);
        dst += 2;
    }
    xhdr->val_offset = (lsxpack_offset_t)(dst - xhdr->buf);
    read_ctx->hbrc_out.state = XOUT_VALUE;
    read_ctx->hbrc_out.off = 0;
    return 0;
}


static int
header_out_begin_dynamic_nameref (struct lsqpack_dec *dec,
                struct header_block_read_ctx *read_ctx,
                struct lsqpack_dec_table_entry *entry, int is_never)
{
    struct lsxpack_header *xhdr;    /* Shorthand */
    size_t need, http1x;
    char *dst;

    assert(!read_ctx->hbrc_out.xhdr);

    http1x = !!(dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X) << 2; /* 0 or 4 */
    need = entry->dte_name_len + http1x;
    read_ctx->hbrc_out.xhdr = xhdr = dec->qpd_dh_if->dhi_prepare_decode(
                                        read_ctx->hbrc_hblock, NULL, need);
    if (!xhdr)
        return -1;

    xhdr->dec_overhead = (uint8_t)http1x;
    if (is_never)
        xhdr->flags |= LSXPACK_NEVER_INDEX;
    qdec_maybe_update_entry_hashes(dec, entry);
    if (entry->dte_flags & DTEF_NAME_HASH)
    {
        xhdr->flags |= LSXPACK_NAME_HASH;
        xhdr->name_hash = entry->dte_name_hash;
    }
    if (entry->dte_flags & DTEF_NAME_IDX)
    {
        xhdr->flags |= LSXPACK_QPACK_IDX;
        xhdr->qpack_index = (uint8_t)entry->dte_name_idx;
    }
    xhdr->name_len = (lsxpack_strlen_t)entry->dte_name_len;
    dst = xhdr->buf + xhdr->name_offset;
    memcpy(dst, DTE_NAME(entry), entry->dte_name_len);
    dst += entry->dte_name_len;
    if (http1x)
    {
        memcpy(dst, ": ", 2);
        dst += 2;
    }
    xhdr->val_offset = (lsxpack_offset_t)(dst - xhdr->buf);
    read_ctx->hbrc_out.state = XOUT_VALUE;
    read_ctx->hbrc_out.off = 0;
    return 0;
}


static int
header_out_begin_literal (struct lsqpack_dec *dec,
        struct header_block_read_ctx *read_ctx, size_t need, int is_never)
{
    struct lsxpack_header *xhdr;    /* Shorthand */
    size_t http1x;

    assert(!read_ctx->hbrc_out.xhdr);

    http1x = !!(dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X) << 2; /* 0 or 4 */
    need += http1x;
    read_ctx->hbrc_out.xhdr = xhdr = dec->qpd_dh_if->dhi_prepare_decode(
                                        read_ctx->hbrc_hblock, NULL, need);
    if (!xhdr)
        return -1;

    xhdr->dec_overhead = (uint8_t)http1x;
    if (is_never)
        xhdr->flags |= LSXPACK_NEVER_INDEX;
    read_ctx->hbrc_out.state = XOUT_NAME;
    read_ctx->hbrc_out.off = 0;
    return 0;
}


static int
header_out_write_name (struct lsqpack_dec *dec,
            struct header_block_read_ctx *read_ctx, size_t nwritten, int done)
{
    struct lsxpack_header *xhdr;    /* Shorthand */

    read_ctx->hbrc_out.off += (unsigned)nwritten;

    if (done)
    {
        xhdr = read_ctx->hbrc_out.xhdr;
        if (dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X)
        {
            if (read_ctx->hbrc_out.off + 2 > xhdr->val_len)
            {
                read_ctx->hbrc_out.xhdr = xhdr = dec->qpd_dh_if
                    ->dhi_prepare_decode(read_ctx->hbrc_hblock, xhdr,
                            read_ctx->hbrc_out.off + 2);
                if (!xhdr)
                    return -1;
            }
            memcpy(xhdr->buf + xhdr->name_offset + read_ctx->hbrc_out.off,
                                                                    ": ", 2);
            xhdr->val_offset = (lsxpack_offset_t)(xhdr->name_offset + read_ctx->hbrc_out.off + 2);
        }
        else
            xhdr->val_offset = (lsxpack_offset_t)(xhdr->name_offset + read_ctx->hbrc_out.off);
        xhdr->name_len = (lsxpack_strlen_t)read_ctx->hbrc_out.off;
        read_ctx->hbrc_out.state = XOUT_VALUE;
        read_ctx->hbrc_out.off = 0;
        if (dec->qpd_opts & (LSQPACK_DEC_OPT_HASH_NAME
                            |LSQPACK_DEC_OPT_HASH_NAMEVAL))
        {
            xhdr->name_hash = XXH32(xhdr->buf + xhdr->name_offset,
                                            xhdr->name_len, LSQPACK_XXH_SEED);
            xhdr->flags |= LSXPACK_NAME_HASH;
        }
    }

    return 0;
}


static int
header_out_write_value (struct lsqpack_dec *dec,
            struct header_block_read_ctx *read_ctx, size_t nwritten, int done)
{
    struct lsxpack_header *xhdr;    /* Shorthand */
    int r;

    read_ctx->hbrc_out.off += (unsigned)nwritten;

    if (done)
    {
        xhdr = read_ctx->hbrc_out.xhdr;
        if (dec->qpd_opts & LSQPACK_DEC_OPT_HTTP1X)
        {
            if (xhdr->val_offset + read_ctx->hbrc_out.off + 2 > xhdr->val_len)
            {
                read_ctx->hbrc_out.xhdr = xhdr = dec->qpd_dh_if
                    ->dhi_prepare_decode(read_ctx->hbrc_hblock, xhdr,
                            xhdr->val_offset + read_ctx->hbrc_out.off + 2);
                if (!xhdr)
                    return -1;
            }
            memcpy(xhdr->buf + xhdr->val_offset + read_ctx->hbrc_out.off,
                                                                "\r\n", 2);
        }
        xhdr->val_len = (lsxpack_strlen_t)read_ctx->hbrc_out.off;
        if (dec->qpd_opts & LSQPACK_DEC_OPT_HASH_NAME)
        {
            assert(xhdr->flags & LSXPACK_NAME_HASH);
            xhdr->nameval_hash = XXH32(xhdr->buf + xhdr->val_offset,
                                            xhdr->val_len, xhdr->name_hash);
            xhdr->flags |= LSXPACK_NAMEVAL_HASH;
        }
        r = dec->qpd_dh_if->dhi_process_header(read_ctx->hbrc_hblock, xhdr);
        if (r == 0)
            dec->qpd_bytes_out += xhdr->name_len + xhdr->val_len;
        ++read_ctx->hbrc_header_count;
        memset(&read_ctx->hbrc_out, 0, sizeof(read_ctx->hbrc_out));
        if (r != 0)
            return -1;
    }

    return 0;
}


static int
header_out_grow_buf (struct lsqpack_dec *dec,
                                    struct header_block_read_ctx *read_ctx)
{
    size_t size, need;
    unsigned off;

    assert(read_ctx->hbrc_out.xhdr);
    if (read_ctx->hbrc_out.state == XOUT_NAME)
        off = read_ctx->hbrc_out.off;
    else
        off = read_ctx->hbrc_out.xhdr->val_offset
            - read_ctx->hbrc_out.xhdr->name_offset + read_ctx->hbrc_out.off;

    /* name_off and val_off are not set until the whole string has been
     * written.  Thus, `size' represents the number of bytes that was
     * not enough to write either the name or the value.  We multiply this
     * number by 1.5.
     */
    assert(read_ctx->hbrc_out.xhdr->val_len >= off);
    size = read_ctx->hbrc_out.xhdr->val_len - off;
    if (size < 2)
        size = 2;
    need = read_ctx->hbrc_out.xhdr->val_len + size / 2;
    if (need > LSXPACK_MAX_STRLEN)
        return -1;
    read_ctx->hbrc_out.xhdr = dec->qpd_dh_if->dhi_prepare_decode(
                        read_ctx->hbrc_hblock, read_ctx->hbrc_out.xhdr, need);
    if (!read_ctx->hbrc_out.xhdr)
        return -1;
    if (read_ctx->hbrc_out.xhdr->val_len < need)
    {
        D_INFO("allocated xhdr size (%zd) is smaller than requested (%zd)",
            (size_t) read_ctx->hbrc_out.xhdr->val_len, need);
        memset(&read_ctx->hbrc_out, 0, sizeof(read_ctx->hbrc_out));
        return -1;
    }
    return 0;
}


static int
guarantee_out_bytes (struct lsqpack_dec *dec,
                        struct header_block_read_ctx *read_ctx, size_t extra)
{
    size_t avail, need;
    unsigned off;

    assert(read_ctx->hbrc_out.xhdr);
    assert(read_ctx->hbrc_out.state == XOUT_VALUE);
    assert(read_ctx->hbrc_out.xhdr->val_offset
                                    >= read_ctx->hbrc_out.xhdr->name_offset);
    off = read_ctx->hbrc_out.xhdr->val_offset
            - read_ctx->hbrc_out.xhdr->name_offset + read_ctx->hbrc_out.off;

    assert(read_ctx->hbrc_out.xhdr->val_len >= off);
    avail = read_ctx->hbrc_out.xhdr->val_len - off;
    if (avail < extra)
    {
        need = read_ctx->hbrc_out.xhdr->val_len + extra - avail;
        read_ctx->hbrc_out.xhdr = dec->qpd_dh_if->dhi_prepare_decode(
                        read_ctx->hbrc_hblock, read_ctx->hbrc_out.xhdr, need);
        if (!read_ctx->hbrc_out.xhdr)
            return -1;
    }
    return 0;
}


static unsigned char *
get_dst (struct lsqpack_dec *dec,
                    struct header_block_read_ctx *read_ctx, size_t *dst_size)
{
    unsigned off;

    assert(read_ctx->hbrc_out.xhdr);
    if (read_ctx->hbrc_out.state == XOUT_NAME)
        off = read_ctx->hbrc_out.off;
    else
        off = read_ctx->hbrc_out.xhdr->val_offset
            - read_ctx->hbrc_out.xhdr->name_offset + read_ctx->hbrc_out.off;

    assert(read_ctx->hbrc_out.xhdr->val_len >= off);
    *dst_size = read_ctx->hbrc_out.xhdr->val_len - off;
    return (unsigned char *) read_ctx->hbrc_out.xhdr->buf
                                + read_ctx->hbrc_out.xhdr->name_offset + off;
}


static unsigned char *
qdec_huff_dec4bits (uint8_t, unsigned char *, struct lsqpack_decode_status *);


struct huff_decode_retval
{
    enum
    {
        HUFF_DEC_OK,
        HUFF_DEC_END_SRC,
        HUFF_DEC_END_DST,
        HUFF_DEC_ERROR,
    }                       status;
    unsigned                n_dst;
    unsigned                n_src;
};


#if LS_QPACK_USE_LARGE_TABLES
static struct huff_decode_retval
huff_decode_fast (const unsigned char *src, int src_len,
            unsigned char *dst, int dst_len,
            struct lsqpack_huff_decode_state *state, int final);
#else
#define lsqpack_huff_decode_full lsqpack_huff_decode
#endif

struct huff_decode_retval
lsqpack_huff_decode_full (const unsigned char *src, int src_len,
            unsigned char *dst, int dst_len,
            struct lsqpack_huff_decode_state *state, int final)
{
    const unsigned char *p_src = src;
    const unsigned char *const src_end = src + src_len;
    unsigned char *p_dst = dst;
    unsigned char *dst_end = dst + dst_len;

    if (dst_len == 0)
        return (struct huff_decode_retval) {
            .status = HUFF_DEC_END_DST,
            .n_dst  = 0,
            .n_src  = 0,
        };

    switch (state->resume)
    {
    case 0:
        state->status.state = 0;
        state->status.eos   = 1;
        FALL_THROUGH;
    case 1:
        while (p_src != src_end)
        {
            if (p_dst == dst_end)
            {
                state->resume = 2;
                return (const struct huff_decode_retval) {
                                .status = HUFF_DEC_END_DST,
                                .n_dst  = (unsigned)dst_len,
                                .n_src  = (unsigned)(p_src - src),
                };
            }
        FALL_THROUGH;
    case 2:
            if ((p_dst = qdec_huff_dec4bits(*p_src >> 4, p_dst, &state->status))
                    == NULL)
                return (struct huff_decode_retval) {
                                                .status = HUFF_DEC_ERROR, };
            if (p_dst == dst_end)
            {
                state->resume = 3;
                return (struct huff_decode_retval) {
                                .status = HUFF_DEC_END_DST,
                                .n_dst  = (unsigned)dst_len,
                                .n_src  = (unsigned)(p_src - src),
                };
            }
        FALL_THROUGH;
    case 3:
            if ((p_dst = qdec_huff_dec4bits(*p_src & 0xf, p_dst, &state->status))
                    == NULL)
                return (struct huff_decode_retval) { .status = HUFF_DEC_ERROR, };
            ++p_src;
        }
    }

    if (final)
        return (struct huff_decode_retval) {
                    .status = state->status.eos ? HUFF_DEC_OK : HUFF_DEC_ERROR,
                    .n_dst  = (unsigned)(p_dst - dst),
                    .n_src  = (unsigned)(p_src - src),
        };
    else
    {
        state->resume = 1;
        return (struct huff_decode_retval) {
                    .status = HUFF_DEC_END_SRC,
                    .n_dst  = (unsigned)(p_dst - dst),
                    .n_src  = (unsigned)(p_src - src),
        };
    }
}


#if LS_QPACK_USE_LARGE_TABLES
static struct huff_decode_retval
lsqpack_huff_decode (const unsigned char *src, int src_len,
            unsigned char *dst, int dst_len,
            struct lsqpack_huff_decode_state *state, int final)
{
    if (state->resume == 0 && final)
        return huff_decode_fast(src, src_len, dst, dst_len, state, final);
    else
        return lsqpack_huff_decode_full(src, src_len, dst, dst_len, state,
                                                                    final);
}
#endif


static void
check_dyn_table_errors (struct header_block_read_ctx *read_ctx,
                                                        lsqpack_abs_id_t id)
{
    if (read_ctx->hbrc_flags & HBRC_LARGEST_REF_SET)
        read_ctx->hbrc_flags |=
            (id == read_ctx->hbrc_largest_ref) << LARGEST_USED_SHIFT;
    else
        read_ctx->hbrc_flags |= HBRC_DYN_USED_IN_ERR;
}


static enum lsqpack_read_header_status
parse_header_data (struct lsqpack_dec *dec,
        struct header_block_read_ctx *read_ctx, const unsigned char *buf,
                                                                size_t bufsz)
{
#define DATA read_ctx->hbrc_parse_ctx_u.data
    const unsigned char *const end = buf + bufsz;
    struct lsqpack_dec_table_entry *entry;
    struct huff_decode_retval hdr;
    unsigned value;
    size_t size, dst_size;
    unsigned char *dst;
    unsigned prefix_bits = ~0u;
    int r;

#define RETURN_ERROR() do { dec->qpd_err.line = __LINE__; goto err; } while (0)

    while (buf < end)
    {
        switch (DATA.state)
        {
        case DATA_STATE_NEXT_INSTRUCTION:
            if (buf[0] & 0x80)
            {
                prefix_bits = 6;
                DATA.is_static = buf[0] & 0x40;
                DATA.dec_int_state.resume = 0;
                DATA.state = DATA_STATE_READ_IHF_IDX;
                goto data_state_read_ihf_idx;
            }
            /* Literal Header Field With Name Reference */
            else if (buf[0] & 0x40)
            {
                prefix_bits = 4;
                DATA.is_never = buf[0] & 0x20;
                DATA.is_static = buf[0] & 0x10;
                DATA.dec_int_state.resume = 0;
                DATA.state = DATA_STATE_READ_LFINR_IDX;
                goto data_state_read_lfinr_idx;
            }
            /* Literal Header Field Without Name Reference */
            else if (buf[0] & 0x20)
            {
                prefix_bits = 3;
                DATA.is_never = buf[0] & 0x10;
                DATA.is_huffman = buf[0] & 0x08;
                DATA.dec_int_state.resume = 0;
                DATA.state = DATA_STATE_READ_LFONR_NAME_LEN;
                goto data_state_read_lfonr_name_len;
            }
            /* Indexed Header Field With Post-Base Index */
            else if (buf[0] & 0x10)
            {
                prefix_bits = 4;
                DATA.dec_int_state.resume = 0;
                DATA.state = DATA_STATE_READ_IPBI_IDX;
                goto data_state_read_ipbi_idx;
            }
            /* Literal Header Field With Post-Base Name Reference */
            else
            {
                prefix_bits = 3;
                DATA.is_never = buf[0] & 0x08;
                DATA.dec_int_state.resume = 0;
                DATA.state = DATA_STATE_READ_LFPBNR_IDX;
                goto data_state_read_lfpbnr_idx;
            }
        case DATA_STATE_READ_IHF_IDX:
  data_state_read_ihf_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &value,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                if (DATA.is_static)
                    r = header_out_static_entry(dec, read_ctx, value);
                else
                {
                    value = ID_MINUS(read_ctx->hbrc_base_index, value);
                    r = header_out_dynamic_entry(dec, read_ctx, value);
                    check_dyn_table_errors(read_ctx, value);
                }
                if (r == 0)
                    DATA.state = DATA_STATE_NEXT_INSTRUCTION;
                else
                    RETURN_ERROR();
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
            break;
        case DATA_STATE_READ_LFINR_IDX:
  data_state_read_lfinr_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &value,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                if (DATA.is_static)
                {
                    if (0 != header_out_begin_static_nameref(dec,
                                            read_ctx, value, DATA.is_never))
                        RETURN_ERROR();
                }
                else
                {
                    value = ID_MINUS(read_ctx->hbrc_base_index, value);
                    entry = qdec_get_table_entry_abs(dec, value);
                    if (!entry)
                        RETURN_ERROR();
                    check_dyn_table_errors(read_ctx, value);
                    if (0 != header_out_begin_dynamic_nameref(dec,
                                            read_ctx, entry, DATA.is_never))
                        RETURN_ERROR();
                }
                DATA.state = DATA_STATE_BEGIN_READ_VAL_LEN;
                break;
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
        case DATA_STATE_BEGIN_READ_VAL_LEN:
            DATA.is_huffman = buf[0] & 0x80;
            prefix_bits = 7;
            DATA.dec_int_state.resume = 0;
            DATA.state = DATA_STATE_READ_VAL_LEN;
            FALL_THROUGH;
        case DATA_STATE_READ_VAL_LEN:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &DATA.left,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                if (DATA.left)
                {
                    if (DATA.is_huffman)
                    {
                        if (0 != guarantee_out_bytes(dec, read_ctx,
                                                    DATA.left + DATA.left / 2))
                            RETURN_ERROR();
                        DATA.dec_huff_state.resume = 0;
                        DATA.state = DATA_STATE_READ_VAL_HUFFMAN;
                    }
                    else
                    {
                        if (0 != guarantee_out_bytes(dec, read_ctx, DATA.left))
                            RETURN_ERROR();
                        DATA.state = DATA_STATE_READ_VAL_PLAIN;
                    }
                }
                else if (0 == header_out_write_value(dec, read_ctx, 0, 1))
                    DATA.state = DATA_STATE_NEXT_INSTRUCTION;
                else
                    RETURN_ERROR();
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
            break;
        case DATA_STATE_READ_VAL_HUFFMAN:
            size = MIN((unsigned) (end - buf), DATA.left);
            if (size == 0)
                RETURN_ERROR();
            dst = get_dst(dec, read_ctx, &dst_size);
            hdr = lsqpack_huff_decode(buf, (int)size, dst, (int)dst_size,
                    &DATA.dec_huff_state, DATA.left == size);
            buf += hdr.n_src;
            DATA.left -= hdr.n_src;
            switch (hdr.status)
            {
            case HUFF_DEC_OK:
                if (0 != header_out_write_value(dec, read_ctx, hdr.n_dst,
                                                            DATA.left == 0))
                    RETURN_ERROR();
                if (DATA.left == 0)
                    DATA.state = DATA_STATE_NEXT_INSTRUCTION;
                break;
            case HUFF_DEC_END_SRC:
                if (hdr.n_dst && 0 != header_out_write_value(dec, read_ctx,
                                                                hdr.n_dst, 0))
                    RETURN_ERROR();
                break;
            case HUFF_DEC_END_DST:
                if (hdr.n_dst && 0 != header_out_write_value(dec, read_ctx,
                                                                hdr.n_dst, 0))
                    RETURN_ERROR();
                if (0 != header_out_grow_buf(dec, read_ctx))
                    RETURN_ERROR();
                break;
            case HUFF_DEC_ERROR: default:
                RETURN_ERROR();
            }
            break;
        case DATA_STATE_READ_VAL_PLAIN:
            size = MIN((unsigned) (end - buf), DATA.left);
            if (size == 0)
                RETURN_ERROR();
            dst = get_dst(dec, read_ctx, &dst_size);
            if (size > dst_size)
                RETURN_ERROR();
            memcpy(dst, buf, size);
            if (0 != header_out_write_value(dec, read_ctx,
                                                size, DATA.left == size))
                RETURN_ERROR();
            DATA.left -= (unsigned)size;
            buf += size;
            if (DATA.left == 0)
                DATA.state = DATA_STATE_NEXT_INSTRUCTION;
            break;
        case DATA_STATE_READ_LFONR_NAME_LEN:
  data_state_read_lfonr_name_len:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &DATA.left,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                size = DATA.is_huffman ? DATA.left + DATA.left / 2 : DATA.left;
                if (0 != header_out_begin_literal(dec, read_ctx, size,
                                                            DATA.is_never))
                    RETURN_ERROR();
                if (DATA.is_huffman)
                {
                    DATA.dec_huff_state.resume = 0;
                    DATA.state = DATA_STATE_READ_NAME_HUFFMAN;
                }
                else
                    DATA.state = DATA_STATE_READ_NAME_PLAIN;
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
            break;
        case DATA_STATE_READ_NAME_HUFFMAN:
            size = MIN((unsigned) (end - buf), DATA.left);
            if (size == 0)
                RETURN_ERROR();
            dst = get_dst(dec, read_ctx, &dst_size);
            hdr = lsqpack_huff_decode(buf, (int)size, dst, (int)dst_size,
                    &DATA.dec_huff_state, DATA.left == size);
            buf += hdr.n_src;
            DATA.left -= hdr.n_src;
            switch (hdr.status)
            {
            case HUFF_DEC_OK:
                if (0 != header_out_write_name(dec, read_ctx, hdr.n_dst,
                                                            DATA.left == 0))
                    RETURN_ERROR();
                if (DATA.left == 0)
                    DATA.state = DATA_STATE_BEGIN_READ_VAL_LEN;
                break;
            case HUFF_DEC_END_SRC:
                if (hdr.n_dst && 0 != header_out_write_name(dec, read_ctx,
                                                                hdr.n_dst, 0))
                    RETURN_ERROR();
                break;
            case HUFF_DEC_END_DST:
                if (hdr.n_dst && 0 != header_out_write_name(dec, read_ctx,
                                                                hdr.n_dst, 0))
                    RETURN_ERROR();
                if (0 != header_out_grow_buf(dec, read_ctx))
                    RETURN_ERROR();
                break;
            case HUFF_DEC_ERROR: default:
                RETURN_ERROR();
            }
            break;
        case DATA_STATE_READ_NAME_PLAIN:
            size = MIN((unsigned) (end - buf), DATA.left);
            if (size == 0)
                RETURN_ERROR();
            dst = get_dst(dec, read_ctx, &dst_size);
            if (size > dst_size)
                RETURN_ERROR();
            memcpy(dst, buf, size);
            if (0 != header_out_write_name(dec, read_ctx,
                                                size, DATA.left == size))
                RETURN_ERROR();
            DATA.left -= (unsigned)size;
            buf += size;
            if (DATA.left == 0)
                DATA.state = DATA_STATE_BEGIN_READ_VAL_LEN;
            break;
        case DATA_STATE_READ_LFPBNR_IDX:
  data_state_read_lfpbnr_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &value,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                value = ID_PLUS(value, read_ctx->hbrc_base_index + 1);
                entry = qdec_get_table_entry_abs(dec, value);
                if (!entry)
                    RETURN_ERROR();
                check_dyn_table_errors(read_ctx, value);
                if (0 != header_out_begin_dynamic_nameref(dec,
                                        read_ctx, entry, DATA.is_never))
                    RETURN_ERROR();
                DATA.state = DATA_STATE_BEGIN_READ_VAL_LEN;
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
            break;
        case DATA_STATE_READ_IPBI_IDX:
  data_state_read_ipbi_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &value,
                                                        &DATA.dec_int_state);
            if (r == 0)
            {
                value = ID_PLUS(read_ctx->hbrc_base_index, value + 1);
                r = header_out_dynamic_entry(dec, read_ctx, value);
                check_dyn_table_errors(read_ctx, value);
                if (r == 0)
                    DATA.state = DATA_STATE_NEXT_INSTRUCTION;
                else
                    RETURN_ERROR();
            }
            else if (r == -1)
                return LQRHS_NEED;
            else
                RETURN_ERROR();
            break;
        case DATA_STATE_BEGIN_READ_LFPBNR_VAL_LEN: FALL_THROUGH;
        case DATA_STATE_READ_LFPBNR_VAL_LEN: FALL_THROUGH;
        default:
            assert(0);
            RETURN_ERROR();
        }
    }

    if (read_ctx->hbrc_size > 0)
        return LQRHS_NEED;
    else if (DATA.state == DATA_STATE_NEXT_INSTRUCTION) {
        if ((read_ctx->hbrc_flags
                & (HBRC_LARGEST_REF_SET|HBRC_LARGEST_REF_USED))
                                                    == HBRC_LARGEST_REF_SET)
            RETURN_ERROR();
        if (read_ctx->hbrc_flags & HBRC_DYN_USED_IN_ERR)
            RETURN_ERROR();
        return LQRHS_DONE;
    }
    else
        RETURN_ERROR();

 err:
    dec->qpd_err.type = LSQPACK_DEC_ERR_LOC_HEADER_BLOCK;
    dec->qpd_err.off = read_ctx->hbrc_orig_size - read_ctx->hbrc_size
                            + (buf - (end - bufsz));
    dec->qpd_err.stream_id = read_ctx->hbrc_stream_id;
    D_DEBUG("header block error on line %d, offset %"PRIu64", stream id "
        "%"PRIu64, dec->qpd_err.line, dec->qpd_err.off, dec->qpd_err.stream_id);
    return LQRHS_ERROR;
#undef DATA
}


static int
qdec_in_future (const struct lsqpack_dec *dec, lsqpack_abs_id_t id)
{
    if (dec->qpd_last_id < dec->qpd_max_entries)
        return id > dec->qpd_last_id
            && id <= dec->qpd_last_id + dec->qpd_max_entries;
    else
        return !(id <= dec->qpd_last_id
            && id >= dec->qpd_last_id - dec->qpd_max_entries + 1);
}


/*
 * [draft-ietf-quic-qpack-09], Section 4.5.1.1:
 *
 * The encoder transforms the Required Insert Count as follows before
 * encoding:
 *
 *    if ReqInsertCount == 0:
 *       EncInsertCount = 0
 *    else:
 *       EncInsertCount = (ReqInsertCount mod (2 * MaxEntries)) + 1
 */
static lsqpack_abs_id_t
dec_max_encoded_RIC (const struct lsqpack_dec *dec)
{
    return dec->qpd_max_entries * 2;
}


static enum lsqpack_read_header_status
parse_header_prefix (struct lsqpack_dec *dec,
        struct header_block_read_ctx *read_ctx, const unsigned char *buf,
                                                                size_t bufsz)
{
    const unsigned char *const end = buf + bufsz;
    unsigned prefix_bits = ~0u;
    int r;

#define RIC read_ctx->hbrc_parse_ctx_u.prefix.u.ric
#define DELB read_ctx->hbrc_parse_ctx_u.prefix.u.delb

    while (buf < end)
    {
        switch (read_ctx->hbrc_parse_ctx_u.prefix.state)
        {
        case PREFIX_STATE_BEGIN_READING_LARGEST_REF:
            prefix_bits = 8;
            DELB.dec_int_state.resume = 0;
            read_ctx->hbrc_parse_ctx_u.prefix.state =
                                            PREFIX_STATE_READ_LARGEST_REF;
            FALL_THROUGH;
        case PREFIX_STATE_READ_LARGEST_REF:
            r = lsqpack_dec_int(&buf, end, prefix_bits, &RIC.value,
                                                        &RIC.dec_int_state);
            if (r == 0)
            {
                if (RIC.value)
                {
                    if (RIC.value > dec_max_encoded_RIC(dec))
                        return LQRHS_ERROR;
                    read_ctx->hbrc_largest_ref = ID_MINUS(RIC.value, 2);
                    read_ctx->hbrc_flags |=
                                    HBRC_LARGEST_REF_READ|HBRC_LARGEST_REF_SET;
                    read_ctx->hbrc_parse_ctx_u.prefix.state
                                            = PREFIX_STATE_BEGIN_READING_BASE_IDX;
                    if (qdec_in_future(dec, read_ctx->hbrc_largest_ref))
                        return LQRHS_BLOCKED;
                    else
                        break;
                }
                else
                {
                    read_ctx->hbrc_flags |= HBRC_LARGEST_REF_READ;
                    read_ctx->hbrc_parse_ctx_u.prefix.state
                                            = PREFIX_STATE_BEGIN_READING_BASE_IDX;
                    break;
                }
            }
            else if (r == -1)
            {
                if (read_ctx->hbrc_orig_size - read_ctx->hbrc_size
                                <= lsqpack_val2len(dec_max_encoded_RIC(dec), 8))
                    return LQRHS_NEED;
                else
                    return LQRHS_ERROR;
            }
            else
                return LQRHS_ERROR;
        case PREFIX_STATE_BEGIN_READING_BASE_IDX:
            DELB.sign = (buf[0] & 0x80) > 0;
            DELB.dec_int_state.resume = 0;
            prefix_bits = 7;
            read_ctx->hbrc_parse_ctx_u.prefix.state =
                                            PREFIX_STATE_READ_DELTA_BASE_IDX;
            FALL_THROUGH;
        case PREFIX_STATE_READ_DELTA_BASE_IDX:
            r = lsqpack_dec_int(&buf, end, prefix_bits, &DELB.value,
                                                        &DELB.dec_int_state);
            if (r == 0)
            {
                if (read_ctx->hbrc_flags & HBRC_LARGEST_REF_SET)
                {
                    if (DELB.sign)
                        read_ctx->hbrc_base_index =
                            ID_MINUS(read_ctx->hbrc_largest_ref, DELB.value + 1);
                    else
                        read_ctx->hbrc_base_index =
                                ID_PLUS(read_ctx->hbrc_largest_ref, DELB.value);
                }
                else    /* From qpack-03: "A header block that does not
                         * reference the dynamic table can use any value
                         * for Base Index"
                         */
                    read_ctx->hbrc_base_index = 0;
                read_ctx->hbrc_parse = parse_header_data;
                read_ctx->hbrc_parse_ctx_u.data.state
                                                = DATA_STATE_NEXT_INSTRUCTION;
                if (end - buf)
                    return parse_header_data(dec, read_ctx, buf, end - buf);
                else
                    return LQRHS_NEED;
            }
            else if (r == -1)
            {
                return LQRHS_NEED;
            }
            else
                return LQRHS_ERROR;
        default:
            assert(0);
            return LQRHS_ERROR;
        }
    }

#undef RIC
#undef DELB

    if (read_ctx->hbrc_size > 0)
        return LQRHS_NEED;
    else
        return LQRHS_ERROR;
}


static size_t
max_to_read (const struct header_block_read_ctx *read_ctx)
{
    if (read_ctx->hbrc_flags & HBRC_LARGEST_REF_READ)
        return read_ctx->hbrc_size;
    else
        return 1;
}


static size_t
qdec_read_header_block (struct hbrc_buf *hbrc_buf,
                                    const unsigned char **buf, size_t sz)
{
    if (sz > hbrc_buf->sz - hbrc_buf->off)
        sz = hbrc_buf->sz - hbrc_buf->off;
    *buf = hbrc_buf->buf + hbrc_buf->off;
    hbrc_buf->off += sz;
    return sz;
}


static enum lsqpack_read_header_status
qdec_read_header (struct lsqpack_dec *dec,
                                    struct header_block_read_ctx *read_ctx)
{
    const unsigned char *buf;
    enum lsqpack_read_header_status st;
    size_t n_to_read;
    size_t buf_sz;

    while (read_ctx->hbrc_size > 0)
    {
        n_to_read = max_to_read(read_ctx);
        buf_sz = qdec_read_header_block(&read_ctx->hbrc_buf, &buf, n_to_read);
        if (buf_sz > 0)
        {
            read_ctx->hbrc_size -= buf_sz;
            st = read_ctx->hbrc_parse(dec, read_ctx, buf, buf_sz);
            if (st == LQRHS_NEED)
            {
                if (read_ctx->hbrc_size == 0)
                    return LQRHS_ERROR;
            }
            else
                return st;
        }
        else
            return LQRHS_NEED;
    }

    return LQRHS_DONE;
}


static void
destroy_header_block_read_ctx (struct lsqpack_dec *dec,
                        struct header_block_read_ctx *read_ctx)
{
    lsqpack_abs_id_t id;

    TAILQ_REMOVE(&dec->qpd_hbrcs, read_ctx, hbrc_next_all);
    if (read_ctx->hbrc_flags & HBRC_BLOCKED)
    {
        id = read_ctx->hbrc_largest_ref & ((1 << LSQPACK_DEC_BLOCKED_BITS) - 1);
        TAILQ_REMOVE(&dec->qpd_blocked_headers[id], read_ctx, hbrc_next_blocked);
        --dec->qpd_n_blocked;
    }
    free(read_ctx);
}


static void
qdec_insert_header_block (struct lsqpack_dec *dec,
                        struct header_block_read_ctx *read_ctx)
{
    TAILQ_INSERT_TAIL(&dec->qpd_hbrcs, read_ctx, hbrc_next_all);
    read_ctx->hbrc_flags |= HBRC_ON_LIST;
}


static int
stash_blocked_header (struct lsqpack_dec *dec,
                        struct header_block_read_ctx *read_ctx)
{
    lsqpack_abs_id_t id;

    if (dec->qpd_n_blocked < dec->qpd_max_risked_streams)
    {
        id = read_ctx->hbrc_largest_ref & ((1 << LSQPACK_DEC_BLOCKED_BITS) - 1);
        TAILQ_INSERT_TAIL(&dec->qpd_blocked_headers[id], read_ctx, hbrc_next_blocked);
        ++dec->qpd_n_blocked;
        read_ctx->hbrc_flags |= HBRC_BLOCKED;
        return 0;
    }
    else
    {
        D_INFO("cannot block another header: reached maximum of %u",
                                                dec->qpd_max_risked_streams);
        return -1;
    }
}


static struct header_block_read_ctx *
find_header_block_read_ctx (struct lsqpack_dec *dec, void *hblock)
{
    struct header_block_read_ctx *read_ctx;

    TAILQ_FOREACH(read_ctx, &dec->qpd_hbrcs, hbrc_next_all)
        if (read_ctx->hbrc_hblock == hblock)
            return read_ctx;

    return NULL;
}


static int
qdec_try_writing_header_ack (struct lsqpack_dec *dec, uint64_t stream_id,
                       unsigned char *dec_buf, size_t *dec_buf_sz)
{
    unsigned char *p = dec_buf;

    if (*dec_buf_sz > 0)
    {
        *dec_buf = 0x80;
        p = lsqpack_enc_int(p, p + *dec_buf_sz, stream_id, 7);
        if (p > dec_buf)
        {
            *dec_buf_sz = p - dec_buf;
            dec->qpd_bytes_in += (unsigned)(p - dec_buf);
            return 0;
        }
    }

    return -1;
}


static void
qdec_maybe_update_largest_known (struct lsqpack_dec *dec, lsqpack_abs_id_t id)
{
    lsqpack_abs_id_t diff;

    diff = ID_MINUS(id, dec->qpd_largest_known_id);
    if (diff > 0 && diff <= dec->qpd_max_entries)
        dec->qpd_largest_known_id = id;
}


static enum lsqpack_read_header_status
qdec_header_process (struct lsqpack_dec *dec,
            struct header_block_read_ctx *read_ctx,
            const unsigned char **buf, size_t bufsz,
            unsigned char *dec_buf, size_t *dec_buf_sz)
{
    struct header_block_read_ctx *read_ctx_copy;
    enum lsqpack_read_header_status st;

    read_ctx->hbrc_buf = (struct hbrc_buf) { *buf, bufsz, 0, };
    st = qdec_read_header(dec, read_ctx);
    switch (st)
    {
    case LQRHS_DONE:
        update_ema(&dec->qpd_hlist_size_ema, read_ctx->hbrc_header_count);
        if ((read_ctx->hbrc_flags & HBRC_LARGEST_REF_SET)
                                                    && dec_buf && dec_buf_sz)
        {
            if (0 == qdec_try_writing_header_ack(dec, read_ctx->hbrc_stream_id,
                                                        dec_buf, dec_buf_sz))
                qdec_maybe_update_largest_known(dec,
                                                read_ctx->hbrc_largest_ref);
            else
            {
                st = LQRHS_ERROR;
                break;
            }
        }
        else if (dec_buf_sz)
            *dec_buf_sz = 0;
        *buf = *buf + read_ctx->hbrc_buf.off;
        dec->qpd_bytes_in += (unsigned)read_ctx->hbrc_orig_size;
        if (dec->qpd_bytes_out > (1u << (sizeof(dec->qpd_bytes_out) * 8 - 1)))
        {
            dec->qpd_bytes_in = (unsigned)((float) dec->qpd_bytes_in
                                        / (float) dec->qpd_bytes_out * 1000);
            dec->qpd_bytes_out = 1000;
            D_DEBUG("reset bytes in/out counters, ratio: %.3f",
                                                        lsqpack_dec_ratio(dec));
        }
        D_DEBUG("header block for stream %"PRIu64" is done",
                                                    read_ctx->hbrc_stream_id);
        break;
    case LQRHS_NEED:
    case LQRHS_BLOCKED:
        if (!(read_ctx->hbrc_flags & HBRC_ON_LIST))
        {
            read_ctx_copy = malloc(sizeof(*read_ctx_copy));
            if (!read_ctx_copy)
            {
                st = LQRHS_ERROR;
                break;
            }
            memcpy(read_ctx_copy, read_ctx, sizeof(*read_ctx));
            read_ctx = read_ctx_copy;
            qdec_insert_header_block(dec, read_ctx);
        }
        if (st == LQRHS_BLOCKED && 0 != stash_blocked_header(dec, read_ctx))
        {
            st = LQRHS_ERROR;
            break;
        }
        *buf = *buf + read_ctx->hbrc_buf.off;
        if (st == LQRHS_NEED)
            D_DEBUG("header block for stream %"PRIu64" needs more bytes",
                                                    read_ctx->hbrc_stream_id);
        else
            D_DEBUG("header block for stream %"PRIu64" is blocked",
                                                    read_ctx->hbrc_stream_id);
        return st;
    case LQRHS_ERROR: default:
        assert(st == LQRHS_ERROR);
        D_DEBUG("header block for stream %"PRIu64" has had an error",
                                                    read_ctx->hbrc_stream_id);
        break;
    }

    if (read_ctx->hbrc_flags & HBRC_ON_LIST)
    {
        destroy_header_block_read_ctx(dec, read_ctx);
    }

    return st;
}


enum lsqpack_read_header_status
lsqpack_dec_header_read (struct lsqpack_dec *dec, void *hblock,
    const unsigned char **buf, size_t bufsz,
    unsigned char *dec_buf, size_t *dec_buf_sz)
{
    struct header_block_read_ctx *read_ctx;

    read_ctx = find_header_block_read_ctx(dec, hblock);
    if (read_ctx)
    {
        D_DEBUG("continue reading header block for stream %"PRIu64,
                                                    read_ctx->hbrc_stream_id);
        return qdec_header_process(dec, read_ctx, buf, bufsz,
                                   dec_buf, dec_buf_sz);
    }
    else
    {
        D_INFO("could not find header block to continue reading");
        return LQRHS_ERROR;
    }
}


enum lsqpack_read_header_status
lsqpack_dec_header_in (struct lsqpack_dec *dec, void *hblock,
            uint64_t stream_id, size_t header_size, const unsigned char **buf,
            size_t bufsz, unsigned char *dec_buf, size_t *dec_buf_sz)
{
    if (header_size < 2)
    {
        D_DEBUG("header block for stream %"PRIu64" is too short "
            "(%zd byte%.*s)", stream_id, header_size, header_size != 1, "s");
        dec->qpd_err = (struct lsqpack_dec_err) {
            .line = __LINE__,
            .type = LSQPACK_DEC_ERR_LOC_HEADER_BLOCK,
            .off = 0,
            .stream_id = stream_id,
        };
        return LQRHS_ERROR;
    }

    struct header_block_read_ctx read_ctx = {
        .hbrc_stream_id = stream_id,
        .hbrc_hblock    = hblock,
        .hbrc_size      = header_size,
        .hbrc_orig_size = header_size,
        .hbrc_parse     = parse_header_prefix,
    };

    D_DEBUG("begin reading header block for stream %"PRIu64, stream_id);
    return qdec_header_process(dec, &read_ctx, buf, bufsz,
                               dec_buf, dec_buf_sz);
}


static void
qdec_drop_oldest_entry (struct lsqpack_dec *dec)
{
    struct lsqpack_dec_table_entry *entry;

    entry = ringbuf_advance_tail(&dec->qpd_dyn_table);
    dec->qpd_cur_capacity -= DTE_SIZE(entry);
    qdec_decref_entry(entry);
}


static void
qdec_remove_overflow_entries (struct lsqpack_dec *dec)
{
    while (dec->qpd_cur_capacity > dec->qpd_cur_max_capacity)
    {
        D_DEBUG("capacity %u, drop entry", dec->qpd_cur_capacity);
        qdec_drop_oldest_entry(dec);
    }
}


static void
qdec_update_max_capacity (struct lsqpack_dec *dec, unsigned new_capacity)
{
    unsigned old_max_entries;
    dec->qpd_cur_max_capacity = new_capacity;
    old_max_entries = dec->qpd_max_entries;
    dec->qpd_max_entries = dec->qpd_cur_max_capacity / DYNAMIC_ENTRY_OVERHEAD;
    if (old_max_entries != dec->qpd_max_entries)
    {
        if (dec->qpd_last_id == dec->qpd_largest_known_id
            && dec->qpd_last_id == old_max_entries * 2 - 1)
        {
            dec->qpd_last_id = dec->qpd_max_entries * 2 - 1;
            dec->qpd_largest_known_id = dec->qpd_max_entries * 2 - 1;
        }
    }
    qdec_remove_overflow_entries(dec);
}


static void
qdec_process_blocked_headers (struct lsqpack_dec *dec)
{
    struct header_block_read_ctx *read_ctx, *next;
    lsqpack_abs_id_t id;

    id = dec->qpd_last_id & ((1 << LSQPACK_DEC_BLOCKED_BITS) - 1);
    for (read_ctx = TAILQ_FIRST(&dec->qpd_blocked_headers[id]); read_ctx;
                                                            read_ctx = next)
    {
        next = TAILQ_NEXT(read_ctx, hbrc_next_blocked);
        if (read_ctx->hbrc_largest_ref == dec->qpd_last_id)
        {
            read_ctx->hbrc_flags &= ~HBRC_BLOCKED;
            TAILQ_REMOVE(&dec->qpd_blocked_headers[id], read_ctx,
                                                            hbrc_next_blocked);
            --dec->qpd_n_blocked;
            D_DEBUG("header block for stream %"PRIu64" has become unblocked",
                read_ctx->hbrc_stream_id);
            dec->qpd_dh_if->dhi_unblocked(read_ctx->hbrc_hblock);
        }
    }
}


int
lsqpack_dec_ici_pending (const struct lsqpack_dec *dec)
{
    return dec->qpd_last_id != dec->qpd_largest_known_id;
}


ssize_t
lsqpack_dec_write_ici (struct lsqpack_dec *dec, unsigned char *buf, size_t sz)
{
    unsigned char *p;
    unsigned count;

    if (dec->qpd_last_id != dec->qpd_largest_known_id)
    {
        if (sz == 0)
            return -1;
        count = ID_MINUS(dec->qpd_last_id, dec->qpd_largest_known_id);
        *buf = 0;
        p = lsqpack_enc_int(buf, buf + sz, count, 6);
        if (p > buf)
        {
            D_DEBUG("wrote ICI: count=%u", count);
            dec->qpd_largest_known_id = dec->qpd_last_id;
            dec->qpd_bytes_in += (unsigned)(p - buf);
            return p - buf;
        }
        else
            return -1;
    }
    else
    {
        D_DEBUG("no ICI instruction necessary: emitting zero bytes");
        return 0;
    }
}


int
lsqpack_dec_unref_stream (struct lsqpack_dec *dec, void *hblock)
{
    struct header_block_read_ctx *read_ctx;

    read_ctx = find_header_block_read_ctx(dec, hblock);
    if (read_ctx)
    {
        D_DEBUG("unreffed header block for stream %"PRIu64,
                                                    read_ctx->hbrc_stream_id);
        destroy_header_block_read_ctx(dec, read_ctx);
        return 0;
    }
    else
    {
        D_INFO("could not find header block to unref");
        return -1;
    }
}


ssize_t
lsqpack_dec_cancel_stream (struct lsqpack_dec *dec, void *hblock,
                                        unsigned char *buf, size_t buf_sz)
{
    struct header_block_read_ctx *read_ctx;
    unsigned char *p;

    read_ctx = find_header_block_read_ctx(dec, hblock);
    if (!read_ctx)
    {
        D_INFO("could not find stream to cancel");
        return 0;
    }

    if (buf_sz == 0)
        return -1;

    *buf = 0x40;
    p = lsqpack_enc_int(buf, buf + buf_sz, read_ctx->hbrc_stream_id, 6);
    if (p > buf)
    {
        D_DEBUG("cancelled stream %"PRIu64"; generate instruction of %u bytes",
            read_ctx->hbrc_stream_id, (unsigned) (p - buf));
        destroy_header_block_read_ctx(dec, read_ctx);
        dec->qpd_bytes_in += (unsigned)(p - buf);
        return p - buf;
    }
    else
    {
        D_WARN("cannot generate Cancel Stream instruction for stream %"PRIu64
            "; buf size=%zu", read_ctx->hbrc_stream_id, buf_sz);
        return -1;
    }
}


ssize_t
lsqpack_dec_cancel_stream_id (struct lsqpack_dec *dec, uint64_t stream_id,
                                        unsigned char *buf, size_t buf_sz)
{
    unsigned char *p;

    /* From qpack-14: "A decoder with a maximum dynamic table capacity
     * equal to zero MAY omit sending Stream Cancellations..."
     */
    if (dec->qpd_max_capacity == 0)
        return 0;

    if (buf_sz == 0)
        return -1;

    *buf = 0x40;
    p = lsqpack_enc_int(buf, buf + buf_sz, stream_id, 6);
    if (p > buf)
    {
        D_DEBUG("generate Cancel Stream %"PRIu64" instruction of %u bytes",
            stream_id, (unsigned) (p - buf));
        dec->qpd_bytes_in += (unsigned)(p - buf);
        return p - buf;
    }
    else
    {
        D_DEBUG("cannot generate Cancel Stream instruction for stream %"PRIu64
            "; buf size=%zu", stream_id, buf_sz);
        return -1;
    }
}


static int
lsqpack_dec_push_entry (struct lsqpack_dec *dec,
                                        struct lsqpack_dec_table_entry *entry)
{
    if (0 == ringbuf_add(&dec->qpd_dyn_table, entry))
    {
        dec->qpd_cur_capacity += DTE_SIZE(entry);
        D_DEBUG("push entry:(`%.*s': `%.*s'), capacity %u",
                                (int) entry->dte_name_len, DTE_NAME(entry),
                                (int) entry->dte_val_len, DTE_VALUE(entry),
                                dec->qpd_cur_capacity);
        dec->qpd_last_id = ID_PLUS(dec->qpd_last_id, 1);
        qdec_remove_overflow_entries(dec);
        qdec_process_blocked_headers(dec);
        if (dec->qpd_cur_capacity <= dec->qpd_cur_max_capacity)
            return 0;
    }

    return -1;
}


int
lsqpack_dec_enc_in (struct lsqpack_dec *dec, const unsigned char *buf,
                                                                size_t buf_sz)
{
    const unsigned char *const end = buf + buf_sz;
    struct lsqpack_dec_table_entry *entry, *new_entry;
    struct huff_decode_retval hdr;
    unsigned prefix_bits = ~0u;
    size_t size;
    int r;

    D_DEBUG("got %zu bytes of encoder stream", buf_sz);
    dec->qpd_bytes_in += (unsigned)buf_sz;

#define WINR dec->qpd_enc_state.ctx_u.with_namref
#define WONR dec->qpd_enc_state.ctx_u.wo_namref
#define DUPL dec->qpd_enc_state.ctx_u.duplicate
#define SDTC dec->qpd_enc_state.ctx_u.sdtc

    while (buf < end)
    {
        switch (dec->qpd_enc_state.resume)
        {
        case DEI_NEXT_INST:
            if (buf[0] & 0x80)
            {
                WINR.is_static = (buf[0] & 0x40) > 0;
                WINR.dec_int_state.resume = 0;
                WINR.reffed_entry = NULL;
                WINR.entry = NULL;
                dec->qpd_enc_state.resume = DEI_WINR_READ_NAME_IDX;
                prefix_bits = 6;
                goto dei_winr_read_name_idx;
            }
            else if (buf[0] & 0x40)
            {
                WONR.is_huffman = (buf[0] & 0x20) > 0;
                WONR.dec_int_state.resume = 0;
                WONR.entry = NULL;
                dec->qpd_enc_state.resume = DEI_WONR_READ_NAME_LEN;
                prefix_bits = 5;
                goto dei_wonr_read_name_idx;
            }
            else if (buf[0] & 0x20)
            {
                SDTC.dec_int_state.resume = 0;
                dec->qpd_enc_state.resume = DEI_SIZE_UPD_READ_IDX;
                prefix_bits = 5;
                goto dei_size_upd_read_idx;
            }
            else
            {
                DUPL.dec_int_state.resume = 0;
                dec->qpd_enc_state.resume = DEI_DUP_READ_IDX;
                prefix_bits = 5;
                goto dei_dup_read_idx;
            }
        case DEI_WINR_READ_NAME_IDX:
  dei_winr_read_name_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits,
                                    &WINR.name_idx, &WINR.dec_int_state);
            if (r == 0)
            {
                if (WINR.is_static)
                {
                    if (WINR.name_idx < QPACK_STATIC_TABLE_SIZE)
                        WINR.reffed_entry = NULL;
                    else
                        return -1;
                }
                else
                {
                    WINR.reffed_entry = qdec_get_table_entry_rel(dec,
                                                                WINR.name_idx);
                    if (!WINR.reffed_entry)
                        return -1;
                    ++WINR.reffed_entry->dte_refcnt;
                }
                dec->qpd_enc_state.resume = DEI_WINR_BEGIN_READ_VAL_LEN;
                break;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
        case DEI_WINR_BEGIN_READ_VAL_LEN:
            WINR.is_huffman = (buf[0] & 0x80) > 0;
            WINR.dec_int_state.resume = 0;
            dec->qpd_enc_state.resume = DEI_WINR_READ_VAL_LEN;
            prefix_bits = 7;
            FALL_THROUGH;
        case DEI_WINR_READ_VAL_LEN:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &WINR.val_len,
                                                        &WINR.dec_int_state);
            if (r == 0)
            {
                if (WINR.is_static)
                {
                    WINR.name_len = static_table[WINR.name_idx].name_len;
                    WINR.name = static_table[WINR.name_idx].name;
                }
                else
                {
                    WINR.name_len = WINR.reffed_entry->dte_name_len;
                    WINR.name = DTE_NAME(WINR.reffed_entry);
                }
                /* This check accounts for the fact that Huffman-encoded string
                 * can shrink.
                 */
                if (WINR.val_len > ((dec->qpd_cur_max_capacity
                                    - WINR.name_len) << (WINR.is_huffman << 1)))
                    return -1;
                if (WINR.is_huffman)
                    WINR.alloced_val_len = WINR.val_len + WINR.val_len / 2;
                else
                    WINR.alloced_val_len = WINR.val_len;
                WINR.entry = malloc(sizeof(*WINR.entry) + WINR.name_len
                                                    + WINR.alloced_val_len);
                if (!WINR.entry)
                    return -1;
                if (WINR.is_static)
                {
                    WINR.entry->dte_flags = DTEF_NAME_HASH | DTEF_NAME_IDX;
                    WINR.entry->dte_name_hash = name_hashes[WINR.name_idx];
                    WINR.entry->dte_name_idx = WINR.name_idx;
                }
                else
                {
                    WINR.entry->dte_flags = WINR.reffed_entry->dte_flags
                                            & (DTEF_NAME_HASH|DTEF_NAME_IDX);
                    WINR.entry->dte_name_hash
                                        = WINR.reffed_entry->dte_name_hash;
                    WINR.entry->dte_name_idx = WINR.reffed_entry->dte_name_idx;
                }
                WINR.entry->dte_name_len = WINR.name_len;
                WINR.nread = 0;
                WINR.val_off = 0;
                if (WINR.val_len)
                {
                    if (WINR.is_huffman)
                    {
                        dec->qpd_enc_state.resume = DEI_WINR_READ_VALUE_HUFFMAN;
                        WINR.dec_huff_state.resume = 0;
                    }
                    else
                        dec->qpd_enc_state.resume = DEI_WINR_READ_VALUE_PLAIN;
                }
                else
                    goto winr_insert_entry;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
            break;
        case DEI_WINR_READ_VALUE_HUFFMAN:
            size = MIN((unsigned) (end - buf), WINR.val_len - WINR.nread);
            hdr = lsqpack_huff_decode(buf, (int)size,
                    (unsigned char *) DTE_VALUE(WINR.entry) + WINR.val_off,
                    WINR.alloced_val_len - WINR.val_off,
                    &WINR.dec_huff_state, WINR.nread + size == WINR.val_len);
            switch (hdr.status)
            {
            case HUFF_DEC_OK:
                buf += hdr.n_src;
                WINR.entry->dte_val_len = WINR.val_off + hdr.n_dst;
                WINR.entry->dte_refcnt = 1;
                memcpy(DTE_NAME(WINR.entry), WINR.name, WINR.name_len);
                if (WINR.reffed_entry)
                {
                    qdec_decref_entry(WINR.reffed_entry);
                    WINR.reffed_entry = NULL;
                }
                r = lsqpack_dec_push_entry(dec, WINR.entry);
                if (0 == r)
                {
                    dec->qpd_enc_state.resume = 0;
                    WINR.entry = NULL;
                    break;
                }
                qdec_decref_entry(WINR.entry);
                WINR.entry = NULL;
                return -1;
            case HUFF_DEC_END_SRC:
                buf += hdr.n_src;
                WINR.nread += hdr.n_src;
                WINR.val_off += hdr.n_dst;
                break;
            case HUFF_DEC_END_DST:
                WINR.alloced_val_len *= 2;
                entry = realloc(WINR.entry, sizeof(*WINR.entry)
                                        + WINR.name_len + WINR.alloced_val_len);
                if (!entry)
                    return -1;
                WINR.entry = entry;
                buf += hdr.n_src;
                WINR.nread += hdr.n_src;
                WINR.val_off += hdr.n_dst;
                break;
            case HUFF_DEC_ERROR: default:
                return -1;
            }
            break;
        case DEI_WINR_READ_VALUE_PLAIN:
            assert(WINR.alloced_val_len >= WINR.val_len);
            size = MIN((unsigned) (end - buf), WINR.val_len - WINR.val_off);
            memcpy(DTE_VALUE(WINR.entry) + WINR.val_off, buf, size);
            WINR.val_off += (unsigned)size;
            buf += size;
            if (WINR.val_off == WINR.val_len)
            {
  winr_insert_entry:
                WINR.entry->dte_val_len = WINR.val_off;
                WINR.entry->dte_refcnt = 1;
                memcpy(DTE_NAME(WINR.entry), WINR.name, WINR.name_len);
                if (WINR.reffed_entry)
                {
                    qdec_decref_entry(WINR.reffed_entry);
                    WINR.reffed_entry = NULL;
                }
                r = lsqpack_dec_push_entry(dec, WINR.entry);
                if (0 == r)
                {
                    dec->qpd_enc_state.resume = 0;
                    WINR.entry = NULL;
                    break;
                }
                qdec_decref_entry(WINR.entry);
                WINR.entry = NULL;
                return -1;
            }
            break;
        case DEI_WONR_READ_NAME_LEN:
  dei_wonr_read_name_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &WONR.str_len,
                                                        &DUPL.dec_int_state);
            if (r == 0)
            {
                /* This check accounts for the fact that Huffman-encoded string
                 * can shrink.
                 */
                if (WONR.str_len > (dec->qpd_cur_max_capacity
                                                    << (WONR.is_huffman << 1)))
                    return -1;
                WONR.alloced_len = WONR.str_len ? WONR.str_len + WONR.str_len / 2 : 16;
                size = sizeof(*new_entry) + WONR.alloced_len;
                WONR.entry = malloc(size);
                if (!WONR.entry)
                    return -1;
                WONR.entry->dte_flags = 0;
                WONR.nread = 0;
                WONR.str_off = 0;
                if (WONR.is_huffman)
                {
                    dec->qpd_enc_state.resume = DEI_WONR_READ_NAME_HUFFMAN;
                    WONR.dec_huff_state.resume = 0;
                }
                else
                    dec->qpd_enc_state.resume = DEI_WONR_READ_NAME_PLAIN;
                break;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
        case DEI_WONR_READ_NAME_HUFFMAN:
            size = MIN((unsigned) (end - buf), WONR.str_len - WONR.nread);
            hdr = lsqpack_huff_decode(buf, (int)size,
                    (unsigned char *) DTE_NAME(WONR.entry) + WONR.str_off,
                    (int)(WONR.alloced_len - WONR.str_off),
                    &WONR.dec_huff_state, WONR.nread + size == WONR.str_len);
            switch (hdr.status)
            {
            case HUFF_DEC_OK:
                buf += hdr.n_src;
                WONR.entry->dte_name_len = WONR.str_off + hdr.n_dst;
                dec->qpd_enc_state.resume = DEI_WONR_BEGIN_READ_VAL_LEN;
                break;
            case HUFF_DEC_END_SRC:
                buf += hdr.n_src;
                WONR.nread += hdr.n_src;
                WONR.str_off += hdr.n_dst;
                break;
            case HUFF_DEC_END_DST:
                WONR.alloced_len *= 2;
                entry = realloc(WONR.entry, sizeof(*WONR.entry)
                                                        + WONR.alloced_len);
                if (!entry)
                    return -1;
                WONR.entry = entry;
                buf += hdr.n_src;
                WONR.nread += hdr.n_src;
                WONR.str_off += hdr.n_dst;
                break;
            case HUFF_DEC_ERROR: default:
                return -1;
            }
            break;
        case DEI_WONR_READ_NAME_PLAIN:
            assert(WONR.alloced_len >= WONR.str_len);
            size = MIN((unsigned) (end - buf), WONR.str_len - WONR.str_off);
            memcpy(DTE_NAME(WONR.entry) + WONR.str_off, buf, size);
            WONR.str_off += (unsigned)size;
            buf += size;
            if (WONR.str_off == WONR.str_len)
            {
                WONR.entry->dte_name_len = WONR.str_off;
                dec->qpd_enc_state.resume = DEI_WONR_BEGIN_READ_VAL_LEN;
            }
            break;
        case DEI_WONR_BEGIN_READ_VAL_LEN:
            WONR.is_huffman = (buf[0] & 0x80) > 0;
            WONR.dec_int_state.resume = 0;
            dec->qpd_enc_state.resume = DEI_WONR_READ_VAL_LEN;
            prefix_bits = 7;
            FALL_THROUGH;
        case DEI_WONR_READ_VAL_LEN:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &WONR.str_len,
                                                        &WONR.dec_int_state);
            if (r == 0)
            {
                /* This check accounts for the fact that Huffman-encoded string
                 * can shrink.
                 */
                if (WONR.str_len > ((dec->qpd_cur_max_capacity
                        - WONR.entry->dte_name_len) << (WONR.is_huffman << 1)))
                    return -1;
                WONR.nread = 0;
                WONR.str_off = 0;
                if (WONR.str_len)
                {
                    if (WONR.is_huffman)
                    {
                        dec->qpd_enc_state.resume = DEI_WONR_READ_VALUE_HUFFMAN;
                        WONR.dec_huff_state.resume = 0;
                    }
                    else
                        dec->qpd_enc_state.resume = DEI_WONR_READ_VALUE_PLAIN;
                }
                else
                    goto wonr_insert_entry;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
            break;
        case DEI_WONR_READ_VALUE_HUFFMAN:
            size = MIN((unsigned) (end - buf), WONR.str_len - WONR.nread);
            hdr = lsqpack_huff_decode(buf, (int)size,
                    (unsigned char *) DTE_VALUE(WONR.entry) + WONR.str_off,
                    WONR.alloced_len - WONR.entry->dte_name_len - WONR.str_off,
                    &WONR.dec_huff_state, WONR.nread + size == WONR.str_len);
            switch (hdr.status)
            {
            case HUFF_DEC_OK:
                buf += hdr.n_src;
                WONR.entry->dte_val_len = WONR.str_off + hdr.n_dst;
                WONR.entry->dte_refcnt = 1;
                r = lsqpack_dec_push_entry(dec, WONR.entry);
                if (0 == r)
                {
                    dec->qpd_enc_state.resume = 0;
                    WONR.entry = NULL;
                    break;
                }
                qdec_decref_entry(WONR.entry);
                WONR.entry = NULL;
                return -1;
            case HUFF_DEC_END_SRC:
                buf += hdr.n_src;
                WONR.nread += hdr.n_src;
                WONR.str_off += hdr.n_dst;
                break;
            case HUFF_DEC_END_DST:
                assert(WONR.alloced_len);
                WONR.alloced_len *= 2;
                entry = realloc(WONR.entry, sizeof(*WONR.entry)
                                                        + WONR.alloced_len);
                if (!entry)
                    return -1;
                WONR.entry = entry;
                buf += hdr.n_src;
                WONR.nread += hdr.n_src;
                WONR.str_off += hdr.n_dst;
                break;
            case HUFF_DEC_ERROR: default:
                return -1;
            }
            break;
        case DEI_WONR_READ_VALUE_PLAIN:
            if (WONR.alloced_len < WONR.entry->dte_name_len + WONR.str_len)
            {
                WONR.alloced_len = WONR.entry->dte_name_len + WONR.str_len;
                entry = realloc(WONR.entry, sizeof(*WONR.entry)
                                                        + WONR.alloced_len);
                if (entry)
                    WONR.entry = entry;
                else
                    return -1;
            }
            size = MIN((unsigned) (end - buf), WONR.str_len - WONR.str_off);
            memcpy(DTE_VALUE(WONR.entry) + WONR.str_off, buf, size);
            WONR.str_off += (unsigned)size;
            buf += size;
            if (WONR.str_off == WONR.str_len)
            {
  wonr_insert_entry:
                WONR.entry->dte_val_len = WONR.str_off;
                WONR.entry->dte_refcnt = 1;
                r = lsqpack_dec_push_entry(dec, WONR.entry);
                if (0 == r)
                {
                    dec->qpd_enc_state.resume = 0;
                    WONR.entry = NULL;
                    break;
                }
                qdec_decref_entry(WONR.entry);
                WONR.entry = NULL;
                return -1;
            }
            break;
        case DEI_DUP_READ_IDX:
  dei_dup_read_idx:
            r = lsqpack_dec_int24(&buf, end, prefix_bits, &DUPL.index,
                                                        &DUPL.dec_int_state);
            if (r == 0)
            {
                entry = qdec_get_table_entry_rel(dec, DUPL.index);
                if (!entry)
                    return -1;
                size = sizeof(*new_entry) + entry->dte_name_len
                                                        + entry->dte_val_len;
                new_entry = malloc(size);
                if (!new_entry)
                    return -1;
                memcpy(new_entry, entry, size);
                new_entry->dte_refcnt = 1;
                if (0 == lsqpack_dec_push_entry(dec, new_entry))
                {
                    dec->qpd_enc_state.resume = 0;
                    break;
                }
                qdec_decref_entry(new_entry);
                return -1;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
        case DEI_SIZE_UPD_READ_IDX:
  dei_size_upd_read_idx:
            r = lsqpack_dec_int(&buf, end, prefix_bits, &SDTC.new_size,
                                                        &SDTC.dec_int_state);
            if (r == 0)
            {
                if (SDTC.new_size <= dec->qpd_max_capacity)
                {
                    dec->qpd_enc_state.resume = 0;
                    D_DEBUG("got TSU=%"PRIu64, SDTC.new_size);
                    qdec_update_max_capacity(dec, (unsigned int) SDTC.new_size);
                    break;
                }
                else
                    return -1;
            }
            else if (r == -1)
                return 0;
            else
                return -1;
        default:
            assert(0);
        }
    }

#undef WINR
#undef WONR
#undef DUPL
#undef SDTC

    return 0;
}


void
lsqpack_dec_print_table (const struct lsqpack_dec *dec, FILE *out)
{
    const struct lsqpack_dec_table_entry *entry;
    struct ringbuf_iter riter;
    lsqpack_abs_id_t id;

    fprintf(out, "Printing decoder table state.\n");
    fprintf(out, "Max capacity: %u; current capacity: %u\n",
        dec->qpd_cur_max_capacity, dec->qpd_cur_capacity);
    id = ID_MINUS(dec->qpd_last_id + 1, ringbuf_count(&dec->qpd_dyn_table));
    for (entry = ringbuf_iter_first(&riter, &dec->qpd_dyn_table);
                                    entry; entry= ringbuf_iter_next(&riter))
    {
        fprintf(out, "%u) %.*s: %.*s\n", id,
            entry->dte_name_len, DTE_NAME(entry),
            entry->dte_val_len, DTE_VALUE(entry));
        id = ID_PLUS(id, 1);
    }
    fprintf(out, "\n");
}


const struct lsqpack_dec_err *
lsqpack_dec_get_err_info (const struct lsqpack_dec *dec)
{
    return &dec->qpd_err;
}

#define SHORTEST_CODE 5

/* This whole pragma business has to do with turning off uninitialized warnings.
 * We do it for gcc and clang.  Other compilers get slightly slower code, where
 * unnecessary initialization is performed.
 */
#if __GNUC__
#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#if __clang__
#pragma GCC diagnostic ignored "-Wunknown-warning-option"
#endif
#endif

static unsigned char *
qenc_huffman_enc (const unsigned char *src, const unsigned char *const src_end,
    unsigned char *dst)
{
    uintptr_t bits;  /* OK not to initialize this variable */
    unsigned bits_used = 0, adj;
    struct encode_el cur_enc_code;
#if __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#pragma GCC diagnostic ignored "-Wuninitialized"
#else
    bits = 0;
#endif
#if LS_QPACK_USE_LARGE_TABLES
    const struct henc *henc;
    uint16_t idx;

    while (src + sizeof(bits) * 8 / SHORTEST_CODE + sizeof(idx) < src_end)
    {
        memcpy(&idx, src, 2);
        henc = &hencs[idx];
        src += 2;
        while (bits_used + henc->lens < sizeof(bits) * 8)
        {
            bits <<= henc->lens;
            bits |= henc->code;
            bits_used += henc->lens;
            memcpy(&idx, src, 2);
            henc = &hencs[idx];
            src += 2;
        }
        if (henc->lens < 64)
        {
            bits <<= sizeof(bits) * 8 - bits_used;
            bits_used = henc->lens - (sizeof(bits) * 8 - bits_used);
            bits |= henc->code >> bits_used;
#if UINTPTR_MAX == 18446744073709551615ull
            *dst++ = (unsigned char)(bits >> 56);
            *dst++ = (unsigned char)(bits >> 48);
            *dst++ = (unsigned char)(bits >> 40);
            *dst++ = (unsigned char)(bits >> 32);
#endif
            *dst++ = (unsigned char)(bits >> 24);
            *dst++ = (unsigned char)(bits >> 16);
            *dst++ = (unsigned char)(bits >> 8);
            *dst++ = (unsigned char)bits;
            bits = henc->code;   /* OK not to clear high bits */
        }
        else
        {
            src -= 2;
            break;
        }
    }
#endif

    while (src != src_end)
    {
        cur_enc_code = encode_table[*src++];
        if (bits_used + cur_enc_code.bits < sizeof(bits) * 8)
        {
            bits <<= cur_enc_code.bits;
            bits |= cur_enc_code.code;
            bits_used += cur_enc_code.bits;
            continue;
        }
        else
        {
            bits <<= sizeof(bits) * 8 - bits_used;
            bits_used = cur_enc_code.bits - (sizeof(bits) * 8 - bits_used);
            bits |= cur_enc_code.code >> bits_used;
#if UINTPTR_MAX == 18446744073709551615ull
            *dst++ = (unsigned char)(bits >> 56);
            *dst++ = (unsigned char)(bits >> 48);
            *dst++ = (unsigned char)(bits >> 40);
            *dst++ = (unsigned char)(bits >> 32);
#endif
            *dst++ = (unsigned char)(bits >> 24);
            *dst++ = (unsigned char)(bits >> 16);
            *dst++ = (unsigned char)(bits >> 8);
            *dst++ = (unsigned char)bits;
            bits = cur_enc_code.code;   /* OK not to clear high bits */
        }
    }

    if (bits_used)
    {
        adj = (bits_used + 7) & -8;     /* Round up to 8 */
        bits <<= adj - bits_used;       /* Align to byte boundary */
        bits |= ((1 << (adj - bits_used)) - 1);  /* EOF */
        switch (adj >> 3)
        {                               /* Write out */
#if UINTPTR_MAX == 18446744073709551615ull
        case 8: *dst++ = (unsigned char)(bits >> 56);
        case 7: *dst++ = (unsigned char)(bits >> 48);
        case 6: *dst++ = (unsigned char)(bits >> 40);
        case 5: *dst++ = (unsigned char)(bits >> 32);
#endif
        case 4: *dst++ = (unsigned char)(bits >> 24);
        case 3: *dst++ = (unsigned char)(bits >> 16);
        case 2: *dst++ = (unsigned char)(bits >> 8);
        default: *dst++ = (unsigned char)bits;
        }
    }
#if __GNUC__
#pragma GCC diagnostic pop
#endif

    return dst;
}


static unsigned
qenc_enc_str_size (const unsigned char *str, unsigned str_len)
{
    unsigned const char *const end = str + str_len;
    unsigned enc_size_bits, enc_size_bytes;

    enc_size_bits = 0;
    while (str < end)
        enc_size_bits += encode_table[*str++].bits;
    enc_size_bytes = enc_size_bits / 8 + ((enc_size_bits & 7) != 0);

    return enc_size_bytes;
}


static unsigned char *
qdec_huff_dec4bits (uint8_t src_4bits, unsigned char *dst,
    struct lsqpack_decode_status *status)
{
    const struct decode_el cur_dec_code =
        decode_tables[status->state][src_4bits];
    if (cur_dec_code.flags & HPACK_HUFFMAN_FLAG_FAIL) {
        return NULL; //failed
    }
    if (cur_dec_code.flags & HPACK_HUFFMAN_FLAG_SYM)
    {
        *dst = cur_dec_code.sym;
        dst++;
    }

    status->state = cur_dec_code.state;
    status->eos = ((cur_dec_code.flags & HPACK_HUFFMAN_FLAG_ACCEPTED) != 0);
    return dst;
}


#if LS_QPACK_USE_LARGE_TABLES
/* The decoder is optimized for the common case.  Most of the time, we decode
 * data whose encoding is 16 bits or shorter.  This lets us use a 64 KB table
 * indexed by two bytes of input and outputs 1, 2, or 3 bytes at a time.
 *
 * In the case a longer code is encoutered, we fall back to the original
 * Huffman decoder that supports all code lengths.
 */
static struct huff_decode_retval
huff_decode_fast (const unsigned char *src, int src_len,
            unsigned char *dst, int dst_len,
            struct lsqpack_huff_decode_state *state, int final)
{
    unsigned char *const orig_dst = dst;
    const unsigned char *const src_end = src + src_len;
    unsigned char *const dst_end = dst + dst_len;
    uintptr_t buf;      /* OK not to initialize the buffer */
    unsigned avail_bits, len;
    struct huff_decode_retval rv;
    struct hdec hdec;
    uint16_t idx;

#if __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#pragma GCC diagnostic ignored "-Wuninitialized"
#else
    buf = 0;
#endif

    avail_bits = 0;
    while (1)
    {
        if (src + sizeof(buf) <= src_end)
        {
            len = (sizeof(buf) * 8 - avail_bits) >> 3;
            avail_bits += len << 3;
            switch (len)
            {
#if UINTPTR_MAX == 18446744073709551615ull
            case 8:
                buf <<= 8;
                buf |= (uintptr_t) *src++;
            case 7:
                buf <<= 8;
                buf |= (uintptr_t) *src++;
            default:
                buf <<= 48;
                buf |= (uintptr_t) *src++ << 40;
                buf |= (uintptr_t) *src++ << 32;
                buf |= (uintptr_t) *src++ << 24;
                buf |= (uintptr_t) *src++ << 16;
#else
            case 4:
                buf <<= 8;
                buf |= (uintptr_t) *src++;
            case 3:
                buf <<= 8;
                buf |= (uintptr_t) *src++;
            default:
                buf <<= 16;
#endif
                buf |= (uintptr_t) *src++ <<  8;
                buf |= (uintptr_t) *src++ <<  0;
            }
        }
        else if (src < src_end)
            do
            {
                buf <<= 8;
                buf |= (uintptr_t) *src++;
                avail_bits += 8;
            }
            while (src < src_end && avail_bits <= sizeof(buf) * 8 - 8);
        else
            break;  /* Normal case terminating condition: out of input */

        if (dst_end - dst >= (ptrdiff_t) (8 * sizeof(buf) / SHORTEST_CODE)
                                                            && avail_bits >= 16)
        {
            /* Fast path: don't check destination bounds */
            do
            {
                idx = (uint16_t)(buf >> (avail_bits - 16));
                hdec = hdecs[idx];
                dst[0] = hdec.out[0];
                dst[1] = hdec.out[1];
                dst[2] = hdec.out[2];
                dst += hdec.lens & 3;
                avail_bits -= hdec.lens >> 2;
            }
            while (avail_bits >= 16 && hdec.lens);
            if (avail_bits < 16)
                continue;
            goto slow_path;
        }
        else
            while (avail_bits >= 16)
            {
                idx = (uint16_t)(buf >> (avail_bits - 16));
                hdec = hdecs[idx];
                len = hdec.lens & 3;
                if (len && dst + len <= dst_end)
                {
                    switch (len)
                    {
                    case 3:
                        *dst++ = hdec.out[0];
                        *dst++ = hdec.out[1];
                        *dst++ = hdec.out[2];
                        break;
                    case 2:
                        *dst++ = hdec.out[0];
                        *dst++ = hdec.out[1];
                        break;
                    default:
                        *dst++ = hdec.out[0];
                        break;
                    }
                    avail_bits -= hdec.lens >> 2;
                }
                else if (dst + len > dst_end)
                    goto dst_ended;
                else
                    goto slow_path;
            }
    }

    if (avail_bits >= SHORTEST_CODE)
    {
        idx = (uint16_t)(buf << (16 - avail_bits));
        idx |= (1 << (16 - avail_bits)) - 1;    /* EOF */
        if (idx == 0xFFFF && avail_bits < 8)
            goto end;
        /* If a byte or more of input is left, this mean there is a valid
         * encoding, not just EOF.
         */
        hdec = hdecs[idx];
        len = hdec.lens & 3;
        if ((unsigned)(hdec.lens >> 2) > avail_bits)
            return (struct huff_decode_retval) {
                .status = HUFF_DEC_ERROR,
                .n_dst  = 0,
                .n_src  = 0,
            };
        if (len && dst + len <= dst_end)
        {
            switch (len)
            {
            case 3:
                *dst++ = hdec.out[0];
                *dst++ = hdec.out[1];
                *dst++ = hdec.out[2];
                break;
            case 2:
                *dst++ = hdec.out[0];
                *dst++ = hdec.out[1];
                break;
            default:
                *dst++ = hdec.out[0];
                break;
            }
            avail_bits -= hdec.lens >> 2;
        }
        else if (dst + len > dst_end)
            goto dst_ended;
        else
            /* This must be an invalid code, otherwise it would have fit */
            return (struct huff_decode_retval) {
                .status = HUFF_DEC_ERROR,
                .n_dst  = 0,
                .n_src  = 0,
            };
    }

    if (avail_bits > 0)
    {
        if (((1u << avail_bits) - 1) != (buf & ((1u << avail_bits) - 1)))
            return (struct huff_decode_retval) { /* Not EOF as expected */
                .status = HUFF_DEC_ERROR,
                .n_dst  = 0,
                .n_src  = 0,
            };
    }
#if __GNUC__
#pragma GCC diagnostic pop
#endif

  end:
    return (struct huff_decode_retval) {
        .status = HUFF_DEC_OK,
        .n_dst  = (unsigned)(dst - orig_dst),
        .n_src  = src_len - (int)(src_end - src),
    };

  dst_ended:
    /* Find previous byte boundary.  It is OK not to consume all input, as we
     * always grow the destination buffer and try again.
     */
    while ((avail_bits & 7) && dst > orig_dst)
        avail_bits += encode_table[ *--dst ].bits;
    assert((avail_bits & 7) == 0);
    src -= avail_bits >> 3;
    return (struct huff_decode_retval) {
        .status = HUFF_DEC_END_DST,
        .n_dst  = dst_len - (int)(dst_end - dst),
        .n_src  = src_len - (int)(src_end - src),
    };

  slow_path:
    /* Find previous byte boundary and finish decoding thence. */
    while ((avail_bits & 7) && dst > orig_dst)
        avail_bits += encode_table[ *--dst ].bits;
    assert((avail_bits & 7) == 0);
    src -= avail_bits >> 3;
    rv = lsqpack_huff_decode_full(src, (int)(src_end - src), dst,
                                  (int)(dst_end - dst), state, final);
    if (rv.status == HUFF_DEC_OK || rv.status == HUFF_DEC_END_DST)
    {
        rv.n_dst += dst_len - (int)(dst_end - dst);
        rv.n_src += src_len - (int)(src_end - src);
    }
    return rv;
}
#endif
#if __GNUC__
#pragma GCC diagnostic pop  /* -Wunknown-pragmas */
#endif
