#ifndef LSXPACK_HEADER_H_v208
#define LSXPACK_HEADER_H_v208

#include <assert.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef LSXPACK_MAX_STRLEN
#define LSXPACK_MAX_STRLEN UINT16_MAX
#endif

typedef int32_t lsxpack_offset_t;
#if LSXPACK_MAX_STRLEN == UINT16_MAX
typedef uint16_t lsxpack_strlen_t;
#elif LSXPACK_MAX_STRLEN == UINT32_MAX
typedef uint32_t lsxpack_strlen_t;
#else
#error unexpected LSXPACK_MAX_STRLEN
#endif

#define LSXPACK_DEL  ((char *)NULL)

enum lsxpack_flag
{
    LSXPACK_HPACK_VAL_MATCHED = 1,
    LSXPACK_QPACK_IDX = 2,
    LSXPACK_APP_IDX   = 4,
    LSXPACK_NAME_HASH = 8,
    LSXPACK_NAMEVAL_HASH = 16,
    LSXPACK_VAL_MATCHED = 32,
    LSXPACK_NEVER_INDEX = 64,
};

/**
 * When header are decoded, it should be stored to @buf starting from @name_offset,
 *    <name>: <value>\r\n
 * So, it can be used directly as HTTP/1.1 header. there are 4 extra characters
 * added.
 *
 * limitation: we currently does not support total header size > 64KB.
 */

struct lsxpack_header
{
    char             *buf;          /* the buffer for headers */
    uint32_t          name_hash;    /* hash value for name */
    uint32_t          nameval_hash; /* hash value for name + value */
    lsxpack_offset_t  name_offset;  /* the offset for name in the buffer */
    lsxpack_offset_t  val_offset;   /* the offset for value in the buffer */
    lsxpack_strlen_t  name_len;     /* the length of name */
    lsxpack_strlen_t  val_len;      /* the length of value */
    uint16_t          chain_next_idx; /* mainly for cookie value chain */
    uint8_t           hpack_index;  /* HPACK static table index */
    uint8_t           qpack_index;  /* QPACK static table index */
    uint8_t           app_index;    /* APP header index */
    enum lsxpack_flag flags:8;      /* combination of lsxpack_flag */
    uint8_t           indexed_type; /* control to disable index or not */
    uint8_t           dec_overhead; /* num of extra bytes written to decoded buffer */
};

typedef struct lsxpack_header lsxpack_header_t;


static inline void
lsxpack_header_set_idx(lsxpack_header_t *hdr, int hpack_idx,
                       const char *val, size_t val_len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->buf = (char *)val;
    hdr->hpack_index = (uint8_t)hpack_idx;
    assert(hpack_idx != 0);
    assert(val_len <= LSXPACK_MAX_STRLEN);
    hdr->val_len = (lsxpack_strlen_t)val_len;
}


static inline void
lsxpack_header_set_qpack_idx(lsxpack_header_t *hdr, int qpack_idx,
                       const char *val, size_t val_len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->buf = (char *)val;
    hdr->qpack_index = (uint8_t)qpack_idx;
    assert(qpack_idx != -1);
    hdr->flags = LSXPACK_QPACK_IDX;
    assert(val_len <= LSXPACK_MAX_STRLEN);
    hdr->val_len = (lsxpack_strlen_t)val_len;
}


static inline void
lsxpack_header_set_offset(lsxpack_header_t *hdr, const char *buf,
                          size_t name_offset, size_t name_len,
                          size_t val_len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->buf = (char *)buf;
    hdr->name_offset = (lsxpack_offset_t)name_offset;
    assert(name_len <= LSXPACK_MAX_STRLEN);
    hdr->name_len = (lsxpack_strlen_t)name_len;
    assert(name_offset + name_len + 2 <= LSXPACK_MAX_STRLEN);
    hdr->val_offset = (lsxpack_offset_t)(name_offset + name_len + 2);
    assert(val_len <= LSXPACK_MAX_STRLEN);
    hdr->val_len = (lsxpack_strlen_t)val_len;
}


static inline void
lsxpack_header_set_offset2(lsxpack_header_t *hdr, const char *buf,
                           size_t name_offset, size_t name_len,
                           size_t val_offset, size_t val_len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->buf = (char *)buf;
    hdr->name_offset = (lsxpack_offset_t)name_offset;
    assert(name_len <= LSXPACK_MAX_STRLEN);
    hdr->name_len = (lsxpack_strlen_t)name_len;
    assert(val_offset <= LSXPACK_MAX_STRLEN);
    hdr->val_offset = (lsxpack_offset_t)val_offset;
    assert(val_len <= LSXPACK_MAX_STRLEN);
    hdr->val_len = (lsxpack_strlen_t)val_len;
}


static inline void
lsxpack_header_prepare_decode(lsxpack_header_t *hdr,
                              char *out, size_t offset, size_t len)
{
    memset(hdr, 0, sizeof(*hdr));
    hdr->buf = out;
    assert(offset <= LSXPACK_MAX_STRLEN);
    hdr->name_offset = (lsxpack_offset_t)offset;
    if (len > LSXPACK_MAX_STRLEN)
        hdr->val_len = LSXPACK_MAX_STRLEN;
    else
        hdr->val_len = (lsxpack_strlen_t)len;
}


static inline const char *
lsxpack_header_get_name(const lsxpack_header_t *hdr)
{
    return (hdr->name_len)? hdr->buf + hdr->name_offset : NULL;
}


static inline const char *
lsxpack_header_get_value(const lsxpack_header_t *hdr)
{   return hdr->buf + hdr->val_offset;  }

static inline size_t
lsxpack_header_get_dec_size(const lsxpack_header_t *hdr)
{   return hdr->name_len + hdr->val_len + hdr->dec_overhead;    }

static inline void
lsxpack_header_mark_val_changed(lsxpack_header_t *hdr)
{
    hdr->flags = (enum lsxpack_flag)(hdr->flags &
       ~(LSXPACK_HPACK_VAL_MATCHED|LSXPACK_VAL_MATCHED|LSXPACK_NAMEVAL_HASH));
}
#ifdef __cplusplus
}
#endif

#endif //LSXPACK_HEADER_H_v208
