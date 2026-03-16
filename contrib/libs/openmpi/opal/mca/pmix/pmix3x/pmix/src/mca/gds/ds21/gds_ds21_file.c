/*
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <pmix_common.h>

#include "src/include/pmix_globals.h"
#include "src/mca/gds/base/base.h"

#include "src/mca/common/dstore/dstore_file.h"
#include "gds_ds21_file.h"

#if 8 > SIZEOF_SIZE_T
#define ESH_REGION_EXTENSION_FLG    0x80000000
#define ESH_REGION_INVALIDATED_FLG  0x40000000
#define ESH_REGION_SIZE_MASK        0x3FFFFFFF
#else
#define ESH_REGION_EXTENSION_FLG    0x8000000000000000
#define ESH_REGION_INVALIDATED_FLG  0x4000000000000000
#define ESH_REGION_SIZE_MASK        0x3FFFFFFFFFFFFFFF
#endif

#define ESH_KV_SIZE_V21(addr)                               \
__pmix_attribute_extension__ ({                             \
    size_t sz;                                              \
    memcpy(&sz, addr, sizeof(size_t));                      \
    /* drop flags in lsb's */                               \
    (sz & ESH_REGION_SIZE_MASK);                            \
})

#define ESH_KNAME_PTR_V21(addr)                             \
    ((char *)addr + 2 * sizeof(size_t))

#define ESH_KNAME_LEN_V21(key)                              \
    (strlen(key) + 1)

#define ESH_DATA_PTR_V21(addr)                              \
__pmix_attribute_extension__ ({                             \
    char *key_ptr = ESH_KNAME_PTR_V21(addr);                \
    size_t kname_len = ESH_KNAME_LEN_V21(key_ptr);          \
    uint8_t *data_ptr =                                     \
        addr + (key_ptr - (char*)addr) + kname_len;         \
    data_ptr;                                               \
})

#define ESH_DATA_SIZE_V21(addr, data_ptr)                   \
__pmix_attribute_extension__ ({                             \
    size_t sz = ESH_KV_SIZE_V21(addr);                      \
    size_t data_size = sz - (data_ptr - addr);              \
    data_size;                                              \
})

#define ESH_KEY_SIZE_V21(key, size)                         \
    (2 * sizeof(size_t) + ESH_KNAME_LEN_V21((char*)key) + size)

/* in ext slot new offset will be stored in case if
 * new data were added for the same process during
 * next commit
 */
#define EXT_SLOT_SIZE_V21()                                 \
    (ESH_KEY_SIZE_V21("", sizeof(size_t)))

static bool pmix_ds21_is_invalid(uint8_t *addr)
{
    size_t sz;
    memcpy(&sz, addr, sizeof(size_t));
    return !!(sz & ESH_REGION_INVALIDATED_FLG);
}

static void pmix_ds21_set_invalid(uint8_t *addr)
{
    size_t sz;
    memcpy(&sz, addr, sizeof(size_t));
    sz |= ESH_REGION_INVALIDATED_FLG;
    memcpy(addr, &sz, sizeof(size_t));
}

static bool pmix_ds21_is_ext_slot(uint8_t *addr)
{
    size_t sz;
    memcpy(&sz, addr, sizeof(size_t));
    return !!(sz & ESH_REGION_EXTENSION_FLG);
}

static size_t pmix_ds21_key_hash(const char *key)
{
    size_t hash = 0;
    int i;
    for(i=0; key[i]; i++) {
        hash += key[i];
    }
    return hash;
}

static bool pmix_ds21_kname_match(uint8_t *addr, const char *key, size_t key_hash)
{
    bool ret = 0;
    size_t hash;
    memcpy(&hash, (char*)addr + sizeof(size_t), sizeof(size_t));
    if( key_hash != hash ) {
        return ret;
    }
    return (0 == strncmp(ESH_KNAME_PTR_V21(addr), key, ESH_KNAME_LEN_V21(key)));
}

static size_t pmix_ds21_kval_size(uint8_t *key)
{
    return ESH_KV_SIZE_V21(key); ;
}

static char* pmix_ds21_key_name_ptr(uint8_t *addr)
{
    return ESH_KNAME_PTR_V21(addr);
}

static size_t pmix_ds21_key_name_len(char *key)
{
    return ESH_KNAME_LEN_V21(key);
}

static uint8_t* pmix_ds21_data_ptr(uint8_t *addr)
{
    return ESH_DATA_PTR_V21(addr);
}

static size_t pmix_ds21_data_size(uint8_t *addr, uint8_t* data_ptr)
{
    return ESH_DATA_SIZE_V21(addr, data_ptr);
}

static size_t pmix_ds21_key_size(char *addr, size_t data_size)
{
    return ESH_KEY_SIZE_V21(addr, data_size);
}

static size_t pmix_ds21_ext_slot_size(void)
{
    return EXT_SLOT_SIZE_V21();
}

static int pmix_ds21_put_key(uint8_t *addr, char *key,
                              void* buffer, size_t size)
{
    size_t flag = 0;
    size_t hash = 0;
    char *addr_ch = (char*)addr;
    if( !strcmp(key, ESH_REGION_EXTENSION) ) {
        /* we have a flag for this special key */
        key = "";
        flag |= ESH_REGION_EXTENSION_FLG;
    }
    size_t sz = ESH_KEY_SIZE_V21(key, size);
    if( ESH_REGION_SIZE_MASK < sz ) {
        return PMIX_ERROR;
    }
    sz |= flag;
    memcpy(addr_ch, &sz, sizeof(size_t));
    hash = pmix_ds21_key_hash(key);
    memcpy(addr_ch + sizeof(size_t), &hash, sizeof(size_t));
    strncpy(addr_ch + 2 * sizeof(size_t), key, ESH_KNAME_LEN_V21(key));
    memcpy(ESH_DATA_PTR_V21(addr), buffer, size);
    return PMIX_SUCCESS;
}

pmix_common_dstore_file_cbs_t pmix_ds21_file_module = {
    .name = "ds21",
    .kval_size = pmix_ds21_kval_size,
    .kname_ptr = pmix_ds21_key_name_ptr,
    .kname_len = pmix_ds21_key_name_len,
    .data_ptr = pmix_ds21_data_ptr,
    .data_size = pmix_ds21_data_size,
    .key_size = pmix_ds21_key_size,
    .ext_slot_size = pmix_ds21_ext_slot_size,
    .put_key = pmix_ds21_put_key,
    .is_invalid = pmix_ds21_is_invalid,
    .is_extslot = pmix_ds21_is_ext_slot,
    .set_invalid = pmix_ds21_set_invalid,
    .key_hash = pmix_ds21_key_hash,
    .key_match = pmix_ds21_kname_match
};
