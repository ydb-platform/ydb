/*
 * Copyright (c) 2018      Mellanox Technologies, Inc.
 *                         All rights reserved.
 *
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
#include "gds_ds12_file.h"

#define ESH_KV_SIZE_V20(addr)                               \
__pmix_attribute_extension__ ({                             \
    size_t sz;                                              \
    memcpy(&sz, addr, sizeof(size_t));                      \
    sz;                                                     \
})

#define ESH_KNAME_PTR_V20(addr)                             \
    ((char *)addr + sizeof(size_t))

#define ESH_KNAME_LEN_V20(key)                              \
__pmix_attribute_extension__ ({                             \
    size_t kname_len = strlen(key) + 1;                     \
    size_t len = (kname_len < ESH_MIN_KEY_LEN) ?            \
    ESH_MIN_KEY_LEN : kname_len;                            \
    len;                                                    \
})

#define ESH_DATA_PTR_V20(addr)                              \
__pmix_attribute_extension__ ({                             \
    size_t kname_len =                                      \
        ESH_KNAME_LEN_V20(ESH_KNAME_PTR_V20(addr));         \
    uint8_t *data_ptr = addr + sizeof(size_t) + kname_len;  \
    data_ptr;                                               \
})

#define ESH_DATA_SIZE_V20(addr, data_ptr)                   \
__pmix_attribute_extension__ ({                             \
    size_t __sz = ESH_KV_SIZE_V20(addr);                      \
    size_t data_size = __sz - (data_ptr - addr);              \
    data_size;                                              \
})

#define ESH_KEY_SIZE_V20(key, size)                         \
    (sizeof(size_t) + ESH_KNAME_LEN_V20((char*)key) + size)

/* in ext slot new offset will be stored in case if
 * new data were added for the same process during
 * next commit
 */
#define EXT_SLOT_SIZE_V20()                                 \
    (ESH_KEY_SIZE_V20(ESH_REGION_EXTENSION, sizeof(size_t)))


#define ESH_PUT_KEY_V20(addr, key, buffer, size)            \
__pmix_attribute_extension__ ({                             \
    size_t sz = ESH_KEY_SIZE_V20(key, size);                \
    memcpy(addr, &sz, sizeof(size_t));                      \
    memset(addr + sizeof(size_t), 0,                        \
        ESH_KNAME_LEN_V20(key));                            \
    strncpy((char *)addr + sizeof(size_t),                  \
            key, ESH_KNAME_LEN_V20(key));                   \
    memcpy(addr + sizeof(size_t) + ESH_KNAME_LEN_V20(key),  \
            buffer, size);                                  \
})

static size_t pmix_ds20_kv_size(uint8_t *key)
{
    size_t size;

    memcpy(&size, key, sizeof(size_t));
    return size;
}

static char* pmix_ds20_key_name_ptr(uint8_t *addr)
{
    return ESH_KNAME_PTR_V20(addr);
}

static size_t pmix_ds20_key_name_len(char *key)
{
    return ESH_KNAME_LEN_V20(key);
}

static uint8_t* pmix_ds20_data_ptr(uint8_t *addr)
{
    return ESH_DATA_PTR_V20(addr);
}

static size_t pmix_ds20_data_size(uint8_t *addr, uint8_t* data_ptr)
{
    return ESH_DATA_SIZE_V20(addr, data_ptr);
}

static size_t pmix_ds20_key_size(char *addr, size_t data_size)
{
    return ESH_KEY_SIZE_V20(addr, data_size);
}

static size_t pmix_ds20_ext_slot_size(void)
{
    return EXT_SLOT_SIZE_V20();
}

static int pmix_ds20_put_key(uint8_t *addr, char *key, void *buf, size_t size)
{
    ESH_PUT_KEY_V20(addr, key, buf, size);
    return PMIX_SUCCESS;
}

static bool pmix_ds20_is_invalid(uint8_t *addr)
{
    bool ret = (0 == strncmp(ESH_REGION_INVALIDATED, ESH_KNAME_PTR_V20(addr),
                            ESH_KNAME_LEN_V20(ESH_KNAME_PTR_V20(addr))));
    return ret;
}

static void pmix_ds20_set_invalid(uint8_t *addr)
{
    strncpy(ESH_KNAME_PTR_V20(addr), ESH_REGION_INVALIDATED,
            ESH_KNAME_LEN_V20(ESH_REGION_INVALIDATED));
}

static bool pmix_ds20_is_ext_slot(uint8_t *addr)
{
    bool ret;
    ret = (0 == strncmp(ESH_REGION_EXTENSION, ESH_KNAME_PTR_V20(addr),
                        ESH_KNAME_LEN_V20(ESH_KNAME_PTR_V20(addr))));
    return ret;
}

static bool pmix_ds20_kname_match(uint8_t *addr, const char *key, size_t key_hash)
{
    bool ret = 0;

    ret = (0 == strncmp(ESH_KNAME_PTR_V20(addr),
                        key, ESH_KNAME_LEN_V20(key)));
    return ret;
}


pmix_common_dstore_file_cbs_t pmix_ds20_file_module = {
    .name = "ds20",
    .kval_size = pmix_ds20_kv_size,
    .kname_ptr = pmix_ds20_key_name_ptr,
    .kname_len = pmix_ds20_key_name_len,
    .data_ptr = pmix_ds20_data_ptr,
    .data_size = pmix_ds20_data_size,
    .key_size = pmix_ds20_key_size,
    .ext_slot_size = pmix_ds20_ext_slot_size,
    .put_key = pmix_ds20_put_key,
    .is_invalid = pmix_ds20_is_invalid,
    .is_extslot = pmix_ds20_is_ext_slot,
    .set_invalid = pmix_ds20_set_invalid,
    .key_hash = NULL,
    .key_match = pmix_ds20_kname_match
};
