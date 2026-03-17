#ifndef DSTORE_FORMAT_H
#define DSTORE_FORMAT_H

typedef size_t (*pmix_common_dstore_kv_size_fn)(uint8_t *addr);
typedef char* (*pmix_common_dstore_key_name_ptr_fn)(uint8_t *addr);
typedef size_t (*pmix_common_dstore_key_name_len_fn)(char *key);
typedef uint8_t* (*pmix_common_dstore_data_ptr_fn)(uint8_t *addr);
typedef size_t (*pmix_common_dstore_data_size_fn)(uint8_t *addr, uint8_t* data_ptr);
typedef size_t (*pmix_common_dstore_key_size_fn)(char *key, size_t data_size);
typedef size_t (*pmix_common_dstore_ext_slot_size_fn)(void);
typedef int (*pmix_common_dstore_put_key_fn)(uint8_t *addr, char *key, void *buf,
                                              size_t size);
typedef bool (*pmix_common_dstore_is_invalid_fn)(uint8_t *addr);
typedef bool (*pmix_common_dstore_is_extslot_fn)(uint8_t *addr);
typedef void (*pmix_common_dstore_set_invalid_fn)(uint8_t *addr);
typedef size_t (*pmix_common_dstore_key_hash_fn)(const char *key);
typedef bool (*pmix_common_dstore_key_match_fn)(uint8_t *addr, const char *key,
                                                  size_t key_hash);

typedef struct {
    const char *name;
    pmix_common_dstore_kv_size_fn kval_size;
    pmix_common_dstore_key_name_ptr_fn kname_ptr;
    pmix_common_dstore_key_name_len_fn kname_len;
    pmix_common_dstore_data_ptr_fn data_ptr;
    pmix_common_dstore_data_size_fn data_size;
    pmix_common_dstore_key_size_fn key_size;
    pmix_common_dstore_ext_slot_size_fn ext_slot_size;
    pmix_common_dstore_put_key_fn put_key;
    pmix_common_dstore_is_invalid_fn is_invalid;
    pmix_common_dstore_is_extslot_fn is_extslot;
    pmix_common_dstore_set_invalid_fn set_invalid;
    pmix_common_dstore_key_hash_fn key_hash;
    pmix_common_dstore_key_match_fn key_match;
} pmix_common_dstore_file_cbs_t;

#define ESH_REGION_EXTENSION        "EXTENSION_SLOT"
#define ESH_REGION_INVALIDATED      "INVALIDATED"
#define ESH_ENV_INITIAL_SEG_SIZE    "INITIAL_SEG_SIZE"
#define ESH_ENV_NS_META_SEG_SIZE    "NS_META_SEG_SIZE"
#define ESH_ENV_NS_DATA_SEG_SIZE    "NS_DATA_SEG_SIZE"
#define ESH_ENV_LINEAR              "SM_USE_LINEAR_SEARCH"

#define ESH_MIN_KEY_LEN             (sizeof(ESH_REGION_INVALIDATED))

#define PMIX_DS_PUT_KEY(rc, ctx, addr, key, buf, size)              \
    do {                                                            \
        rc = PMIX_ERROR;                                            \
        if ((ctx)->file_cbs && (ctx)->file_cbs->put_key) {          \
            rc = (ctx)->file_cbs->put_key(addr, key, buf, size);         \
        }                                                           \
    } while(0)

#define PMIX_DS_KV_SIZE(ctx, addr)                            \
__pmix_attribute_extension__ ({                                     \
    size_t size = 0;                                                \
    if ((ctx)->file_cbs && (ctx)->file_cbs->kval_size) {            \
        size = (ctx)->file_cbs->kval_size(addr);                    \
    }                                                               \
    size;                                                           \
})

#define PMIX_DS_KNAME_PTR(ctx, addr)                                \
__pmix_attribute_extension__ ({                                     \
    char *name_ptr = NULL;                                          \
    if ((ctx)->file_cbs && (ctx)->file_cbs->kname_ptr) {            \
        name_ptr = (ctx)->file_cbs->kname_ptr(addr);                \
    }                                                               \
    name_ptr;                                                       \
})

#define PMIX_DS_KNAME_LEN(ctx, addr)                                \
__pmix_attribute_extension__ ({                                     \
    size_t len = 0;                                                 \
    if ((ctx)->file_cbs && (ctx)->file_cbs->kname_len) {            \
        len = (ctx)->file_cbs->kname_len((char*)addr);              \
    }                                                               \
    len;                                                            \
})

#define PMIX_DS_DATA_PTR(ctx, addr)                                 \
__pmix_attribute_extension__ ({                                     \
    uint8_t *data_ptr = NULL;                                       \
    if ((ctx)->file_cbs && (ctx)->file_cbs->data_ptr) {             \
        data_ptr = (ctx)->file_cbs->data_ptr(addr);                 \
    }                                                               \
    data_ptr;                                                       \
})

#define PMIX_DS_DATA_SIZE(ctx, addr, data_ptr)                      \
__pmix_attribute_extension__ ({                                     \
    size_t size = 0;                                                \
    if ((ctx)->file_cbs && (ctx)->file_cbs->data_size) {            \
        size = (ctx)->file_cbs->data_size(addr, data_ptr);          \
    }                                                               \
    size;                                                           \
})

#define PMIX_DS_KEY_SIZE(ctx, key, data_size)                       \
__pmix_attribute_extension__ ({                                     \
    size_t __size = 0;                                              \
    if ((ctx)->file_cbs && (ctx)->file_cbs->key_size) {             \
        __size = (ctx)->file_cbs->key_size(key, data_size);         \
    }                                                               \
    __size;                                                         \
})

#define PMIX_DS_SLOT_SIZE(ctx)                                      \
__pmix_attribute_extension__ ({                                     \
    size_t __size = 0;                                              \
    if ((ctx)->file_cbs && (ctx)->file_cbs->ext_slot_size) {            \
        __size = (ctx)->file_cbs->ext_slot_size();                      \
    }                                                               \
    __size;                                                         \
})

#define PMIX_DS_KEY_HASH(ctx, key)                                  \
__pmix_attribute_extension__ ({                                     \
    size_t keyhash = 0;                                             \
    if ((ctx)->file_cbs && (ctx)->file_cbs->key_hash) {             \
        keyhash = (ctx)->file_cbs->key_hash(key);                   \
    }                                                               \
    keyhash;                                                        \
})

#define PMIX_DS_KEY_MATCH(ctx, addr, key, hash)                     \
__pmix_attribute_extension__ ({                                     \
    int ret = 0;                                                    \
    if ((ctx)->file_cbs && (ctx)->file_cbs->key_match) {            \
        ret = (ctx)->file_cbs->key_match(addr, key, hash);          \
    }                                                               \
    ret;                                                            \
})

#define PMIX_DS_KEY_IS_INVALID(ctx, addr)                           \
__pmix_attribute_extension__ ({                                     \
    int ret = 0;                                                    \
    if ((ctx)->file_cbs && (ctx)->file_cbs->is_invalid) {           \
        ret = (ctx)->file_cbs->is_invalid(addr);                    \
    }                                                               \
    ret;                                                            \
})

#define PMIX_DS_KEY_SET_INVALID(ctx, addr)                          \
    do {                                                            \
        if ((ctx)->file_cbs && (ctx)->file_cbs->set_invalid) {      \
            (ctx)->file_cbs->set_invalid(addr);                     \
        }                                                           \
    } while(0)

#define PMIX_DS_KEY_IS_EXTSLOT(ctx, addr)                           \
__pmix_attribute_extension__ ({                                     \
    int ret = 0;                                                    \
    if ((ctx)->file_cbs && (ctx)->file_cbs->is_invalid) {           \
        ret = (ctx)->file_cbs->is_extslot(addr);                    \
    }                                                               \
    ret;                                                            \
})


#endif // DSTORE_FORMAT_H
