/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/allocator.h>
#include <aws/common/mutex.h>
#include <aws/common/thread.h>

#include <dlfcn.h>

#include <aws/cal/private/opensslcrypto_common.h>

#if defined(AWS_LIBCRYPTO_LOG_RESOLVE)
#    define FLOGF(...)                                                                                                 \
        do {                                                                                                           \
            fprintf(stderr, "AWS libcrypto resolve: ");                                                                \
            fprintf(stderr, __VA_ARGS__);                                                                              \
            fprintf(stderr, "\n");                                                                                     \
        } while (0)
#else
#    define FLOGF(...)
#endif

static struct openssl_hmac_ctx_table hmac_ctx_table;
static struct openssl_evp_md_ctx_table evp_md_ctx_table;

struct openssl_hmac_ctx_table *g_aws_openssl_hmac_ctx_table = NULL;
struct openssl_evp_md_ctx_table *g_aws_openssl_evp_md_ctx_table = NULL;

/* weak refs to libcrypto functions to force them to at least try to link
 * and avoid dead-stripping
 */
/* 1.1 */
extern HMAC_CTX *HMAC_CTX_new(void) __attribute__((weak)) __attribute__((used));
extern void HMAC_CTX_free(HMAC_CTX *) __attribute__((weak)) __attribute__((used));
extern int HMAC_CTX_reset(HMAC_CTX *) __attribute__((weak)) __attribute__((used));

/* 1.0.2 */
extern void HMAC_CTX_init(HMAC_CTX *) __attribute__((weak)) __attribute__((used));
extern void HMAC_CTX_cleanup(HMAC_CTX *) __attribute__((weak)) __attribute__((used));

/* common */
extern int HMAC_Update(HMAC_CTX *, const unsigned char *, size_t) __attribute__((weak)) __attribute__((used));
extern int HMAC_Final(HMAC_CTX *, unsigned char *, unsigned int *) __attribute__((weak)) __attribute__((used));
extern int HMAC_Init_ex(HMAC_CTX *, const void *, int, const EVP_MD *, ENGINE *) __attribute__((weak))
__attribute__((used));

/* libcrypto 1.1 stub for init */
static void s_hmac_ctx_init_noop(HMAC_CTX *ctx) {
    (void)ctx;
}

/* libcrypto 1.1 stub for clean_up */
static void s_hmac_ctx_clean_up_noop(HMAC_CTX *ctx) {
    (void)ctx;
}

/* libcrypto 1.0 shim for new */
static HMAC_CTX *s_hmac_ctx_new(void) {
    AWS_PRECONDITION(
        g_aws_openssl_hmac_ctx_table->init_fn != s_hmac_ctx_init_noop &&
        "libcrypto 1.0 init called on libcrypto 1.1 vtable");
    HMAC_CTX *ctx = aws_mem_calloc(aws_default_allocator(), 1, 300);
    AWS_FATAL_ASSERT(ctx && "Unable to allocate to HMAC_CTX");
    g_aws_openssl_hmac_ctx_table->init_fn(ctx);
    return ctx;
}

/* libcrypto 1.0 shim for free */
static void s_hmac_ctx_free(HMAC_CTX *ctx) {
    AWS_PRECONDITION(ctx);
    AWS_PRECONDITION(
        g_aws_openssl_hmac_ctx_table->clean_up_fn != s_hmac_ctx_clean_up_noop &&
        "libcrypto 1.0 clean_up called on libcrypto 1.1 vtable");
    g_aws_openssl_hmac_ctx_table->clean_up_fn(ctx);
    aws_mem_release(aws_default_allocator(), ctx);
}

/* libcrypto 1.0 shim for reset, matches HMAC_CTX_reset semantics */
static int s_hmac_ctx_reset(HMAC_CTX *ctx) {
    AWS_PRECONDITION(ctx);
    AWS_PRECONDITION(
        g_aws_openssl_hmac_ctx_table->init_fn != s_hmac_ctx_init_noop &&
        g_aws_openssl_hmac_ctx_table->clean_up_fn != s_hmac_ctx_clean_up_noop &&
        "libcrypto 1.0 reset called on libcrypto 1.1 vtable");
    g_aws_openssl_hmac_ctx_table->clean_up_fn(ctx);
    g_aws_openssl_hmac_ctx_table->init_fn(ctx);
    return 1;
}

static struct aws_mutex *s_libcrypto_locks = NULL;
static struct aws_allocator *s_libcrypto_allocator = NULL;

static void s_locking_fn(int mode, int n, const char *unused0, int unused1) {
    (void)unused0;
    (void)unused1;

    if (mode & CRYPTO_LOCK) {
        aws_mutex_lock(&s_libcrypto_locks[n]);
    } else {
        aws_mutex_unlock(&s_libcrypto_locks[n]);
    }
}

static unsigned long s_id_fn(void) {
    return (unsigned long)aws_thread_current_thread_id();
}

enum aws_libcrypto_version {
    AWS_LIBCRYPTO_NONE = 0,
    AWS_LIBCRYPTO_1_0_2,
    AWS_LIBCRYPTO_1_1_1,
    AWS_LIBCRYPTO_LC,
} s_libcrypto_version = AWS_LIBCRYPTO_NONE;

static int s_resolve_libcrypto_hmac(enum aws_libcrypto_version version, void *module) {
    hmac_ctx_init init_fn = HMAC_CTX_init;
    hmac_ctx_clean_up clean_up_fn = HMAC_CTX_cleanup;
    hmac_ctx_new new_fn = HMAC_CTX_new;
    hmac_ctx_free free_fn = HMAC_CTX_free;
    hmac_ctx_reset reset_fn = HMAC_CTX_reset;
    hmac_ctx_update update_fn = HMAC_Update;
    hmac_ctx_final final_fn = HMAC_Final;
    hmac_ctx_init_ex init_ex_fn = HMAC_Init_ex;

    /* were symbols bound by static linking? */
    bool has_102_symbols = init_fn && clean_up_fn && update_fn && final_fn && init_ex_fn;
    bool has_111_symbols = new_fn && free_fn && update_fn && final_fn && init_ex_fn && reset_fn;

    if (version == AWS_LIBCRYPTO_NONE) {
        if (has_102_symbols) {
            version = AWS_LIBCRYPTO_1_0_2;
            FLOGF("auto-resolving libcrypto 1.0.2");
        } else if (has_111_symbols) {
            version = AWS_LIBCRYPTO_1_1_1;
            FLOGF("auto-resolving libcrypto 1.1.1");
        } else {
            /* not pre-linked, need to ask for a specific version */
            return AWS_LIBCRYPTO_NONE;
        }
    }

    if (has_102_symbols) {
        FLOGF("found static libcrypto 1.0.2 HMAC");
    }
    if (has_111_symbols) {
        FLOGF("found static libcrypto 1.1.1 HMAC");
    }

    /* If symbols aren't already found, try to find the requested version */
    /* when built as a shared lib, and multiple versions of openssl are possibly
     * available (e.g. brazil), select 1.0.2 by default for consistency */
    if (!has_102_symbols && version == AWS_LIBCRYPTO_1_0_2) {
        *(void **)(&init_fn) = dlsym(module, "HMAC_CTX_init");
        *(void **)(&clean_up_fn) = dlsym(module, "HMAC_CTX_cleanup");
        *(void **)(&update_fn) = dlsym(module, "HMAC_Update");
        *(void **)(&final_fn) = dlsym(module, "HMAC_Final");
        *(void **)(&init_ex_fn) = dlsym(module, "HMAC_Init_ex");
        if (init_fn) {
            FLOGF("found dynamic libcrypto 1.0.2 HMAC symbols");
        }
    }

    if (!has_111_symbols && version == AWS_LIBCRYPTO_1_1_1) {
        *(void **)(&new_fn) = dlsym(module, "HMAC_CTX_new");
        *(void **)(&reset_fn) = dlsym(module, "HMAC_CTX_reset");
        *(void **)(&free_fn) = dlsym(module, "HMAC_CTX_free");
        *(void **)(&update_fn) = dlsym(module, "HMAC_Update");
        *(void **)(&final_fn) = dlsym(module, "HMAC_Final");
        *(void **)(&init_ex_fn) = dlsym(module, "HMAC_Init_ex");
        if (new_fn) {
            FLOGF("found dynamic libcrypto 1.1.1 HMAC symbols");
        }
    }

    /* Fill out the vtable for the requested version */
    if (version == AWS_LIBCRYPTO_1_0_2 && init_fn) {
        hmac_ctx_table.new_fn = s_hmac_ctx_new;
        hmac_ctx_table.reset_fn = s_hmac_ctx_reset;
        hmac_ctx_table.free_fn = s_hmac_ctx_free;
        hmac_ctx_table.init_fn = init_fn;
        hmac_ctx_table.clean_up_fn = clean_up_fn;
    } else if (version == AWS_LIBCRYPTO_1_1_1 && new_fn) {
        hmac_ctx_table.new_fn = new_fn;
        hmac_ctx_table.reset_fn = reset_fn;
        hmac_ctx_table.free_fn = free_fn;
        hmac_ctx_table.init_fn = s_hmac_ctx_init_noop;
        hmac_ctx_table.clean_up_fn = s_hmac_ctx_clean_up_noop;
    }
    hmac_ctx_table.update_fn = update_fn;
    hmac_ctx_table.final_fn = final_fn;
    hmac_ctx_table.init_ex_fn = init_ex_fn;
    g_aws_openssl_hmac_ctx_table = &hmac_ctx_table;

    return version;
}

/* EVP_MD_CTX API */
/* 1.0.2 NOTE: these are macros in 1.1.x, so we have to undef them to weak link */
#if defined(EVP_MD_CTX_create)
#    pragma push_macro("EVP_MD_CTX_create")
#    undef EVP_MD_CTX_create
#endif
extern EVP_MD_CTX *EVP_MD_CTX_create(void) __attribute__((weak, used));
static evp_md_ctx_new s_EVP_MD_CTX_create = EVP_MD_CTX_create;
#if defined(EVP_MD_CTX_create)
#    pragma pop_macro("EVP_MD_CTX_create")
#endif

#if defined(EVP_MD_CTX_destroy)
#    pragma push_macro("EVP_MD_CTX_destroy")
#    undef EVP_MD_CTX_destroy
#endif
extern void EVP_MD_CTX_destroy(EVP_MD_CTX *) __attribute__((weak, used));
static evp_md_ctx_free s_EVP_MD_CTX_destroy = EVP_MD_CTX_destroy;
#if defined(EVP_MD_CTX_destroy)
#    pragma pop_macro("EVP_MD_CTX_destroy")
#endif

/* 1.1 */
extern EVP_MD_CTX *EVP_MD_CTX_new(void) __attribute__((weak, used));
extern void EVP_MD_CTX_free(EVP_MD_CTX *) __attribute__((weak, used));

/* common */
extern int EVP_DigestInit_ex(EVP_MD_CTX *, const EVP_MD *, ENGINE *) __attribute__((weak, used));
extern int EVP_DigestUpdate(EVP_MD_CTX *, const void *, size_t) __attribute__((weak, used));
extern int EVP_DigestFinal_ex(EVP_MD_CTX *, unsigned char *, unsigned int *) __attribute__((weak, used));

static int s_resolve_libcrypto_md(enum aws_libcrypto_version version, void *module) {
    /* OpenSSL changed the EVP api in 1.1 to use new/free verbs */
    evp_md_ctx_new md_new_fn = EVP_MD_CTX_new;
    evp_md_ctx_new md_create_fn = s_EVP_MD_CTX_create;
    evp_md_ctx_free md_free_fn = EVP_MD_CTX_free;
    evp_md_ctx_free md_destroy_fn = s_EVP_MD_CTX_destroy;
    evp_md_ctx_digest_init_ex md_init_ex_fn = EVP_DigestInit_ex;
    evp_md_ctx_digest_update md_update_fn = EVP_DigestUpdate;
    evp_md_ctx_digest_final_ex md_final_ex_fn = EVP_DigestFinal_ex;

    bool has_102_symbols = md_create_fn && md_destroy_fn && md_init_ex_fn && md_update_fn && md_final_ex_fn;
    bool has_111_symbols = md_new_fn && md_free_fn && md_init_ex_fn && md_update_fn && md_final_ex_fn;

    if (has_102_symbols) {
        FLOGF("found static libcrypto 1.0.2 EVP_MD");
    }
    if (has_111_symbols) {
        FLOGF("found static libcrypto 1.1.1 EVP_MD");
    }

    if (!has_102_symbols && version == AWS_LIBCRYPTO_1_0_2) {
        *(void **)(&md_create_fn) = dlsym(module, "EVP_MD_CTX_create");
        *(void **)(&md_destroy_fn) = dlsym(module, "EVP_MD_CTX_destroy");
        *(void **)(&md_init_ex_fn) = dlsym(module, "EVP_DigestInit_ex");
        *(void **)(&md_update_fn) = dlsym(module, "EVP_DigestUpdate");
        *(void **)(&md_final_ex_fn) = dlsym(module, "EVP_DigestFinal_ex");
        if (md_create_fn) {
            FLOGF("found dynamic libcrypto 1.0.2 EVP_MD symbols");
        }
    }

    if (!has_111_symbols && version == AWS_LIBCRYPTO_1_1_1) {
        *(void **)(&md_new_fn) = dlsym(module, "EVP_MD_CTX_new");
        *(void **)(&md_free_fn) = dlsym(module, "EVP_MD_CTX_free");
        *(void **)(&md_init_ex_fn) = dlsym(module, "EVP_DigestInit_ex");
        *(void **)(&md_update_fn) = dlsym(module, "EVP_DigestUpdate");
        *(void **)(&md_final_ex_fn) = dlsym(module, "EVP_DigestFinal_ex");
        if (md_new_fn) {
            FLOGF("found dynamic libcrypto 1.1.1 EVP_MD symbols");
        }
    }

    /* Add the found symbols to the vtable */
    if (version == AWS_LIBCRYPTO_1_0_2 && md_create_fn) {
        evp_md_ctx_table.new_fn = md_create_fn;
        evp_md_ctx_table.free_fn = md_destroy_fn;
        evp_md_ctx_table.init_ex_fn = md_init_ex_fn;
        evp_md_ctx_table.update_fn = md_update_fn;
        evp_md_ctx_table.final_ex_fn = md_final_ex_fn;
        g_aws_openssl_evp_md_ctx_table = &evp_md_ctx_table;
        return version;
    } else if (version == AWS_LIBCRYPTO_1_1_1 && md_new_fn) {
        evp_md_ctx_table.new_fn = md_new_fn;
        evp_md_ctx_table.free_fn = md_free_fn;
        evp_md_ctx_table.init_ex_fn = md_init_ex_fn;
        evp_md_ctx_table.update_fn = md_update_fn;
        evp_md_ctx_table.final_ex_fn = md_final_ex_fn;
        g_aws_openssl_evp_md_ctx_table = &evp_md_ctx_table;
        return version;
    }

    return AWS_LIBCRYPTO_NONE;
}

static int s_resolve_libcrypto_symbols(enum aws_libcrypto_version version, void *module) {
    int found_version = s_resolve_libcrypto_hmac(version, module);
    if (!found_version) {
        return AWS_LIBCRYPTO_NONE;
    }
    if (!s_resolve_libcrypto_md(found_version, module)) {
        return AWS_LIBCRYPTO_NONE;
    }
    return found_version;
}

static int s_resolve_libcrypto_version(enum aws_libcrypto_version version) {
    const char *libcrypto_102 = "libcrypto.so.1.0.0";
    const char *libcrypto_111 = "libcrypto.so.1.1";
    switch (version) {
        case AWS_LIBCRYPTO_NONE: {
            FLOGF("searching process and loaded modules");
            void *process = dlopen(NULL, RTLD_NOW);
            AWS_FATAL_ASSERT(process && "Unable to load symbols from process space");
            int result = s_resolve_libcrypto_symbols(version, process);
            dlclose(process);
            return result;
        }
        case AWS_LIBCRYPTO_1_0_2: {
            FLOGF("loading libcrypto 1.0.2");
            void *module = dlopen(libcrypto_102, RTLD_NOW);
            if (module) {
                FLOGF("resolving against libcrypto 1.0.2");
                int result = s_resolve_libcrypto_symbols(version, module);
                dlclose(module);
                return result;
            }
            FLOGF("libcrypto 1.0.2 not found");
            break;
        }
        case AWS_LIBCRYPTO_1_1_1: {
            FLOGF("loading libcrypto 1.1.1");
            void *module = dlopen(libcrypto_111, RTLD_NOW);
            if (module) {
                FLOGF("resolving against libcrypto 1.1.1");
                int result = s_resolve_libcrypto_symbols(version, module);
                dlclose(module);
                return result;
            }
            FLOGF("libcrypto 1.1.1 not found");
            break;
        }
        default:
            AWS_FATAL_ASSERT("Attempted to use an unsupported version of libcrypto");
    }
    return AWS_LIBCRYPTO_NONE;
}

static int s_resolve_libcrypto(void) {
    if (s_libcrypto_version != AWS_LIBCRYPTO_NONE) {
        return s_libcrypto_version;
    }

    /* Try to auto-resolve against what's linked in/process space */
    FLOGF("Attempting auto-resolve against static linkage");
    s_libcrypto_version = s_resolve_libcrypto_version(AWS_LIBCRYPTO_NONE);
    /* try 1.0.2 */
    if (s_libcrypto_version == AWS_LIBCRYPTO_NONE) {
        FLOGF("Attempting resolve against libcrypto 1.0.2");
        s_libcrypto_version = s_resolve_libcrypto_version(AWS_LIBCRYPTO_1_0_2);
    }
    /* try 1.1.1 */
    if (s_libcrypto_version == AWS_LIBCRYPTO_NONE) {
        FLOGF("Attempting resolve against libcrypto 1.1.1");
        s_libcrypto_version = s_resolve_libcrypto_version(AWS_LIBCRYPTO_1_1_1);
    }

    return s_libcrypto_version;
}

/* Ignore warnings about how CRYPTO_get_locking_callback() always returns NULL on 1.1.1 */
#if !defined(__GNUC__) || (__GNUC__ >= 4 && __GNUC_MINOR__ > 1)
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Waddress"
#endif

void aws_cal_platform_init(struct aws_allocator *allocator) {
    s_libcrypto_allocator = allocator;

    int version = s_resolve_libcrypto();
    AWS_FATAL_ASSERT(version != AWS_LIBCRYPTO_NONE && "libcrypto could not be resolved");

    /* Ensure that libcrypto 1.0.2 has working locking mechanisms. This code is macro'ed
     * by libcrypto to be a no-op on 1.1.1 */
    if (!CRYPTO_get_locking_callback()) {
        /* on 1.1.1 this is a no-op */
        CRYPTO_set_locking_callback(s_locking_fn);
        if (CRYPTO_get_locking_callback() == s_locking_fn) {
            s_libcrypto_locks = aws_mem_acquire(allocator, sizeof(struct aws_mutex) * CRYPTO_num_locks());
            AWS_FATAL_ASSERT(s_libcrypto_locks);
            size_t lock_count = (size_t)CRYPTO_num_locks();
            for (size_t i = 0; i < lock_count; ++i) {
                aws_mutex_init(&s_libcrypto_locks[i]);
            }
        }
    }

    if (!CRYPTO_get_id_callback()) {
        CRYPTO_set_id_callback(s_id_fn);
    }
}

void aws_cal_platform_clean_up(void) {
    if (CRYPTO_get_locking_callback() == s_locking_fn) {
        CRYPTO_set_locking_callback(NULL);
        size_t lock_count = (size_t)CRYPTO_num_locks();
        for (size_t i = 0; i < lock_count; ++i) {
            aws_mutex_clean_up(&s_libcrypto_locks[i]);
        }
        aws_mem_release(s_libcrypto_allocator, s_libcrypto_locks);
    }

    if (CRYPTO_get_id_callback() == s_id_fn) {
        CRYPTO_set_id_callback(NULL);
    }
}
#if !defined(__GNUC__) || (__GNUC__ >= 4 && __GNUC_MINOR__ > 1)
#    pragma GCC diagnostic pop
#endif
