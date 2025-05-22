/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/cal.h>
#include <aws/common/allocator.h>
#include <aws/common/logging.h>
#include <aws/common/mutex.h>
#include <aws/common/thread.h>

#include <dlfcn.h>

#include <aws/cal/private/opensslcrypto_common.h>

static struct openssl_hmac_ctx_table hmac_ctx_table;
static struct openssl_evp_md_ctx_table evp_md_ctx_table;

struct openssl_hmac_ctx_table *g_aws_openssl_hmac_ctx_table = NULL;
struct openssl_evp_md_ctx_table *g_aws_openssl_evp_md_ctx_table = NULL;

static struct aws_allocator *s_libcrypto_allocator = NULL;

/* weak refs to libcrypto functions to force them to at least try to link
 * and avoid dead-stripping
 */
#if defined(OPENSSL_IS_AWSLC)
extern HMAC_CTX *HMAC_CTX_new(void) __attribute__((weak, used));
extern void HMAC_CTX_free(HMAC_CTX *) __attribute__((weak, used));
extern void HMAC_CTX_reset(HMAC_CTX *) __attribute__((weak, used));
extern void HMAC_CTX_init(HMAC_CTX *) __attribute__((weak, used));
extern void HMAC_CTX_cleanup(HMAC_CTX *) __attribute__((weak, used));
extern int HMAC_Update(HMAC_CTX *, const unsigned char *, size_t) __attribute__((weak, used));
extern int HMAC_Final(HMAC_CTX *, unsigned char *, unsigned int *) __attribute__((weak, used));
extern int HMAC_Init_ex(HMAC_CTX *, const void *, size_t, const EVP_MD *, ENGINE *) __attribute__((weak, used));
#else
/* 1.1 */
extern HMAC_CTX *HMAC_CTX_new(void) __attribute__((weak, used));
extern void HMAC_CTX_free(HMAC_CTX *) __attribute__((weak, used));
extern int HMAC_CTX_reset(HMAC_CTX *) __attribute__((weak, used));

/* 1.0.2 */
extern void HMAC_CTX_init(HMAC_CTX *) __attribute__((weak, used));
extern void HMAC_CTX_cleanup(HMAC_CTX *) __attribute__((weak, used));

/* common */
extern int HMAC_Update(HMAC_CTX *, const unsigned char *, size_t) __attribute__((weak, used));
extern int HMAC_Final(HMAC_CTX *, unsigned char *, unsigned int *) __attribute__((weak, used));
extern int HMAC_Init_ex(HMAC_CTX *, const void *, int, const EVP_MD *, ENGINE *) __attribute__((weak, used));

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
    HMAC_CTX *ctx = aws_mem_calloc(s_libcrypto_allocator, 1, 300);
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
    aws_mem_release(s_libcrypto_allocator, ctx);
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

#endif /* !OPENSSL_IS_AWSLC */

enum aws_libcrypto_version {
    AWS_LIBCRYPTO_NONE = 0,
    AWS_LIBCRYPTO_1_0_2,
    AWS_LIBCRYPTO_1_1_1,
    AWS_LIBCRYPTO_LC,
} s_libcrypto_version = AWS_LIBCRYPTO_NONE;

bool s_resolve_hmac_102(void *module) {
#if !defined(OPENSSL_IS_AWSLC)
    hmac_ctx_init init_fn = (hmac_ctx_init)HMAC_CTX_init;
    hmac_ctx_clean_up clean_up_fn = (hmac_ctx_clean_up)HMAC_CTX_cleanup;
    hmac_ctx_update update_fn = (hmac_ctx_update)HMAC_Update;
    hmac_ctx_final final_fn = (hmac_ctx_final)HMAC_Final;
    hmac_ctx_init_ex init_ex_fn = (hmac_ctx_init_ex)HMAC_Init_ex;

    /* were symbols bound by static linking? */
    bool has_102_symbols = init_fn && clean_up_fn && update_fn && final_fn && init_ex_fn;
    if (has_102_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static libcrypto 1.0.2 HMAC symbols");
    } else {
        /* If symbols aren't already found, try to find the requested version */
        *(void **)(&init_fn) = dlsym(module, "HMAC_CTX_init");
        *(void **)(&clean_up_fn) = dlsym(module, "HMAC_CTX_cleanup");
        *(void **)(&update_fn) = dlsym(module, "HMAC_Update");
        *(void **)(&final_fn) = dlsym(module, "HMAC_Final");
        *(void **)(&init_ex_fn) = dlsym(module, "HMAC_Init_ex");
        if (init_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic libcrypto 1.0.2 HMAC symbols");
        }
    }

    if (init_fn) {
        hmac_ctx_table.new_fn = (hmac_ctx_new)s_hmac_ctx_new;
        hmac_ctx_table.reset_fn = (hmac_ctx_reset)s_hmac_ctx_reset;
        hmac_ctx_table.free_fn = s_hmac_ctx_free;
        hmac_ctx_table.init_fn = init_fn;
        hmac_ctx_table.clean_up_fn = clean_up_fn;
        hmac_ctx_table.update_fn = update_fn;
        hmac_ctx_table.final_fn = final_fn;
        hmac_ctx_table.init_ex_fn = init_ex_fn;
        g_aws_openssl_hmac_ctx_table = &hmac_ctx_table;
        return true;
    }
#endif
    return false;
}

bool s_resolve_hmac_111(void *module) {
#if !defined(OPENSSL_IS_AWSLC)
    hmac_ctx_new new_fn = (hmac_ctx_new)HMAC_CTX_new;
    hmac_ctx_free free_fn = (hmac_ctx_free)HMAC_CTX_free;
    hmac_ctx_reset reset_fn = (hmac_ctx_reset)HMAC_CTX_reset;
    hmac_ctx_update update_fn = (hmac_ctx_update)HMAC_Update;
    hmac_ctx_final final_fn = (hmac_ctx_final)HMAC_Final;
    hmac_ctx_init_ex init_ex_fn = (hmac_ctx_init_ex)HMAC_Init_ex;

    /* were symbols bound by static linking? */
    bool has_111_symbols = new_fn && free_fn && update_fn && final_fn && init_ex_fn && reset_fn;

    if (has_111_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static libcrypto 1.1.1 HMAC symbols");
    } else {
        *(void **)(&new_fn) = dlsym(module, "HMAC_CTX_new");
        *(void **)(&reset_fn) = dlsym(module, "HMAC_CTX_reset");
        *(void **)(&free_fn) = dlsym(module, "HMAC_CTX_free");
        *(void **)(&update_fn) = dlsym(module, "HMAC_Update");
        *(void **)(&final_fn) = dlsym(module, "HMAC_Final");
        *(void **)(&init_ex_fn) = dlsym(module, "HMAC_Init_ex");
        if (new_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic libcrypto 1.1.1 HMAC symbols");
        }
    }

    if (new_fn) {
        hmac_ctx_table.new_fn = new_fn;
        hmac_ctx_table.reset_fn = reset_fn;
        hmac_ctx_table.free_fn = free_fn;
        hmac_ctx_table.init_fn = s_hmac_ctx_init_noop;
        hmac_ctx_table.clean_up_fn = s_hmac_ctx_clean_up_noop;
        hmac_ctx_table.update_fn = update_fn;
        hmac_ctx_table.final_fn = final_fn;
        hmac_ctx_table.init_ex_fn = init_ex_fn;
        g_aws_openssl_hmac_ctx_table = &hmac_ctx_table;
        return true;
    }
#endif
    return false;
}

bool s_resolve_hmac_lc(void *module) {
#if defined(OPENSSL_IS_AWSLC)
    hmac_ctx_init init_fn = (hmac_ctx_init)HMAC_CTX_init;
    hmac_ctx_clean_up clean_up_fn = (hmac_ctx_clean_up)HMAC_CTX_cleanup;
    hmac_ctx_new new_fn = (hmac_ctx_new)HMAC_CTX_new;
    hmac_ctx_free free_fn = (hmac_ctx_free)HMAC_CTX_free;
    hmac_ctx_reset reset_fn = (hmac_ctx_reset)HMAC_CTX_reset;
    hmac_ctx_update update_fn = (hmac_ctx_update)HMAC_Update;
    hmac_ctx_final final_fn = (hmac_ctx_final)HMAC_Final;
    hmac_ctx_init_ex init_ex_fn = (hmac_ctx_init_ex)HMAC_Init_ex;

    /* were symbols bound by static linking? */
    bool has_awslc_symbols = new_fn && free_fn && update_fn && final_fn && init_fn && init_ex_fn && reset_fn;

    /* If symbols aren't already found, try to find the requested version */
    /* when built as a shared lib, and multiple versions of libcrypto are possibly
     * available (e.g. brazil), select AWS-LC by default for consistency */
    if (has_awslc_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static aws-lc HMAC symbols");
    } else {
        *(void **)(&new_fn) = dlsym(module, "HMAC_CTX_new");
        *(void **)(&reset_fn) = dlsym(module, "HMAC_CTX_reset");
        *(void **)(&free_fn) = dlsym(module, "HMAC_CTX_free");
        *(void **)(&update_fn) = dlsym(module, "HMAC_Update");
        *(void **)(&final_fn) = dlsym(module, "HMAC_Final");
        *(void **)(&init_ex_fn) = dlsym(module, "HMAC_Init_ex");
        if (new_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic aws-lc HMAC symbols");
        }
    }

    if (new_fn) {
        /* Fill out the vtable for the requested version */
        hmac_ctx_table.new_fn = new_fn;
        hmac_ctx_table.reset_fn = reset_fn;
        hmac_ctx_table.free_fn = free_fn;
        hmac_ctx_table.init_fn = init_fn;
        hmac_ctx_table.clean_up_fn = clean_up_fn;
        hmac_ctx_table.update_fn = update_fn;
        hmac_ctx_table.final_fn = final_fn;
        hmac_ctx_table.init_ex_fn = init_ex_fn;
        g_aws_openssl_hmac_ctx_table = &hmac_ctx_table;
        return true;
    }
#endif
    return false;
}

static enum aws_libcrypto_version s_resolve_libcrypto_hmac(enum aws_libcrypto_version version, void *module) {
    switch (version) {
        case AWS_LIBCRYPTO_LC:
            return s_resolve_hmac_lc(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_1_1_1:
            return s_resolve_hmac_111(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_1_0_2:
            return s_resolve_hmac_102(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_NONE:
            AWS_FATAL_ASSERT(!"Attempted to resolve invalid libcrypto HMAC API version AWS_LIBCRYPTO_NONE");
    }

    return AWS_LIBCRYPTO_NONE;
}

#if !defined(OPENSSL_IS_AWSLC)
/* EVP_MD_CTX API */
/* 1.0.2 NOTE: these are macros in 1.1.x, so we have to undef them to weak link */
#    if defined(EVP_MD_CTX_create)
#        pragma push_macro("EVP_MD_CTX_create")
#        undef EVP_MD_CTX_create
#    endif
extern EVP_MD_CTX *EVP_MD_CTX_create(void) __attribute__((weak, used));
static evp_md_ctx_new s_EVP_MD_CTX_create = EVP_MD_CTX_create;
#    if defined(EVP_MD_CTX_create)
#        pragma pop_macro("EVP_MD_CTX_create")
#    endif

#    if defined(EVP_MD_CTX_destroy)
#        pragma push_macro("EVP_MD_CTX_destroy")
#        undef EVP_MD_CTX_destroy
#    endif
extern void EVP_MD_CTX_destroy(EVP_MD_CTX *) __attribute__((weak, used));
static evp_md_ctx_free s_EVP_MD_CTX_destroy = EVP_MD_CTX_destroy;
#    if defined(EVP_MD_CTX_destroy)
#        pragma pop_macro("EVP_MD_CTX_destroy")
#    endif
#endif /* !OPENSSL_IS_AWSLC */

extern EVP_MD_CTX *EVP_MD_CTX_new(void) __attribute__((weak, used));
extern void EVP_MD_CTX_free(EVP_MD_CTX *) __attribute__((weak, used));
extern int EVP_DigestInit_ex(EVP_MD_CTX *, const EVP_MD *, ENGINE *) __attribute__((weak, used));
extern int EVP_DigestUpdate(EVP_MD_CTX *, const void *, size_t) __attribute__((weak, used));
extern int EVP_DigestFinal_ex(EVP_MD_CTX *, unsigned char *, unsigned int *) __attribute__((weak, used));

bool s_resolve_md_102(void *module) {
#if !defined(OPENSSL_IS_AWSLC)
    evp_md_ctx_new md_create_fn = s_EVP_MD_CTX_create;
    evp_md_ctx_free md_destroy_fn = s_EVP_MD_CTX_destroy;
    evp_md_ctx_digest_init_ex md_init_ex_fn = EVP_DigestInit_ex;
    evp_md_ctx_digest_update md_update_fn = EVP_DigestUpdate;
    evp_md_ctx_digest_final_ex md_final_ex_fn = EVP_DigestFinal_ex;

    bool has_102_symbols = md_create_fn && md_destroy_fn && md_init_ex_fn && md_update_fn && md_final_ex_fn;

    if (has_102_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static libcrypto 1.0.2 EVP_MD symbols");
    } else {
        *(void **)(&md_create_fn) = dlsym(module, "EVP_MD_CTX_create");
        *(void **)(&md_destroy_fn) = dlsym(module, "EVP_MD_CTX_destroy");
        *(void **)(&md_init_ex_fn) = dlsym(module, "EVP_DigestInit_ex");
        *(void **)(&md_update_fn) = dlsym(module, "EVP_DigestUpdate");
        *(void **)(&md_final_ex_fn) = dlsym(module, "EVP_DigestFinal_ex");
        if (md_create_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic libcrypto 1.0.2 EVP_MD symbols");
        }
    }

    if (md_create_fn) {
        evp_md_ctx_table.new_fn = md_create_fn;
        evp_md_ctx_table.free_fn = md_destroy_fn;
        evp_md_ctx_table.init_ex_fn = md_init_ex_fn;
        evp_md_ctx_table.update_fn = md_update_fn;
        evp_md_ctx_table.final_ex_fn = md_final_ex_fn;
        g_aws_openssl_evp_md_ctx_table = &evp_md_ctx_table;
        return true;
    }
#endif
    return false;
}

bool s_resolve_md_111(void *module) {
#if !defined(OPENSSL_IS_AWSLC)
    evp_md_ctx_new md_new_fn = EVP_MD_CTX_new;
    evp_md_ctx_free md_free_fn = EVP_MD_CTX_free;
    evp_md_ctx_digest_init_ex md_init_ex_fn = EVP_DigestInit_ex;
    evp_md_ctx_digest_update md_update_fn = EVP_DigestUpdate;
    evp_md_ctx_digest_final_ex md_final_ex_fn = EVP_DigestFinal_ex;

    bool has_111_symbols = md_new_fn && md_free_fn && md_init_ex_fn && md_update_fn && md_final_ex_fn;
    if (has_111_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static libcrypto 1.1.1 EVP_MD symbols");
    } else {
        *(void **)(&md_new_fn) = dlsym(module, "EVP_MD_CTX_new");
        *(void **)(&md_free_fn) = dlsym(module, "EVP_MD_CTX_free");
        *(void **)(&md_init_ex_fn) = dlsym(module, "EVP_DigestInit_ex");
        *(void **)(&md_update_fn) = dlsym(module, "EVP_DigestUpdate");
        *(void **)(&md_final_ex_fn) = dlsym(module, "EVP_DigestFinal_ex");
        if (md_new_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic libcrypto 1.1.1 EVP_MD symbols");
        }
    }

    if (md_new_fn) {
        evp_md_ctx_table.new_fn = md_new_fn;
        evp_md_ctx_table.free_fn = md_free_fn;
        evp_md_ctx_table.init_ex_fn = md_init_ex_fn;
        evp_md_ctx_table.update_fn = md_update_fn;
        evp_md_ctx_table.final_ex_fn = md_final_ex_fn;
        g_aws_openssl_evp_md_ctx_table = &evp_md_ctx_table;
        return true;
    }
#endif
    return false;
}

bool s_resolve_md_lc(void *module) {
#if defined(OPENSSL_IS_AWSLC)
    evp_md_ctx_new md_new_fn = EVP_MD_CTX_new;
    evp_md_ctx_new md_create_fn = EVP_MD_CTX_new;
    evp_md_ctx_free md_free_fn = EVP_MD_CTX_free;
    evp_md_ctx_free md_destroy_fn = EVP_MD_CTX_destroy;
    evp_md_ctx_digest_init_ex md_init_ex_fn = EVP_DigestInit_ex;
    evp_md_ctx_digest_update md_update_fn = EVP_DigestUpdate;
    evp_md_ctx_digest_final_ex md_final_ex_fn = EVP_DigestFinal_ex;

    bool has_awslc_symbols =
        md_new_fn && md_create_fn && md_free_fn && md_destroy_fn && md_init_ex_fn && md_update_fn && md_final_ex_fn;

    if (has_awslc_symbols) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found static aws-lc libcrypto 1.1.1 EVP_MD symbols");
    } else {
        *(void **)(&md_new_fn) = dlsym(module, "EVP_MD_CTX_new");
        *(void **)(&md_free_fn) = dlsym(module, "EVP_MD_CTX_free");
        *(void **)(&md_init_ex_fn) = dlsym(module, "EVP_DigestInit_ex");
        *(void **)(&md_update_fn) = dlsym(module, "EVP_DigestUpdate");
        *(void **)(&md_final_ex_fn) = dlsym(module, "EVP_DigestFinal_ex");
        if (md_new_fn) {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "found dynamic aws-lc libcrypto 1.1.1 EVP_MD symbols");
        }
    }

    if (md_new_fn) {
        /* Add the found symbols to the vtable */
        evp_md_ctx_table.new_fn = md_new_fn;
        evp_md_ctx_table.free_fn = md_free_fn;
        evp_md_ctx_table.init_ex_fn = md_init_ex_fn;
        evp_md_ctx_table.update_fn = md_update_fn;
        evp_md_ctx_table.final_ex_fn = md_final_ex_fn;
        g_aws_openssl_evp_md_ctx_table = &evp_md_ctx_table;
        return true;
    }
#endif
    return false;
}

static enum aws_libcrypto_version s_resolve_libcrypto_md(enum aws_libcrypto_version version, void *module) {
    switch (version) {
        case AWS_LIBCRYPTO_LC:
            return s_resolve_md_lc(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_1_1_1:
            return s_resolve_md_111(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_1_0_2:
            return s_resolve_md_102(module) ? version : AWS_LIBCRYPTO_NONE;
        case AWS_LIBCRYPTO_NONE:
            AWS_FATAL_ASSERT(!"Attempted to resolve invalid libcrypto MD API version AWS_LIBCRYPTO_NONE");
    }

    return AWS_LIBCRYPTO_NONE;
}

static enum aws_libcrypto_version s_resolve_libcrypto_symbols(enum aws_libcrypto_version version, void *module) {
    enum aws_libcrypto_version found_version = s_resolve_libcrypto_hmac(version, module);
    if (found_version == AWS_LIBCRYPTO_NONE) {
        return AWS_LIBCRYPTO_NONE;
    }
    found_version = s_resolve_libcrypto_md(found_version, module);
    if (found_version == AWS_LIBCRYPTO_NONE) {
        return AWS_LIBCRYPTO_NONE;
    }
    return found_version;
}

static enum aws_libcrypto_version s_resolve_libcrypto_lib(void) {
    const char *libcrypto_102 = "libcrypto.so.1.0.0";
    const char *libcrypto_111 = "libcrypto.so.1.1";

    AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "loading libcrypto 1.0.2");
    void *module = dlopen(libcrypto_102, RTLD_NOW);
    if (module) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "resolving against libcrypto 1.0.2");
        enum aws_libcrypto_version result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_0_2, module);
        if (result == AWS_LIBCRYPTO_1_0_2) {
            return result;
        }
        dlclose(module);
    } else {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "libcrypto 1.0.2 not found");
    }

    AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "loading libcrypto 1.1.1");
    module = dlopen(libcrypto_111, RTLD_NOW);
    if (module) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "resolving against libcrypto 1.1.1");
        enum aws_libcrypto_version result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_1_1, module);
        if (result == AWS_LIBCRYPTO_1_1_1) {
            return result;
        }
        dlclose(module);
    } else {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "libcrypto 1.1.1 not found");
    }

    AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "loading libcrypto.so");
    module = dlopen("libcrypto.so", RTLD_NOW);
    if (module) {
        unsigned long (*openssl_version_num)(void) = NULL;
        *(void **)(&openssl_version_num) = dlsym(module, "OpenSSL_version_num");
        if (openssl_version_num) {
            unsigned long version = openssl_version_num();
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "libcrypto.so reported version is 0x%lx", version);
            enum aws_libcrypto_version result = AWS_LIBCRYPTO_NONE;
            if (version >= 0x10101000L) {
                AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "probing libcrypto.so for aws-lc symbols");
                result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_LC, module);
                if (result == AWS_LIBCRYPTO_NONE) {
                    AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "probing libcrypto.so for 1.1.1 symbols");
                    result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_1_1, module);
                }
            } else if (version >= 0x10002000L) {
                AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "probing libcrypto.so for 1.0.2 symbols");
                result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_0_2, module);
            } else {
                AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "libcrypto.so reported version is unsupported");
            }
            if (result != AWS_LIBCRYPTO_NONE) {
                return result;
            }
        } else {
            AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "Unable to determine version of libcrypto.so");
        }
        dlclose(module);
    } else {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "libcrypto.so not found");
    }

    return AWS_LIBCRYPTO_NONE;
}

static void *s_libcrypto_module = NULL;

static enum aws_libcrypto_version s_resolve_libcrypto(void) {
    if (s_libcrypto_version != AWS_LIBCRYPTO_NONE) {
        return s_libcrypto_version;
    }

    /* Try to auto-resolve against what's linked in/process space */
    AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "searching process and loaded modules");
    void *process = dlopen(NULL, RTLD_NOW);
#if 0
    // dlopen is not supported in musl. It's ok to pass NULL to s_resolve_libcrypto_symbols,
    // as dlsym handles it well according to man.
    AWS_FATAL_ASSERT(process && "Unable to load symbols from process space");
#endif
    enum aws_libcrypto_version result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_LC, process);
    if (result == AWS_LIBCRYPTO_NONE) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "did not find aws-lc symbols linked");
        result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_0_2, process);
    }
    if (result == AWS_LIBCRYPTO_NONE) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "did not find libcrypto 1.0.2 symbols linked");
        result = s_resolve_libcrypto_symbols(AWS_LIBCRYPTO_1_1_1, process);
    }
    dlclose(process);

    if (result == AWS_LIBCRYPTO_NONE) {
        AWS_LOGF_DEBUG(AWS_LS_CAL_LIBCRYPTO_RESOLVE, "did not find libcrypto 1.1.1 symbols linked");
        AWS_LOGF_DEBUG(
            AWS_LS_CAL_LIBCRYPTO_RESOLVE,
            "libcrypto symbols were not statically linked, searching for shared libraries");
        result = s_resolve_libcrypto_lib();
    }

    return result;
}

/* Ignore warnings about how CRYPTO_get_locking_callback() always returns NULL on 1.1.1 */
#if !defined(__GNUC__) || (__GNUC__ * 100 + __GNUC_MINOR__ * 10 > 410)
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Waddress"
#endif

/* Openssl 1.0.x requires special handling for its locking callbacks or else it's not thread safe */
#if !defined(OPENSSL_IS_AWSLC)
static struct aws_mutex *s_libcrypto_locks = NULL;

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
#endif

void aws_cal_platform_init(struct aws_allocator *allocator) {
    int version = s_resolve_libcrypto();
    AWS_FATAL_ASSERT(version != AWS_LIBCRYPTO_NONE && "libcrypto could not be resolved");
    AWS_FATAL_ASSERT(g_aws_openssl_evp_md_ctx_table);
    AWS_FATAL_ASSERT(g_aws_openssl_hmac_ctx_table);

    s_libcrypto_allocator = allocator;

#if !defined(OPENSSL_IS_AWSLC)
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
#endif
}

void aws_cal_platform_clean_up(void) {
#if !defined(OPENSSL_IS_AWSLC)
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
#endif

    if (s_libcrypto_module) {
        dlclose(s_libcrypto_module);
    }

    s_libcrypto_allocator = NULL;
}
#if !defined(__GNUC__) || (__GNUC__ >= 4 && __GNUC_MINOR__ > 1)
#    pragma GCC diagnostic pop
#endif
