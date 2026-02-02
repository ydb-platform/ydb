#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __has_feature
#   define VHD_HAS_FEATURE(x) __has_feature(x)
#else
#   define VHD_HAS_FEATURE(x) 0
#endif

#define HUGE_PAGE_SIZE 0x40000000 // 1G, works also for 2M pages alignment

/*////////////////////////////////////////////////////////////////////////////*/

#if !defined(NDEBUG)
#   define VHD_DEBUG
#endif

/*////////////////////////////////////////////////////////////////////////////*/

#if !defined(containerof)
#   define containerof(ptr, type, member) \
    ((type *) ((char *)(ptr) - offsetof(type, member)))
#endif

#if !defined(countof)
#   define countof(a) (sizeof(a) / sizeof(*a))
#endif

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

#ifdef __cplusplus
#   define VHD_STATIC_ASSERT(pred) static_assert((pred), __STRINGIFY(pred))
#elif (__STDC_VERSION__ >= 201112L)
#   define VHD_STATIC_ASSERT(pred)  _Static_assert((pred), __STRINGIFY(pred))
#else
#   error Implement me
#endif

/* TODO: compiler-specifics for non-gcc? */
#ifdef __GNUC__
#   define __STRINGIFY(x)           #x
#   define VHD_NORETURN             __attribute__((noreturn))
#   define VHD_TYPEOF               __typeof
#   define VHD_PACKED               __attribute__((packed))

#define VHD_ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))

/* Return 0-based index of first least significant bit set in 32-bit value */
static inline int vhd_find_first_bit32(uint32_t val)
{
    VHD_STATIC_ASSERT(sizeof(val) == sizeof(int));
    return __builtin_ctz(val);
}

/* Return 0-based index of first least significant bit set in 64-bit value */
static inline int vhd_find_first_bit64(uint64_t val)
{
    VHD_STATIC_ASSERT(sizeof(val) == sizeof(long long));
    return __builtin_ctzll(val);
}

#else
#   error Implement me
#endif

/*
 * MIN/MAX implementations with intuitive behavior:
 * - type safety
 * - exactly-once evaluation of both arguments
 * Note: unsuitable in constant expressions
 */
#define __safe_cmp(a, op, b)                             \
    ({                                                  \
        typeof(1 ? (a) : (b)) _a = (a), _b = (b);       \
        _a op _b ? _a : _b;                              \
    })

#undef MIN
#define MIN(a, b)       __safe_cmp(a, <, b)
#undef MAX
#define MAX(a, b)       __safe_cmp(a, >, b)

/*////////////////////////////////////////////////////////////////////////////*/

static inline void VHD_NORETURN _vhd_verify_helper(
    const char *what,
    const char *file,
    unsigned long line)
{
    /* TODO: smarter logging */
    fprintf(stderr, "Verify failed: \"%s\" at %s:%lu\n", what, file, line);
    abort();
}

#define VHD_ASSERT(cond) assert(cond)
#define VHD_UNREACHABLE() __builtin_unreachable()

/* Verify is not compiled out in release builds */
#define VHD_VERIFY(cond)                                  \
    do {                                                  \
        if (!(cond)) {                                    \
            _vhd_verify_helper(#cond, __FILE__, __LINE__); \
        }                                                 \
    } while (0)

/*////////////////////////////////////////////////////////////////////////////*/

#ifdef VHD_MEMCHECK
#   include <valgrind/memcheck.h>
#   define VHD_MEMCHECK_DEFINED(addr, len)   \
    VALGRIND_MAKE_MEM_DEFINED(addr, len)
#   define VHD_MEMCHECK_UNDEFINED(addr, len) \
    VALGRIND_MAKE_MEM_UNDEFINED(addr, len)
#else
#   define VHD_MEMCHECK_DEFINED(addr, len)
#   define VHD_MEMCHECK_UNDEFINED(addr, len)
#endif

/*////////////////////////////////////////////////////////////////////////////*/

#define VHD_ALIGN_UP(x, a) ({ \
    VHD_TYPEOF(x) __mask = (VHD_TYPEOF(x))(a) - 1; \
    ((x) + __mask) & ~__mask; \
})
#define VHD_ALIGN_DOWN(x, a)    ((x) & ~((VHD_TYPEOF(x))(a) - 1))
#define VHD_IS_ALIGNED(x, a)    (!((x) & ((VHD_TYPEOF(x))(a) - 1)))
#define VHD_ALIGN_PTR_UP(x, a)  (VHD_TYPEOF(x))VHD_ALIGN_UP((uintptr_t)x, a)

static inline void *vhd_alloc(size_t bytes)
{
    /* malloc actually accepts 0 sizes, but this is still most likely a bug.. */
    VHD_ASSERT(bytes != 0);

    void *p = malloc(bytes);
    VHD_VERIFY(p != NULL);
    return p;
}

static inline void *vhd_zalloc(size_t bytes)
{
    /* calloc actually accepts 0 sizes, but this is still most likely a bug.. */
    VHD_ASSERT(bytes != 0);

    void *p = calloc(bytes, 1);
    VHD_VERIFY(p != NULL);
    return p;
}

static inline void *vhd_calloc(size_t nmemb, size_t size)
{
    VHD_ASSERT(nmemb != 0 && size != 0);

    void *p = calloc(nmemb, size);
    VHD_VERIFY(p != NULL);
    return p;
}

/* TODO: aligned alloc */

static inline void vhd_free(void *p)
{
    free(p);
}

static inline char *vhd_strdup(const char *s) __attribute__((malloc));
static inline char *vhd_strdup(const char *s)
{
    size_t len;
    char *t;

    if (!s) {
        return NULL;
    }

    len = strlen(s) + 1;
    t = (char *)vhd_alloc(len);
    memcpy(t, s, len);
    return t;
}

static inline char *vhd_strdup_printf(const char *fmt, ...)
    __attribute__((format(printf, 1, 2), malloc));
static inline char *vhd_strdup_printf(const char *fmt, ...)
{
    int len;
    size_t size;
    char *ret;
    va_list args;

    va_start(args, fmt);
    len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);

    if (len < 0) {
        return NULL;
    }

    size = (size_t)len + 1;
    ret = (char *)vhd_alloc(size);

    va_start(args, fmt);
    len = vsnprintf(ret, size, fmt, args);
    va_end(args);

    if (len < 0) {
        vhd_free(ret);
        return NULL;
    }
    return ret;
}

int init_platform_page_size(void);

extern size_t platform_page_size;

#ifdef __cplusplus
}
#endif
