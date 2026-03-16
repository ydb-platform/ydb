#ifndef MS_COMMON_H
#define MS_COMMON_H

#ifdef __GNUC__
#define MS_LIKELY(pred) __builtin_expect(!!(pred), 1)
#define MS_UNLIKELY(pred) __builtin_expect(!!(pred), 0)
#else
#define MS_LIKELY(pred) (pred)
#define MS_UNLIKELY(pred) (pred)
#endif

#ifdef __GNUC__
#define MS_INLINE __attribute__((always_inline)) inline
#define MS_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define MS_INLINE __forceinline
#define MS_NOINLINE __declspec(noinline)
#else
#define MS_INLINE inline
#define MS_NOINLINE
#endif

#endif
