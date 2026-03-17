/*
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Simple macros to quickly compute a hash value from a string.
 *
 */

#ifndef PMIX_HASH_STRING_H
#define PMIX_HASH_STRING_H

/**
 *  Compute the hash value and the string length simultaneously
 *
 *  @param str (IN)     The string which will be parsed   (char*)
 *  @param hash (OUT)   Where the hash value will be stored (uint32_t)
 *  @param length (OUT) The computed length of the string (uint32_t)
 */
#define PMIX_HASH_STRLEN( str, hash, length ) \
    do {                                      \
        register const char *_str = (str);    \
        register uint32_t    _hash = 0;       \
        register uint32_t    _len = 0;        \
                                              \
        while( *_str ) {                      \
            _len++;                           \
            _hash += *_str++;                 \
            _hash += (_hash << 10);           \
            _hash ^= (_hash >> 6);            \
        }                                     \
                                              \
        _hash += (_hash << 3);                \
        _hash ^= (_hash >> 11);               \
        (hash) = (_hash + (_hash << 15));     \
        (length)  = _len;                     \
    } while (0)

/**
 *  Compute the hash value
 *
 *  @param str (IN)     The string which will be parsed   (char*)
 *  @param hash (OUT)   Where the hash value will be stored (uint32_t)
 */
#define PMIX_HASH_STR( str, hash )            \
    do {                                      \
        register const char *_str = (str);    \
        register uint32_t    _hash = 0;       \
                                              \
        while( *_str ) {                      \
            _hash += *_str++;                 \
            _hash += (_hash << 10);           \
            _hash ^= (_hash >> 6);            \
        }                                     \
                                              \
        _hash += (_hash << 3);                \
        _hash ^= (_hash >> 11);               \
        (hash) = (_hash + (_hash << 15));     \
    } while (0)

#endif  /* PMIX_HASH_STRING_H */
