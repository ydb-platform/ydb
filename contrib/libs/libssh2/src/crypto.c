/* Copyright (C) Viktor Szakats
 *
 * SPDX-License-Identifier: BSD-3-Clause
 */

#define LIBSSH2_CRYPTO_C
#include "libssh2_priv.h"

#if defined(LIBSSH2_OPENSSL) || defined(LIBSSH2_WOLFSSL)
#include "openssl.c"
#elif defined(LIBSSH2_LIBGCRYPT)
#error #include "libgcrypt.c"
#elif defined(LIBSSH2_MBEDTLS)
#error #include "mbedtls.c"
#elif defined(LIBSSH2_OS400QC3)
#error #include "os400qc3.c"
#elif defined(LIBSSH2_WINCNG)
#error #include "wincng.c"
#endif
