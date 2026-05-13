#include <contrib/libs/openssl/redef.h>
/* WARNING: do not edit! */
/*
 * Copyright 2016 The OpenSSL Project Authors. All Rights Reserved.
 *
 * Licensed under the OpenSSL license (the "License").  You may not use
 * this file except in compliance with the License.  You can obtain a copy
 * in the file LICENSE in the source distribution or at
 * https://www.openssl.org/source/license.html
 */

#ifndef OSSL_CRYPTO_BN_CONF_H
# define OSSL_CRYPTO_BN_CONF_H

/* WASM32/Emscripten LP32: sizeof(long)==4, not 8. */

#undef SIXTY_FOUR_BIT_LONG
#define SIXTY_FOUR_BIT
#undef THIRTY_TWO_BIT

#endif
