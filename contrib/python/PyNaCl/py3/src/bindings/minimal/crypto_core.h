/* Copyright 2020 Donald Stufft and individual contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef SODIUM_LIBRARY_MINIMAL
static const int PYNACL_HAS_CRYPTO_CORE_ED25519 = 0;

size_t (*crypto_core_ed25519_bytes)() = NULL;
size_t (*crypto_core_ed25519_scalarbytes)(void) = NULL;
size_t (*crypto_core_ed25519_nonreducedscalarbytes)(void) = NULL;

int (*crypto_core_ed25519_is_valid_point)(const unsigned char *) = NULL;
int (*crypto_core_ed25519_add)(unsigned char *, const unsigned char *, const unsigned char *) = NULL;
int (*crypto_core_ed25519_sub)(unsigned char *, const unsigned char *, const unsigned char *) = NULL;

int (*crypto_core_ed25519_scalar_invert)(unsigned char *, const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_negate)(unsigned char *, const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_complement)(unsigned char *, const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_add)(unsigned char *, const unsigned char *,
                                       const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_sub)(unsigned char *, const unsigned char *,
                                       const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_mul)(unsigned char *, const unsigned char *,
                                       const unsigned char *) = NULL;
void (*crypto_core_ed25519_scalar_reduce)(unsigned char *, const unsigned char *) = NULL;
#else
static const int PYNACL_HAS_CRYPTO_CORE_ED25519 = 1;
#endif
