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
static const int PYNACL_HAS_CRYPTO_PWHASH_SCRYPTSALSA208SHA256 = 0;

size_t (*crypto_pwhash_scryptsalsa208sha256_saltbytes)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_strbytes)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_bytes_min)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_bytes_max)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_passwd_min)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_passwd_max)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_opslimit_min)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_opslimit_max)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_memlimit_min)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_memlimit_max)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_opslimit_interactive)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_memlimit_interactive)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_opslimit_sensitive)(void) = NULL;
size_t (*crypto_pwhash_scryptsalsa208sha256_memlimit_sensitive)(void) = NULL;

const char *(*crypto_pwhash_scryptsalsa208sha256_strprefix)(void) = NULL;

int (*crypto_pwhash_scryptsalsa208sha256_ll)(const uint8_t * const,
                                              size_t,
                                              const uint8_t *,
                                              size_t,
                                              uint64_t, uint32_t, uint32_t,
                                              uint8_t *, size_t) = NULL;

/* #define crypto_pwhash_scryptsalsa208sha256_STRBYTES 102 */
int (*crypto_pwhash_scryptsalsa208sha256_str)(char [102],
                                               const char * const,
                                               unsigned long long,
                                               unsigned long long,
                                               size_t) = NULL;

int (*crypto_pwhash_scryptsalsa208sha256_str_verify)(const char [102],
                                                      const char * const,
                                                      unsigned long long) = NULL;
#else
static const int PYNACL_HAS_CRYPTO_PWHASH_SCRYPTSALSA208SHA256 = 1;
#endif
