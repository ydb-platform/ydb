/* Copyright 2014 Donald Stufft and individual contributors
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

static const int PYNACL_HAS_CRYPTO_PWHASH_SCRYPTSALSA208SHA256;

size_t crypto_pwhash_scryptsalsa208sha256_saltbytes(void);
size_t crypto_pwhash_scryptsalsa208sha256_strbytes(void);
size_t crypto_pwhash_scryptsalsa208sha256_bytes_min(void);
size_t crypto_pwhash_scryptsalsa208sha256_bytes_max(void);
size_t crypto_pwhash_scryptsalsa208sha256_passwd_min(void);
size_t crypto_pwhash_scryptsalsa208sha256_passwd_max(void);
size_t crypto_pwhash_scryptsalsa208sha256_opslimit_min(void);
size_t crypto_pwhash_scryptsalsa208sha256_opslimit_max(void);
size_t crypto_pwhash_scryptsalsa208sha256_memlimit_min(void);
size_t crypto_pwhash_scryptsalsa208sha256_memlimit_max(void);
size_t crypto_pwhash_scryptsalsa208sha256_opslimit_interactive(void);
size_t crypto_pwhash_scryptsalsa208sha256_memlimit_interactive(void);
size_t crypto_pwhash_scryptsalsa208sha256_opslimit_sensitive(void);
size_t crypto_pwhash_scryptsalsa208sha256_memlimit_sensitive(void);

const char *crypto_pwhash_scryptsalsa208sha256_strprefix(void);

int crypto_pwhash_scryptsalsa208sha256_ll(const uint8_t * const passwd,
                                           size_t passwdlen,
                                           const uint8_t * salt,
                                           size_t saltlen,
                                           uint64_t N, uint32_t r, uint32_t p,
                                           uint8_t * buf, size_t buflen);

/* #define crypto_pwhash_scryptsalsa208sha256_STRBYTES 102 */
int crypto_pwhash_scryptsalsa208sha256_str(char out[102],
                                            const char * const passwd,
                                            unsigned long long passwdlen,
                                            unsigned long long opslimit,
                                            size_t memlimit);

int crypto_pwhash_scryptsalsa208sha256_str_verify(const char str[102],
                                                   const char * const passwd,
                                                   unsigned long long passwdlen);

/*
 *  argon2 bindings
 */

/*
 *  general argon2 limits
 */

size_t crypto_pwhash_strbytes(void);
size_t crypto_pwhash_saltbytes(void);
size_t crypto_pwhash_bytes_min(void);
size_t crypto_pwhash_bytes_max(void);
size_t crypto_pwhash_passwd_min(void);
size_t crypto_pwhash_passwd_max(void);

/*
 *  available algorithms identifiers
 */

int crypto_pwhash_alg_default(void);
int crypto_pwhash_alg_argon2i13(void);
int crypto_pwhash_alg_argon2id13(void);

/*
 *  argon2i recommended limits
 */

size_t crypto_pwhash_argon2i_memlimit_interactive(void);
size_t crypto_pwhash_argon2i_memlimit_moderate(void);
size_t crypto_pwhash_argon2i_memlimit_sensitive(void);
size_t crypto_pwhash_argon2i_memlimit_min(void);
size_t crypto_pwhash_argon2i_memlimit_max(void);
size_t crypto_pwhash_argon2i_opslimit_min(void);
size_t crypto_pwhash_argon2i_opslimit_max(void);
size_t crypto_pwhash_argon2i_opslimit_interactive(void);
size_t crypto_pwhash_argon2i_opslimit_moderate(void);
size_t crypto_pwhash_argon2i_opslimit_sensitive(void);

/*
 *  argon2id recommended limits
 */

size_t crypto_pwhash_argon2id_memlimit_interactive(void);
size_t crypto_pwhash_argon2id_memlimit_moderate(void);
size_t crypto_pwhash_argon2id_memlimit_sensitive(void);
size_t crypto_pwhash_argon2id_memlimit_min(void);
size_t crypto_pwhash_argon2id_memlimit_max(void);
size_t crypto_pwhash_argon2id_opslimit_min(void);
size_t crypto_pwhash_argon2id_opslimit_max(void);
size_t crypto_pwhash_argon2id_opslimit_interactive(void);
size_t crypto_pwhash_argon2id_opslimit_moderate(void);
size_t crypto_pwhash_argon2id_opslimit_sensitive(void);

/*
 *  libsodium's default argon2 algorithm recommended limits
 */

size_t crypto_pwhash_memlimit_interactive(void);
size_t crypto_pwhash_memlimit_moderate(void);
size_t crypto_pwhash_memlimit_sensitive(void);
size_t crypto_pwhash_memlimit_min(void);
size_t crypto_pwhash_memlimit_max(void);
size_t crypto_pwhash_opslimit_min(void);
size_t crypto_pwhash_opslimit_max(void);
size_t crypto_pwhash_opslimit_interactive(void);
size_t crypto_pwhash_opslimit_moderate(void);
size_t crypto_pwhash_opslimit_sensitive(void);

/*
 * Modular crypt string prefix for implemented argon2 constructions
 */

const char *crypto_pwhash_argon2i_strprefix(void);
const char *crypto_pwhash_argon2id_strprefix(void);

/*
 *  crypto_pwhash raw constructs
 */

int crypto_pwhash(unsigned char * const out, unsigned long long outlen,
		  const char * const passwd, unsigned long long passwdlen,
		  const unsigned char * const salt,
		  unsigned long long opslimit, size_t memlimit, int alg);

/*
 *  #define crypto_pwhash_argon2i_STRBYTES 128U
 *  #define crypto_pwhash_STRBYTES crypto_pwhash_argon2i_STRBYTES
 */

int crypto_pwhash_str_alg(char out[128],
                          const char * const passwd,
                          unsigned long long passwdlen,
                          unsigned long long opslimit,
                          size_t memlimit,
                          int alg);

int crypto_pwhash_str_verify(const char str[128],
                             const char * const passwd,
                             unsigned long long passwdlen);

int crypto_pwhash_str_needs_rehash(const char str[128],
                                   unsigned long long opslimit, size_t memlimit);
