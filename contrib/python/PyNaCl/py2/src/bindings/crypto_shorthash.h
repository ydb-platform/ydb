/* Copyright 2013 Donald Stufft and individual contributors
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

static const int PYNACL_HAS_CRYPTO_SHORTHASH_SIPHASHX24;

// size_t crypto_hash_bytes();

size_t crypto_shorthash_siphash24_bytes();
size_t crypto_shorthash_siphash24_keybytes();

int crypto_shorthash_siphash24(unsigned char *out, const unsigned char *in,
                               unsigned long long inlen, const unsigned char *k);

size_t crypto_shorthash_siphashx24_bytes();
size_t crypto_shorthash_siphashx24_keybytes();

int crypto_shorthash_siphashx24(unsigned char *out, const unsigned char *in,
                                unsigned long long inlen, const unsigned char *k);

