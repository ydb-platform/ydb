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
static const int PYNACL_HAS_CRYPTO_SHORTHASH_SIPHASHX24 = 0;

size_t (*crypto_shorthash_siphashx24_bytes)() = NULL;
size_t (*crypto_shorthash_siphashx24_keybytes)() = NULL;

int (*crypto_shorthash_siphashx24)(unsigned char *, const unsigned char *,
                                   unsigned long long, const unsigned char *) = NULL;
#else
static const int PYNACL_HAS_CRYPTO_SHORTHASH_SIPHASHX24 = 1;
#endif
