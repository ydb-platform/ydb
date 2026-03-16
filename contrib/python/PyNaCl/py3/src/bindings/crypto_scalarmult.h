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

static const int PYNACL_HAS_CRYPTO_SCALARMULT_ED25519;

size_t crypto_scalarmult_bytes();
size_t crypto_scalarmult_scalarbytes();
size_t crypto_scalarmult_ed25519_bytes();
size_t crypto_scalarmult_ed25519_scalarbytes();

int crypto_scalarmult_base(unsigned char *q, const unsigned char *n);
int crypto_scalarmult(unsigned char *q, const unsigned char *n, const unsigned char *p);
int crypto_scalarmult_ed25519(unsigned char *q, const unsigned char *n, const unsigned char *p);
int crypto_scalarmult_ed25519_base(unsigned char *q, const unsigned char *n);
int crypto_scalarmult_ed25519_noclamp(unsigned char *q, const unsigned char *n,
                                      const unsigned char *p);
int crypto_scalarmult_ed25519_base_noclamp(unsigned char *q, const unsigned char *n);
