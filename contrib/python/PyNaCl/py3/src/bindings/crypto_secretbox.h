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

size_t crypto_secretbox_keybytes();
size_t crypto_secretbox_noncebytes();
size_t crypto_secretbox_zerobytes();
size_t crypto_secretbox_boxzerobytes();
size_t crypto_secretbox_macbytes();
size_t crypto_secretbox_messagebytes_max();


int crypto_secretbox(unsigned char *c,        const unsigned char *m,
                     unsigned long long mlen, const unsigned char *n,
               const unsigned char *k);

int crypto_secretbox_open(unsigned char *m,        const unsigned char *c,
                          unsigned long long clen, const unsigned char *n,
                    const unsigned char *k);
