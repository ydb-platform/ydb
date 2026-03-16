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

size_t crypto_box_secretkeybytes();
size_t crypto_box_publickeybytes();
size_t crypto_box_seedbytes();
size_t crypto_box_zerobytes();
size_t crypto_box_boxzerobytes();
size_t crypto_box_noncebytes();
size_t crypto_box_beforenmbytes();
size_t crypto_box_sealbytes();


int crypto_box_keypair(unsigned char *pk, unsigned char *sk);

int crypto_box_seed_keypair(unsigned char *pk, unsigned char *sk,
                            const unsigned char *seed);

int crypto_box(unsigned char *c,        const unsigned char *m,
               unsigned long long mlen, const unsigned char *n,
         const unsigned char *pk,       const unsigned char *sk);

int crypto_box_open(unsigned char *m,        const unsigned char *c,
                    unsigned long long clen, const unsigned char *n,
              const unsigned char *pk,       const unsigned char *sk);

int crypto_box_beforenm(unsigned char *k, const unsigned char *pk,
                  const unsigned char *sk);

int crypto_box_afternm(unsigned char *c,        const unsigned char *m,
                       unsigned long long mlen, const unsigned char *n,
                 const unsigned char *k);

int crypto_box_open_afternm(unsigned char *m,        const unsigned char *c,
                            unsigned long long clen, const unsigned char *n,
                      const unsigned char *k);

int crypto_box_seal(unsigned char *c, const unsigned char *m,
                    unsigned long long mlen, const unsigned char *pk);

int crypto_box_seal_open(unsigned char *m, const unsigned char *c,
                         unsigned long long clen,
                         const unsigned char *pk, const unsigned char *sk);
