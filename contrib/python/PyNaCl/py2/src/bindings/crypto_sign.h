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

size_t crypto_sign_bytes();
// size_t crypto_sign_seedbytes();
size_t crypto_sign_publickeybytes();
size_t crypto_sign_secretkeybytes();
size_t crypto_sign_ed25519ph_statebytes();


int crypto_sign_keypair(unsigned char *pk, unsigned char *sk);

int crypto_sign_seed_keypair(unsigned char *pk, unsigned char *sk,
                       const unsigned char *seed);

int crypto_sign(unsigned char *sm, unsigned long long *smlen,
          const unsigned char *m,  unsigned long long mlen,
          const unsigned char *sk);

int crypto_sign_open(unsigned char *m,  unsigned long long *mlen,
               const unsigned char *sm, unsigned long long smlen,
               const unsigned char *pk);

int crypto_sign_ed25519_pk_to_curve25519(unsigned char *curve25519_pk,
                                         const unsigned char *ed25519_pk);

int crypto_sign_ed25519_sk_to_curve25519(unsigned char *curve25519_sk,
                                         const unsigned char *ed25519_sk);

/*
 * We use crypto_sign_ed25519ph_state * as
 * a pointer to a opaque buffer,
 * therefore the following typedef makes sense:
 */

typedef void crypto_sign_ed25519ph_state;

int crypto_sign_ed25519ph_init(crypto_sign_ed25519ph_state *state);

int crypto_sign_ed25519ph_update(crypto_sign_ed25519ph_state *state,
                                 const unsigned char *m,
				 unsigned long long mlen);

int crypto_sign_ed25519ph_final_create(crypto_sign_ed25519ph_state *state,
                                       unsigned char *sig,
                                       unsigned long long *siglen_p,
                                       const unsigned char *sk);

int crypto_sign_ed25519ph_final_verify(crypto_sign_ed25519ph_state *state,
                                       unsigned char *sig,
                                       const unsigned char *pk);
