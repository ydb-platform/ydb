/* Copyright 2018 Donald Stufft and individual contributors
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

size_t crypto_kx_publickeybytes(void);
size_t crypto_kx_secretkeybytes(void);
size_t crypto_kx_seedbytes(void);
size_t crypto_kx_sessionkeybytes(void);

int crypto_kx_seed_keypair(unsigned char pk[32],
                           unsigned char sk[32],
                           const unsigned char seed[32]);

int crypto_kx_keypair(unsigned char pk[32],
                      unsigned char sk[32]);

int crypto_kx_client_session_keys(unsigned char rx[32],
                                  unsigned char tx[32],
                                  const unsigned char client_pk[32],
                                  const unsigned char client_sk[32],
                                  const unsigned char server_pk[32]);

int crypto_kx_server_session_keys(unsigned char rx[32],
                                  unsigned char tx[32],
                                  const unsigned char server_pk[32],
                                  const unsigned char server_sk[32],
                                  const unsigned char client_pk[32]);
