/* Copyright 2017 Donald Stufft and individual contributors
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

size_t crypto_aead_chacha20poly1305_ietf_keybytes(void);
size_t crypto_aead_chacha20poly1305_ietf_nsecbytes(void);
size_t crypto_aead_chacha20poly1305_ietf_npubbytes(void);
size_t crypto_aead_chacha20poly1305_ietf_abytes(void);
size_t crypto_aead_chacha20poly1305_ietf_messagebytes_max(void);

size_t crypto_aead_chacha20poly1305_keybytes(void);
size_t crypto_aead_chacha20poly1305_nsecbytes(void);
size_t crypto_aead_chacha20poly1305_npubbytes(void);
size_t crypto_aead_chacha20poly1305_abytes(void);
size_t crypto_aead_chacha20poly1305_messagebytes_max(void);

size_t crypto_aead_xchacha20poly1305_ietf_keybytes(void);
size_t crypto_aead_xchacha20poly1305_ietf_nsecbytes(void);
size_t crypto_aead_xchacha20poly1305_ietf_npubbytes(void);
size_t crypto_aead_xchacha20poly1305_ietf_abytes(void);
size_t crypto_aead_xchacha20poly1305_ietf_messagebytes_max(void);

int crypto_aead_chacha20poly1305_ietf_encrypt(unsigned char *c,
                                              unsigned long long *clen,
                                              const unsigned char *m,
                                              unsigned long long mlen,
                                              const unsigned char *ad,
                                              unsigned long long adlen,
                                              const unsigned char *nsec,
                                              const unsigned char *npub,
                                              const unsigned char *k);

int crypto_aead_chacha20poly1305_ietf_decrypt(unsigned char *m,
                                              unsigned long long *mlen,
                                              unsigned char *nsec,
                                              const unsigned char *c,
                                              unsigned long long clen,
                                              const unsigned char *ad,
                                              unsigned long long adlen,
                                              const unsigned char *npub,
                                              const unsigned char *k);

int crypto_aead_chacha20poly1305_encrypt(unsigned char *c,
        			         unsigned long long *clen,
        			         const unsigned char *m,
        			         unsigned long long mlen,
        			         const unsigned char *ad,
        			         unsigned long long adlen,
        			         const unsigned char *nsec,
        			         const unsigned char *npub,
        			         const unsigned char *k);

int crypto_aead_chacha20poly1305_decrypt(unsigned char *m,
        			         unsigned long long *mlen,
        			         unsigned char *nsec,
        			         const unsigned char *c,
        			         unsigned long long clen,
        			         const unsigned char *ad,
        			         unsigned long long adlen,
        			         const unsigned char *npub,
        			         const unsigned char *k);

int crypto_aead_xchacha20poly1305_ietf_encrypt(unsigned char *c,
                                              unsigned long long *clen,
                                              const unsigned char *m,
                                              unsigned long long mlen,
                                              const unsigned char *ad,
                                              unsigned long long adlen,
                                              const unsigned char *nsec,
                                              const unsigned char *npub,
                                              const unsigned char *k);

int crypto_aead_xchacha20poly1305_ietf_decrypt(unsigned char *m,
                                              unsigned long long *mlen,
                                              unsigned char *nsec,
                                              const unsigned char *c,
                                              unsigned long long clen,
                                              const unsigned char *ad,
                                              unsigned long long adlen,
                                              const unsigned char *npub,
                                              const unsigned char *k);
