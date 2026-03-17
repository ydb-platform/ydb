/* Copyright 2013-2018 Donald Stufft and individual contributors
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

size_t crypto_secretstream_xchacha20poly1305_abytes(void);
size_t crypto_secretstream_xchacha20poly1305_headerbytes(void);
size_t crypto_secretstream_xchacha20poly1305_keybytes(void);
size_t crypto_secretstream_xchacha20poly1305_messagebytes_max(void);

unsigned char crypto_secretstream_xchacha20poly1305_tag_message(void);
unsigned char crypto_secretstream_xchacha20poly1305_tag_push(void);
unsigned char crypto_secretstream_xchacha20poly1305_tag_rekey(void);
unsigned char crypto_secretstream_xchacha20poly1305_tag_final(void);

/*
 * We use crypto_secretstream_xchacha20poly1305_state * as
 * a pointer to an opaque buffer,
 * therefore the following typedef makes sense:
 */

typedef void crypto_secretstream_xchacha20poly1305_state;

size_t crypto_secretstream_xchacha20poly1305_statebytes(void);

void crypto_secretstream_xchacha20poly1305_keygen
   (unsigned char *k);

int crypto_secretstream_xchacha20poly1305_init_push (crypto_secretstream_xchacha20poly1305_state *state,
    unsigned char *header,
    const unsigned char *k);

int crypto_secretstream_xchacha20poly1305_push
   (crypto_secretstream_xchacha20poly1305_state *state,
    unsigned char *c, unsigned long long *clen_p,
    const unsigned char *m, unsigned long long mlen,
    const unsigned char *ad, unsigned long long adlen, unsigned char tag);

int crypto_secretstream_xchacha20poly1305_init_pull
   (crypto_secretstream_xchacha20poly1305_state *state,
    const unsigned char *header,
    const unsigned char *k);

int crypto_secretstream_xchacha20poly1305_pull
   (crypto_secretstream_xchacha20poly1305_state *state,
    unsigned char *m, unsigned long long *mlen_p, unsigned char *tag_p,
    const unsigned char *c, unsigned long long clen,
    const unsigned char *ad, unsigned long long adlen);

void crypto_secretstream_xchacha20poly1305_rekey
    (crypto_secretstream_xchacha20poly1305_state *state);
