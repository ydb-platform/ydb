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

static const int PYNACL_HAS_CRYPTO_CORE_ED25519;

size_t crypto_core_ed25519_bytes();
size_t crypto_core_ed25519_scalarbytes(void);
size_t crypto_core_ed25519_nonreducedscalarbytes(void);

int crypto_core_ed25519_is_valid_point(const unsigned char *p);
int crypto_core_ed25519_add(unsigned char *r, const unsigned char *p, const unsigned char *q);
int crypto_core_ed25519_sub(unsigned char *r, const unsigned char *p, const unsigned char *q);

int crypto_core_ed25519_scalar_invert(unsigned char *recip, const unsigned char *s);
void crypto_core_ed25519_scalar_negate(unsigned char *neg, const unsigned char *s);
void crypto_core_ed25519_scalar_complement(unsigned char *comp, const unsigned char *s);
void crypto_core_ed25519_scalar_add(unsigned char *z, const unsigned char *x,
                                    const unsigned char *y);
void crypto_core_ed25519_scalar_sub(unsigned char *z, const unsigned char *x,
                                    const unsigned char *y);
void crypto_core_ed25519_scalar_mul(unsigned char *z, const unsigned char *x,
                                    const unsigned char *y);
void crypto_core_ed25519_scalar_reduce(unsigned char *r, const unsigned char *s);
