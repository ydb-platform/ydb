/* Copyright 2013-2017 Donald Stufft and individual contributors
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

void sodium_memzero(void * const pnt, const size_t len);
int sodium_memcmp(const void * const b1_, const void * const b2_, size_t len);

int sodium_mlock(void * const addr, const size_t len);
int sodium_munlock(void * const addr, const size_t len);

int sodium_pad(size_t *padded_buflen_p, unsigned char *buf,
               size_t unpadded_buflen, size_t blocksize, size_t max_buflen);
int sodium_unpad(size_t *unpadded_buflen_p, const unsigned char *buf,
                 size_t padded_buflen, size_t blocksize);

void sodium_increment(unsigned char *n, const size_t nlen);
void sodium_add(unsigned char *a, unsigned char *b, const size_t len);
