# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os.path

from cffi import FFI


BLOWFISH_DIR = os.path.join(os.path.dirname(__file__), "_csrc")


ffi = FFI()

ffi.cdef("""
int bcrypt_hashpass(const char *, const char *, char *, size_t);
int encode_base64(char *, const uint8_t *, size_t);
int bcrypt_pbkdf(const char *, size_t, const uint8_t *, size_t,
                 uint8_t *, size_t, unsigned int);
int timingsafe_bcmp(const void *, const void *, size_t);
""")

ffi.set_source(
    "_bcrypt",
    """
    #include "pycabcrypt.h"
    """,
    sources=[
        os.path.join(BLOWFISH_DIR, "blf.c"),
        os.path.join(BLOWFISH_DIR, "bcrypt.c"),
        os.path.join(BLOWFISH_DIR, "bcrypt_pbkdf.c"),
        os.path.join(BLOWFISH_DIR, "sha2.c"),
        os.path.join(BLOWFISH_DIR, "timingsafe_bcmp.c"),
    ],
    include_dirs=[BLOWFISH_DIR],
)
