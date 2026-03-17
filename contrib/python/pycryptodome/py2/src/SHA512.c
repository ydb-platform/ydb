/*
 * An implementation of the SHA-512 hash function.
 *
 * The Federal Information Processing Standards (FIPS) Specification 
 * can be found here (FIPS 180-3):
 *   http://csrc.nist.gov/publications/PubsFIPS.html
 * 
 * Written in 2010 by Lorenz Quack <don@amberfisharts.com>
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain.  To
 * the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ===================================================================
 *
 */

#define MODULE_NAME SHA512
#define DIGEST_SIZE (512/8)
#define WORD_SIZE 8

#include "common.h"

/* Initial Values H for SHA-512, SHA-512/224 and SHA-512/256 */
static const uint64_t H_SHA_512[3][8] = {
    {
    0x6a09e667f3bcc908ULL,
    0xbb67ae8584caa73bULL,
    0x3c6ef372fe94f82bULL,
    0xa54ff53a5f1d36f1ULL,
    0x510e527fade682d1ULL,
    0x9b05688c2b3e6c1fULL,
    0x1f83d9abfb41bd6bULL,
    0x5be0cd19137e2179ULL
    },
    {
    0x8C3D37C819544DA2ULL,
    0x73E1996689DCD4D6ULL,
    0x1DFAB7AE32FF9C82ULL,
    0x679DD514582F9FCFULL,
    0x0F6D2B697BD44DA8ULL,
    0x77E36F7304C48942ULL,
    0x3F9D85A86A1D36C8ULL,
    0x1112E6AD91D692A1ULL
    },
    {
    0x22312194FC2BF72CULL,
    0x9F555FA3C84C64C2ULL,
    0x2393B86B6F53B151ULL,
    0x963877195940EABDULL,
    0x96283EE2A88EFFE3ULL,
    0xBE5E1E2553863992ULL,
    0x2B0199FC2C85B8AAULL,
    0x0EB72DDC81C52CA2ULL
    }
};

#include "hash_SHA2_template.c"
