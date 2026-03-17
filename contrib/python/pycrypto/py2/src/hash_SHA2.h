/*
 * An generic header for the SHA-2 hash family.
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

#ifndef __HASH_SHA2_H
#define __HASH_SHA2_H

/* check if implementation set the correct macros */
#ifndef MODULE_NAME
#error SHA2 Implementation must define MODULE_NAME before including this header
#endif

#ifndef DIGEST_SIZE
#error SHA2 Implementation must define DIGEST_SIZE before including this header
#else
#define DIGEST_SIZE_BITS (DIGEST_SIZE*8)
#endif

#ifndef BLOCK_SIZE
#error SHA2 Implementation must define BLOCK_SIZE before including this header
#else
#define BLOCK_SIZE_BITS (BLOCK_SIZE*8)
#endif

#ifndef WORD_SIZE
#error SHA2 Implementation must define WORD_SIZE before including this header
#else
#if ((WORD_SIZE != 4) && (WORD_SIZE != 8))
#error WORD_SIZE must be either 4 or 8
#else
#define WORD_SIZE_BITS (WORD_SIZE*8)
#endif
#endif

#ifndef SCHEDULE_SIZE
#error SHA2 Implementation must define SCHEDULE_SIZE before including this header
#endif

/* define some helper macros */
#define PADDING_SIZE (2 * WORD_SIZE)
#define LAST_BLOCK_SIZE (BLOCK_SIZE - PADDING_SIZE)

/* define generic SHA-2 family functions */
#define Ch(x,y,z)   ((x & y) ^ (~x & z))
#define Maj(x,y,z)  ((x & y) ^ (x & z) ^ (y & z))
#define ROTR(x, n)  (((x)>>((n)&(WORD_SIZE_BITS-1)))|((x)<<(WORD_SIZE_BITS-((n)&(WORD_SIZE_BITS-1)))))
#define SHR(x, n)   ((x)>>(n))

/* determine fixed size types */
#if defined(__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) || defined(__GNUC__)
#include <stdint.h>
typedef uint8_t				U8;
typedef uint32_t			U32;
typedef uint64_t			U64;
#elif defined(_MSC_VER)
typedef unsigned char		U8;
typedef unsigned __int64	U64;
typedef unsigned int		U32;
#elif defined(__sun) || defined(__sun__)
#include <sys/inttypes.h>
typedef uint8_t				U8;
typedef uint32_t			U32;
typedef uint64_t			U64;
#endif

/* typedef a sha2_word_t type of appropriate size */
#if (WORD_SIZE_BITS == 64)
typedef U64 sha2_word_t;
#elif (WORD_SIZE_BITS == 32)
typedef U32 sha2_word_t;
#else
#error According to the FIPS Standard WORD_SIZE_BITS must be either 32 or 64
#endif

/* define the hash_state structure */
typedef struct{
    sha2_word_t state[8];
    int curlen;
    sha2_word_t length_upper, length_lower;
    unsigned char buf[BLOCK_SIZE];
} hash_state;

#endif /* __HASH_SHA2_H */
