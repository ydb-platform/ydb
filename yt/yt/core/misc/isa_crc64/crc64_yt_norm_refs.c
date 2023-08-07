/**********************************************************************
  Copyright(c) 2011-2016 Intel Corporation All rights reserved.
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#pragma GCC diagnostic ignored "-Wunused-function"

#include <stdint.h>

#define MAX_ITER	8

////////////////////////////////////////////////////////////////////////////////


/*
    This file contains base and reference c-based implementations of CRC64,
    which also originate from ISA-l with the following modifications:

        1) `crc64_yt_norm_ref` is the same as `crc64_*_norm_ref` functions from `crc64_ref.c`,
    but with yt-specific polynomial variable.

        2) `crc64_yt_norm_base` is the same as `crc64_*_norm_base` functions from `crc64_base.c`,
    but with yt-specific lookup table. example of code which generates such table may be found at 
    `yt/yt/experiments/benchmark_crc/main.cpp`
*/


////////////////////////////////////////////////////////////////////////////////

static inline uint64_t crc64_yt_norm_ref(uint64_t seed, const uint8_t * buf, uint64_t len)
{
	uint64_t rem = ~seed;
	unsigned int i, j;

	uint64_t poly = 0xE543279765927881;	// YT-specific polynomial

	for (i = 0; i < len; i++) {
		rem = rem ^ ((uint64_t) buf[i] << 56);
		for (j = 0; j < MAX_ITER; j++) {
			rem = (rem & 0x8000000000000000ULL ? poly : 0) ^ (rem << 1);
		}
	}
	return ~rem;
}

////////////////////////////////////////////////////////////////////////////////

uint64_t crc64_yt_norm_base(uint64_t seed, const uint8_t * buf, uint64_t len);

static const uint64_t crc64_yt_norm_table[256] = {
    0x0000000000000000ULL, 0xe543279765927881ULL, 0x2fc568b9aeb68983ULL, 0xca864f2ecb24f102ULL,
    0x5f8ad1735d6d1306ULL, 0xbac9f6e438ff6b87ULL, 0x704fb9caf3db9a85ULL, 0x950c9e5d9649e204ULL,
    0xbf15a2e6bada260cULL, 0x5a568571df485e8dULL, 0x90d0ca5f146caf8fULL, 0x7593edc871fed70eULL,
    0xe09f7395e7b7350aULL, 0x05dc540282254d8bULL, 0xcf5a1b2c4901bc89ULL, 0x2a193cbb2c93c408ULL,
    0x9b68625a10263499ULL, 0x7e2b45cd75b44c18ULL, 0xb4ad0ae3be90bd1aULL, 0x51ee2d74db02c59bULL,
    0xc4e2b3294d4b279fULL, 0x21a194be28d95f1eULL, 0xeb27db90e3fdae1cULL, 0x0e64fc07866fd69dULL,
    0x247dc0bcaafc1295ULL, 0xc13ee72bcf6e6a14ULL, 0x0bb8a805044a9b16ULL, 0xeefb8f9261d8e397ULL,
    0x7bf711cff7910193ULL, 0x9eb4365892037912ULL, 0x5432797659278810ULL, 0xb1715ee13cb5f091ULL,
    0xd393e32345de11b3ULL, 0x36d0c4b4204c6932ULL, 0xfc568b9aeb689830ULL, 0x1915ac0d8efae0b1ULL,
    0x8c19325018b302b5ULL, 0x695a15c77d217a34ULL, 0xa3dc5ae9b6058b36ULL, 0x469f7d7ed397f3b7ULL,
    0x6c8641c5ff0437bfULL, 0x89c566529a964f3eULL, 0x4343297c51b2be3cULL, 0xa6000eeb3420c6bdULL,
    0x330c90b6a26924b9ULL, 0xd64fb721c7fb5c38ULL, 0x1cc9f80f0cdfad3aULL, 0xf98adf98694dd5bbULL,
    0x48fb817955f8252aULL, 0xadb8a6ee306a5dabULL, 0x673ee9c0fb4eaca9ULL, 0x827dce579edcd428ULL,
    0x1771500a0895362cULL, 0xf232779d6d074eadULL, 0x38b438b3a623bfafULL, 0xddf71f24c3b1c72eULL,
    0xf7ee239fef220326ULL, 0x12ad04088ab07ba7ULL, 0xd82b4b2641948aa5ULL, 0x3d686cb12406f224ULL,
    0xa864f2ecb24f1020ULL, 0x4d27d57bd7dd68a1ULL, 0x87a19a551cf999a3ULL, 0x62e2bdc2796be122ULL,
    0x4264e1d1ee2e5be7ULL, 0xa727c6468bbc2366ULL, 0x6da189684098d264ULL, 0x88e2aeff250aaae5ULL,
    0x1dee30a2b34348e1ULL, 0xf8ad1735d6d13060ULL, 0x322b581b1df5c162ULL, 0xd7687f8c7867b9e3ULL,
    0xfd71433754f47debULL, 0x183264a03166056aULL, 0xd2b42b8efa42f468ULL, 0x37f70c199fd08ce9ULL,
    0xa2fb924409996eedULL, 0x47b8b5d36c0b166cULL, 0x8d3efafda72fe76eULL, 0x687ddd6ac2bd9fefULL,
    0xd90c838bfe086f7eULL, 0x3c4fa41c9b9a17ffULL, 0xf6c9eb3250bee6fdULL, 0x138acca5352c9e7cULL,
    0x868652f8a3657c78ULL, 0x63c5756fc6f704f9ULL, 0xa9433a410dd3f5fbULL, 0x4c001dd668418d7aULL,
    0x6619216d44d24972ULL, 0x835a06fa214031f3ULL, 0x49dc49d4ea64c0f1ULL, 0xac9f6e438ff6b870ULL,
    0x3993f01e19bf5a74ULL, 0xdcd0d7897c2d22f5ULL, 0x165698a7b709d3f7ULL, 0xf315bf30d29bab76ULL,
    0x91f702f2abf04a54ULL, 0x74b42565ce6232d5ULL, 0xbe326a4b0546c3d7ULL, 0x5b714ddc60d4bb56ULL,
    0xce7dd381f69d5952ULL, 0x2b3ef416930f21d3ULL, 0xe1b8bb38582bd0d1ULL, 0x04fb9caf3db9a850ULL,
    0x2ee2a014112a6c58ULL, 0xcba1878374b814d9ULL, 0x0127c8adbf9ce5dbULL, 0xe464ef3ada0e9d5aULL,
    0x716871674c477f5eULL, 0x942b56f029d507dfULL, 0x5ead19dee2f1f6ddULL, 0xbbee3e4987638e5cULL,
    0x0a9f60a8bbd67ecdULL, 0xefdc473fde44064cULL, 0x255a08111560f74eULL, 0xc0192f8670f28fcfULL,
    0x5515b1dbe6bb6dcbULL, 0xb056964c8329154aULL, 0x7ad0d962480de448ULL, 0x9f93fef52d9f9cc9ULL,
    0xb58ac24e010c58c1ULL, 0x50c9e5d9649e2040ULL, 0x9a4faaf7afbad142ULL, 0x7f0c8d60ca28a9c3ULL,
    0xea00133d5c614bc7ULL, 0x0f4334aa39f33346ULL, 0xc5c57b84f2d7c244ULL, 0x20865c139745bac5ULL,
    0x84c9c3a3dc5cb7ceULL, 0x618ae434b9cecf4fULL, 0xab0cab1a72ea3e4dULL, 0x4e4f8c8d177846ccULL,
    0xdb4312d08131a4c8ULL, 0x3e003547e4a3dc49ULL, 0xf4867a692f872d4bULL, 0x11c55dfe4a1555caULL,
    0x3bdc6145668691c2ULL, 0xde9f46d20314e943ULL, 0x141909fcc8301841ULL, 0xf15a2e6bada260c0ULL,
    0x6456b0363beb82c4ULL, 0x811597a15e79fa45ULL, 0x4b93d88f955d0b47ULL, 0xaed0ff18f0cf73c6ULL,
    0x1fa1a1f9cc7a8357ULL, 0xfae2866ea9e8fbd6ULL, 0x3064c94062cc0ad4ULL, 0xd527eed7075e7255ULL,
    0x402b708a91179051ULL, 0xa568571df485e8d0ULL, 0x6fee18333fa119d2ULL, 0x8aad3fa45a336153ULL,
    0xa0b4031f76a0a55bULL, 0x45f724881332dddaULL, 0x8f716ba6d8162cd8ULL, 0x6a324c31bd845459ULL,
    0xff3ed26c2bcdb65dULL, 0x1a7df5fb4e5fcedcULL, 0xd0fbbad5857b3fdeULL, 0x35b89d42e0e9475fULL,
    0x575a20809982a67dULL, 0xb2190717fc10defcULL, 0x789f483937342ffeULL, 0x9ddc6fae52a6577fULL,
    0x08d0f1f3c4efb57bULL, 0xed93d664a17dcdfaULL, 0x2715994a6a593cf8ULL, 0xc256bedd0fcb4479ULL,
    0xe84f826623588071ULL, 0x0d0ca5f146caf8f0ULL, 0xc78aeadf8dee09f2ULL, 0x22c9cd48e87c7173ULL,
    0xb7c553157e359377ULL, 0x528674821ba7ebf6ULL, 0x98003bacd0831af4ULL, 0x7d431c3bb5116275ULL,
    0xcc3242da89a492e4ULL, 0x2971654dec36ea65ULL, 0xe3f72a6327121b67ULL, 0x06b40df4428063e6ULL,
    0x93b893a9d4c981e2ULL, 0x76fbb43eb15bf963ULL, 0xbc7dfb107a7f0861ULL, 0x593edc871fed70e0ULL,
    0x7327e03c337eb4e8ULL, 0x9664c7ab56eccc69ULL, 0x5ce288859dc83d6bULL, 0xb9a1af12f85a45eaULL,
    0x2cad314f6e13a7eeULL, 0xc9ee16d80b81df6fULL, 0x036859f6c0a52e6dULL, 0xe62b7e61a53756ecULL,
    0xc6ad22723272ec29ULL, 0x23ee05e557e094a8ULL, 0xe9684acb9cc465aaULL, 0x0c2b6d5cf9561d2bULL,
    0x9927f3016f1fff2fULL, 0x7c64d4960a8d87aeULL, 0xb6e29bb8c1a976acULL, 0x53a1bc2fa43b0e2dULL,
    0x79b8809488a8ca25ULL, 0x9cfba703ed3ab2a4ULL, 0x567de82d261e43a6ULL, 0xb33ecfba438c3b27ULL,
    0x263251e7d5c5d923ULL, 0xc3717670b057a1a2ULL, 0x09f7395e7b7350a0ULL, 0xecb41ec91ee12821ULL,
    0x5dc540282254d8b0ULL, 0xb88667bf47c6a031ULL, 0x720028918ce25133ULL, 0x97430f06e97029b2ULL,
    0x024f915b7f39cbb6ULL, 0xe70cb6cc1aabb337ULL, 0x2d8af9e2d18f4235ULL, 0xc8c9de75b41d3ab4ULL,
    0xe2d0e2ce988efebcULL, 0x0793c559fd1c863dULL, 0xcd158a773638773fULL, 0x2856ade053aa0fbeULL,
    0xbd5a33bdc5e3edbaULL, 0x5819142aa071953bULL, 0x929f5b046b556439ULL, 0x77dc7c930ec71cb8ULL,
    0x153ec15177acfd9aULL, 0xf07de6c6123e851bULL, 0x3afba9e8d91a7419ULL, 0xdfb88e7fbc880c98ULL,
    0x4ab410222ac1ee9cULL, 0xaff737b54f53961dULL, 0x6571789b8477671fULL, 0x80325f0ce1e51f9eULL,
    0xaa2b63b7cd76db96ULL, 0x4f684420a8e4a317ULL, 0x85ee0b0e63c05215ULL, 0x60ad2c9906522a94ULL,
    0xf5a1b2c4901bc890ULL, 0x10e29553f589b011ULL, 0xda64da7d3ead4113ULL, 0x3f27fdea5b3f3992ULL,
    0x8e56a30b678ac903ULL, 0x6b15849c0218b182ULL, 0xa193cbb2c93c4080ULL, 0x44d0ec25acae3801ULL,
    0xd1dc72783ae7da05ULL, 0x349f55ef5f75a284ULL, 0xfe191ac194515386ULL, 0x1b5a3d56f1c32b07ULL,
    0x314301eddd50ef0fULL, 0xd400267ab8c2978eULL, 0x1e86695473e6668cULL, 0xfbc54ec316741e0dULL,
    0x6ec9d09e803dfc09ULL, 0x8b8af709e5af8488ULL, 0x410cb8272e8b758aULL, 0xa44f9fb04b190d0bULL
};

uint64_t crc64_yt_norm_base(uint64_t seed, const uint8_t * buf, uint64_t len)
{
	uint64_t i, crc = ~seed;

	for (i = 0; i < len; i++) {
		uint8_t byte = buf[i];
		crc = crc64_yt_norm_table[((crc >> 56) ^ byte) & 0xff] ^ (crc << 8);
	}

	return ~crc;
}

////////////////////////////////////////////////////////////////////////////////
