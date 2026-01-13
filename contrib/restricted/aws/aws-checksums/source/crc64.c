/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/crc.h>
#include <aws/checksums/private/crc64_priv.h>
#include <aws/checksums/private/crc_util.h>
#include <aws/common/cpuid.h>

large_buffer_apply_impl(crc64, uint64_t)

    AWS_ALIGNED_TYPEDEF(uint8_t, checksums_maxks_shifts_type[6][16], 16);

// Intel PSHUFB / ARM VTBL patterns for left/right shifts and masks
checksums_maxks_shifts_type aws_checksums_masks_shifts = {
    {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, //
    {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, // left/right
                                                                                                      // shifts
    {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, //
    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //
    {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, // byte masks
    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, //
};

AWS_ALIGNED_TYPEDEF(aws_checksums_crc64_constants_t, checksums_constants, 16);

/* clang-format off */

// Pre-computed bit-reflected constants for CRC64NVME
// The actual exponents are reduced by 1 to compensate for bit-reflection (e.g. x^1024 is actually x^1023)
checksums_constants aws_checksums_crc64nvme_constants = {
    .x2048 =
        {0x37ccd3e14069cabc,
         0xa043808c0f782663, // x^2112 mod P(x) / x^2048 mod P(x)
         0x37ccd3e14069cabc,
         0xa043808c0f782663, // duplicated 3 times to support 64 byte avx512 loads
         0x37ccd3e14069cabc,
         0xa043808c0f782663,
         0x37ccd3e14069cabc,
         0xa043808c0f782663},
    .x1536 =
        {0x758ee09da263e275,
         0x6d2d13de8038b4ca, // x^1600 mod P(x) / x^1536 mod P(x)
         0x758ee09da263e275,
         0x6d2d13de8038b4ca, // duplicated 3 times to support 64 byte avx512 loads
         0x758ee09da263e275,
         0x6d2d13de8038b4ca,
         0x758ee09da263e275,
         0x6d2d13de8038b4ca},
    .x1024 =
        {0xa1ca681e733f9c40,
         0x5f852fb61e8d92dc, // x^1088 mod P(x) / x^1024 mod P(x)
         0xa1ca681e733f9c40,
         0x5f852fb61e8d92dc, // duplicated 3 times to support 64 byte avx512 loads
         0xa1ca681e733f9c40,
         0x5f852fb61e8d92dc,
         0xa1ca681e733f9c40,
         0x5f852fb61e8d92dc},
    .x512 =
        {0x0c32cdb31e18a84a,
         0x62242240ace5045a, // x^576 mod P(x) / x^512 mod P(x)
         0x0c32cdb31e18a84a,
         0x62242240ace5045a, // duplicated 3 times to support 64 byte avx512 loads
         0x0c32cdb31e18a84a,
         0x62242240ace5045a,
         0x0c32cdb31e18a84a,
         0x62242240ace5045a},
    .x384 = {0xbdd7ac0ee1a4a0f0, 0xa3ffdc1fe8e82a8b},    //  x^448 mod P(x) / x^384 mod P(x)
    .x256 = {0xb0bc2e589204f500, 0xe1e0bb9d45d7a44c},    //  x^320 mod P(x) / x^256 mod P(x)
    .x128 = {0xeadc41fd2ba3d420, 0x21e9761e252621ac},    //  x^192 mod P(x) / x^128 mod P(x)
    .mu_poly = {0x27ecfa329aef9f77, 0x34d926535897936b}, // Barrett mu / polynomial P(x) (bit-reflected)
    .trailing =
        {
            // trailing input constants for data lengths of 1-15 bytes
            {0x04f28def5347786c, 0x7f6ef0c830358979}, // 1 trailing bytes:  x^72 mod P(x) /   x^8 mod P(x)
            {0x49e1df807414fdef, 0x8776a97d73bddf69}, // 2 trailing bytes:  x^80 mod P(x) /  x^15 mod P(x)
            {0x52734ea3e726fc54, 0xff6e4e1f4e4038be}, // 3 trailing bytes:  x^88 mod P(x) /  x^24 mod P(x)
            {0x668ab3bbc976d29d, 0x8211147cbaf96306}, // 4 trailing bytes:  x^96 mod P(x) /  x^32 mod P(x)
            {0xf2fa1fae5f5c1165, 0x373d15f784905d1e}, // 5 trailing bytes: x^104 mod P(x) /  x^40 mod P(x)
            {0x9065cb6e6d39918a, 0xe9742a79ef04a5d4}, // 6 trailing bytes: x^110 mod P(x) /  x^48 mod P(x)
            {0xc23dfbc6ca591ca3, 0xfc5d27f6bf353971}, // 7 trailing bytes: x^110 mod P(x) /  x^56 mod P(x)
            {0xeadc41fd2ba3d420, 0x21e9761e252621ac}, // 8 trailing bytes: x^120 mod P(x) /  x^64 mod P(x)
            {0xf12b2236ec577cd6, 0x04f28def5347786c}, // 9 trailing bytes: x^128 mod P(x) /  x^72 mod P(x)
            {0x0298996e905d785a, 0x49e1df807414fdef}, // 10 trailing bytes: x^144 mod P(x) /  x^80 mod P(x)
            {0xf779b03b943ff311, 0x52734ea3e726fc54}, // 11 trailing bytes: x^152 mod P(x) /  x^88 mod P(x)
            {0x07797643831fd90b, 0x668ab3bbc976d29d}, // 12 trailing bytes: x^160 mod P(x) /  x^96 mod P(x)
            {0x27a8849a7bc97a27, 0xf2fa1fae5f5c1165}, // 13 trailing bytes: x^168 mod P(x) / x^104 mod P(x)
            {0xb937a2d843183b7c, 0x9065cb6e6d39918a}, // 14 trailing bytes: x^176 mod P(x) / x^112 mod P(x)
            {0x31bce594cbbacd2d, 0xc23dfbc6ca591ca3}, // 15 trailing bytes: x^184 mod P(x) / x^120 mod P(x)
        },
    .shift_factors = {
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x7f6ef0c830358979, 0x0100000000000000},
            {0x8776a97d73bddf69, 0x0001000000000000},
            {0xff6e4e1f4e4038be, 0x0000010000000000},
            {0x8211147cbaf96306, 0x0000000100000000},
            {0x373d15f784905d1e, 0x0000000001000000},
            {0xe9742a79ef04a5d4, 0x0000000000010000},
            {0xfc5d27f6bf353971, 0x0000000000000100},
            {0x21e9761e252621ac, 0x0000000000000001},
            {0x04f28def5347786c, 0x7f6ef0c830358979},
            {0x49e1df807414fdef, 0x8776a97d73bddf69},
            {0x52734ea3e726fc54, 0xff6e4e1f4e4038be},
            {0x668ab3bbc976d29d, 0x8211147cbaf96306},
            {0xf2fa1fae5f5c1165, 0x373d15f784905d1e},
            {0x9065cb6e6d39918a, 0xe9742a79ef04a5d4},
            {0xc23dfbc6ca591ca3, 0xfc5d27f6bf353971}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xeadc41fd2ba3d420, 0x21e9761e252621ac},
            {0xb0bc2e589204f500, 0xe1e0bb9d45d7a44c},
            {0xbdd7ac0ee1a4a0f0, 0xa3ffdc1fe8e82a8b},
            {0x0c32cdb31e18a84a, 0x62242240ace5045a},
            {0x7b0ab10dd0f809fe, 0x03363823e6e791e5},
            {0x3c255f5ebc414423, 0x34f5a24e22d66e90},
            {0xd083dd594d96319d, 0x946588403d4adcbc},
            {0xa1ca681e733f9c40, 0x5f852fb61e8d92dc},
            {0xcd72351bf13cb8ca, 0x3bee332187cc60f7},
            {0xb0fffabea073832e, 0x66650420c4bfb826},
            {0xee25ff27102e240d, 0xf62e65588693c72c},
            {0x758ee09da263e275, 0x6d2d13de8038b4ca},
            {0x3872b6300d5e5d6f, 0xba7a3407e09207aa},
            {0x3f2930bb5e9d61c5, 0x0d1476de2f12000f},
            {0xeab05d4357a9b42f, 0x224f0e5bd4980292}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x37ccd3e14069cabc, 0xa043808c0f782663},
            {0xd0b3aa0ed6d54ae0, 0xc2409e2537aa5eb1},
            {0xc63523cbce2dffb6, 0x9e7c76d179e3fd2e},
            {0x6f60f5401265f28b, 0xe09504c2e6ddbb7f},
            {0x777baa5d0c3aa6b4, 0x2ac96ab9fb3914d1},
            {0xcd4f39a745b53231, 0x951cb5b5cb3c3e58},
            {0x45a5f5237ff77da5, 0xa53b1866091d47b7},
            {0x0aa2b36d1ef9775c, 0xb3996580f54d9368},
            {0xa0cd02a843934630, 0x9d243c1b5ba595d2},
            {0x80749c005721922a, 0xe8eafebf51f00d99},
            {0x83916d69c3a08b56, 0x93f4f545fca08ce0},
            {0x37b7c0a505fa0d79, 0x6ab65033a0558ae7},
            {0x766c2fd12be2438f, 0xd5d9c9ee3cf274e8},
            {0x61aab9457246d7ef, 0x5ba822479a292627},
            {0x2302ef8482eb6427, 0xbcd080852f93c47e}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xd8389d6e62c5ad4c, 0x4166ce950735e4f4},
            {0x3a31c8503d50e487, 0x5b1c330db529ab06},
            {0x0535cba97d64a0ad, 0x2da73f4d18465a70},
            {0xfd5154a0e05b3f13, 0x859be984066ec6ba},
            {0xc3c0da1e9827435a, 0xa0270010a349b763},
            {0x9e779699c4a9bfa7, 0x74a2e57eab95f420},
            {0xc6d1842f04f6caa2, 0x0a2fc89a5c2984ec},
            {0xddcf468ad2d38949, 0x3af0e497e1e4d072},
            {0xed77a31bf4476e6a, 0x996c3c3a054cb0b5},
            {0x78640307d56ff128, 0x488fb139897050d0},
            {0x0c47b0c7a55c3ec5, 0x118906aedee775b4},
            {0x06ee80a09c0887cc, 0x644e16e93358ceff},
            {0x530efd0395186f98, 0xbe79336227f3a112},
            {0x547a77c559026ac1, 0x78cec148adfc0b91},
            {0x8c2cc3822d2bbd3e, 0xfba9372d284d9e59}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x8815283c57f27cf3, 0x46002f9207f104c1},
            {0x1a3017a8bc10bc4e, 0xba5c3f9a5be147d4},
            {0x9c7f1b53de571511, 0x4109a8f396167bb5},
            {0x81df53237e4d432f, 0xaff32afd1ef2efef},
            {0x23c790a55300090c, 0xd575407640c7bdf1},
            {0x022abbad877a93e4, 0x7b728bb6275cb7d6},
            {0x76933f621df78392, 0xf3fae15adce61fc8},
            {0x6e3469207454ace0, 0x477b4484fefad21e},
            {0x44d4f024b78020bc, 0x2ff27ce6b1aaede4},
            {0x7be7a8f8b5611434, 0x6722feaa870051a7},
            {0x71943f4fb12f1caf, 0x7b8b3c84ef71b19b},
            {0x086133dd165a16d1, 0xd94773f7c45af924},
            {0x9a502138399245d5, 0xe8ebea1b9742b6bb},
            {0xb7975b945ffb8ce3, 0xc37b4292ad4c9846},
            {0x72fb2f1b4e46688b, 0x78af8dc5920fa5c5}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xaf6e902c828bb3c5, 0x68f52a64646e9e0e},
            {0x2f2b124154c58cf7, 0x385500f585519dd4},
            {0x78ae48245447c263, 0x91dd76af8e4d3b67},
            {0x3b73a237b21f2df6, 0x03fc7c4f716bbbab},
            {0x2794b31560b96e54, 0xe47cab9a57cdd3f1},
            {0xdb706736e703da09, 0x788a557a0677b83b},
            {0x7cf5657f32f5ee02, 0x4ce9fef3a03036b1},
            {0xa8efb9210a197082, 0x4ae8229113ece164},
            {0xfbfce97f96ebdade, 0x800f281d14bbc945},
            {0x1c24b397510d01df, 0xedbac14078db4951},
            {0xaad247d073c61950, 0xdef41794761ee129},
            {0x81cd4212b23f4204, 0xb932c785e0889ef0},
            {0x40821f273806b587, 0xfa11db1792428921},
            {0x6f9c1e27262fc52c, 0xb6d43ec6a156f54d},
            {0xe6a1f505ee67b0f0, 0xdbfa12c4a9690c36}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x2d5c0545f549646f, 0x95094d88bc77ea40},
            {0xbc25c7c7fe8c93b0, 0x166e53d761341c95},
            {0xc74ace1797d056c5, 0x4ba2719b7a97d596},
            {0x57fdb66cddcbbe82, 0x548b3bd9ab72b081},
            {0xf86de3e9e365a28a, 0xb01d2af0752648f6},
            {0xbd8d63808da1fdf1, 0xf8502752c2d20fa9},
            {0x3a5fc225ceb9ea4b, 0x855fe66535e59e62},
            {0x16e9a47db27f7ac4, 0xaa93b6f3e0530fb7},
            {0x37aa4578122f96c0, 0x007bb3e281d680bc},
            {0xb78e59570eb2633b, 0x618e8bd4e60bc623},
            {0x411117e86ad1fbfc, 0x25982a6047b0de18},
            {0xacded6014f36ad71, 0x6a78bb959e21e7ce},
            {0x283588272e04f55f, 0xcd36a08a5e44f846},
            {0xa2eae357255ee4e7, 0xb691486a604298cd},
            {0xd159da93de5a4e83, 0xf46bce1dd2ffb108}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x0665b28bc4c63e24, 0x5670c2a4b9829765},
            {0x7449aced9b6db2e5, 0xa844023bcfc96901},
            {0x08a87b121f9e6a50, 0xf2b4064f85a523d0},
            {0xe1a233e017211537, 0xa3b51afc0af8bd66},
            {0x5c5a40746e0f5dac, 0x2098bf45503abbe5},
            {0x7a5dc2272146586b, 0x55e1db1d59c9ce22},
            {0x25d1307c625b2c52, 0x227c7ee538ba4154},
            {0x8276d3abb8d03c31, 0x0bbb3614b3ebb8d5},
            {0xb3678bf91229d2ee, 0xd1f929cea3f79529},
            {0x439fafe285d30069, 0x567dd80fe98a161b},
            {0xba4dbd812c059ba3, 0x1be66ab3477a3bbc},
            {0xefa02c2748678372, 0x0dc64ba3f81413ec},
            {0x4d35e6f4e7133590, 0x9998ec3533b53ca4},
            {0x485af12356c32b1a, 0x82889ab7a174df10},
            {0x6fb7a81805d014c2, 0xfdd7f72fedd9ddb0}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xb8e9a571ca5cc4d6, 0xe7704b67fe3ee39e},
            {0x20fe9257da22a70a, 0xbb686d8a80dd76e8},
            {0xb578ab4a92c5221a, 0xd318af1ad4fa86cf},
            {0x9dd663d00b7c92ea, 0xc02497b480e9c4ff},
            {0x4b6c7a68bab57fc8, 0x12028d96d2dedf22},
            {0xcfaf84df3b4fd85c, 0xc449d1643fa02c46},
            {0x3f31b570b02aa3ca, 0x89a09b621dd795e0},
            {0xe8a353e53899d539, 0xbd723ba8d4a0ae87},
            {0x1ddd89be2f1c1a4b, 0xd305d16108b7087d},
            {0x8d98d99adbe81b97, 0x80d7c8011f328403},
            {0x83b17684c15a9daf, 0x36fb1c17ef388d2a},
            {0x20a8baeda52f80a8, 0xb332083fa1c95a88},
            {0x260315fad7e4a736, 0x44dd26d6f455b51a},
            {0x64600d74581c5695, 0xb2fc72d9479b525d},
            {0x2152d3830020ed74, 0x050789f6ebb0baf7}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x3cdd11e012ccea54, 0x7b1e1a90883ce9e1},
            {0x70628dd9600f7407, 0x1b83aba0e2a07689},
            {0x76b7a029736ba4e7, 0x4891dddc6b657eaa},
            {0xb458448fb1f3188e, 0x34157603f0a7dcd6},
            {0xe360ae0d0aae42c8, 0x96102a61fa903b95},
            {0x83d29d54d3f9cc49, 0x0d21de4c2fb52704},
            {0x5215c7f9c41086e9, 0xdd2f3472ff12f8cf},
            {0xf80a781e22970d06, 0x7e104f86691f042a},
            {0x68bb2199c2a4e79b, 0x5172bc58c615f3e5},
            {0x4ae19cb6bc57d0a4, 0x10cb8b385c156242},
            {0x80e5b77b4b6a4ff1, 0xd4e4d62f3634e40f},
            {0x77a20eb826034b54, 0x8a679a8bd16a0c13},
            {0xfd1e36a6f263e32c, 0x9b82fe32e61a4890},
            {0x85d5d55c00131b90, 0x3a1637bc77377b26},
            {0xaf01148feb43cd2f, 0xeff6c4741200464d}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x83e0ddefe7a45e8e, 0x9d6d188a0c268c0e},
            {0xefeb04aa604fd5f6, 0x948dedbcbbe69c14},
            {0x5e40243f5d32b2c5, 0x34479bb750628b82},
            {0xb93cf11024b2375c, 0x5b0abad438af51ba},
            {0xf0c85d48ec6469d4, 0x440e49a309d099b3},
            {0xd423924cb69352b5, 0xa83d6d501a1f42de},
            {0x459c052601fb6f49, 0xf30eff24f4ba7b93},
            {0x67117c542a7f11dc, 0x39c892e29ab5234f},
            {0x31b58290c181b388, 0x7712c3fcf4997a84},
            {0x7d41bf4526bf6d81, 0x83a4be134ab89db9},
            {0x4075eb419783abc4, 0x0b1d0c7d1f8cc311},
            {0x4a8dc8767e023bb4, 0x5cf94b4fb8c16a96},
            {0xdee405707e0dd5d1, 0x31a06356eb9010d3},
            {0xb319c67d9be7031c, 0xe7e0096d81887c59},
            {0xf6f3d4d8ce8007a6, 0x2096c9cec17af654}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x8f55439af3bd9d2c, 0xa465e82ff8f8e600},
            {0xdc5ac2efc27a21d7, 0x7cc93936965822df},
            {0x8f07a9fb1a633e12, 0x2889293ee4ac8a41},
            {0x7de04ef8e56fa700, 0x9ef26991b5b059af},
            {0x2909710e746c770c, 0xc7f557bcba367bb3},
            {0x427a0847c44b0bb9, 0xead55f1828f1d65a},
            {0x29cf69ff94c08228, 0x1bdd74cbb239d65e},
            {0x664394781b7b5c3e, 0x2303ef90c5d2844c},
            {0xa9c58bf7865e8f0c, 0xdb3ecfa71b2413ad},
            {0xe60d18c971ec8421, 0x8fad79b09cb04d7d},
            {0x5040deee460498c3, 0xb1695ee414d008db},
            {0x54a9e27612ab4d18, 0x3a5e0b5758cbda67},
            {0x7ea99b17b4b98f31, 0x2f0a5a0075b1a682},
            {0x9c744520390ec1db, 0xb56cf53356b7e1cc},
            {0x9f213f6b1aceb107, 0x4b907b2fd5e34069}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xba25b65a6eca9f9a, 0x089ebc585c9e0649},
            {0x1d3d887d1a2ab661, 0x03ca03d9ac82486f},
            {0x862e744223bdce33, 0x92bcb733562fa89e},
            {0x41dddd9a7a4f68f6, 0x51587923d484fe95},
            {0x3e07184c2a543c9f, 0xa3bc73ad62c69f4d},
            {0xcdb22a47101af1d1, 0xad69344ae204d507},
            {0x8a264c80a3d34065, 0x5bd59816120c0a00},
            {0xeb3adca706841044, 0x5ee2c0aa74bd4030},
            {0x9c69dc6fd29a1d34, 0x6feb154228453f78},
            {0xbddeb6de041a4aec, 0x9c1544387ff8c7d4},
            {0x9453f5298e17ed0a, 0x3bd3d623c3939cbb},
            {0x08610ff7c0404d41, 0xc116574e87db2fe4},
            {0xe10b0d78b92d08e8, 0xf9ad46eaec4431d9},
            {0xea344500dd0a56cd, 0x63ed0e605a0ed0c9},
            {0xdc9f6b3f86d30a5d, 0xc31df4bb3cf09acf}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x8f89e9a63939620e, 0x79a6f16b933e4e44},
            {0x0e840889029f84e9, 0x36598f099bd0f3db},
            {0xca6404a7914bf5c0, 0x2ba39a573d6a3a82},
            {0x9bd56272c33c810e, 0x4a35e540a1479edb},
            {0xeaf7936f273d6de7, 0x08dc8ce10912f553},
            {0xabc26c8a25c29a0d, 0xaf85d12c2c327b77},
            {0x6f4ea1ff2314be27, 0x57b4e8aab69d5dfd},
            {0xc528fff39c5f1ae0, 0x3f92f762000463b5},
            {0x2416dad900b13411, 0x02968c8cce36c87a},
            {0x604e693162a4cf06, 0x7b9831345f9ce8e4},
            {0xaceaa9dbc9db47ec, 0x3b200897594d3224},
            {0xc89029217abcdda4, 0x857fc04a017e47b8},
            {0x38df49279bc4896a, 0x388481ec8819d189},
            {0x8204137658a9dfb8, 0xa998abdadfb22862},
            {0xaccf425cf56639ab, 0xfc0b631d2a4084f4}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0xaa5b458c2aae3097, 0x69061a237909e68c},
            {0xb9942ad4b44c9f36, 0x185094e576ee8b53},
            {0xdb0b6d7135c53e44, 0xc95a1e2c201b8699},
            {0x2492df83f84df962, 0x2a080359358eb362},
            {0x034b645e77aad536, 0x568459818b19cb95},
            {0xd5769c1fa00d79d1, 0xd26e080014835b38},
            {0x6be82feaadccd8b7, 0xbf827f8c5f10a0d3},
            {0xdad738b04a1fd28e, 0xe31ac9bb87f78e1f},
            {0xf489f5c243f753cc, 0x0652d134d0d85cc3},
            {0x6116a8ce1707d602, 0xa7eb4c774d5d9b3a},
            {0x1526699767a5c863, 0x21ed8a32f45b580d},
            {0xa832c53a9ceb5bb1, 0xf9944094fd278e14},
            {0xe026fe5af1e378a4, 0x7847b2bc52e3f82d},
            {0x2461a0d71de23b62, 0xbccd3eab3f0e9eb9},
            {0x0aeff45a85e7848e, 0x9e0b1672fd087e6d}
        },
        {
            {0x0000000000000000, 0x0000000000000000},
            {0x5e86ee647e7e465e, 0x2910ba4b04c03faf},
            {0x9a6c9329ac4bc9b5, 0x8000000000000000},
            {0x2f4377323f3f232f, 0x8ee4ce0c2e2bd662},
            {0xd75adabd7a6e2d6f, 0x4000000000000000},
            {0x8dcd28b0b3d45822, 0x477267061715eb31},
            {0xf1c1fe77117cdf02, 0x2000000000000000},
            {0x46e6945859ea2c11, 0xb9d5a0aaa7c13c2d},
            {0x78e0ff3b88be6f81, 0x1000000000000000},
            {0xb91fd90580bedfbd, 0xc686437cffab57a3},
            {0xa61cecb46814fe75, 0x0800000000000000},
            {0xc6e37fab6c14a66b, 0xf92fb297d39e6264},
            {0xc962e5739841b68f, 0x0400000000000000},
            {0xf91d2cfc1a419a80, 0x7c97d94be9cf3132},
            {0xfedde190606b12f2, 0x0200000000000000},
            {0x7c8e967e0d20cd40, 0x3e4beca5f4e79899}
        }
    }
};
/* clang-format on */

static uint64_t (*s_crc64nvme_fn_ptr)(const uint8_t *input, int length, uint64_t prev_crc64) = NULL;
static uint64_t (*s_crc64nvme_combine_fn_ptr)(uint64_t crc1, uint64_t crc2, uint64_t len2) = NULL;

void aws_checksums_crc64_init(void) {
    if (s_crc64nvme_fn_ptr == NULL) {
#if defined(AWS_USE_CPU_EXTENSIONS) && defined(AWS_ARCH_INTEL_X64) && !(defined(_MSC_VER) && _MSC_VER < 1920)
#    if defined(AWS_HAVE_AVX512_INTRINSICS)
        if (aws_cpu_has_feature(AWS_CPU_FEATURE_AVX512) && aws_cpu_has_feature(AWS_CPU_FEATURE_VPCLMULQDQ)) {
            s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_intel_avx512;
        } else
#    endif
#    if defined(AWS_HAVE_CLMUL) && defined(AWS_HAVE_AVX2_INTRINSICS)
            if (aws_cpu_has_feature(AWS_CPU_FEATURE_CLMUL) && aws_cpu_has_feature(AWS_CPU_FEATURE_AVX2)) {
            s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_intel_clmul;
        } else {
            s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_sw;
        }
#    endif
#    if !(defined(AWS_HAVE_AVX512_INTRINSICS) || (defined(AWS_HAVE_CLMUL) && defined(AWS_HAVE_AVX2_INTRINSICS)))
        s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_sw;
#    endif

#elif defined(AWS_USE_CPU_EXTENSIONS) && defined(AWS_ARCH_ARM64) && defined(AWS_HAVE_ARMv8_1)
        if (aws_cpu_has_feature(AWS_CPU_FEATURE_ARM_CRYPTO) && aws_cpu_has_feature(AWS_CPU_FEATURE_ARM_PMULL)) {
            s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_arm_pmull;
        } else {
            s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_sw;
        }
#else // this branch being taken means it's not arm64 and not intel with avx extensions
        s_crc64nvme_fn_ptr = aws_checksums_crc64nvme_sw;
#endif
    }

    if (s_crc64nvme_combine_fn_ptr == NULL) {
        // arm only fancy version for now. still need to implement x64
#if defined(AWS_USE_CPU_EXTENSIONS) && defined(AWS_ARCH_ARM64) && defined(AWS_HAVE_ARMv8_1)
        if (aws_cpu_has_feature(AWS_CPU_FEATURE_ARM_PMULL)) {
            s_crc64nvme_combine_fn_ptr = aws_checksums_crc64nvme_combine_arm_pmull;
        } else {
            s_crc64nvme_combine_fn_ptr = aws_checksums_crc64nvme_combine_sw;
        }
#else // this branch being taken means it's not arm64 and not intel with avx extensions
        s_crc64nvme_combine_fn_ptr = aws_checksums_crc64nvme_combine_sw;
#endif
    }
}

uint64_t aws_checksums_crc64nvme(const uint8_t *input, int length, uint64_t prev_crc64) {
    if (AWS_UNLIKELY(s_crc64nvme_fn_ptr == NULL || s_crc64nvme_combine_fn_ptr == NULL)) {
        aws_checksums_crc64_init();
    }

    return s_crc64nvme_fn_ptr(input, length, prev_crc64);
}

uint64_t aws_checksums_crc64nvme_ex(const uint8_t *input, size_t length, uint64_t previous_crc64) {
    return aws_large_buffer_apply_crc64(aws_checksums_crc64nvme, input, length, previous_crc64);
}

uint64_t aws_checksums_crc64nvme_combine(uint64_t crc1, uint64_t crc2, uint64_t len2) {
    if (AWS_UNLIKELY(s_crc64nvme_fn_ptr == NULL || s_crc64nvme_combine_fn_ptr == NULL)) {
        aws_checksums_crc64_init();
    }

    return s_crc64nvme_combine_fn_ptr(crc1, crc2, len2);
}
