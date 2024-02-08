#include "checksum.h"
#include "checksum_helpers.h"

#include <yt/yt/core/misc/isa_crc64/checksum.h>

#ifdef YT_USE_SSE42
    #include <util/system/cpu_id.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_SSE42

namespace NCrcNative0xE543279765927881 {

using namespace NCrc;

// http://www.intel.com/content/dam/www/public/us/en/documents/white-papers/fast-crc-computation-generic-polynomials-pclmulqdq-paper.pdf

constexpr ui64 Poly = ULL(0xE543279765927881);
constexpr ui64 Mu = ULL(0x9d9034581c0766b0); // DivXPow_64(128, poly)

constexpr ui64 XPow64ModPoly = ULL(0xe543279765927881); // ModXPow_64(64, poly)
constexpr ui64 XPow128ModPoly = ULL(0xf9c49baae9f0beb0); // ModXPow_64(128, poly)
constexpr ui64 XPow192ModPoly = ULL(0xd0665f166605dcc4); // ModXPow_64(128 + 64, poly)
constexpr ui64 XPow512ModPoly = ULL(0x209670022e7af509); // ModXPow_64(512, poly)
constexpr ui64 XPow576ModPoly = ULL(0xd85c929ddd7a7d69); // ModXPow_64(512 + 64, poly)

__m128i FoldTo128(const __m128i* buf128, size_t buflen, __m128i result)
{
    const __m128i FoldBy128_128 = _mm_set_epi64x(XPow192ModPoly, XPow128ModPoly);
    const __m128i FoldBy512_128 = _mm_set_epi64x(XPow576ModPoly, XPow512ModPoly);

    if (buflen >= 3) {
        __m128i result0, result1, result2, result3;

        result0 = result;
        result1 = AlignedLoad(buf128++);
        result2 = AlignedLoad(buf128++);
        result3 = AlignedLoad(buf128++);
        buflen -= 3;

        size_t count = buflen / 4;
        buflen = buflen % 4;

        while (count--) {
            result0 = Fold(result0, AlignedLoad(buf128 + 0), FoldBy512_128);
            result1 = Fold(result1, AlignedLoad(buf128 + 1), FoldBy512_128);
            result2 = Fold(result2, AlignedLoad(buf128 + 2), FoldBy512_128);
            result3 = Fold(result3, AlignedLoad(buf128 + 3), FoldBy512_128);
            buf128 += 4;
        }

        result1 = Fold(result0, result1, FoldBy128_128);
        result2 = Fold(result1, result2, FoldBy128_128);
        result = Fold(result2, result3, FoldBy128_128);
    }

    while (buflen--) {
        result = Fold(result, AlignedLoad(buf128++), FoldBy128_128);
    }

    return result;
}

ui64 Crc(const void* buf, size_t buflen, ui64 seed) Y_NO_SANITIZE("memory")
{
    const ui8* ptr = reinterpret_cast<const ui8*>(buf);

    const __m128i FoldBy64_128 = _mm_set_epi64x(XPow128ModPoly, XPow64ModPoly);
    const __m128i FoldBy128_128 = _mm_set_epi64x(XPow192ModPoly, XPow128ModPoly);

    __m128i result = _mm_set_epi64x(seed, 0);

    if (buflen >= 16) {
        result = _mm_xor_si128(result, UnalignedLoad(ptr));
        buflen -= 16;
        ptr += 16;

        if (buflen >= 16) {
            if (size_t offset = 16 - (reinterpret_cast<size_t>(ptr) % 16)) {
                result = FoldTail(result, UnalignedLoad(ptr, offset), FoldBy128_128, offset);
                buflen -= offset;
                ptr += offset;
            }

            size_t rest = buflen % 16;
            result = FoldTo128(reinterpret_cast<const __m128i*>(ptr), buflen / 16, result);
            ptr += buflen - rest;
            buflen = rest;
        }

        if (buflen) {
            result = FoldTail(result, UnalignedLoad(ptr, buflen), FoldBy128_128, buflen);
            buflen -= buflen;
            ptr += buflen;
        }

        result = Fold(result, FoldBy64_128);
    } else if (buflen) {
        __m128i tail = _mm_shift_right_si128(_mm_shift_left_si128(result, buflen), 8);
        result = _mm_shift_right_si128(_mm_xor_si128(result, UnalignedLoad(ptr, buflen)), 16 - buflen);
        result = Fold(result, tail, FoldBy64_128);
    } else {
        result = _mm_shift_right_si128(result, 8);
    }

    return BarretReduction(result, Poly, Mu);
}

} // namespace NCrcNative0xE543279765927881

#endif

////////////////////////////////////////////////////////////////////////////////

namespace NCrcTable0xE543279765927881 {

ui64 CrcLookup[8][256] = {
    {
        ULL(0x0000000000000000), ULL(0x81789265972743e5), ULL(0x8389b6aeb968c52f), ULL(0x02f124cb2e4f86ca),
        ULL(0x06136d5d73d18a5f), ULL(0x876bff38e4f6c9ba), ULL(0x859adbf3cab94f70), ULL(0x04e249965d9e0c95),
        ULL(0x0c26dabae6a215bf), ULL(0x8d5e48df7185565a), ULL(0x8faf6c145fcad090), ULL(0x0ed7fe71c8ed9375),
        ULL(0x0a35b7e795739fe0), ULL(0x8b4d25820254dc05), ULL(0x89bc01492c1b5acf), ULL(0x08c4932cbb3c192a),
        ULL(0x993426105a62689b), ULL(0x184cb475cd452b7e), ULL(0x1abd90bee30aadb4), ULL(0x9bc502db742dee51),
        ULL(0x9f274b4d29b3e2c4), ULL(0x1e5fd928be94a121), ULL(0x1caefde390db27eb), ULL(0x9dd66f8607fc640e),
        ULL(0x9512fcaabcc07d24), ULL(0x146a6ecf2be73ec1), ULL(0x169b4a0405a8b80b), ULL(0x97e3d861928ffbee),
        ULL(0x930191f7cf11f77b), ULL(0x127903925836b49e), ULL(0x1088275976793254), ULL(0x91f0b53ce15e71b1),
        ULL(0xb311de4523e393d3), ULL(0x32694c20b4c4d036), ULL(0x309868eb9a8b56fc), ULL(0xb1e0fa8e0dac1519),
        ULL(0xb502b3185032198c), ULL(0x347a217dc7155a69), ULL(0x368b05b6e95adca3), ULL(0xb7f397d37e7d9f46),
        ULL(0xbf3704ffc541866c), ULL(0x3e4f969a5266c589), ULL(0x3cbeb2517c294343), ULL(0xbdc62034eb0e00a6),
        ULL(0xb92469a2b6900c33), ULL(0x385cfbc721b74fd6), ULL(0x3aaddf0c0ff8c91c), ULL(0xbbd54d6998df8af9),
        ULL(0x2a25f8557981fb48), ULL(0xab5d6a30eea6b8ad), ULL(0xa9ac4efbc0e93e67), ULL(0x28d4dc9e57ce7d82),
        ULL(0x2c3695080a507117), ULL(0xad4e076d9d7732f2), ULL(0xafbf23a6b338b438), ULL(0x2ec7b1c3241ff7dd),
        ULL(0x260322ef9f23eef7), ULL(0xa77bb08a0804ad12), ULL(0xa58a9441264b2bd8), ULL(0x24f20624b16c683d),
        ULL(0x20104fb2ecf264a8), ULL(0xa168ddd77bd5274d), ULL(0xa399f91c559aa187), ULL(0x22e16b79c2bde262),
        ULL(0xe75b2eeed1e16442), ULL(0x6623bc8b46c627a7), ULL(0x64d298406889a16d), ULL(0xe5aa0a25ffaee288),
        ULL(0xe14843b3a230ee1d), ULL(0x6030d1d63517adf8), ULL(0x62c1f51d1b582b32), ULL(0xe3b967788c7f68d7),
        ULL(0xeb7df454374371fd), ULL(0x6a056631a0643218), ULL(0x68f442fa8e2bb4d2), ULL(0xe98cd09f190cf737),
        ULL(0xed6e99094492fba2), ULL(0x6c160b6cd3b5b847), ULL(0x6ee72fa7fdfa3e8d), ULL(0xef9fbdc26add7d68),
        ULL(0x7e6f08fe8b830cd9), ULL(0xff179a9b1ca44f3c), ULL(0xfde6be5032ebc9f6), ULL(0x7c9e2c35a5cc8a13),
        ULL(0x787c65a3f8528686), ULL(0xf904f7c66f75c563), ULL(0xfbf5d30d413a43a9), ULL(0x7a8d4168d61d004c),
        ULL(0x7249d2446d211966), ULL(0xf3314021fa065a83), ULL(0xf1c064ead449dc49), ULL(0x70b8f68f436e9fac),
        ULL(0x745abf191ef09339), ULL(0xf5222d7c89d7d0dc), ULL(0xf7d309b7a7985616), ULL(0x76ab9bd230bf15f3),
        ULL(0x544af0abf202f791), ULL(0xd53262ce6525b474), ULL(0xd7c346054b6a32be), ULL(0x56bbd460dc4d715b),
        ULL(0x52599df681d37dce), ULL(0xd3210f9316f43e2b), ULL(0xd1d02b5838bbb8e1), ULL(0x50a8b93daf9cfb04),
        ULL(0x586c2a1114a0e22e), ULL(0xd914b8748387a1cb), ULL(0xdbe59cbfadc82701), ULL(0x5a9d0eda3aef64e4),
        ULL(0x5e7f474c67716871), ULL(0xdf07d529f0562b94), ULL(0xddf6f1e2de19ad5e), ULL(0x5c8e6387493eeebb),
        ULL(0xcd7ed6bba8609f0a), ULL(0x4c0644de3f47dcef), ULL(0x4ef7601511085a25), ULL(0xcf8ff270862f19c0),
        ULL(0xcb6dbbe6dbb11555), ULL(0x4a1529834c9656b0), ULL(0x48e40d4862d9d07a), ULL(0xc99c9f2df5fe939f),
        ULL(0xc1580c014ec28ab5), ULL(0x40209e64d9e5c950), ULL(0x42d1baaff7aa4f9a), ULL(0xc3a928ca608d0c7f),
        ULL(0xc74b615c3d1300ea), ULL(0x4633f339aa34430f), ULL(0x44c2d7f2847bc5c5), ULL(0xc5ba4597135c8620),
        ULL(0xceb75cdca3c3c984), ULL(0x4fcfceb934e48a61), ULL(0x4d3eea721aab0cab), ULL(0xcc4678178d8c4f4e),
        ULL(0xc8a43181d01243db), ULL(0x49dca3e44735003e), ULL(0x4b2d872f697a86f4), ULL(0xca55154afe5dc511),
        ULL(0xc29186664561dc3b), ULL(0x43e91403d2469fde), ULL(0x411830c8fc091914), ULL(0xc060a2ad6b2e5af1),
        ULL(0xc482eb3b36b05664), ULL(0x45fa795ea1971581), ULL(0x470b5d958fd8934b), ULL(0xc673cff018ffd0ae),
        ULL(0x57837accf9a1a11f), ULL(0xd6fbe8a96e86e2fa), ULL(0xd40acc6240c96430), ULL(0x55725e07d7ee27d5),
        ULL(0x519017918a702b40), ULL(0xd0e885f41d5768a5), ULL(0xd219a13f3318ee6f), ULL(0x5361335aa43fad8a),
        ULL(0x5ba5a0761f03b4a0), ULL(0xdadd32138824f745), ULL(0xd82c16d8a66b718f), ULL(0x595484bd314c326a),
        ULL(0x5db6cd2b6cd23eff), ULL(0xdcce5f4efbf57d1a), ULL(0xde3f7b85d5bafbd0), ULL(0x5f47e9e0429db835),
        ULL(0x7da6829980205a57), ULL(0xfcde10fc170719b2), ULL(0xfe2f343739489f78), ULL(0x7f57a652ae6fdc9d),
        ULL(0x7bb5efc4f3f1d008), ULL(0xfacd7da164d693ed), ULL(0xf83c596a4a991527), ULL(0x7944cb0fddbe56c2),
        ULL(0x7180582366824fe8), ULL(0xf0f8ca46f1a50c0d), ULL(0xf209ee8ddfea8ac7), ULL(0x73717ce848cdc922),
        ULL(0x7793357e1553c5b7), ULL(0xf6eba71b82748652), ULL(0xf41a83d0ac3b0098), ULL(0x756211b53b1c437d),
        ULL(0xe492a489da4232cc), ULL(0x65ea36ec4d657129), ULL(0x671b1227632af7e3), ULL(0xe6638042f40db406),
        ULL(0xe281c9d4a993b893), ULL(0x63f95bb13eb4fb76), ULL(0x61087f7a10fb7dbc), ULL(0xe070ed1f87dc3e59),
        ULL(0xe8b47e333ce02773), ULL(0x69ccec56abc76496), ULL(0x6b3dc89d8588e25c), ULL(0xea455af812afa1b9),
        ULL(0xeea7136e4f31ad2c), ULL(0x6fdf810bd816eec9), ULL(0x6d2ea5c0f6596803), ULL(0xec5637a5617e2be6),
        ULL(0x29ec72327222adc6), ULL(0xa894e057e505ee23), ULL(0xaa65c49ccb4a68e9), ULL(0x2b1d56f95c6d2b0c),
        ULL(0x2fff1f6f01f32799), ULL(0xae878d0a96d4647c), ULL(0xac76a9c1b89be2b6), ULL(0x2d0e3ba42fbca153),
        ULL(0x25caa8889480b879), ULL(0xa4b23aed03a7fb9c), ULL(0xa6431e262de87d56), ULL(0x273b8c43bacf3eb3),
        ULL(0x23d9c5d5e7513226), ULL(0xa2a157b0707671c3), ULL(0xa050737b5e39f709), ULL(0x2128e11ec91eb4ec),
        ULL(0xb0d854222840c55d), ULL(0x31a0c647bf6786b8), ULL(0x3351e28c91280072), ULL(0xb22970e9060f4397),
        ULL(0xb6cb397f5b914f02), ULL(0x37b3ab1accb60ce7), ULL(0x35428fd1e2f98a2d), ULL(0xb43a1db475dec9c8),
        ULL(0xbcfe8e98cee2d0e2), ULL(0x3d861cfd59c59307), ULL(0x3f773836778a15cd), ULL(0xbe0faa53e0ad5628),
        ULL(0xbaede3c5bd335abd), ULL(0x3b9571a02a141958), ULL(0x3964556b045b9f92), ULL(0xb81cc70e937cdc77),
        ULL(0x9afdac7751c13e15), ULL(0x1b853e12c6e67df0), ULL(0x19741ad9e8a9fb3a), ULL(0x980c88bc7f8eb8df),
        ULL(0x9ceec12a2210b44a), ULL(0x1d96534fb537f7af), ULL(0x1f6777849b787165), ULL(0x9e1fe5e10c5f3280),
        ULL(0x96db76cdb7632baa), ULL(0x17a3e4a82044684f), ULL(0x1552c0630e0bee85), ULL(0x942a5206992cad60),
        ULL(0x90c81b90c4b2a1f5), ULL(0x11b089f55395e210), ULL(0x1341ad3e7dda64da), ULL(0x92393f5beafd273f),
        ULL(0x03c98a670ba3568e), ULL(0x82b118029c84156b), ULL(0x80403cc9b2cb93a1), ULL(0x0138aeac25ecd044),
        ULL(0x05dae73a7872dcd1), ULL(0x84a2755fef559f34), ULL(0x86535194c11a19fe), ULL(0x072bc3f1563d5a1b),
        ULL(0x0fef50dded014331), ULL(0x8e97c2b87a2600d4), ULL(0x8c66e6735469861e), ULL(0x0d1e7416c34ec5fb),
        ULL(0x09fc3d809ed0c96e), ULL(0x8884afe509f78a8b), ULL(0x8a758b2e27b80c41), ULL(0x0b0d194bb09f4fa4)
    }, {
        ULL(0x0000000000000000), ULL(0x1d172bddd0a0d0ec), ULL(0xbb56c4df3666e23c), ULL(0xa641ef02e6c632d0),
        ULL(0x76ad88bf6dccc479), ULL(0x6bbaa362bd6c1495), ULL(0xcdfb4c605baa2645), ULL(0xd0ec67bd8b0af6a9),
        ULL(0xec5a117fdb9889f3), ULL(0xf14d3aa20b38591f), ULL(0x570cd5a0edfe6bcf), ULL(0x4a1bfe7d3d5ebb23),
        ULL(0x9af799c0b6544d8a), ULL(0x87e0b21d66f49d66), ULL(0x21a15d1f8032afb6), ULL(0x3cb676c250927f5a),
        ULL(0x59cdb09b21165002), ULL(0x44da9b46f1b680ee), ULL(0xe29b74441770b23e), ULL(0xff8c5f99c7d062d2),
        ULL(0x2f6038244cda947b), ULL(0x327713f99c7a4497), ULL(0x9436fcfb7abc7647), ULL(0x8921d726aa1ca6ab),
        ULL(0xb597a1e4fa8ed9f1), ULL(0xa8808a392a2e091d), ULL(0x0ec1653bcce83bcd), ULL(0x13d64ee61c48eb21),
        ULL(0xc33a295b97421d88), ULL(0xde2d028647e2cd64), ULL(0x786ced84a124ffb4), ULL(0x657bc65971842f58),
        ULL(0xb29a6137432ca004), ULL(0xaf8d4aea938c70e8), ULL(0x09cca5e8754a4238), ULL(0x14db8e35a5ea92d4),
        ULL(0xc437e9882ee0647d), ULL(0xd920c255fe40b491), ULL(0x7f612d5718868641), ULL(0x6276068ac82656ad),
        ULL(0x5ec0704898b429f7), ULL(0x43d75b954814f91b), ULL(0xe596b497aed2cbcb), ULL(0xf8819f4a7e721b27),
        ULL(0x286df8f7f578ed8e), ULL(0x357ad32a25d83d62), ULL(0x933b3c28c31e0fb2), ULL(0x8e2c17f513bedf5e),
        ULL(0xeb57d1ac623af006), ULL(0xf640fa71b29a20ea), ULL(0x50011573545c123a), ULL(0x4d163eae84fcc2d6),
        ULL(0x9dfa59130ff6347f), ULL(0x80ed72cedf56e493), ULL(0x26ac9dcc3990d643), ULL(0x3bbbb611e93006af),
        ULL(0x070dc0d3b9a279f5), ULL(0x1a1aeb0e6902a919), ULL(0xbc5b040c8fc49bc9), ULL(0xa14c2fd15f644b25),
        ULL(0x71a0486cd46ebd8c), ULL(0x6cb763b104ce6d60), ULL(0xcaf68cb3e2085fb0), ULL(0xd7e1a76e32a88f5c),
        ULL(0x6435c36e86584009), ULL(0x7922e8b356f890e5), ULL(0xdf6307b1b03ea235), ULL(0xc2742c6c609e72d9),
        ULL(0x12984bd1eb948470), ULL(0x0f8f600c3b34549c), ULL(0xa9ce8f0eddf2664c), ULL(0xb4d9a4d30d52b6a0),
        ULL(0x886fd2115dc0c9fa), ULL(0x9578f9cc8d601916), ULL(0x333916ce6ba62bc6), ULL(0x2e2e3d13bb06fb2a),
        ULL(0xfec25aae300c0d83), ULL(0xe3d57173e0acdd6f), ULL(0x45949e71066aefbf), ULL(0x5883b5acd6ca3f53),
        ULL(0x3df873f5a74e100b), ULL(0x20ef582877eec0e7), ULL(0x86aeb72a9128f237), ULL(0x9bb99cf7418822db),
        ULL(0x4b55fb4aca82d472), ULL(0x5642d0971a22049e), ULL(0xf0033f95fce4364e), ULL(0xed1414482c44e6a2),
        ULL(0xd1a2628a7cd699f8), ULL(0xccb54957ac764914), ULL(0x6af4a6554ab07bc4), ULL(0x77e38d889a10ab28),
        ULL(0xa70fea35111a5d81), ULL(0xba18c1e8c1ba8d6d), ULL(0x1c592eea277cbfbd), ULL(0x014e0537f7dc6f51),
        ULL(0xd6afa259c574e00d), ULL(0xcbb8898415d430e1), ULL(0x6df96686f3120231), ULL(0x70ee4d5b23b2d2dd),
        ULL(0xa0022ae6a8b82474), ULL(0xbd15013b7818f498), ULL(0x1b54ee399edec648), ULL(0x0643c5e44e7e16a4),
        ULL(0x3af5b3261eec69fe), ULL(0x27e298fbce4cb912), ULL(0x81a377f9288a8bc2), ULL(0x9cb45c24f82a5b2e),
        ULL(0x4c583b997320ad87), ULL(0x514f1044a3807d6b), ULL(0xf70eff4645464fbb), ULL(0xea19d49b95e69f57),
        ULL(0x8f6212c2e462b00f), ULL(0x9275391f34c260e3), ULL(0x3434d61dd2045233), ULL(0x2923fdc002a482df),
        ULL(0xf9cf9a7d89ae7476), ULL(0xe4d8b1a0590ea49a), ULL(0x42995ea2bfc8964a), ULL(0x5f8e757f6f6846a6),
        ULL(0x633803bd3ffa39fc), ULL(0x7e2f2860ef5ae910), ULL(0xd86ec762099cdbc0), ULL(0xc579ecbfd93c0b2c),
        ULL(0x15958b025236fd85), ULL(0x0882a0df82962d69), ULL(0xaec34fdd64501fb9), ULL(0xb3d46400b4f0cf55),
        ULL(0xc86a86dd0cb18012), ULL(0xd57dad00dc1150fe), ULL(0x733c42023ad7622e), ULL(0x6e2b69dfea77b2c2),
        ULL(0xbec70e62617d446b), ULL(0xa3d025bfb1dd9487), ULL(0x0591cabd571ba657), ULL(0x1886e16087bb76bb),
        ULL(0x243097a2d72909e1), ULL(0x3927bc7f0789d90d), ULL(0x9f66537de14febdd), ULL(0x827178a031ef3b31),
        ULL(0x529d1f1dbae5cd98), ULL(0x4f8a34c06a451d74), ULL(0xe9cbdbc28c832fa4), ULL(0xf4dcf01f5c23ff48),
        ULL(0x91a736462da7d010), ULL(0x8cb01d9bfd0700fc), ULL(0x2af1f2991bc1322c), ULL(0x37e6d944cb61e2c0),
        ULL(0xe70abef9406b1469), ULL(0xfa1d952490cbc485), ULL(0x5c5c7a26760df655), ULL(0x414b51fba6ad26b9),
        ULL(0x7dfd2739f63f59e3), ULL(0x60ea0ce4269f890f), ULL(0xc6abe3e6c059bbdf), ULL(0xdbbcc83b10f96b33),
        ULL(0x0b50af869bf39d9a), ULL(0x1647845b4b534d76), ULL(0xb0066b59ad957fa6), ULL(0xad1140847d35af4a),
        ULL(0x7af0e7ea4f9d2016), ULL(0x67e7cc379f3df0fa), ULL(0xc1a6233579fbc22a), ULL(0xdcb108e8a95b12c6),
        ULL(0x0c5d6f552251e46f), ULL(0x114a4488f2f13483), ULL(0xb70bab8a14370653), ULL(0xaa1c8057c497d6bf),
        ULL(0x96aaf6959405a9e5), ULL(0x8bbddd4844a57909), ULL(0x2dfc324aa2634bd9), ULL(0x30eb199772c39b35),
        ULL(0xe0077e2af9c96d9c), ULL(0xfd1055f72969bd70), ULL(0x5b51baf5cfaf8fa0), ULL(0x464691281f0f5f4c),
        ULL(0x233d57716e8b7014), ULL(0x3e2a7cacbe2ba0f8), ULL(0x986b93ae58ed9228), ULL(0x857cb873884d42c4),
        ULL(0x5590dfce0347b46d), ULL(0x4887f413d3e76481), ULL(0xeec61b1135215651), ULL(0xf3d130cce58186bd),
        ULL(0xcf67460eb513f9e7), ULL(0xd2706dd365b3290b), ULL(0x743182d183751bdb), ULL(0x6926a90c53d5cb37),
        ULL(0xb9caceb1d8df3d9e), ULL(0xa4dde56c087fed72), ULL(0x029c0a6eeeb9dfa2), ULL(0x1f8b21b33e190f4e),
        ULL(0xac5f45b38ae9c01b), ULL(0xb1486e6e5a4910f7), ULL(0x1709816cbc8f2227), ULL(0x0a1eaab16c2ff2cb),
        ULL(0xdaf2cd0ce7250462), ULL(0xc7e5e6d13785d48e), ULL(0x61a409d3d143e65e), ULL(0x7cb3220e01e336b2),
        ULL(0x400554cc517149e8), ULL(0x5d127f1181d19904), ULL(0xfb5390136717abd4), ULL(0xe644bbceb7b77b38),
        ULL(0x36a8dc733cbd8d91), ULL(0x2bbff7aeec1d5d7d), ULL(0x8dfe18ac0adb6fad), ULL(0x90e93371da7bbf41),
        ULL(0xf592f528abff9019), ULL(0xe885def57b5f40f5), ULL(0x4ec431f79d997225), ULL(0x53d31a2a4d39a2c9),
        ULL(0x833f7d97c6335460), ULL(0x9e28564a1693848c), ULL(0x3869b948f055b65c), ULL(0x257e929520f566b0),
        ULL(0x19c8e457706719ea), ULL(0x04dfcf8aa0c7c906), ULL(0xa29e20884601fbd6), ULL(0xbf890b5596a12b3a),
        ULL(0x6f656ce81dabdd93), ULL(0x72724735cd0b0d7f), ULL(0xd433a8372bcd3faf), ULL(0xc92483eafb6def43),
        ULL(0x1ec52484c9c5601f), ULL(0x03d20f591965b0f3), ULL(0xa593e05bffa38223), ULL(0xb884cb862f0352cf),
        ULL(0x6868ac3ba409a466), ULL(0x757f87e674a9748a), ULL(0xd33e68e4926f465a), ULL(0xce29433942cf96b6),
        ULL(0xf29f35fb125de9ec), ULL(0xef881e26c2fd3900), ULL(0x49c9f124243b0bd0), ULL(0x54dedaf9f49bdb3c),
        ULL(0x8432bd447f912d95), ULL(0x99259699af31fd79), ULL(0x3f64799b49f7cfa9), ULL(0x2273524699571f45),
        ULL(0x4708941fe8d3301d), ULL(0x5a1fbfc23873e0f1), ULL(0xfc5e50c0deb5d221), ULL(0xe1497b1d0e1502cd),
        ULL(0x31a51ca0851ff464), ULL(0x2cb2377d55bf2488), ULL(0x8af3d87fb3791658), ULL(0x97e4f3a263d9c6b4),
        ULL(0xab528560334bb9ee), ULL(0xb645aebde3eb6902), ULL(0x100441bf052d5bd2), ULL(0x0d136a62d58d8b3e),
        ULL(0xddff0ddf5e877d97), ULL(0xc0e826028e27ad7b), ULL(0x66a9c90068e19fab), ULL(0x7bbee2ddb8414f47)
    }, {
        ULL(0x0000000000000000), ULL(0x90d50cbb19620125), ULL(0x20ab197633c4024a), ULL(0xb07e15cd2aa6036f),
        ULL(0x405633ec66880594), ULL(0xd0833f577fea04b1), ULL(0x60fd2a9a554c07de), ULL(0xf02826214c2e06fb),
        ULL(0x01d4f4bd5a3748cd), ULL(0x9101f806435549e8), ULL(0x217fedcb69f34a87), ULL(0xb1aae17070914ba2),
        ULL(0x4182c7513cbf4d59), ULL(0xd157cbea25dd4c7c), ULL(0x6129de270f7b4f13), ULL(0xf1fcd29c16194e36),
        ULL(0x83d07b1e2249d37f), ULL(0x130577a53b2bd25a), ULL(0xa37b6268118dd135), ULL(0x33ae6ed308efd010),
        ULL(0xc38648f244c1d6eb), ULL(0x535344495da3d7ce), ULL(0xe32d51847705d4a1), ULL(0x73f85d3f6e67d584),
        ULL(0x82048fa3787e9bb2), ULL(0x12d18318611c9a97), ULL(0xa2af96d54bba99f8), ULL(0x327a9a6e52d898dd),
        ULL(0xc252bc4f1ef69e26), ULL(0x5287b0f407949f03), ULL(0xe2f9a5392d329c6c), ULL(0x722ca98234509d49),
        ULL(0x06a1f73c4492a6ff), ULL(0x9674fb875df0a7da), ULL(0x260aee4a7756a4b5), ULL(0xb6dfe2f16e34a590),
        ULL(0x46f7c4d0221aa36b), ULL(0xd622c86b3b78a24e), ULL(0x665cdda611dea121), ULL(0xf689d11d08bca004),
        ULL(0x077503811ea5ee32), ULL(0x97a00f3a07c7ef17), ULL(0x27de1af72d61ec78), ULL(0xb70b164c3403ed5d),
        ULL(0x4723306d782deba6), ULL(0xd7f63cd6614fea83), ULL(0x6788291b4be9e9ec), ULL(0xf75d25a0528be8c9),
        ULL(0x85718c2266db7580), ULL(0x15a480997fb974a5), ULL(0xa5da9554551f77ca), ULL(0x350f99ef4c7d76ef),
        ULL(0xc527bfce00537014), ULL(0x55f2b37519317131), ULL(0xe58ca6b83397725e), ULL(0x7559aa032af5737b),
        ULL(0x84a5789f3cec3d4d), ULL(0x14707424258e3c68), ULL(0xa40e61e90f283f07), ULL(0x34db6d52164a3e22),
        ULL(0xc4f34b735a6438d9), ULL(0x542647c8430639fc), ULL(0xe458520569a03a93), ULL(0x748d5ebe70c23bb6),
        ULL(0x8d3a7d1c1f030e1a), ULL(0x1def71a706610f3f), ULL(0xad91646a2cc70c50), ULL(0x3d4468d135a50d75),
        ULL(0xcd6c4ef0798b0b8e), ULL(0x5db9424b60e90aab), ULL(0xedc757864a4f09c4), ULL(0x7d125b3d532d08e1),
        ULL(0x8cee89a1453446d7), ULL(0x1c3b851a5c5647f2), ULL(0xac4590d776f0449d), ULL(0x3c909c6c6f9245b8),
        ULL(0xccb8ba4d23bc4343), ULL(0x5c6db6f63ade4266), ULL(0xec13a33b10784109), ULL(0x7cc6af80091a402c),
        ULL(0x0eea06023d4add65), ULL(0x9e3f0ab92428dc40), ULL(0x2e411f740e8edf2f), ULL(0xbe9413cf17ecde0a),
        ULL(0x4ebc35ee5bc2d8f1), ULL(0xde69395542a0d9d4), ULL(0x6e172c986806dabb), ULL(0xfec220237164db9e),
        ULL(0x0f3ef2bf677d95a8), ULL(0x9febfe047e1f948d), ULL(0x2f95ebc954b997e2), ULL(0xbf40e7724ddb96c7),
        ULL(0x4f68c15301f5903c), ULL(0xdfbdcde818979119), ULL(0x6fc3d82532319276), ULL(0xff16d49e2b539353),
        ULL(0x8b9b8a205b91a8e5), ULL(0x1b4e869b42f3a9c0), ULL(0xab3093566855aaaf), ULL(0x3be59fed7137ab8a),
        ULL(0xcbcdb9cc3d19ad71), ULL(0x5b18b577247bac54), ULL(0xeb66a0ba0eddaf3b), ULL(0x7bb3ac0117bfae1e),
        ULL(0x8a4f7e9d01a6e028), ULL(0x1a9a722618c4e10d), ULL(0xaae467eb3262e262), ULL(0x3a316b502b00e347),
        ULL(0xca194d71672ee5bc), ULL(0x5acc41ca7e4ce499), ULL(0xeab2540754eae7f6), ULL(0x7a6758bc4d88e6d3),
        ULL(0x084bf13e79d87b9a), ULL(0x989efd8560ba7abf), ULL(0x28e0e8484a1c79d0), ULL(0xb835e4f3537e78f5),
        ULL(0x481dc2d21f507e0e), ULL(0xd8c8ce6906327f2b), ULL(0x68b6dba42c947c44), ULL(0xf863d71f35f67d61),
        ULL(0x099f058323ef3357), ULL(0x994a09383a8d3272), ULL(0x29341cf5102b311d), ULL(0xb9e1104e09493038),
        ULL(0x49c9366f456736c3), ULL(0xd91c3ad45c0537e6), ULL(0x69622f1976a33489), ULL(0xf9b723a26fc135ac),
        ULL(0x1a75fa383e061c34), ULL(0x8aa0f68327641d11), ULL(0x3adee34e0dc21e7e), ULL(0xaa0beff514a01f5b),
        ULL(0x5a23c9d4588e19a0), ULL(0xcaf6c56f41ec1885), ULL(0x7a88d0a26b4a1bea), ULL(0xea5ddc1972281acf),
        ULL(0x1ba10e85643154f9), ULL(0x8b74023e7d5355dc), ULL(0x3b0a17f357f556b3), ULL(0xabdf1b484e975796),
        ULL(0x5bf73d6902b9516d), ULL(0xcb2231d21bdb5048), ULL(0x7b5c241f317d5327), ULL(0xeb8928a4281f5202),
        ULL(0x99a581261c4fcf4b), ULL(0x09708d9d052dce6e), ULL(0xb90e98502f8bcd01), ULL(0x29db94eb36e9cc24),
        ULL(0xd9f3b2ca7ac7cadf), ULL(0x4926be7163a5cbfa), ULL(0xf958abbc4903c895), ULL(0x698da7075061c9b0),
        ULL(0x9871759b46788786), ULL(0x08a479205f1a86a3), ULL(0xb8da6ced75bc85cc), ULL(0x280f60566cde84e9),
        ULL(0xd827467720f08212), ULL(0x48f24acc39928337), ULL(0xf88c5f0113348058), ULL(0x685953ba0a56817d),
        ULL(0x1cd40d047a94bacb), ULL(0x8c0101bf63f6bbee), ULL(0x3c7f14724950b881), ULL(0xacaa18c95032b9a4),
        ULL(0x5c823ee81c1cbf5f), ULL(0xcc573253057ebe7a), ULL(0x7c29279e2fd8bd15), ULL(0xecfc2b2536babc30),
        ULL(0x1d00f9b920a3f206), ULL(0x8dd5f50239c1f323), ULL(0x3dabe0cf1367f04c), ULL(0xad7eec740a05f169),
        ULL(0x5d56ca55462bf792), ULL(0xcd83c6ee5f49f6b7), ULL(0x7dfdd32375eff5d8), ULL(0xed28df986c8df4fd),
        ULL(0x9f04761a58dd69b4), ULL(0x0fd17aa141bf6891), ULL(0xbfaf6f6c6b196bfe), ULL(0x2f7a63d7727b6adb),
        ULL(0xdf5245f63e556c20), ULL(0x4f87494d27376d05), ULL(0xfff95c800d916e6a), ULL(0x6f2c503b14f36f4f),
        ULL(0x9ed082a702ea2179), ULL(0x0e058e1c1b88205c), ULL(0xbe7b9bd1312e2333), ULL(0x2eae976a284c2216),
        ULL(0xde86b14b646224ed), ULL(0x4e53bdf07d0025c8), ULL(0xfe2da83d57a626a7), ULL(0x6ef8a4864ec42782),
        ULL(0x974f87242105122e), ULL(0x079a8b9f3867130b), ULL(0xb7e49e5212c11064), ULL(0x273192e90ba31141),
        ULL(0xd719b4c8478d17ba), ULL(0x47ccb8735eef169f), ULL(0xf7b2adbe744915f0), ULL(0x6767a1056d2b14d5),
        ULL(0x969b73997b325ae3), ULL(0x064e7f2262505bc6), ULL(0xb6306aef48f658a9), ULL(0x26e566545194598c),
        ULL(0xd6cd40751dba5f77), ULL(0x46184cce04d85e52), ULL(0xf66659032e7e5d3d), ULL(0x66b355b8371c5c18),
        ULL(0x149ffc3a034cc151), ULL(0x844af0811a2ec074), ULL(0x3434e54c3088c31b), ULL(0xa4e1e9f729eac23e),
        ULL(0x54c9cfd665c4c4c5), ULL(0xc41cc36d7ca6c5e0), ULL(0x7462d6a05600c68f), ULL(0xe4b7da1b4f62c7aa),
        ULL(0x154b0887597b899c), ULL(0x859e043c401988b9), ULL(0x35e011f16abf8bd6), ULL(0xa5351d4a73dd8af3),
        ULL(0x551d3b6b3ff38c08), ULL(0xc5c837d026918d2d), ULL(0x75b6221d0c378e42), ULL(0xe5632ea615558f67),
        ULL(0x91ee70186597b4d1), ULL(0x013b7ca37cf5b5f4), ULL(0xb145696e5653b69b), ULL(0x219065d54f31b7be),
        ULL(0xd1b843f4031fb145), ULL(0x416d4f4f1a7db060), ULL(0xf1135a8230dbb30f), ULL(0x61c6563929b9b22a),
        ULL(0x903a84a53fa0fc1c), ULL(0x00ef881e26c2fd39), ULL(0xb0919dd30c64fe56), ULL(0x204491681506ff73),
        ULL(0xd06cb7495928f988), ULL(0x40b9bbf2404af8ad), ULL(0xf0c7ae3f6aecfbc2), ULL(0x6012a284738efae7),
        ULL(0x123e0b0647de67ae), ULL(0x82eb07bd5ebc668b), ULL(0x32951270741a65e4), ULL(0xa2401ecb6d7864c1),
        ULL(0x526838ea2156623a), ULL(0xc2bd34513834631f), ULL(0x72c3219c12926070), ULL(0xe2162d270bf06155),
        ULL(0x13eaffbb1de92f63), ULL(0x833ff300048b2e46), ULL(0x3341e6cd2e2d2d29), ULL(0xa394ea76374f2c0c),
        ULL(0x53bccc577b612af7), ULL(0xc369c0ec62032bd2), ULL(0x7317d52148a528bd), ULL(0xe3c2d99a51c72998)
    }, {
        ULL(0x0000000000000000), ULL(0x34eaf4717c0c3868), ULL(0x68d4e9e3f81870d0), ULL(0x5c3e1d92841448b8),
        ULL(0x51d041a26616a345), ULL(0x653ab5d31a1a9b2d), ULL(0x3904a8419e0ed395), ULL(0x0dee5c30e202ebfd),
        ULL(0xa2a08344cd2c468b), ULL(0x964a7735b1207ee3), ULL(0xca746aa73534365b), ULL(0xfe9e9ed649380e33),
        ULL(0xf370c2e6ab3ae5ce), ULL(0xc79a3697d736dda6), ULL(0x9ba42b055322951e), ULL(0xaf4edf742f2ead76),
        ULL(0xc53995ec0d7ecff3), ULL(0xf1d3619d7172f79b), ULL(0xaded7c0ff566bf23), ULL(0x9907887e896a874b),
        ULL(0x94e9d44e6b686cb6), ULL(0xa003203f176454de), ULL(0xfc3d3dad93701c66), ULL(0xc8d7c9dcef7c240e),
        ULL(0x679916a8c0528978), ULL(0x5373e2d9bc5eb110), ULL(0x0f4dff4b384af9a8), ULL(0x3ba70b3a4446c1c0),
        ULL(0x3649570aa6442a3d), ULL(0x02a3a37bda481255), ULL(0x5e9dbee95e5c5aed), ULL(0x6a774a9822506285),
        ULL(0x0b0bb8bc8cdbdd02), ULL(0x3fe14ccdf0d7e56a), ULL(0x63df515f74c3add2), ULL(0x5735a52e08cf95ba),
        ULL(0x5adbf91eeacd7e47), ULL(0x6e310d6f96c1462f), ULL(0x320f10fd12d50e97), ULL(0x06e5e48c6ed936ff),
        ULL(0xa9ab3bf841f79b89), ULL(0x9d41cf893dfba3e1), ULL(0xc17fd21bb9efeb59), ULL(0xf595266ac5e3d331),
        ULL(0xf87b7a5a27e138cc), ULL(0xcc918e2b5bed00a4), ULL(0x90af93b9dff9481c), ULL(0xa44567c8a3f57074),
        ULL(0xce322d5081a512f1), ULL(0xfad8d921fda92a99), ULL(0xa6e6c4b379bd6221), ULL(0x920c30c205b15a49),
        ULL(0x9fe26cf2e7b3b1b4), ULL(0xab0898839bbf89dc), ULL(0xf73685111fabc164), ULL(0xc3dc716063a7f90c),
        ULL(0x6c92ae144c89547a), ULL(0x58785a6530856c12), ULL(0x044647f7b49124aa), ULL(0x30acb386c89d1cc2),
        ULL(0x3d42efb62a9ff73f), ULL(0x09a81bc75693cf57), ULL(0x55960655d28787ef), ULL(0x617cf224ae8bbf87),
        ULL(0x1616707919b7bb05), ULL(0x22fc840865bb836d), ULL(0x7ec2999ae1afcbd5), ULL(0x4a286deb9da3f3bd),
        ULL(0x47c631db7fa11840), ULL(0x732cc5aa03ad2028), ULL(0x2f12d83887b96890), ULL(0x1bf82c49fbb550f8),
        ULL(0xb4b6f33dd49bfd8e), ULL(0x805c074ca897c5e6), ULL(0xdc621ade2c838d5e), ULL(0xe888eeaf508fb536),
        ULL(0xe566b29fb28d5ecb), ULL(0xd18c46eece8166a3), ULL(0x8db25b7c4a952e1b), ULL(0xb958af0d36991673),
        ULL(0xd32fe59514c974f6), ULL(0xe7c511e468c54c9e), ULL(0xbbfb0c76ecd10426), ULL(0x8f11f80790dd3c4e),
        ULL(0x82ffa43772dfd7b3), ULL(0xb61550460ed3efdb), ULL(0xea2b4dd48ac7a763), ULL(0xdec1b9a5f6cb9f0b),
        ULL(0x718f66d1d9e5327d), ULL(0x456592a0a5e90a15), ULL(0x195b8f3221fd42ad), ULL(0x2db17b435df17ac5),
        ULL(0x205f2773bff39138), ULL(0x14b5d302c3ffa950), ULL(0x488bce9047ebe1e8), ULL(0x7c613ae13be7d980),
        ULL(0x1d1dc8c5956c6607), ULL(0x29f73cb4e9605e6f), ULL(0x75c921266d7416d7), ULL(0x4123d55711782ebf),
        ULL(0x4ccd8967f37ac542), ULL(0x78277d168f76fd2a), ULL(0x241960840b62b592), ULL(0x10f394f5776e8dfa),
        ULL(0xbfbd4b815840208c), ULL(0x8b57bff0244c18e4), ULL(0xd769a262a058505c), ULL(0xe3835613dc546834),
        ULL(0xee6d0a233e5683c9), ULL(0xda87fe52425abba1), ULL(0x86b9e3c0c64ef319), ULL(0xb25317b1ba42cb71),
        ULL(0xd8245d299812a9f4), ULL(0xeccea958e41e919c), ULL(0xb0f0b4ca600ad924), ULL(0x841a40bb1c06e14c),
        ULL(0x89f41c8bfe040ab1), ULL(0xbd1ee8fa820832d9), ULL(0xe120f568061c7a61), ULL(0xd5ca01197a104209),
        ULL(0x7a84de6d553eef7f), ULL(0x4e6e2a1c2932d717), ULL(0x1250378ead269faf), ULL(0x26bac3ffd12aa7c7),
        ULL(0x2b549fcf33284c3a), ULL(0x1fbe6bbe4f247452), ULL(0x4380762ccb303cea), ULL(0x776a825db73c0482),
        ULL(0x2c2ce0f2326e770b), ULL(0x18c614834e624f63), ULL(0x44f80911ca7607db), ULL(0x7012fd60b67a3fb3),
        ULL(0x7dfca1505478d44e), ULL(0x491655212874ec26), ULL(0x152848b3ac60a49e), ULL(0x21c2bcc2d06c9cf6),
        ULL(0x8e8c63b6ff423180), ULL(0xba6697c7834e09e8), ULL(0xe6588a55075a4150), ULL(0xd2b27e247b567938),
        ULL(0xdf5c2214995492c5), ULL(0xebb6d665e558aaad), ULL(0xb788cbf7614ce215), ULL(0x83623f861d40da7d),
        ULL(0xe915751e3f10b8f8), ULL(0xddff816f431c8090), ULL(0x81c19cfdc708c828), ULL(0xb52b688cbb04f040),
        ULL(0xb8c534bc59061bbd), ULL(0x8c2fc0cd250a23d5), ULL(0xd011dd5fa11e6b6d), ULL(0xe4fb292edd125305),
        ULL(0x4bb5f65af23cfe73), ULL(0x7f5f022b8e30c61b), ULL(0x23611fb90a248ea3), ULL(0x178bebc87628b6cb),
        ULL(0x1a65b7f8942a5d36), ULL(0x2e8f4389e826655e), ULL(0x72b15e1b6c322de6), ULL(0x465baa6a103e158e),
        ULL(0x2727584ebeb5aa09), ULL(0x13cdac3fc2b99261), ULL(0x4ff3b1ad46addad9), ULL(0x7b1945dc3aa1e2b1),
        ULL(0x76f719ecd8a3094c), ULL(0x421ded9da4af3124), ULL(0x1e23f00f20bb799c), ULL(0x2ac9047e5cb741f4),
        ULL(0x8587db0a7399ec82), ULL(0xb16d2f7b0f95d4ea), ULL(0xed5332e98b819c52), ULL(0xd9b9c698f78da43a),
        ULL(0xd4579aa8158f4fc7), ULL(0xe0bd6ed9698377af), ULL(0xbc83734bed973f17), ULL(0x8869873a919b077f),
        ULL(0xe21ecda2b3cb65fa), ULL(0xd6f439d3cfc75d92), ULL(0x8aca24414bd3152a), ULL(0xbe20d03037df2d42),
        ULL(0xb3ce8c00d5ddc6bf), ULL(0x87247871a9d1fed7), ULL(0xdb1a65e32dc5b66f), ULL(0xeff0919251c98e07),
        ULL(0x40be4ee67ee72371), ULL(0x7454ba9702eb1b19), ULL(0x286aa70586ff53a1), ULL(0x1c805374faf36bc9),
        ULL(0x116e0f4418f18034), ULL(0x2584fb3564fdb85c), ULL(0x79bae6a7e0e9f0e4), ULL(0x4d5012d69ce5c88c),
        ULL(0x3a3a908b2bd9cc0e), ULL(0x0ed064fa57d5f466), ULL(0x52ee7968d3c1bcde), ULL(0x66048d19afcd84b6),
        ULL(0x6bead1294dcf6f4b), ULL(0x5f00255831c35723), ULL(0x033e38cab5d71f9b), ULL(0x37d4ccbbc9db27f3),
        ULL(0x989a13cfe6f58a85), ULL(0xac70e7be9af9b2ed), ULL(0xf04efa2c1eedfa55), ULL(0xc4a40e5d62e1c23d),
        ULL(0xc94a526d80e329c0), ULL(0xfda0a61cfcef11a8), ULL(0xa19ebb8e78fb5910), ULL(0x95744fff04f76178),
        ULL(0xff03056726a703fd), ULL(0xcbe9f1165aab3b95), ULL(0x97d7ec84debf732d), ULL(0xa33d18f5a2b34b45),
        ULL(0xaed344c540b1a0b8), ULL(0x9a39b0b43cbd98d0), ULL(0xc607ad26b8a9d068), ULL(0xf2ed5957c4a5e800),
        ULL(0x5da38623eb8b4576), ULL(0x6949725297877d1e), ULL(0x35776fc0139335a6), ULL(0x019d9bb16f9f0dce),
        ULL(0x0c73c7818d9de633), ULL(0x389933f0f191de5b), ULL(0x64a72e62758596e3), ULL(0x504dda130989ae8b),
        ULL(0x31312837a702110c), ULL(0x05dbdc46db0e2964), ULL(0x59e5c1d45f1a61dc), ULL(0x6d0f35a5231659b4),
        ULL(0x60e16995c114b249), ULL(0x540b9de4bd188a21), ULL(0x08358076390cc299), ULL(0x3cdf74074500faf1),
        ULL(0x9391ab736a2e5787), ULL(0xa77b5f0216226fef), ULL(0xfb45429092362757), ULL(0xcfafb6e1ee3a1f3f),
        ULL(0xc241ead10c38f4c2), ULL(0xf6ab1ea07034ccaa), ULL(0xaa950332f4208412), ULL(0x9e7ff743882cbc7a),
        ULL(0xf408bddbaa7cdeff), ULL(0xc0e249aad670e697), ULL(0x9cdc54385264ae2f), ULL(0xa836a0492e689647),
        ULL(0xa5d8fc79cc6a7dba), ULL(0x91320808b06645d2), ULL(0xcd0c159a34720d6a), ULL(0xf9e6e1eb487e3502),
        ULL(0x56a83e9f67509874), ULL(0x6242caee1b5ca01c), ULL(0x3e7cd77c9f48e8a4), ULL(0x0a96230de344d0cc),
        ULL(0x07787f3d01463b31), ULL(0x33928b4c7d4a0359), ULL(0x6fac96def95e4be1), ULL(0x5b4662af85527389)
    }, {
        ULL(0x0000000000000000), ULL(0x5858c0e565dcee16), ULL(0xb0b080cbcbb8dd2d), ULL(0xe8e8402eae64333b),
        ULL(0x606101979771bb5b), ULL(0x3839c172f2ad554d), ULL(0xd0d1815c5cc96676), ULL(0x888941b939158860),
        ULL(0xc0c2022e2fe376b7), ULL(0x989ac2cb4a3f98a1), ULL(0x707282e5e45bab9a), ULL(0x282a42008187458c),
        ULL(0xa0a303b9b892cdec), ULL(0xf8fbc35cdd4e23fa), ULL(0x10138372732a10c1), ULL(0x484b439716f6fed7),
        ULL(0x01fd9739c9e1ae8b), ULL(0x59a557dcac3d409d), ULL(0xb14d17f2025973a6), ULL(0xe915d71767859db0),
        ULL(0x619c96ae5e9015d0), ULL(0x39c4564b3b4cfbc6), ULL(0xd12c16659528c8fd), ULL(0x8974d680f0f426eb),
        ULL(0xc13f9517e602d83c), ULL(0x996755f283de362a), ULL(0x718f15dc2dba0511), ULL(0x29d7d5394866eb07),
        ULL(0xa15e948071736367), ULL(0xf906546514af8d71), ULL(0x11ee144bbacbbe4a), ULL(0x49b6d4aedf17505c),
        ULL(0x8382bd1605e41ef2), ULL(0xdbda7df36038f0e4), ULL(0x33323dddce5cc3df), ULL(0x6b6afd38ab802dc9),
        ULL(0xe3e3bc819295a5a9), ULL(0xbbbb7c64f7494bbf), ULL(0x53533c4a592d7884), ULL(0x0b0bfcaf3cf19692),
        ULL(0x4340bf382a076845), ULL(0x1b187fdd4fdb8653), ULL(0xf3f03ff3e1bfb568), ULL(0xaba8ff1684635b7e),
        ULL(0x2321beafbd76d31e), ULL(0x7b797e4ad8aa3d08), ULL(0x93913e6476ce0e33), ULL(0xcbc9fe811312e025),
        ULL(0x827f2a2fcc05b079), ULL(0xda27eacaa9d95e6f), ULL(0x32cfaae407bd6d54), ULL(0x6a976a0162618342),
        ULL(0xe21e2bb85b740b22), ULL(0xba46eb5d3ea8e534), ULL(0x52aeab7390ccd60f), ULL(0x0af66b96f5103819),
        ULL(0x42bd2801e3e6c6ce), ULL(0x1ae5e8e4863a28d8), ULL(0xf20da8ca285e1be3), ULL(0xaa55682f4d82f5f5),
        ULL(0x22dc299674977d95), ULL(0x7a84e973114b9383), ULL(0x926ca95dbf2fa0b8), ULL(0xca3469b8daf34eae),
        ULL(0x877de9489def7e01), ULL(0xdf2529adf8339017), ULL(0x37cd69835657a32c), ULL(0x6f95a966338b4d3a),
        ULL(0xe71ce8df0a9ec55a), ULL(0xbf44283a6f422b4c), ULL(0x57ac6814c1261877), ULL(0x0ff4a8f1a4faf661),
        ULL(0x47bfeb66b20c08b6), ULL(0x1fe72b83d7d0e6a0), ULL(0xf70f6bad79b4d59b), ULL(0xaf57ab481c683b8d),
        ULL(0x27deeaf1257db3ed), ULL(0x7f862a1440a15dfb), ULL(0x976e6a3aeec56ec0), ULL(0xcf36aadf8b1980d6),
        ULL(0x86807e71540ed08a), ULL(0xded8be9431d23e9c), ULL(0x3630feba9fb60da7), ULL(0x6e683e5ffa6ae3b1),
        ULL(0xe6e17fe6c37f6bd1), ULL(0xbeb9bf03a6a385c7), ULL(0x5651ff2d08c7b6fc), ULL(0x0e093fc86d1b58ea),
        ULL(0x46427c5f7beda63d), ULL(0x1e1abcba1e31482b), ULL(0xf6f2fc94b0557b10), ULL(0xaeaa3c71d5899506),
        ULL(0x26237dc8ec9c1d66), ULL(0x7e7bbd2d8940f370), ULL(0x9693fd032724c04b), ULL(0xcecb3de642f82e5d),
        ULL(0x04ff545e980b60f3), ULL(0x5ca794bbfdd78ee5), ULL(0xb44fd49553b3bdde), ULL(0xec171470366f53c8),
        ULL(0x649e55c90f7adba8), ULL(0x3cc6952c6aa635be), ULL(0xd42ed502c4c20685), ULL(0x8c7615e7a11ee893),
        ULL(0xc43d5670b7e81644), ULL(0x9c659695d234f852), ULL(0x748dd6bb7c50cb69), ULL(0x2cd5165e198c257f),
        ULL(0xa45c57e72099ad1f), ULL(0xfc04970245454309), ULL(0x14ecd72ceb217032), ULL(0x4cb417c98efd9e24),
        ULL(0x0502c36751eace78), ULL(0x5d5a03823436206e), ULL(0xb5b243ac9a521355), ULL(0xedea8349ff8efd43),
        ULL(0x6563c2f0c69b7523), ULL(0x3d3b0215a3479b35), ULL(0xd5d3423b0d23a80e), ULL(0x8d8b82de68ff4618),
        ULL(0xc5c0c1497e09b8cf), ULL(0x9d9801ac1bd556d9), ULL(0x75704182b5b165e2), ULL(0x2d288167d06d8bf4),
        ULL(0xa5a1c0dee9780394), ULL(0xfdf9003b8ca4ed82), ULL(0x1511401522c0deb9), ULL(0x4d4980f0471c30af),
        ULL(0x0efbd2913adffd02), ULL(0x56a312745f031314), ULL(0xbe4b525af167202f), ULL(0xe61392bf94bbce39),
        ULL(0x6e9ad306adae4659), ULL(0x36c213e3c872a84f), ULL(0xde2a53cd66169b74), ULL(0x8672932803ca7562),
        ULL(0xce39d0bf153c8bb5), ULL(0x9661105a70e065a3), ULL(0x7e895074de845698), ULL(0x26d19091bb58b88e),
        ULL(0xae58d128824d30ee), ULL(0xf60011cde791def8), ULL(0x1ee851e349f5edc3), ULL(0x46b091062c2903d5),
        ULL(0x0f0645a8f33e5389), ULL(0x575e854d96e2bd9f), ULL(0xbfb6c56338868ea4), ULL(0xe7ee05865d5a60b2),
        ULL(0x6f67443f644fe8d2), ULL(0x373f84da019306c4), ULL(0xdfd7c4f4aff735ff), ULL(0x878f0411ca2bdbe9),
        ULL(0xcfc44786dcdd253e), ULL(0x979c8763b901cb28), ULL(0x7f74c74d1765f813), ULL(0x272c07a872b91605),
        ULL(0xafa546114bac9e65), ULL(0xf7fd86f42e707073), ULL(0x1f15c6da80144348), ULL(0x474d063fe5c8ad5e),
        ULL(0x8d796f873f3be3f0), ULL(0xd521af625ae70de6), ULL(0x3dc9ef4cf4833edd), ULL(0x65912fa9915fd0cb),
        ULL(0xed186e10a84a58ab), ULL(0xb540aef5cd96b6bd), ULL(0x5da8eedb63f28586), ULL(0x05f02e3e062e6b90),
        ULL(0x4dbb6da910d89547), ULL(0x15e3ad4c75047b51), ULL(0xfd0bed62db60486a), ULL(0xa5532d87bebca67c),
        ULL(0x2dda6c3e87a92e1c), ULL(0x7582acdbe275c00a), ULL(0x9d6aecf54c11f331), ULL(0xc5322c1029cd1d27),
        ULL(0x8c84f8bef6da4d7b), ULL(0xd4dc385b9306a36d), ULL(0x3c3478753d629056), ULL(0x646cb89058be7e40),
        ULL(0xece5f92961abf620), ULL(0xb4bd39cc04771836), ULL(0x5c5579e2aa132b0d), ULL(0x040db907cfcfc51b),
        ULL(0x4c46fa90d9393bcc), ULL(0x141e3a75bce5d5da), ULL(0xfcf67a5b1281e6e1), ULL(0xa4aebabe775d08f7),
        ULL(0x2c27fb074e488097), ULL(0x747f3be22b946e81), ULL(0x9c977bcc85f05dba), ULL(0xc4cfbb29e02cb3ac),
        ULL(0x89863bd9a7308303), ULL(0xd1defb3cc2ec6d15), ULL(0x3936bb126c885e2e), ULL(0x616e7bf70954b038),
        ULL(0xe9e73a4e30413858), ULL(0xb1bffaab559dd64e), ULL(0x5957ba85fbf9e575), ULL(0x010f7a609e250b63),
        ULL(0x494439f788d3f5b4), ULL(0x111cf912ed0f1ba2), ULL(0xf9f4b93c436b2899), ULL(0xa1ac79d926b7c68f),
        ULL(0x292538601fa24eef), ULL(0x717df8857a7ea0f9), ULL(0x9995b8abd41a93c2), ULL(0xc1cd784eb1c67dd4),
        ULL(0x887bace06ed12d88), ULL(0xd0236c050b0dc39e), ULL(0x38cb2c2ba569f0a5), ULL(0x6093eccec0b51eb3),
        ULL(0xe81aad77f9a096d3), ULL(0xb0426d929c7c78c5), ULL(0x58aa2dbc32184bfe), ULL(0x00f2ed5957c4a5e8),
        ULL(0x48b9aece41325b3f), ULL(0x10e16e2b24eeb529), ULL(0xf8092e058a8a8612), ULL(0xa051eee0ef566804),
        ULL(0x28d8af59d643e064), ULL(0x70806fbcb39f0e72), ULL(0x98682f921dfb3d49), ULL(0xc030ef777827d35f),
        ULL(0x0a0486cfa2d49df1), ULL(0x525c462ac70873e7), ULL(0xbab40604696c40dc), ULL(0xe2ecc6e10cb0aeca),
        ULL(0x6a65875835a526aa), ULL(0x323d47bd5079c8bc), ULL(0xdad50793fe1dfb87), ULL(0x828dc7769bc11591),
        ULL(0xcac684e18d37eb46), ULL(0x929e4404e8eb0550), ULL(0x7a76042a468f366b), ULL(0x222ec4cf2353d87d),
        ULL(0xaaa785761a46501d), ULL(0xf2ff45937f9abe0b), ULL(0x1a1705bdd1fe8d30), ULL(0x424fc558b4226326),
        ULL(0x0bf911f66b35337a), ULL(0x53a1d1130ee9dd6c), ULL(0xbb49913da08dee57), ULL(0xe31151d8c5510041),
        ULL(0x6b981061fc448821), ULL(0x33c0d08499986637), ULL(0xdb2890aa37fc550c), ULL(0x8370504f5220bb1a),
        ULL(0xcb3b13d844d645cd), ULL(0x9363d33d210aabdb), ULL(0x7b8b93138f6e98e0), ULL(0x23d353f6eab276f6),
        ULL(0xab5a124fd3a7fe96), ULL(0xf302d2aab67b1080), ULL(0x1bea9284181f23bb), ULL(0x43b252617dc3cdad)
    }, {
        ULL(0x0000000000000000), ULL(0x1cf6a52375befb05), ULL(0x38ec4b47ea7cf70b), ULL(0x241aee649fc20c0e),
        ULL(0x70d8978ed4f9ee17), ULL(0x6c2e32ada1471512), ULL(0x4834dcc93e85191c), ULL(0x54c279ea4b3be219),
        ULL(0xe0b02f1da9f3dd2f), ULL(0xfc468a3edc4d262a), ULL(0xd85c645a438f2a24), ULL(0xc4aac1793631d121),
        ULL(0x9068b8937d0a3338), ULL(0x8c9e1db008b4c83d), ULL(0xa884f3d49776c433), ULL(0xb47256f7e2c83f36),
        ULL(0xc0615f3a52e7bb5f), ULL(0xdc97fa192759405a), ULL(0xf88d147db89b4c54), ULL(0xe47bb15ecd25b751),
        ULL(0xb0b9c8b4861e5548), ULL(0xac4f6d97f3a0ae4d), ULL(0x885583f36c62a243), ULL(0x94a326d019dc5946),
        ULL(0x20d17027fb146670), ULL(0x3c27d5048eaa9d75), ULL(0x183d3b601168917b), ULL(0x04cb9e4364d66a7e),
        ULL(0x5009e7a92fed8867), ULL(0x4cff428a5a537362), ULL(0x68e5aceec5917f6c), ULL(0x741309cdb02f8469),
        ULL(0x80c3be74a4ce77bf), ULL(0x9c351b57d1708cba), ULL(0xb82ff5334eb280b4), ULL(0xa4d950103b0c7bb1),
        ULL(0xf01b29fa703799a8), ULL(0xeced8cd9058962ad), ULL(0xc8f762bd9a4b6ea3), ULL(0xd401c79eeff595a6),
        ULL(0x607391690d3daa90), ULL(0x7c85344a78835195), ULL(0x589fda2ee7415d9b), ULL(0x44697f0d92ffa69e),
        ULL(0x10ab06e7d9c44487), ULL(0x0c5da3c4ac7abf82), ULL(0x28474da033b8b38c), ULL(0x34b1e88346064889),
        ULL(0x40a2e14ef629cce0), ULL(0x5c54446d839737e5), ULL(0x784eaa091c553beb), ULL(0x64b80f2a69ebc0ee),
        ULL(0x307a76c022d022f7), ULL(0x2c8cd3e3576ed9f2), ULL(0x08963d87c8acd5fc), ULL(0x146098a4bd122ef9),
        ULL(0xa012ce535fda11cf), ULL(0xbce46b702a64eaca), ULL(0x98fe8514b5a6e6c4), ULL(0x84082037c0181dc1),
        ULL(0xd0ca59dd8b23ffd8), ULL(0xcc3cfcfefe9d04dd), ULL(0xe826129a615f08d3), ULL(0xf4d0b7b914e1f3d6),
        ULL(0x81ffef8cdfbaac9b), ULL(0x9d094aafaa04579e), ULL(0xb913a4cb35c65b90), ULL(0xa5e501e84078a095),
        ULL(0xf12778020b43428c), ULL(0xedd1dd217efdb989), ULL(0xc9cb3345e13fb587), ULL(0xd53d966694814e82),
        ULL(0x614fc091764971b4), ULL(0x7db965b203f78ab1), ULL(0x59a38bd69c3586bf), ULL(0x45552ef5e98b7dba),
        ULL(0x1197571fa2b09fa3), ULL(0x0d61f23cd70e64a6), ULL(0x297b1c5848cc68a8), ULL(0x358db97b3d7293ad),
        ULL(0x419eb0b68d5d17c4), ULL(0x5d681595f8e3ecc1), ULL(0x7972fbf16721e0cf), ULL(0x65845ed2129f1bca),
        ULL(0x3146273859a4f9d3), ULL(0x2db0821b2c1a02d6), ULL(0x09aa6c7fb3d80ed8), ULL(0x155cc95cc666f5dd),
        ULL(0xa12e9fab24aecaeb), ULL(0xbdd83a88511031ee), ULL(0x99c2d4ecced23de0), ULL(0x853471cfbb6cc6e5),
        ULL(0xd1f60825f05724fc), ULL(0xcd00ad0685e9dff9), ULL(0xe91a43621a2bd3f7), ULL(0xf5ece6416f9528f2),
        ULL(0x013c51f87b74db24), ULL(0x1dcaf4db0eca2021), ULL(0x39d01abf91082c2f), ULL(0x2526bf9ce4b6d72a),
        ULL(0x71e4c676af8d3533), ULL(0x6d126355da33ce36), ULL(0x49088d3145f1c238), ULL(0x55fe2812304f393d),
        ULL(0xe18c7ee5d287060b), ULL(0xfd7adbc6a739fd0e), ULL(0xd96035a238fbf100), ULL(0xc59690814d450a05),
        ULL(0x9154e96b067ee81c), ULL(0x8da24c4873c01319), ULL(0xa9b8a22cec021f17), ULL(0xb54e070f99bce412),
        ULL(0xc15d0ec22993607b), ULL(0xddababe15c2d9b7e), ULL(0xf9b14585c3ef9770), ULL(0xe547e0a6b6516c75),
        ULL(0xb185994cfd6a8e6c), ULL(0xad733c6f88d47569), ULL(0x8969d20b17167967), ULL(0x959f772862a88262),
        ULL(0x21ed21df8060bd54), ULL(0x3d1b84fcf5de4651), ULL(0x19016a986a1c4a5f), ULL(0x05f7cfbb1fa2b15a),
        ULL(0x5135b65154995343), ULL(0x4dc313722127a846), ULL(0x69d9fd16bee5a448), ULL(0x752f5835cb5b5f4d),
        ULL(0x83874d7c28521ad2), ULL(0x9f71e85f5dece1d7), ULL(0xbb6b063bc22eedd9), ULL(0xa79da318b79016dc),
        ULL(0xf35fdaf2fcabf4c5), ULL(0xefa97fd189150fc0), ULL(0xcbb391b516d703ce), ULL(0xd74534966369f8cb),
        ULL(0x6337626181a1c7fd), ULL(0x7fc1c742f41f3cf8), ULL(0x5bdb29266bdd30f6), ULL(0x472d8c051e63cbf3),
        ULL(0x13eff5ef555829ea), ULL(0x0f1950cc20e6d2ef), ULL(0x2b03bea8bf24dee1), ULL(0x37f51b8bca9a25e4),
        ULL(0x43e612467ab5a18d), ULL(0x5f10b7650f0b5a88), ULL(0x7b0a590190c95686), ULL(0x67fcfc22e577ad83),
        ULL(0x333e85c8ae4c4f9a), ULL(0x2fc820ebdbf2b49f), ULL(0x0bd2ce8f4430b891), ULL(0x17246bac318e4394),
        ULL(0xa3563d5bd3467ca2), ULL(0xbfa09878a6f887a7), ULL(0x9bba761c393a8ba9), ULL(0x874cd33f4c8470ac),
        ULL(0xd38eaad507bf92b5), ULL(0xcf780ff6720169b0), ULL(0xeb62e192edc365be), ULL(0xf79444b1987d9ebb),
        ULL(0x0344f3088c9c6d6d), ULL(0x1fb2562bf9229668), ULL(0x3ba8b84f66e09a66), ULL(0x275e1d6c135e6163),
        ULL(0x739c64865865837a), ULL(0x6f6ac1a52ddb787f), ULL(0x4b702fc1b2197471), ULL(0x57868ae2c7a78f74),
        ULL(0xe3f4dc15256fb042), ULL(0xff02793650d14b47), ULL(0xdb189752cf134749), ULL(0xc7ee3271baadbc4c),
        ULL(0x932c4b9bf1965e55), ULL(0x8fdaeeb88428a550), ULL(0xabc000dc1beaa95e), ULL(0xb736a5ff6e54525b),
        ULL(0xc325ac32de7bd632), ULL(0xdfd30911abc52d37), ULL(0xfbc9e77534072139), ULL(0xe73f425641b9da3c),
        ULL(0xb3fd3bbc0a823825), ULL(0xaf0b9e9f7f3cc320), ULL(0x8b1170fbe0fecf2e), ULL(0x97e7d5d89540342b),
        ULL(0x2395832f77880b1d), ULL(0x3f63260c0236f018), ULL(0x1b79c8689df4fc16), ULL(0x078f6d4be84a0713),
        ULL(0x534d14a1a371e50a), ULL(0x4fbbb182d6cf1e0f), ULL(0x6ba15fe6490d1201), ULL(0x7757fac53cb3e904),
        ULL(0x0278a2f0f7e8b649), ULL(0x1e8e07d382564d4c), ULL(0x3a94e9b71d944142), ULL(0x26624c94682aba47),
        ULL(0x72a0357e2311585e), ULL(0x6e56905d56afa35b), ULL(0x4a4c7e39c96daf55), ULL(0x56badb1abcd35450),
        ULL(0xe2c88ded5e1b6b66), ULL(0xfe3e28ce2ba59063), ULL(0xda24c6aab4679c6d), ULL(0xc6d26389c1d96768),
        ULL(0x92101a638ae28571), ULL(0x8ee6bf40ff5c7e74), ULL(0xaafc5124609e727a), ULL(0xb60af4071520897f),
        ULL(0xc219fdcaa50f0d16), ULL(0xdeef58e9d0b1f613), ULL(0xfaf5b68d4f73fa1d), ULL(0xe60313ae3acd0118),
        ULL(0xb2c16a4471f6e301), ULL(0xae37cf6704481804), ULL(0x8a2d21039b8a140a), ULL(0x96db8420ee34ef0f),
        ULL(0x22a9d2d70cfcd039), ULL(0x3e5f77f479422b3c), ULL(0x1a459990e6802732), ULL(0x06b33cb3933edc37),
        ULL(0x52714559d8053e2e), ULL(0x4e87e07aadbbc52b), ULL(0x6a9d0e1e3279c925), ULL(0x766bab3d47c73220),
        ULL(0x82bb1c845326c1f6), ULL(0x9e4db9a726983af3), ULL(0xba5757c3b95a36fd), ULL(0xa6a1f2e0cce4cdf8),
        ULL(0xf2638b0a87df2fe1), ULL(0xee952e29f261d4e4), ULL(0xca8fc04d6da3d8ea), ULL(0xd679656e181d23ef),
        ULL(0x620b3399fad51cd9), ULL(0x7efd96ba8f6be7dc), ULL(0x5ae778de10a9ebd2), ULL(0x4611ddfd651710d7),
        ULL(0x12d3a4172e2cf2ce), ULL(0x0e2501345b9209cb), ULL(0x2a3fef50c45005c5), ULL(0x36c94a73b1eefec0),
        ULL(0x42da43be01c17aa9), ULL(0x5e2ce69d747f81ac), ULL(0x7a3608f9ebbd8da2), ULL(0x66c0adda9e0376a7),
        ULL(0x3202d430d53894be), ULL(0x2ef47113a0866fbb), ULL(0x0aee9f773f4463b5), ULL(0x16183a544afa98b0),
        ULL(0xa26a6ca3a832a786), ULL(0xbe9cc980dd8c5c83), ULL(0x9a8627e4424e508d), ULL(0x867082c737f0ab88),
        ULL(0xd2b2fb2d7ccb4991), ULL(0xce445e0e0975b294), ULL(0xea5eb06a96b7be9a), ULL(0xf6a81549e309459f)
    }, {
        ULL(0x0000000000000000), ULL(0x8777099dc7837741), ULL(0x0eef123a8f07ef82), ULL(0x89981ba7488498c3),
        ULL(0x9da6b71189289de0), ULL(0x1ad1be8c4eabeaa1), ULL(0x9349a52b062f7262), ULL(0x143eacb6c1ac0523),
        ULL(0xbb35fd4685767924), ULL(0x3c42f4db42f50e65), ULL(0xb5daef7c0a7196a6), ULL(0x32ade6e1cdf2e1e7),
        ULL(0x26934a570c5ee4c4), ULL(0xa1e443cacbdd9385), ULL(0x287c586d83590b46), ULL(0xaf0b51f044da7c07),
        ULL(0x766bfa8d0aedf248), ULL(0xf11cf310cd6e8509), ULL(0x7884e8b785ea1dca), ULL(0xfff3e12a42696a8b),
        ULL(0xebcd4d9c83c56fa8), ULL(0x6cba4401444618e9), ULL(0xe5225fa60cc2802a), ULL(0x6255563bcb41f76b),
        ULL(0xcd5e07cb8f9b8b6c), ULL(0x4a290e564818fc2d), ULL(0xc3b115f1009c64ee), ULL(0x44c61c6cc71f13af),
        ULL(0x50f8b0da06b3168c), ULL(0xd78fb947c13061cd), ULL(0x5e17a2e089b4f90e), ULL(0xd960ab7d4e378e4f),
        ULL(0xecd6f41b15dae591), ULL(0x6ba1fd86d25992d0), ULL(0xe239e6219add0a13), ULL(0x654eefbc5d5e7d52),
        ULL(0x7170430a9cf27871), ULL(0xf6074a975b710f30), ULL(0x7f9f513013f597f3), ULL(0xf8e858add476e0b2),
        ULL(0x57e3095d90ac9cb5), ULL(0xd09400c0572febf4), ULL(0x590c1b671fab7337), ULL(0xde7b12fad8280476),
        ULL(0xca45be4c19840155), ULL(0x4d32b7d1de077614), ULL(0xc4aaac769683eed7), ULL(0x43dda5eb51009996),
        ULL(0x9abd0e961f3717d9), ULL(0x1dca070bd8b46098), ULL(0x94521cac9030f85b), ULL(0x1325153157b38f1a),
        ULL(0x071bb987961f8a39), ULL(0x806cb01a519cfd78), ULL(0x09f4abbd191865bb), ULL(0x8e83a220de9b12fa),
        ULL(0x2188f3d09a416efd), ULL(0xa6fffa4d5dc219bc), ULL(0x2f67e1ea1546817f), ULL(0xa810e877d2c5f63e),
        ULL(0xbc2e44c11369f31d), ULL(0x3b594d5cd4ea845c), ULL(0xb2c156fb9c6e1c9f), ULL(0x35b65f665bed6bde),
        ULL(0x59d57b52bd9388c6), ULL(0xdea272cf7a10ff87), ULL(0x573a696832946744), ULL(0xd04d60f5f5171005),
        ULL(0xc473cc4334bb1526), ULL(0x4304c5def3386267), ULL(0xca9cde79bbbcfaa4), ULL(0x4debd7e47c3f8de5),
        ULL(0xe2e0861438e5f1e2), ULL(0x65978f89ff6686a3), ULL(0xec0f942eb7e21e60), ULL(0x6b789db370616921),
        ULL(0x7f463105b1cd6c02), ULL(0xf8313898764e1b43), ULL(0x71a9233f3eca8380), ULL(0xf6de2aa2f949f4c1),
        ULL(0x2fbe81dfb77e7a8e), ULL(0xa8c9884270fd0dcf), ULL(0x215193e53879950c), ULL(0xa6269a78fffae24d),
        ULL(0xb21836ce3e56e76e), ULL(0x356f3f53f9d5902f), ULL(0xbcf724f4b15108ec), ULL(0x3b802d6976d27fad),
        ULL(0x948b7c99320803aa), ULL(0x13fc7504f58b74eb), ULL(0x9a646ea3bd0fec28), ULL(0x1d13673e7a8c9b69),
        ULL(0x092dcb88bb209e4a), ULL(0x8e5ac2157ca3e90b), ULL(0x07c2d9b2342771c8), ULL(0x80b5d02ff3a40689),
        ULL(0xb5038f49a8496d57), ULL(0x327486d46fca1a16), ULL(0xbbec9d73274e82d5), ULL(0x3c9b94eee0cdf594),
        ULL(0x28a538582161f0b7), ULL(0xafd231c5e6e287f6), ULL(0x264a2a62ae661f35), ULL(0xa13d23ff69e56874),
        ULL(0x0e36720f2d3f1473), ULL(0x89417b92eabc6332), ULL(0x00d96035a238fbf1), ULL(0x87ae69a865bb8cb0),
        ULL(0x9390c51ea4178993), ULL(0x14e7cc836394fed2), ULL(0x9d7fd7242b106611), ULL(0x1a08deb9ec931150),
        ULL(0xc36875c4a2a49f1f), ULL(0x441f7c596527e85e), ULL(0xcd8767fe2da3709d), ULL(0x4af06e63ea2007dc),
        ULL(0x5ecec2d52b8c02ff), ULL(0xd9b9cb48ec0f75be), ULL(0x5021d0efa48bed7d), ULL(0xd756d97263089a3c),
        ULL(0x785d888227d2e63b), ULL(0xff2a811fe051917a), ULL(0x76b29ab8a8d509b9), ULL(0xf1c593256f567ef8),
        ULL(0xe5fb3f93aefa7bdb), ULL(0x628c360e69790c9a), ULL(0xeb142da921fd9459), ULL(0x6c632434e67ee318),
        ULL(0x33d265c1ed005268), ULL(0xb4a56c5c2a832529), ULL(0x3d3d77fb6207bdea), ULL(0xba4a7e66a584caab),
        ULL(0xae74d2d06428cf88), ULL(0x2903db4da3abb8c9), ULL(0xa09bc0eaeb2f200a), ULL(0x27ecc9772cac574b),
        ULL(0x88e7988768762b4c), ULL(0x0f90911aaff55c0d), ULL(0x86088abde771c4ce), ULL(0x017f832020f2b38f),
        ULL(0x15412f96e15eb6ac), ULL(0x9236260b26ddc1ed), ULL(0x1bae3dac6e59592e), ULL(0x9cd93431a9da2e6f),
        ULL(0x45b99f4ce7eda020), ULL(0xc2ce96d1206ed761), ULL(0x4b568d7668ea4fa2), ULL(0xcc2184ebaf6938e3),
        ULL(0xd81f285d6ec53dc0), ULL(0x5f6821c0a9464a81), ULL(0xd6f03a67e1c2d242), ULL(0x518733fa2641a503),
        ULL(0xfe8c620a629bd904), ULL(0x79fb6b97a518ae45), ULL(0xf0637030ed9c3686), ULL(0x771479ad2a1f41c7),
        ULL(0x632ad51bebb344e4), ULL(0xe45ddc862c3033a5), ULL(0x6dc5c72164b4ab66), ULL(0xeab2cebca337dc27),
        ULL(0xdf0491daf8dab7f9), ULL(0x587398473f59c0b8), ULL(0xd1eb83e077dd587b), ULL(0x569c8a7db05e2f3a),
        ULL(0x42a226cb71f22a19), ULL(0xc5d52f56b6715d58), ULL(0x4c4d34f1fef5c59b), ULL(0xcb3a3d6c3976b2da),
        ULL(0x64316c9c7daccedd), ULL(0xe3466501ba2fb99c), ULL(0x6ade7ea6f2ab215f), ULL(0xeda9773b3528561e),
        ULL(0xf997db8df484533d), ULL(0x7ee0d2103307247c), ULL(0xf778c9b77b83bcbf), ULL(0x700fc02abc00cbfe),
        ULL(0xa96f6b57f23745b1), ULL(0x2e1862ca35b432f0), ULL(0xa780796d7d30aa33), ULL(0x20f770f0bab3dd72),
        ULL(0x34c9dc467b1fd851), ULL(0xb3bed5dbbc9caf10), ULL(0x3a26ce7cf41837d3), ULL(0xbd51c7e1339b4092),
        ULL(0x125a961177413c95), ULL(0x952d9f8cb0c24bd4), ULL(0x1cb5842bf846d317), ULL(0x9bc28db63fc5a456),
        ULL(0x8ffc2100fe69a175), ULL(0x088b289d39ead634), ULL(0x8113333a716e4ef7), ULL(0x06643aa7b6ed39b6),
        ULL(0x6a071e935093daae), ULL(0xed70170e9710adef), ULL(0x64e80ca9df94352c), ULL(0xe39f05341817426d),
        ULL(0xf7a1a982d9bb474e), ULL(0x70d6a01f1e38300f), ULL(0xf94ebbb856bca8cc), ULL(0x7e39b225913fdf8d),
        ULL(0xd132e3d5d5e5a38a), ULL(0x5645ea481266d4cb), ULL(0xdfddf1ef5ae24c08), ULL(0x58aaf8729d613b49),
        ULL(0x4c9454c45ccd3e6a), ULL(0xcbe35d599b4e492b), ULL(0x427b46fed3cad1e8), ULL(0xc50c4f631449a6a9),
        ULL(0x1c6ce41e5a7e28e6), ULL(0x9b1bed839dfd5fa7), ULL(0x1283f624d579c764), ULL(0x95f4ffb912fab025),
        ULL(0x81ca530fd356b506), ULL(0x06bd5a9214d5c247), ULL(0x8f2541355c515a84), ULL(0x085248a89bd22dc5),
        ULL(0xa7591958df0851c2), ULL(0x202e10c5188b2683), ULL(0xa9b60b62500fbe40), ULL(0x2ec102ff978cc901),
        ULL(0x3affae495620cc22), ULL(0xbd88a7d491a3bb63), ULL(0x3410bc73d92723a0), ULL(0xb367b5ee1ea454e1),
        ULL(0x86d1ea8845493f3f), ULL(0x01a6e31582ca487e), ULL(0x883ef8b2ca4ed0bd), ULL(0x0f49f12f0dcda7fc),
        ULL(0x1b775d99cc61a2df), ULL(0x9c0054040be2d59e), ULL(0x15984fa343664d5d), ULL(0x92ef463e84e53a1c),
        ULL(0x3de417cec03f461b), ULL(0xba931e5307bc315a), ULL(0x330b05f44f38a999), ULL(0xb47c0c6988bbded8),
        ULL(0xa042a0df4917dbfb), ULL(0x2735a9428e94acba), ULL(0xaeadb2e5c6103479), ULL(0x29dabb7801934338),
        ULL(0xf0ba10054fa4cd77), ULL(0x77cd19988827ba36), ULL(0xfe55023fc0a322f5), ULL(0x79220ba2072055b4),
        ULL(0x6d1ca714c68c5097), ULL(0xea6bae89010f27d6), ULL(0x63f3b52e498bbf15), ULL(0xe484bcb38e08c854),
        ULL(0x4b8fed43cad2b453), ULL(0xccf8e4de0d51c312), ULL(0x4560ff7945d55bd1), ULL(0xc217f6e482562c90),
        ULL(0xd6295a5243fa29b3), ULL(0x515e53cf84795ef2), ULL(0xd8c64868ccfdc631), ULL(0x5fb141f50b7eb170)
    }, {
        ULL(0x0000000000000000), ULL(0x66a4cb82db01a4d0), ULL(0x4d30056020240b44), ULL(0x2b94cee2fb25af94),
        ULL(0x9a600ac040481688), ULL(0xfcc4c1429b49b258), ULL(0xd7500fa0606c1dcc), ULL(0xb1f4c422bb6db91c),
        ULL(0xb5b986e516b76ff5), ULL(0xd31d4d67cdb6cb25), ULL(0xf8898385369364b1), ULL(0x9e2d4807ed92c061),
        ULL(0x2fd98c2556ff797d), ULL(0x497d47a78dfeddad), ULL(0x62e9894576db7239), ULL(0x044d42c7addad6e9),
        ULL(0xeb0b9faeba499c0f), ULL(0x8daf542c614838df), ULL(0xa63b9ace9a6d974b), ULL(0xc09f514c416c339b),
        ULL(0x716b956efa018a87), ULL(0x17cf5eec21002e57), ULL(0x3c5b900eda2581c3), ULL(0x5aff5b8c01242513),
        ULL(0x5eb2194bacfef3fa), ULL(0x3816d2c977ff572a), ULL(0x13821c2b8cdaf8be), ULL(0x7526d7a957db5c6e),
        ULL(0xc4d2138becb6e572), ULL(0xa276d80937b741a2), ULL(0x89e216ebcc92ee36), ULL(0xef46dd6917934ae6),
        ULL(0xd6173e5d7593381f), ULL(0xb0b3f5dfae929ccf), ULL(0x9b273b3d55b7335b), ULL(0xfd83f0bf8eb6978b),
        ULL(0x4c77349d35db2e97), ULL(0x2ad3ff1feeda8a47), ULL(0x014731fd15ff25d3), ULL(0x67e3fa7fcefe8103),
        ULL(0x63aeb8b8632457ea), ULL(0x050a733ab825f33a), ULL(0x2e9ebdd843005cae), ULL(0x483a765a9801f87e),
        ULL(0xf9ceb278236c4162), ULL(0x9f6a79faf86de5b2), ULL(0xb4feb71803484a26), ULL(0xd25a7c9ad849eef6),
        ULL(0x3d1ca1f3cfdaa410), ULL(0x5bb86a7114db00c0), ULL(0x702ca493effeaf54), ULL(0x16886f1134ff0b84),
        ULL(0xa77cab338f92b298), ULL(0xc1d860b154931648), ULL(0xea4cae53afb6b9dc), ULL(0x8ce865d174b71d0c),
        ULL(0x88a52716d96dcbe5), ULL(0xee01ec94026c6f35), ULL(0xc5952276f949c0a1), ULL(0xa331e9f422486471),
        ULL(0x12c52dd69925dd6d), ULL(0x7461e654422479bd), ULL(0x5ff528b6b901d629), ULL(0x3951e334620072f9),
        ULL(0xac2f7cbaea26713e), ULL(0xca8bb7383127d5ee), ULL(0xe11f79daca027a7a), ULL(0x87bbb2581103deaa),
        ULL(0x364f767aaa6e67b6), ULL(0x50ebbdf8716fc366), ULL(0x7b7f731a8a4a6cf2), ULL(0x1ddbb898514bc822),
        ULL(0x1996fa5ffc911ecb), ULL(0x7f3231dd2790ba1b), ULL(0x54a6ff3fdcb5158f), ULL(0x320234bd07b4b15f),
        ULL(0x83f6f09fbcd90843), ULL(0xe5523b1d67d8ac93), ULL(0xcec6f5ff9cfd0307), ULL(0xa8623e7d47fca7d7),
        ULL(0x4724e314506fed31), ULL(0x218028968b6e49e1), ULL(0x0a14e674704be675), ULL(0x6cb02df6ab4a42a5),
        ULL(0xdd44e9d41027fbb9), ULL(0xbbe02256cb265f69), ULL(0x9074ecb43003f0fd), ULL(0xf6d02736eb02542d),
        ULL(0xf29d65f146d882c4), ULL(0x9439ae739dd92614), ULL(0xbfad609166fc8980), ULL(0xd909ab13bdfd2d50),
        ULL(0x68fd6f310690944c), ULL(0x0e59a4b3dd91309c), ULL(0x25cd6a5126b49f08), ULL(0x4369a1d3fdb53bd8),
        ULL(0x7a3842e79fb54921), ULL(0x1c9c896544b4edf1), ULL(0x37084787bf914265), ULL(0x51ac8c056490e6b5),
        ULL(0xe0584827dffd5fa9), ULL(0x86fc83a504fcfb79), ULL(0xad684d47ffd954ed), ULL(0xcbcc86c524d8f03d),
        ULL(0xcf81c402890226d4), ULL(0xa9250f8052038204), ULL(0x82b1c162a9262d90), ULL(0xe4150ae072278940),
        ULL(0x55e1cec2c94a305c), ULL(0x33450540124b948c), ULL(0x18d1cba2e96e3b18), ULL(0x7e750020326f9fc8),
        ULL(0x9133dd4925fcd52e), ULL(0xf79716cbfefd71fe), ULL(0xdc03d82905d8de6a), ULL(0xbaa713abded97aba),
        ULL(0x0b53d78965b4c3a6), ULL(0x6df71c0bbeb56776), ULL(0x4663d2e94590c8e2), ULL(0x20c7196b9e916c32),
        ULL(0x248a5bac334bbadb), ULL(0x422e902ee84a1e0b), ULL(0x69ba5ecc136fb19f), ULL(0x0f1e954ec86e154f),
        ULL(0xbeea516c7303ac53), ULL(0xd84e9aeea8020883), ULL(0xf3da540c5327a717), ULL(0x957e9f8e882603c7),
        ULL(0x585ff874d54de27c), ULL(0x3efb33f60e4c46ac), ULL(0x156ffd14f569e938), ULL(0x73cb36962e684de8),
        ULL(0xc23ff2b49505f4f4), ULL(0xa49b39364e045024), ULL(0x8f0ff7d4b521ffb0), ULL(0xe9ab3c566e205b60),
        ULL(0xede67e91c3fa8d89), ULL(0x8b42b51318fb2959), ULL(0xa0d67bf1e3de86cd), ULL(0xc672b07338df221d),
        ULL(0x7786745183b29b01), ULL(0x1122bfd358b33fd1), ULL(0x3ab67131a3969045), ULL(0x5c12bab378973495),
        ULL(0xb35467da6f047e73), ULL(0xd5f0ac58b405daa3), ULL(0xfe6462ba4f207537), ULL(0x98c0a9389421d1e7),
        ULL(0x29346d1a2f4c68fb), ULL(0x4f90a698f44dcc2b), ULL(0x6404687a0f6863bf), ULL(0x02a0a3f8d469c76f),
        ULL(0x06ede13f79b31186), ULL(0x60492abda2b2b556), ULL(0x4bdde45f59971ac2), ULL(0x2d792fdd8296be12),
        ULL(0x9c8debff39fb070e), ULL(0xfa29207de2faa3de), ULL(0xd1bdee9f19df0c4a), ULL(0xb719251dc2dea89a),
        ULL(0x8e48c629a0deda63), ULL(0xe8ec0dab7bdf7eb3), ULL(0xc378c34980fad127), ULL(0xa5dc08cb5bfb75f7),
        ULL(0x1428cce9e096cceb), ULL(0x728c076b3b97683b), ULL(0x5918c989c0b2c7af), ULL(0x3fbc020b1bb3637f),
        ULL(0x3bf140ccb669b596), ULL(0x5d558b4e6d681146), ULL(0x76c145ac964dbed2), ULL(0x10658e2e4d4c1a02),
        ULL(0xa1914a0cf621a31e), ULL(0xc735818e2d2007ce), ULL(0xeca14f6cd605a85a), ULL(0x8a0584ee0d040c8a),
        ULL(0x654359871a97466c), ULL(0x03e79205c196e2bc), ULL(0x28735ce73ab34d28), ULL(0x4ed79765e1b2e9f8),
        ULL(0xff2353475adf50e4), ULL(0x998798c581def434), ULL(0xb21356277afb5ba0), ULL(0xd4b79da5a1faff70),
        ULL(0xd0fadf620c202999), ULL(0xb65e14e0d7218d49), ULL(0x9dcada022c0422dd), ULL(0xfb6e1180f705860d),
        ULL(0x4a9ad5a24c683f11), ULL(0x2c3e1e2097699bc1), ULL(0x07aad0c26c4c3455), ULL(0x610e1b40b74d9085),
        ULL(0xf47084ce3f6b9342), ULL(0x92d44f4ce46a3792), ULL(0xb94081ae1f4f9806), ULL(0xdfe44a2cc44e3cd6),
        ULL(0x6e108e0e7f2385ca), ULL(0x08b4458ca422211a), ULL(0x23208b6e5f078e8e), ULL(0x458440ec84062a5e),
        ULL(0x41c9022b29dcfcb7), ULL(0x276dc9a9f2dd5867), ULL(0x0cf9074b09f8f7f3), ULL(0x6a5dccc9d2f95323),
        ULL(0xdba908eb6994ea3f), ULL(0xbd0dc369b2954eef), ULL(0x96990d8b49b0e17b), ULL(0xf03dc60992b145ab),
        ULL(0x1f7b1b6085220f4d), ULL(0x79dfd0e25e23ab9d), ULL(0x524b1e00a5060409), ULL(0x34efd5827e07a0d9),
        ULL(0x851b11a0c56a19c5), ULL(0xe3bfda221e6bbd15), ULL(0xc82b14c0e54e1281), ULL(0xae8fdf423e4fb651),
        ULL(0xaac29d85939560b8), ULL(0xcc6656074894c468), ULL(0xe7f298e5b3b16bfc), ULL(0x8156536768b0cf2c),
        ULL(0x30a29745d3dd7630), ULL(0x56065cc708dcd2e0), ULL(0x7d929225f3f97d74), ULL(0x1b3659a728f8d9a4),
        ULL(0x2267ba934af8ab5d), ULL(0x44c3711191f90f8d), ULL(0x6f57bff36adca019), ULL(0x09f37471b1dd04c9),
        ULL(0xb807b0530ab0bdd5), ULL(0xdea37bd1d1b11905), ULL(0xf537b5332a94b691), ULL(0x93937eb1f1951241),
        ULL(0x97de3c765c4fc4a8), ULL(0xf17af7f4874e6078), ULL(0xdaee39167c6bcfec), ULL(0xbc4af294a76a6b3c),
        ULL(0x0dbe36b61c07d220), ULL(0x6b1afd34c70676f0), ULL(0x408e33d63c23d964), ULL(0x262af854e7227db4),
        ULL(0xc96c253df0b13752), ULL(0xafc8eebf2bb09382), ULL(0x845c205dd0953c16), ULL(0xe2f8ebdf0b9498c6),
        ULL(0x530c2ffdb0f921da), ULL(0x35a8e47f6bf8850a), ULL(0x1e3c2a9d90dd2a9e), ULL(0x7898e11f4bdc8e4e),
        ULL(0x7cd5a3d8e60658a7), ULL(0x1a71685a3d07fc77), ULL(0x31e5a6b8c62253e3), ULL(0x57416d3a1d23f733),
        ULL(0xe6b5a918a64e4e2f), ULL(0x8011629a7d4feaff), ULL(0xab85ac78866a456b), ULL(0xcd2167fa5d6be1bb)
    }
};

ui64 ReverseBytes(ui64 v)
{
    ui64 r = v;
    int s = sizeof(v) - 1;

    for (v >>= 8; v; v >>= 8)
    {
        r <<= 8;
        r |= v & 0xff;
        s--;
    }

    r <<= 8 * s;
    return r;
}

ui64 Crc(const void* buf, size_t buflen, ui64 crcinit)
{
    crcinit = ReverseBytes(crcinit);

    const unsigned char * ptrChar = (const unsigned char *) buf;

    while ((reinterpret_cast<size_t>(ptrChar) & 0x7) && buflen) {
        crcinit = CrcLookup[0][(crcinit ^ *ptrChar++) & 0xff] ^ (crcinit >> 8);
        --buflen;
    }

    const ui64* ptr = (const ui64 *) ptrChar;

    for (; buflen >= 8; buflen -= 8) {
        ui64 val = crcinit ^ *ptr++;

        crcinit =
            CrcLookup[7][ val        & 0xff] ^
            CrcLookup[6][(val >>  8) & 0xff] ^
            CrcLookup[5][(val >> 16) & 0xff] ^
            CrcLookup[4][(val >> 24) & 0xff] ^
            CrcLookup[3][(val >> 32) & 0xff] ^
            CrcLookup[2][(val >> 40) & 0xff] ^
            CrcLookup[1][(val >> 48) & 0xff] ^
            CrcLookup[0][ val >> 56        ];
    }

    ptrChar = (const unsigned char *) ptr;

    while (buflen--) {
        crcinit = CrcLookup[0][(crcinit ^ *ptrChar++) & 0xff] ^ (crcinit >> 8);
    }

    return ReverseBytes(crcinit);
}

} // namespace NCrcTable0xE543279765927881

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 CrcImpl(const void* data, size_t length, ui64 seed)
{
#ifdef YT_USE_SSE42
    static const bool Native = NX86::CachedHaveSSE42() && NX86::CachedHavePCLMUL();
    if (Native) {
        return NIsaCrc64::CrcImplFast(data, length, seed);
    }
#endif
    return NIsaCrc64::CrcImplBase(data, length, seed);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TChecksum CombineChecksums(const std::vector<TChecksum>& blockChecksums)
{
    TChecksum combined = NullChecksum;
    for (auto checksum : blockChecksums) {
        HashCombine(combined, checksum);
    }
    return combined;
}

TChecksum GetChecksum(TRef data, TChecksum seed)
{
    return CrcImpl(data.Begin(), data.Size(), seed);
}

////////////////////////////////////////////////////////////////////////////////

TChecksumInput::TChecksumInput(IInputStream* input)
    : Input_(input)
{ }

TChecksum TChecksumInput::GetChecksum() const
{
    return Checksum_;
}

size_t TChecksumInput::DoRead(void* buf, size_t len)
{
    size_t res = Input_->Read(buf, len);
    Checksum_ = CrcImpl(buf, res, Checksum_);
    return res;
}

////////////////////////////////////////////////////////////////////////////////

TChecksumOutput::TChecksumOutput(IOutputStream* output)
    : Output_(output)
{ }

TChecksum TChecksumOutput::GetChecksum() const
{
    return Checksum_;
}

void TChecksumOutput::DoWrite(const void* buf, size_t len)
{
    Output_->Write(buf, len);
    Checksum_ = CrcImpl(buf, len, Checksum_);
}

void TChecksumOutput::DoFlush()
{
    Output_->Flush();
}

void TChecksumOutput::DoFinish()
{
    Output_->Finish();
}

////////////////////////////////////////////////////////////////////////////////

TChecksumAsyncOutput::TChecksumAsyncOutput(IAsyncOutputStreamPtr underlyingStream)
    : UnderlyingStream_(std::move(underlyingStream))
{ }

TFuture<void> TChecksumAsyncOutput::Close()
{
    return UnderlyingStream_->Close();
}

TFuture<void> TChecksumAsyncOutput::Write(const TSharedRef& block)
{
    return UnderlyingStream_->Write(block)
        .Apply(BIND([=, this, this_ = MakeStrong(this)] {
            Checksum_ = NYT::GetChecksum(block, Checksum_);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
