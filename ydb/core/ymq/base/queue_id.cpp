#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NKikimr::NSQS {

// Queue id breakdown in bits [15][1][64][7][13]:
// [15] - service id
// [1] - reserved == 0
// [64] - globally unique number
// [7] - reserved == 0
// [13] - partial hash result for account name (it's not really important to have stable hash impl)
// total: 15 + 1 + 64 + 7 + 13 == 100

class TQueueId {
public:
    TQueueId(const ui16 serviceId, const ui64 uniqueNum, const TString& accountName) {
        char* bufPtr = Buf;

        // first 15 bits
        Y_ABORT_UNLESS(serviceId <= 0x7FFF);
        ConvertAndAppendLastNBits(serviceId, 15, bufPtr);

        // next 65 bits (1 bit is reserved and 64 are the place for the unique num)
        *bufPtr++ = SmallIntToBase32(uniqueNum >> 60); // first 5 bits
        ConvertAndAppendLastNBits(uniqueNum, 60, bufPtr); // 60 other bits

        // last 20 bits
        const ui32 appendix = THash<TString>()(accountName) & 0x1FFF; // use last 13 bits only
        ConvertAndAppendLastNBits(appendix, 20, bufPtr);

        Y_ABORT_UNLESS(bufPtr == &Buf[20]);

        *bufPtr = '\0';
    }

    TString AsString() const {
        return TString(Buf);
    }

private:
    static const ui64 BasicMask = 0x1F; // 11111 as binary
    static const ui8 Radix = 5;

    char Buf[21] = {};

private:
    char SmallIntToBase32(const ui8 v) const {
        Y_ABORT_UNLESS(v < 32);

        if (v < 10) {
            return '0' + v;
        } else {
            return 'a' + v - 10;
        }
    }

    void ConvertAndAppendLastNBits(const ui64 num, const ui8 n, char*& output) const {
        Y_ABORT_UNLESS(n % Radix == 0);

        for (int i = n / Radix - 1; i >= 0; --i) {
            const ui8 shift = i * Radix;
            const ui64 mask = BasicMask << shift;

            *output++ = SmallIntToBase32((num & mask) >> shift);
        }
    }
};

TString MakeQueueId(const ui16 serviceId, const ui64 uniqueNum, const TString& accountName) {
    return TQueueId(serviceId, uniqueNum, accountName).AsString();
}

} // namespace NKikimr::NSQS
