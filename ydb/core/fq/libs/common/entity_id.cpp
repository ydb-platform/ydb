#include "entity_id.h"
#include <util/stream/str.h>
#include <util/system/yassert.h>
#include <util/datetime/base.h>
#include <util/system/unaligned_mem.h>
#include <util/random/easy.h>
#include <util/stream/format.h>
#include <util/string/ascii.h>

namespace NFq {

// used ascii order: IntToChar[i] < IntToChar[i+1]
constexpr char IntToChar[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', // 8
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', // 16
    'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', // 24
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', // 32
};

static bool IsValidSymbol(char symbol) {
    return '0' <= symbol && symbol <= '9' || 'a' <= symbol && symbol <= 'v';
}

// 30 bits = 6 symbols
TString Conv(ui32 n) {
    char buf[7] = {0};
    for (int i = 0; i < 6; i++) {
        buf[5 - i] = IntToChar[n & 0x1f];
        n >>= 5;
    }
    return buf;
}

// 55 bits = 11 symbols
TString ConvTime(ui64 time) {
    char buf[12] = {0};
    for (int i = 0; i < 11; i++) {
        buf[10 - i] = IntToChar[time & 0x1f];
        time >>= 5;
    }
    return buf;
}

/*
- prefix [2 symbols (10 bit)] - service identifier
- type [1 symbol (5 bit)] - resource type (query, job, connection, binding, result, ...)
- reversed time [11 symbols (55 bit)] - for provides sorting properties by creation time
- random number [6 symbols (30 bit)] - a random number to ensure the uniqueness of the keys within a microsecond
*/
TString GetEntityIdAsString(const TString& prefix, EEntityType type) {
    Y_ABORT_UNLESS(prefix.size() == 2, "prefix must contain exactly two characters");
    Y_ABORT_UNLESS(IsValidSymbol(static_cast<char>(type)), "not valid character for type");
    Y_ABORT_UNLESS(IsValidSymbol(prefix[0]), "not valid character for prefix[0]");
    Y_ABORT_UNLESS(IsValidSymbol(prefix[1]), "not valid character for prefix[1]");
    return GetEntityIdAsString(prefix, type, TInstant::Now(), RandomNumber<ui32>());
}

TString GetEntityIdAsString(const TString& prefix, EEntityType type, TInstant now, ui32 rnd) {
    // The maximum time that fits in 55 bits. Necessary for the correct inversion of time
    static constexpr TInstant MaxSeconds = TInstant::FromValue(0x7FFFFFFFFFFFFFULL);
    Y_ABORT_UNLESS(MaxSeconds >= now, "lifetime of the id interval has gone beyond the boundaries");
    ui64 reversedTime = (MaxSeconds - now).GetValue();
    TStringStream stream;
    stream << prefix << static_cast<char>(type) << ConvTime(reversedTime) << Conv(rnd);
    return stream.Str();
}

struct TEntityIdGenerator : public IEntityIdGenerator {
    TEntityIdGenerator(const TString& prefix)
        : Prefix(prefix)
    {
        Y_ABORT_UNLESS(Prefix.size() == 2);
    }

    TString Generate(EEntityType type) override {
        return GetEntityIdAsString(Prefix, type);
    }

private:
    TString Prefix;
};

IEntityIdGenerator::TPtr CreateEntityIdGenerator(const TString& prefix) {
    return MakeIntrusive<TEntityIdGenerator>(prefix);
}

} // namespace NFq
