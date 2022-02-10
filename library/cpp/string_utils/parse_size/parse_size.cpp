#include "parse_size.h"

#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/string/cast.h>
#include <util/stream/output.h>

namespace {
    enum ESuffixShifts {
        ESS_KILO_BYTES = 10,
        ESS_MEGA_BYTES = 20,
        ESS_GIGA_BYTES = 30,
        ESS_TERA_BYTES = 40,
    };

    bool TryShiftValue(ui64& value, ui64 shift) {
        if (value > (Max<ui64>() >> shift)) {
            return false;
        }

        value <<= shift;
        return true;
    }

    ui64 ShiftValue(ui64 value, ui64 shift) {
        if (!TryShiftValue(value, shift)) {
            ythrow yexception() << "value overflow '" << value << " << " << shift << "'";
        } else {
            return value;
        }
    }

}

namespace NSize {
    ui64 ParseSize(TStringBuf str) {
        if (! str.size())
            ythrow yexception() << "Wrong size " << str;
        char suff = tolower(str[str.size() - 1]);
        if (isdigit(suff))
            return FromString<ui64>(str);
        ui64 shift = 1;
        switch (suff) {
            case 'k':
                shift = ESS_KILO_BYTES;
                break;
            case 'm':
                shift = ESS_MEGA_BYTES;
                break;
            case 'g':
                shift = ESS_GIGA_BYTES;
                break;
            case 't':
                shift = ESS_TERA_BYTES;
                break;
            default:
                ythrow yexception() << "Unknown suffix " << str;
        }

        ui64 value = FromString<ui64>(str.substr(0, str.size() - 1));

        if (!TryShiftValue(value, shift)) {
            ythrow yexception() << "Value overflow " << str;
        } else {
            return value;
        }
    }

    TSize FromKiloBytes(ui64 value) {
        return TSize(ShiftValue(value, ESS_KILO_BYTES));
    }

    TSize FromMegaBytes(ui64 value) {
        return TSize(ShiftValue(value, ESS_MEGA_BYTES));
    }

    TSize FromGigaBytes(ui64 value) {
        return TSize(ShiftValue(value, ESS_GIGA_BYTES));
    }

    TSize FromTeraBytes(ui64 value) {
        return TSize(ShiftValue(value, ESS_TERA_BYTES));
    }

}

template <>
NSize::TSize FromStringImpl<NSize::TSize>(const char* data, size_t len) {
    return NSize::TSize(NSize::ParseSize(TStringBuf(data, len)));
}

template <>
void Out<NSize::TSize>(IOutputStream& os, const NSize::TSize& size) {
    os << size.GetValue();
}
