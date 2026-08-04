#include <library/cpp/string_utils/parse_size/parse_size.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <cctype>

namespace {

bool TryShiftLeft(ui64 value, unsigned shift, ui64& out) {
    if (value > (Max<ui64>() >> shift)) {
        return false;
    }
    out = value << shift;
    return true;
}

bool TryModelParseSize(TStringBuf input, ui64& out) {
    if (!input) {
        return false;
    }

    const unsigned char last = static_cast<unsigned char>(input.back());
    if (std::isdigit(last)) {
        return TryFromString<ui64>(input, out);
    }

    unsigned shift = 0;
    switch (std::tolower(last)) {
        case 'k': shift = 10; break;
        case 'm': shift = 20; break;
        case 'g': shift = 30; break;
        case 't': shift = 40; break;
        default: return false;
    }

    ui64 value = 0;
    return TryFromString<ui64>(input.Head(input.size() - 1), value)
        && TryShiftLeft(value, shift, out);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    const TString input = provider.ConsumeRandomLengthString(128);

    ui64 expected = 0;
    const bool expectedOk = TryModelParseSize(input, expected);
    try {
        const ui64 parsed = NSize::ParseSize(input);
        Y_ABORT_UNLESS(expectedOk && parsed == expected);
        const NSize::TSize typed = FromString<NSize::TSize>(input);
        Y_ABORT_UNLESS(typed.GetValue() == parsed);
        Y_ABORT_UNLESS(ToString(typed) == ToString(parsed));
    } catch (const yexception&) {
        Y_ABORT_UNLESS(!expectedOk);
    }

    const ui64 value = provider.ConsumeIntegral<ui64>()
        >> provider.ConsumeIntegralInRange<int>(0, 16);
    try {
        const NSize::TSize kb = NSize::FromKiloBytes(value);
        Y_ABORT_UNLESS(kb.GetValue() == (value << 10));
    } catch (const yexception&) {
        Y_ABORT_UNLESS(value > (Max<ui64>() >> 10));
    }
    return 0;
}
