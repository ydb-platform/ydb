#include <library/cpp/string_utils/parse_vector/vector_parser.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/array_size.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/cast.h>
#include <util/string/strip.h>
#include <util/system/yassert.h>

namespace {

bool ModelParseVector(TStringBuf input, TVector<int>& result, char delim, bool useEmpty) {
    size_t pos = 0;
    while (true) {
        const size_t next = input.find(delim, pos);
        const TStringBuf token = next == TStringBuf::npos
            ? input.SubStr(pos)
            : input.SubStr(pos, next - pos);
        const TStringBuf stripped = StripString(token);
        if (useEmpty || stripped) {
            int value = 0;
            if (!TryFromString<int>(stripped, value)) {
                return false;
            }
            result.push_back(value);
        }
        if (next == TStringBuf::npos) {
            return true;
        }
        pos = next + 1;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    static constexpr char Delims[] = {',', ';', '|', ':', ' ', '\t'};
    const char delim = Delims[provider.ConsumeIntegralInRange<size_t>(0, Y_ARRAY_SIZE(Delims) - 1)];
    const bool useEmpty = provider.ConsumeBool();
    const TString input = provider.ConsumeRandomLengthString(1024);

    TVector<int> actual;
    TVector<int> expected;
    const bool actualOk = TryParseStringToVector<int>(input, actual, delim, useEmpty);
    const bool expectedOk = ModelParseVector(input, expected, delim, useEmpty);
    Y_ABORT_UNLESS(actualOk == expectedOk);
    if (actualOk) {
        Y_ABORT_UNLESS(actual == expected);
    }
    return 0;
}
