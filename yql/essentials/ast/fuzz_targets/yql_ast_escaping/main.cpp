#include <yql/essentials/ast/yql_ast_escaping.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace {

void ExerciseArbitraryRoundTrip(TStringBuf atom, char quoteChar) {
    TString escaped;
    TStringOutput escapedOut(escaped);
    NYql::EscapeArbitraryAtom(atom, quoteChar, &escapedOut);

    size_t readBytes = 0;
    TString unescaped;
    TStringOutput unescapedOut(unescaped);
    const TStringBuf escapedBody(escaped.data() + 1, escaped.size() - 1);
    const auto result = NYql::UnescapeArbitraryAtom(escapedBody, quoteChar, &unescapedOut, &readBytes);
    (void)NYql::UnescapeResultToString(result);
    (void)readBytes;

    TString escapedAgain;
    TStringOutput escapedAgainOut(escapedAgain);
    NYql::EscapeArbitraryAtom(unescaped, quoteChar, &escapedAgainOut);
}

void ExerciseBinaryRoundTrip(TStringBuf atom, char quoteChar) {
    TString escaped;
    TStringOutput escapedOut(escaped);
    NYql::EscapeBinaryAtom(atom, quoteChar, &escapedOut);

    size_t readBytes = 0;
    TString unescaped;
    TStringOutput unescapedOut(unescaped);
    const TStringBuf escapedBody(escaped.data() + 2, escaped.size() - 2);
    const auto result = NYql::UnescapeBinaryAtom(escapedBody, quoteChar, &unescapedOut, &readBytes);
    (void)NYql::UnescapeResultToString(result);
    (void)readBytes;
}

void ExerciseDirectUnescape(TStringBuf text, char endChar) {
    size_t readBytes = 0;
    TString unescaped;
    TStringOutput out(unescaped);
    const auto result = NYql::UnescapeArbitraryAtom(text, endChar, &out, &readBytes);
    (void)NYql::UnescapeResultToString(result);
    (void)readBytes;

    TString binary;
    TStringOutput binaryOut(binary);
    const auto binaryResult = NYql::UnescapeBinaryAtom(text, endChar, &binaryOut, &readBytes);
    (void)NYql::UnescapeResultToString(binaryResult);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    const TString atom = fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, 16 * 1024));
    const TString randomText = fdp.ConsumeRemainingBytesAsString();

    for (char quoteChar : {'"', '\''}) {
        try {
            ExerciseArbitraryRoundTrip(atom, quoteChar);
        } catch (...) {
        }

        try {
            ExerciseBinaryRoundTrip(atom, quoteChar);
        } catch (...) {
        }
    }

    for (char endChar : {'"', '\'', '`'}) {
        try {
            ExerciseDirectUnescape(randomText, endChar);
        } catch (...) {
        }
    }

    return 0;
}
