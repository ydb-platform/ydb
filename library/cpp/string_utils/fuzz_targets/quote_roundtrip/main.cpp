#include <library/cpp/string_utils/quote/quote.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/system/yassert.h>

namespace {

TString QuoteRet(TStringBuf input, const char* safe) {
    TString output;
    output.ReserveAndResize(CgiEscapeBufLen(input.size()));
    output.resize(Quote(output.begin(), input, safe) - output.data());
    return output;
}

void CheckBufferApi(TStringBuf input) {
    TString cgi;
    cgi.ReserveAndResize(CgiEscapeBufLen(input.size()));
    char* cgiEnd = CGIEscape(cgi.begin(), input.data(), input.size());
    cgi.resize(cgiEnd - cgi.data());
    Y_ABORT_UNLESS(cgi == CGIEscapeRet(input));
    Y_ABORT_UNLESS(CGIUnescapeRet(cgi) == input);

    TString url;
    url.ReserveAndResize(CgiEscapeBufLen(input.size()));
    char* urlEnd = UrlEscape(url.begin(), input, true);
    url.resize(urlEnd - url.data());
    Y_ABORT_UNLESS(url == UrlEscapeRet(input, true));
    Y_ABORT_UNLESS(UrlUnescapeRet(url) == input);
}

void CheckInPlaceApi(TStringBuf input) {
    TString cgi = TString(input);
    CGIEscape(cgi);
    CGIUnescape(cgi);
    Y_ABORT_UNLESS(cgi == input);

    TString url = TString(input);
    UrlEscape(url, true);
    UrlUnescape(url);
    Y_ABORT_UNLESS(url == input);

    TString quoted = TString(input);
    Quote(quoted, "/-_.~");
    Y_ABORT_UNLESS(CGIUnescapeRet(quoted) == input);
}

void FuzzQuoteRoundTrip(FuzzedDataProvider& provider) {
    const TString input = provider.ConsumeRandomLengthString(1024);

    CheckBufferApi(input);
    CheckInPlaceApi(input);

    for (const char* safe : {"", "/", "/-_.~"}) {
        const TString quoted = QuoteRet(input, safe);
        Y_ABORT_UNLESS(CGIUnescapeRet(quoted) == input);
    }

    const TString escaped = provider.ConsumeRemainingBytesAsString();
    const TString unescaped = CGIUnescapeRet(escaped);
    Y_ABORT_UNLESS(CGIUnescapeRet(CGIEscapeRet(unescaped)) == unescaped);
    Y_ABORT_UNLESS(UrlUnescapeRet(UrlEscapeRet(unescaped, true)) == unescaped);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    FuzzQuoteRoundTrip(provider);

    return 0;
}
