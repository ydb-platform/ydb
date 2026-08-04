#include <library/cpp/string_utils/url/url.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/yassert.h>

#include <cstring>

namespace {

TString MakeUrlLike(TString input) {
    static const TStringBuf Alphabet(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&'()*+,;=% ");
    for (char& c : input) {
        c = Alphabet[static_cast<unsigned char>(c) % Alphabet.size()];
    }
    return input;
}

bool IsSubBuf(TStringBuf whole, TStringBuf part) {
    return !part || (part.data() >= whole.data() && part.end() <= whole.end());
}

void CheckSeparateUrl(TStringBuf url) {
    TStringBuf sanitized;
    TStringBuf query;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(url, sanitized, query, fragment);

    TStringBuf withoutFragment;
    TStringBuf expectedFragment;
    if (!url.TrySplit('#', withoutFragment, expectedFragment)) {
        withoutFragment = url;
        expectedFragment = "";
    }

    TStringBuf expectedSanitized;
    TStringBuf expectedQuery;
    if (!withoutFragment.TrySplit('?', expectedSanitized, expectedQuery)) {
        expectedSanitized = withoutFragment;
        expectedQuery = "";
    }

    Y_ABORT_UNLESS(sanitized == expectedSanitized);
    Y_ABORT_UNLESS(query == expectedQuery);
    Y_ABORT_UNLESS(fragment == expectedFragment);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    const TString owned = MakeUrlLike(provider.ConsumeRandomLengthString(1024));
    const TStringBuf url = owned;

    const size_t httpPrefix = GetHttpPrefixSize(url);
    Y_ABORT_UNLESS(httpPrefix <= url.size());
    Y_ABORT_UNLESS(CutHttpPrefix(url) == url.Tail(httpPrefix));
    Y_ABORT_UNLESS(IsSubBuf(url, CutHttpPrefix(url, true)));

    const size_t schemePrefix = GetSchemePrefixSize(url);
    Y_ABORT_UNLESS(schemePrefix <= url.size());
    Y_ABORT_UNLESS(GetSchemePrefix(url).size() == schemePrefix);
    Y_ABORT_UNLESS(CutSchemePrefix(url) == url.Tail(schemePrefix));

    const auto split = NUrl::SplitUrlToHostAndPath(url);
    Y_ABORT_UNLESS(IsSubBuf(url, split.host));
    Y_ABORT_UNLESS(IsSubBuf(url, split.path));
    Y_ABORT_UNLESS(TString(split.host) + TString(split.path) == owned);

    TStringBuf host;
    TStringBuf path;
    SplitUrlToHostAndPath(url, host, path);
    Y_ABORT_UNLESS(host == split.host && path == split.path);

    TString hostString;
    TString pathString;
    SplitUrlToHostAndPath(url, hostString, pathString);
    Y_ABORT_UNLESS(hostString == split.host && pathString == split.path);
    CheckSeparateUrl(url);

    const TString added = AddSchemePrefix(owned, "http");
    Y_ABORT_UNLESS(AddSchemePrefix(added, "http") == added);
    if (GetSchemePrefixSize(owned) == 0) {
        Y_ABORT_UNLESS(added.StartsWith("http://"));
    }

    Y_ABORT_UNLESS(IsSubBuf(url, GetHost(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, GetHostAndPort(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, GetSchemeHost(url, false)));
    Y_ABORT_UNLESS(IsSubBuf(url, GetSchemeHostAndPort(url, false, false)));
    Y_ABORT_UNLESS(IsSubBuf(url, GetOnlyHost(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, CutWWWPrefix(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, CutWWWNumberedPrefix(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, CutMPrefix(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, CutUrlPrefixes(url)));
    Y_ABORT_UNLESS(IsSubBuf(url, RemoveFinalSlash(url)));

    const TStringBuf pathAndQuery = GetPathAndQuery(url, provider.ConsumeBool());
    Y_ABORT_UNLESS(pathAndQuery == TStringBuf("/") || IsSubBuf(url, pathAndQuery));

    TStringBuf scheme;
    TStringBuf parsedHost;
    ui16 port = 0;
    const bool tryOk = TryGetSchemeHostAndPort(url, scheme, parsedHost, port);
    Y_ABORT_UNLESS(IsSubBuf(url, scheme) && IsSubBuf(url, parsedHost));
    if (tryOk) {
        TStringBuf scheme2;
        TStringBuf host2;
        ui16 port2 = 0;
        GetSchemeHostAndPort(url, scheme2, host2, port2);
        Y_ABORT_UNLESS(scheme == scheme2 && parsedHost == host2 && port == port2);
    } else {
        try {
            TStringBuf scheme2;
            TStringBuf host2;
            ui16 port2 = 0;
            GetSchemeHostAndPort(url, scheme2, host2, port2);
            Y_ABORT("GetSchemeHostAndPort unexpectedly accepted an invalid port");
        } catch (const yexception&) {
        }
    }

    const TString token = MakeUrlLike(provider.ConsumeRandomLengthString(16));
    (void)DoesUrlPathStartWithToken(url, token);

    char nameBuf[2048];
    (void)NormalizeUrlName(nameBuf, url, sizeof(nameBuf));
    Y_ABORT_UNLESS(std::memchr(nameBuf, '\0', sizeof(nameBuf)) != nullptr);

    char hostBuf[2048];
    (void)NormalizeHostName(hostBuf, GetHostAndPort(url), sizeof(hostBuf), provider.ConsumeIntegral<ui16>());
    Y_ABORT_UNLESS(std::memchr(hostBuf, '\0', sizeof(hostBuf)) != nullptr);
    return 0;
}
