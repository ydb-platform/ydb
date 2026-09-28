#include "rfc7239_forwarded.h"

#include <library/cpp/string_utils/url/url.h>

#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/strip.h>

#include <utility>

namespace NKikimr::NHttp {
namespace {

void SkipOws(TStringBuf& s) {
    while (!s.empty() && (s[0] == ' ' || s[0] == '\t')) {
        s = s.SubStr(1);
    }
}

bool IsHttpTchar(char c) {
    return IsAsciiAlnum(c)
        || c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\''
        || c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_'
        || c == '`' || c == '|' || c == '~';
}

bool ParseHttpToken(TStringBuf& s, TStringBuf& token) {
    size_t n = 0;
    while (n < s.size() && IsHttpTchar(s[n])) {
        ++n;
    }
    if (n == 0) {
        return false;
    }
    token = s.SubStr(0, n);
    s = s.SubStr(n);
    return true;
}

// RFC 7230 quoted-string / quoted-pair.
bool ParseHttpQuotedString(TStringBuf& s, TString& value) {
    if (s.empty() || s[0] != '"') {
        return false;
    }
    s = s.SubStr(1);
    value.clear();
    while (!s.empty()) {
        const char c = s[0];
        s = s.SubStr(1);
        if (c == '"') {
            return true;
        }
        if (c == '\\') {
            if (s.empty()) {
                return false;
            }
            value.push_back(s[0]);
            s = s.SubStr(1);
            continue;
        }
        value.push_back(c);
    }
    return false;
}

// RFC 7239: value = token / quoted-string. Unquoted host:port and node:port are
// accepted too: ":" is not tchar, so the ABNF requires quotes, but Host / node
// values commonly appear unquoted.
bool ParseForwardedPairValue(TStringBuf& s, TString& value) {
    SkipOws(s);
    if (ParseHttpQuotedString(s, value)) {
        return !value.empty();
    }
    size_t n = 0;
    while (n < s.size() && s[n] != ';' && s[n] != ',') {
        ++n;
    }
    value = TString{StripString(s.SubStr(0, n))};
    s = s.SubStr(n);
    return !value.empty();
}

bool IsValidRequestHost(TStringBuf host) {
    return !host.empty() && host.find_first_of("/?#") == TStringBuf::npos;
}

std::pair<TStringBuf, TMaybe<ui16>> SplitHostAndPort(TStringBuf host) {
    TStringBuf scheme;
    TStringBuf hostname;
    ui16 port = 0;
    if (TryGetSchemeHostAndPort(host, scheme, hostname, port)) {
        return {hostname, port != 0 ? TMaybe<ui16>(port) : Nothing()};
    }

    // Port is present but not a ui16. RFC 7230 Host is uri-host [ ":" port ]; keep uri-host.
    TStringBuf hostOnly;
    TStringBuf unusedPort;
    host.TryRSplit(':', hostOnly, unusedPort);
    return {hostOnly, Nothing()};
}

TString ToLowerAscii(TStringBuf s) {
    TString out;
    out.reserve(s.size());
    for (char c : s) {
        out.push_back(AsciiToLower(c));
    }
    return out;
}

} // namespace

TRfc7239Forwarded ParseRfc7239Forwarded(TStringBuf header) {
    TRfc7239Forwarded result;
    TStringBuf s = header;
    while (!s.empty()) {
        SkipOws(s);
        if (s.empty()) {
            break;
        }
        if (s[0] == ',' || s[0] == ';') {
            s = s.SubStr(1);
            continue;
        }

        TStringBuf name;
        if (!ParseHttpToken(s, name)) {
            while (!s.empty() && s[0] != ';' && s[0] != ',') {
                s = s.SubStr(1);
            }
            continue;
        }

        SkipOws(s);
        if (s.empty() || s[0] != '=') {
            continue;
        }
        s = s.SubStr(1);

        TString value;
        if (!ParseForwardedPairValue(s, value)) {
            continue;
        }

        if (result.Host.empty() && AsciiEqualsIgnoreCase(name, "host") && IsValidRequestHost(value)) {
            auto [hostname, port] = SplitHostAndPort(value);
            if (IsValidRequestHost(hostname)) {
                result.Host = TString{hostname};
                result.Port = port;
            }
        } else if (result.Proto.empty() && AsciiEqualsIgnoreCase(name, "proto")) {
            result.Proto = ToLowerAscii(value);
        } else if (result.For.empty() && AsciiEqualsIgnoreCase(name, "for")) {
            result.For = std::move(value);
        } else if (result.By.empty() && AsciiEqualsIgnoreCase(name, "by")) {
            result.By = std::move(value);
        }
    }
    return result;
}

} // namespace NKikimr::NHttp
