#include "punycode.h"
#include <idna.h>
#include <punycode.h>
#include <util/charset/wide.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

#include <cstdlib>

static inline void CheckPunycodeResult(int rc) {
    if (rc != PUNYCODE_SUCCESS)
        ythrow TPunycodeError() << punycode_strerror(static_cast<Punycode_status>(rc));
}

static inline void CheckIdnaResult(int rc) {
    if (rc != IDNA_SUCCESS)
        ythrow TPunycodeError() << idna_strerror(static_cast<Idna_rc>(rc));
}

// UTF-32 helpers

static inline void AppendWideToUtf32(const TWtringBuf& in, TVector<ui32>& out) {
    out.reserve(out.size() + in.size() + 1);

    const wchar16* b = in.begin();
    const wchar16* e = in.end();
    while (b < e) {
        out.push_back(ReadSymbolAndAdvance(b, e));
    }
}

static inline void AppendUtf32ToWide(const ui32* in, size_t len, TUtf16String& out) {
    out.reserve(out.size() + len);

    const ui32* b = in;
    const ui32* e = in + len;
    for (; b != e; ++b) {
        WriteSymbol(wchar32(*b), out);
    }
}

TStringBuf WideToPunycode(const TWtringBuf& in16, TString& out) {
    TVector<ui32> in32;
    AppendWideToUtf32(in16, in32);
    size_t outlen = in32.size();

    int rc;
    do {
        outlen *= 2;
        out.ReserveAndResize(outlen);
        rc = punycode_encode(in32.size(), in32.data(), nullptr, &outlen, out.begin());
    } while (rc == PUNYCODE_BIG_OUTPUT);

    CheckPunycodeResult(rc);

    out.resize(outlen);
    return out;
}

TWtringBuf PunycodeToWide(const TStringBuf& in, TUtf16String& out16) {
    size_t outlen = in.size();
    TVector<ui32> out32(outlen);

    int rc = punycode_decode(in.size(), in.data(), &outlen, out32.begin(), nullptr);
    CheckPunycodeResult(rc);

    AppendUtf32ToWide(out32.begin(), outlen, out16);
    return out16;
}

namespace {
    template <typename TChar>
    struct TIdnaResult {
        TChar* Data = nullptr;

        ~TIdnaResult() {
            free(Data);
        }
    };
}

TString HostNameToPunycode(const TWtringBuf& unicodeHost) {
    TVector<ui32> in32;
    AppendWideToUtf32(unicodeHost, in32);
    in32.push_back(0);

    TIdnaResult<char> out;
    int rc = idna_to_ascii_4z(in32.begin(), &out.Data, 0);
    CheckIdnaResult(rc);

    return out.Data;
}

TUtf16String PunycodeToHostName(const TStringBuf& punycodeHost) {
    if (!IsStringASCII(punycodeHost.begin(), punycodeHost.end()))
        ythrow TPunycodeError() << "Non-ASCII punycode input";

    size_t len = punycodeHost.size();
    TVector<ui32> in32(len + 1, 0);
    for (size_t i = 0; i < len; ++i)
        in32[i] = static_cast<ui8>(punycodeHost[i]);
    in32[len] = 0;

    TIdnaResult<ui32> out;
    int rc = idna_to_unicode_4z4z(in32.begin(), &out.Data, 0);
    CheckIdnaResult(rc);

    TUtf16String decoded;
    AppendUtf32ToWide(out.Data, std::char_traits<ui32>::length(out.Data), decoded);
    return decoded;
}

TString ForceHostNameToPunycode(const TWtringBuf& unicodeHost) {
    try {
        return HostNameToPunycode(unicodeHost);
    } catch (const TPunycodeError&) {
        return WideToUTF8(unicodeHost);
    }
}

TUtf16String ForcePunycodeToHostName(const TStringBuf& punycodeHost) {
    try {
        return PunycodeToHostName(punycodeHost);
    } catch (const TPunycodeError&) {
        return UTF8ToWide(punycodeHost);
    }
}

bool CanBePunycodeHostName(const TStringBuf& host) {
    if (!IsStringASCII(host.begin(), host.end()))
        return false;

    static constexpr TStringBuf ACE = "xn--";

    TStringBuf tail(host);
    while (tail) {
        const TStringBuf label = tail.NextTok('.');
        if (label.StartsWith(ACE))
            return true;
    }

    return false;
}
