#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>

// Simplified arcadia wrappers for contrib/libs/libidn/

// Raw strings encoder/decoder: does not prepend with ACE prefix ("xn--"),
// does not limit input length. Throws TPunycodeError on any internal error.
// Returned strbuf points to @out data.
TStringBuf WideToPunycode(const TWtringBuf& in, TString& out);
TWtringBuf PunycodeToWide(const TStringBuf& in, TUtf16String& out);

inline TString WideToPunycode(const TWtringBuf& in) {
    TString out;
    WideToPunycode(in, out);
    return out;
}

inline TUtf16String PunycodeToWide(const TStringBuf& in) {
    TUtf16String out;
    PunycodeToWide(in, out);
    return out;
}

// Encode a sequence of point-separated domain labels
// into a sequence of corresponding punycode labels.
// Labels containing non-ASCII characters are prefixed with ACE prefix ("xn--").
// Limits maximal encoded domain label length to IDNA_LABEL_MAX_LENGTH (255 by default).
// Throws TPunycodeError on failure.
TString HostNameToPunycode(const TWtringBuf& unicodeHost);
TUtf16String PunycodeToHostName(const TStringBuf& punycodeHost);

// Robust versions: on failure return original input, converted to/from UTF8
TString ForceHostNameToPunycode(const TWtringBuf& unicodeHost);
TUtf16String ForcePunycodeToHostName(const TStringBuf& punycodeHost);

// True if @host looks like punycode domain label sequence,
// containing at least one ACE-prefixed label.
// Note that this function does not check all requied IDNA constraints
// (max label length, empty non-root domains, etc.)
bool CanBePunycodeHostName(const TStringBuf& host);

class TPunycodeError: public yexception {
};
