#include "uri.h"
#include "parse.h"

#include <idna.h>

#include <library/cpp/charset/recyr.hh>
#include <util/charset/wide.h>
#include <util/memory/tempbuf.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>
#include <util/system/sys_alloc.h>

namespace NUri {
    TMallocPtr<char> TUri::IDNToAscii(const wchar32* idna) {
        // XXX: don't use punycode_encode directly as it doesn't include
        // proper stringprep and splitting on dot-equivalent characters
        char* buf;
        static_assert(sizeof(*idna) == sizeof(ui32), "fixme");
        if (IDNA_SUCCESS != idna_to_ascii_4z((const uint32_t*) idna, &buf, 0)) {
            buf = nullptr;
        }
        return buf;
    }

    TMallocPtr<char> TUri::IDNToAscii(const TStringBuf& host, ECharset enc) {
        TTempBuf buf(sizeof(wchar32) * (1 + host.length()));
        wchar32* wbuf = reinterpret_cast<wchar32*>(buf.Data());

        const size_t written = NDetail::NBaseOps::Recode(host, wbuf, enc).length();
        wbuf[written] = 0;

        return IDNToAscii(wbuf);
    }

    TStringBuf TUri::HostToAscii(TStringBuf host, TMallocPtr<char>& buf, bool hasExtended, bool allowIDN, ECharset enc) {
        TStringBuf outHost; // store the result here before returning it, to get RVO

        size_t buflen = 0;

        if (hasExtended && !allowIDN) {
            return outHost; // definitely can't convert
        }
        // charset-recode: RFC 3986, 3.2.2, requires percent-encoded non-ASCII
        // chars in reg-name to be UTF-8 so convert to UTF-8 prior to decoding
        const bool recoding = CODES_UTF8 != enc && hasExtended;
        if (recoding) {
            size_t nrd, nwr;
            buflen = host.length() * 4;
            buf.Reset(static_cast<char*>(y_allocate(buflen)));
            if (RECODE_OK != Recode(enc, CODES_UTF8, host.data(), buf.Get(), host.length(), buflen, nrd, nwr)) {
                return outHost;
            }
            host = TStringBuf(buf.Get(), nwr);
        }

        // percent-decode
        if (0 == buflen) {
            buflen = host.length();
            buf.Reset(static_cast<char*>(y_allocate(buflen)));
        }
        // decoding shortens so writing over host in buf is OK
        TMemoryWriteBuffer out(buf.Get(), buflen);
        TEncoder decoder(out, FeatureDecodeANY | FeatureToLower);
        const ui64 outFlags = decoder.ReEncode(host);
        hasExtended = 0 != (outFlags & FeatureEncodeExtendedASCII);

        // check again
        if (hasExtended && !allowIDN) {
            return outHost;
        }

        host = out.Str();

        // convert to punycode if needed
        if (!hasExtended) {
            outHost = host;
            return outHost;
        }

        TMallocPtr<char> puny;
        try {
            puny = IDNToAscii(host);
        } catch (const yexception& /* exc */) {
        }

        if (!puny) {
            // XXX: try user charset unless UTF8 or converted to it
            if (CODES_UTF8 == enc || recoding) {
                return outHost;
            }
            try {
                puny = IDNToAscii(host, enc);
            } catch (const yexception& /* exc */) {
                return outHost;
            }
            if (!puny) {
                return outHost;
            }
        }

        buf = puny;
        outHost = buf.Get();

        return outHost;
    }

    TStringBuf TUri::HostToAscii(const TStringBuf& host, TMallocPtr<char>& buf, bool allowIDN, ECharset enc) {
        // find what we have
        ui64 haveFlags = 0;
        for (size_t i = 0; i != host.length(); ++i) {
            haveFlags |= TEncoder::GetFlags(host[i]).FeatFlags;
        }
        // interested in encoded characters or (if IDN is allowed) extended ascii
        TStringBuf outHost;
        const bool haveExtended = haveFlags & FeatureEncodeExtendedASCII;

        if (!haveExtended || allowIDN) {
            if (!haveExtended && 0 == (haveFlags & FeatureDecodeANY)) {
                outHost = host;
            } else {
                outHost = HostToAscii(host, buf, haveExtended, allowIDN, enc);
            }
        }
        return outHost;
    }

    static inline bool AppendField(TMemoryWriteBuffer& out, TField::EField field, const TStringBuf& value, ui64 flags) {
        if (value.empty()) {
            return false;
        }
        if (flags & TFeature::FeaturesAllEncoder) {
            TUri::ReEncodeField(out, value, field, flags);
        } else {
            out << value;
        }
        return true;
    }

    TState::EParsed TUri::AssignImpl(const TParser& parser, TScheme::EKind defaultScheme) {
        Clear();

        TState::EParsed status = parser.State;
        if (ParsedBadFormat <= status) {
            return status;
        }
        const TSection& scheme = parser.Get(FieldScheme);
        const TSchemeInfo& schemeInfo = SetSchemeImpl(parser.Scheme);

        // set the scheme always if available
        if (schemeInfo.Str.empty() && scheme.IsSet()) {
            FldSet(FieldScheme, scheme.Get());
        }
        if (ParsedOK != status) {
            return status;
        }
        size_t buflen = 0;

        // special processing for fields

        const bool convertIDN = parser.Flags & FeatureConvertHostIDN;
        ui64 flags = parser.Flags.Allow;
        if (convertIDN) {
            flags |= FeatureAllowHostIDN | FeatureCheckHost;
        }

        // process non-ASCII host for punycode

        TMallocPtr<char> hostPtr;
        TStringBuf hostAsciiBuf;
        bool inHostNonAsciiChars = false;

        const TSection& host = parser.Get(FieldHost);
        if (host.IsSet() && !FldIsSet(FieldHost)) {
            const bool allowIDN = (flags & FeatureAllowHostIDN);
            const TStringBuf hostBuf = host.Get();

            // if we know we have and allow extended-ASCII chars, no need to check further
            if (allowIDN && (host.GetFlagsAllPlaintext() & FeatureEncodeExtendedASCII)) {
                hostAsciiBuf = HostToAscii(hostBuf, hostPtr, true, true, parser.Enc);
            } else {
                hostAsciiBuf = HostToAscii(hostBuf, hostPtr, allowIDN, parser.Enc);
            }

            if (hostAsciiBuf.empty()) {
                status = ParsedBadHost; // exists but cannot be converted

            } else if (hostBuf.data() != hostAsciiBuf.data()) {
                inHostNonAsciiChars = true;

                buflen += 1 + hostAsciiBuf.length();
                if (convertIDN) {
                    FldMarkSet(FieldHost); // so that we don't process host below
                }
            }
        }

        // add unprocessed fields

        for (int i = 0; i < FieldUrlMAX; ++i) {
            const EField field = EField(i);
            const TSection& section = parser.Get(field);

            if (section.IsSet() && !FldIsSet(field)) {
                buflen += 1 + section.EncodedLen(); // includes null
            }
        }
        if (0 == buflen) { // no more sections set?
            return status;
        }

        // process #! fragments
        // https://developers.google.com/webmasters/ajax-crawling/docs/specification

        static const TStringBuf escapedFragment(TStringBuf("_escaped_fragment_="));

        bool encodeHashBang = false;
        TStringBuf queryBeforeEscapedFragment;
        TStringBuf queryEscapedFragment;

        if (!FldIsSet(FieldFrag) && !FldIsSet(FieldQuery)) {
            const TSection& frag = parser.Get(FieldFrag);

            if (frag.IsSet()) {
                if (0 != (parser.Flags & FeatureHashBangToEscapedFragment)) {
                    const TStringBuf fragBuf = frag.Get();
                    if (!fragBuf.empty() && '!' == fragBuf[0]) {
                        encodeHashBang = true;
                        // '!' will make space for '&' or '\0' if needed
                        buflen += escapedFragment.length();
                        buflen += 2 * fragBuf.length(); // we don't know how many will be encoded
                    }
                }
            } else {
                const TSection& query = parser.Get(FieldQuery);
                if (query.IsSet()) {
                    // FeatureHashBangToEscapedFragment has preference
                    if (FeatureEscapedToHashBangFragment == (parser.Flags & FeaturesEscapedFragment)) {
                        const TStringBuf queryBuf = query.Get();

                        queryBuf.RSplit('&', queryBeforeEscapedFragment, queryEscapedFragment);
                        if (queryEscapedFragment.StartsWith(escapedFragment)) {
                            queryEscapedFragment.Skip(escapedFragment.length());
                            buflen += 2; // for '!' and '\0' in fragment
                            buflen -= escapedFragment.length();
                        } else {
                            queryEscapedFragment.Clear();
                        }
                    }
                }
            }
        }

        // now set all fields prior to validating

        Alloc(buflen);

        TMemoryWriteBuffer out(Buffer.data(), Buffer.size());
        for (int i = 0; i < FieldUrlMAX; ++i) {
            const EField field = EField(i);

            const TSection& section = parser.Get(field);
            if (!section.IsSet() || FldIsSet(field)) {
                continue;
            }
            if (FieldQuery == field && encodeHashBang) {
                continue;
            }
            if (FieldFrag == field && queryEscapedFragment.IsInited()) {
                continue;
            }

            char* beg = out.Buf();
            TStringBuf value = section.Get();
            ui64 careFlags = section.GetFlagsEncode();

            if (field == FieldQuery) {
                if (queryEscapedFragment.IsInited()) {
                    out << '!';
                    if (!queryEscapedFragment.empty()) {
                        ReEncodeToField(
                            out, queryEscapedFragment,
                            FieldQuery, FeatureDecodeANY | careFlags,
                            FieldFrag, FeatureDecodeANY | parser.GetFieldFlags(FieldFrag)
                        );
                    }
                    FldSetNoDirty(FieldFrag, TStringBuf(beg, out.Buf()));
                    if (queryBeforeEscapedFragment.empty()) {
                        continue;
                    }
                    out << '\0';
                    beg = out.Buf();
                    value = queryBeforeEscapedFragment;
                }
            } else if (field == FieldFrag) {
                if (encodeHashBang) {
                    const TSection& query = parser.Get(FieldQuery);
                    if (query.IsSet() && AppendField(out, FieldQuery, query.Get(), query.GetFlagsEncode())) {
                        out << '&';
                    }
                    out << escapedFragment;
                    value.Skip(1); // skip '!'
                    ReEncodeToField(
                        out, value,
                        FieldFrag, careFlags,
                        FieldQuery, parser.GetFieldFlags(FieldQuery)
                    );
                    FldSetNoDirty(FieldQuery, TStringBuf(beg, out.Buf()));
                    continue;
                }
            }

            AppendField(out, field, value, careFlags);
            char* end = out.Buf();

            if (careFlags & FeaturePathOperation) {
                if (!PathOperation(beg, end, PathOperationFlag(parser.Flags))) {
                    return ParsedBadPath;
                }
                Y_ASSERT(beg >= out.Beg());
                out.SetPos(end);
            }
            FldSetNoDirty(field, TStringBuf(beg, end));

            // special character case
            const ui64 checkChars = section.GetFlagsAllPlaintext() & FeaturesCheckSpecialChar;
            if (0 != checkChars) { // has unencoded special chars: check permission
                const ui64 allowChars = parser.GetFieldFlags(field) & checkChars;
                if (checkChars != allowChars) {
                    status = ParsedBadFormat;
                }
            }
            out << '\0';
        }

        if (inHostNonAsciiChars) {
            char* beg = out.Buf();
            out << hostAsciiBuf;
            const EField field = convertIDN ? FieldHost : FieldHostAscii;
            FldSetNoDirty(field, TStringBuf(beg, out.Buf()));
            out << '\0';
        }

        Buffer.Resize(out.Len());

        if (GetScheme() == SchemeEmpty && SchemeEmpty != defaultScheme) {
            if (SchemeUnknown == defaultScheme) {
                status = ParsedBadScheme;
            } else {
                SetSchemeImpl(defaultScheme);
            }
        }
        if (0 == (parser.Flags & FeatureAllowEmptyPath)) {
            CheckMissingFields();
        }

        const TStringBuf& port = GetField(FieldPort);
        if (!port.empty() && !TryFromString<ui16>(port, Port)) {
            status = ParsedBadPort;
        }
        if (ParsedOK != status) {
            return status;
        }
        // run validity checks now that all fields are set

        // check the host for DNS compliance
        if (0 != (flags & FeatureCheckHost)) {
            if (hostAsciiBuf.empty()) {
                hostAsciiBuf = GetField(FieldHost);
            }
            if (!hostAsciiBuf.empty()) {
                // IP literal
                if ('[' != hostAsciiBuf[0] || ']' != hostAsciiBuf.back()) {
                    status = CheckHost(hostAsciiBuf);
                }
            }
        }
        return status;
    }

    TState::EParsed TUri::ParseImpl(const TStringBuf& url, const TParseFlags& flags, ui32 maxlen, TScheme::EKind defaultScheme, ECharset enc) {
        Clear();

        if (url.empty()) {
            return ParsedEmpty;
        }
        if (maxlen > 0 && url.length() > maxlen) {
            return ParsedTooLong;
        }
        const TParser parser(flags, url, enc);

        return AssignImpl(parser, defaultScheme);
    }

    TState::EParsed TUri::Parse(const TStringBuf& url, const TParseFlags& flags, const TStringBuf& url_base, ui32 maxlen, ECharset enc) {
        const TParseFlags parseFlags = url_base.empty() ? flags : flags.Exclude(FeatureNoRelPath);
        TState::EParsed status = ParseImpl(url, parseFlags, maxlen, SchemeEmpty, enc);

        if (ParsedOK != status) {
            return status;
        }
        if (!url_base.empty() && !IsValidAbs()) {
            TUri base;
            status = base.ParseImpl(url_base, flags, maxlen, SchemeEmpty, enc);
            if (ParsedOK != status) {
                return status;
            }
            Merge(base, PathOperationFlag(flags));
        }
        Rewrite();
        return status;
    }

    TState::EParsed TUri::Parse(const TStringBuf& url, const TUri& base, const TParseFlags& flags, ui32 maxlen, ECharset enc) {
        const TState::EParsed status = ParseImpl(url, flags, maxlen, SchemeEmpty, enc);
        if (ParsedOK != status) {
            return status;
        }
        if (!IsValidAbs()) {
            Merge(base, PathOperationFlag(flags));
        }
        Rewrite();
        return status;
    }

    TState::EParsed TUri::ParseAbsUri(const TStringBuf& url, const TParseFlags& flags, ui32 maxlen, TScheme::EKind defaultScheme, ECharset enc) {
        const TState::EParsed status = ParseImpl(url, flags | FeatureNoRelPath, maxlen, defaultScheme, enc);

        if (ParsedOK != status) {
            return status;
        }
        if (IsNull(FlagHost)) {
            return ParsedBadHost;
        }
        Rewrite();
        return ParsedOK;
    }

}
