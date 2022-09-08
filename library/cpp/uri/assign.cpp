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

    static const TStringBuf ESCAPED_FRAGMENT(TStringBuf("_escaped_fragment_="));

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

    class THashBangModifier {
    public:
        TStringBuf HashBang;
        TStringBuf Query;

        bool FromFragmentToHashBang = false;
        bool FromQueryToFragment = false;
        bool FromFragmentToQuery = false;

        THashBangModifier() = default;

        bool ParseHashBangFromFragment(const TParser& parser) {
            const TSection& fragment = parser.Get(TField::FieldFragment);
            if (fragment.IsSet()) {
                HashBang = fragment.Get();
                if (!HashBang.empty() && '!' == HashBang[0]) {
                    HashBang.Skip(1); // remove !
                    return true;
                }
            }
            return false;
        }

        bool ParseHashBangFromQuery(const TParser& parser) {
            const TSection& query = parser.Get(TField::FieldQuery);
            if (query.IsSet()) {
                query.Get().RSplit('&', Query, HashBang);
                if (HashBang.StartsWith(ESCAPED_FRAGMENT)) {
                    HashBang.Skip(ESCAPED_FRAGMENT.length());
                    return true;
                }
            }
            return false;
        }

        void Parse(const TParser& parser, size_t& buflen) {
            if (0 != (parser.Flags & TFeature::FeatureFragmentToHashBang)) {
                if (ParseHashBangFromFragment(parser)) {
                    FromFragmentToHashBang = true;
                    buflen += 1; // for '\0'
                    buflen += 2 * HashBang.length(); // encode
                }
            } else if (0 != (parser.Flags & TFeature::FeatureHashBangToEscapedFragment)) {
                if (ParseHashBangFromFragment(parser)) {
                    FromFragmentToQuery = true;
                    buflen += ESCAPED_FRAGMENT.length();
                    buflen += 2 * HashBang.length(); // encode
                }
            } else if (0 != (parser.Flags & TFeature::FeatureEscapedToHashBangFragment)) {
                if (ParseHashBangFromQuery(parser)) {
                    FromQueryToFragment = true;
                    buflen += 2; // for '!' and '\0'
                    buflen -= ESCAPED_FRAGMENT.length();
                }
            }
        }

        bool AppendQuery(TMemoryWriteBuffer& out, const TParser& parser) const {
            const TSection& query = parser.Get(TField::FieldQuery);
            if (FromQueryToFragment) {
                return AppendField(out, TField::FieldQuery, Query, query.GetFlagsEncode());
            }
            if (FromFragmentToQuery) {
                if (AppendField(out, TField::FieldQuery, query.Get(), query.GetFlagsEncode())) {
                    out << '&';
                }
                out << ESCAPED_FRAGMENT;
                const TSection& fragment = parser.Get(TField::FieldFragment);
                TUri::ReEncodeToField(
                    out, HashBang,
                    TField::FieldFragment, fragment.GetFlagsEncode(),
                    TField::FieldQuery, parser.GetFieldFlags(TField::FieldQuery)
                );
                return true;
            }
            if (!query.IsSet()) {
                return false;
            }
            AppendField(out, TField::FieldQuery, query.Get(), query.GetFlagsEncode());
            return true; // may be empty
        }

        bool AppendHashBang(TMemoryWriteBuffer& out, const TParser& parser) const {
            if (FromFragmentToHashBang) {
                const TSection& fragment = parser.Get(TField::FieldFragment);
                TUri::ReEncodeToField(
                    out, HashBang,
                    TField::FieldFragment, fragment.GetFlagsEncode(),
                    TField::FieldHashBang, parser.GetFieldFlags(TField::FieldHashBang)
                );
                return true;
            }
            return false;
        }

        bool AppendFragment(TMemoryWriteBuffer& out, const TParser& parser) const {
            if (FromFragmentToQuery || FromFragmentToHashBang) {
                return false;
            }
            if (FromQueryToFragment) {
                const TSection& query = parser.Get(TField::FieldQuery);
                out << '!';
                TUri::ReEncodeToField(
                    out, HashBang,
                    TField::FieldQuery, TFeature::FeatureDecodeANY | query.GetFlagsEncode(),
                    TField::FieldFragment, TFeature::FeatureDecodeANY | parser.GetFieldFlags(TField::FieldFragment)
                );
                return true;
            }
            const TSection& fragment = parser.Get(TField::FieldFragment);
            if (!fragment.IsSet()) {
                return false;
            }
            AppendField(out, TField::FieldQuery, fragment.Get(), fragment.GetFlagsEncode());
            return true;
        }
    };

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

        for (ui32 i = 0; i < FieldUrlMAX; ++i) {
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

        THashBangModifier modifier;
        if (!FldIsSet(FieldFragment) && !FldIsSet(FieldQuery)) {
            modifier.Parse(parser, buflen);
        }

        // now set all fields prior to validating

        Alloc(buflen);

        TMemoryWriteBuffer out(Buffer.data(), Buffer.size());
        for (ui32 i = 0; i < FieldUrlMAX; ++i) {
            const EField field = EField(i);
            if (FldIsSet(field)) {
                continue;
            }
            const TSection& section = parser.Get(field);
            char* beg = out.Buf();

            if (field == FieldQuery) {
                if (!modifier.AppendQuery(out, parser)) {
                    continue;
                }
            } else if (field == FieldHashBang) {
                if (!modifier.AppendHashBang(out, parser)) {
                    continue;
                }
            } else if (field == FieldFragment) {
                if (!modifier.AppendFragment(out, parser)) {
                    continue;
                }
            } else {
                if (!section.IsSet()) {
                    continue;
                }
                AppendField(out, field, section.Get(), section.GetFlagsEncode()); // may be empty
            }

            // path operations case
            char* end = out.Buf();
            if (section.GetFlagsEncode() & FeaturePathOperation) {
                if (!PathOperation(beg, end, PathOperationFlag(parser.Flags))) {
                    return ParsedBadPath;
                }
                Y_ASSERT(beg >= out.Beg());
                out.SetPos(end);
            }
            FldSetNoDirty(field, TStringBuf(beg, end));
            out << '\0';

            // special character case
            const ui64 checkChars = section.GetFlagsAllPlaintext() & FeaturesCheckSpecialChar;
            if (0 != checkChars) { // has unencoded special chars: check permission
                const ui64 allowChars = parser.GetFieldFlags(field) & checkChars;
                if (checkChars != allowChars) {
                    status = ParsedBadFormat;
                }
            }
        }

        if (inHostNonAsciiChars) {
            char* beg = out.Buf();
            out << hostAsciiBuf;
            auto field = convertIDN ? FieldHost : FieldHostAscii;
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
            return ParsedBadPort;
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
