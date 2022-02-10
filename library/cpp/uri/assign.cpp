#include "uri.h"
#include "parse.h"

#include <contrib/libs/libidn/idna.h>

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
        if (IDNA_SUCCESS != idna_to_ascii_4z((const uint32_t*)idna, &buf, 0))
            buf = nullptr;
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
        TStringBuf outhost; // store the result here before returning it, to get RVO

        size_t buflen = 0;

        if (hasExtended && !allowIDN)
            return outhost; // definitely can't convert

        // charset-recode: RFC 3986, 3.2.2, requires percent-encoded non-ASCII
        // chars in reg-name to be UTF-8 so convert to UTF-8 prior to decoding
        const bool recoding = CODES_UTF8 != enc && hasExtended;
        if (recoding) {
            size_t nrd, nwr;
            buflen = host.length() * 4;
            buf.Reset(static_cast<char*>(y_allocate(buflen)));
            if (RECODE_OK != Recode(enc, CODES_UTF8, host.data(), buf.Get(), host.length(), buflen, nrd, nwr))
                return outhost;
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
        const long outFlags = decoder.ReEncode(host);
        hasExtended = 0 != (outFlags & FeatureEncodeExtendedASCII);

        // check again
        if (hasExtended && !allowIDN)
            return outhost;

        host = out.Str();

        // convert to punycode if needed
        if (!hasExtended) {
            outhost = host;
            return outhost;
        }

        TMallocPtr<char> puny;
        try {
            puny = IDNToAscii(host);
        } catch (const yexception& /* exc */) {
        }

        if (!puny) {
            // XXX: try user charset unless UTF8 or converted to it
            if (CODES_UTF8 == enc || recoding)
                return outhost;
            try {
                puny = IDNToAscii(host, enc);
            } catch (const yexception& /* exc */) {
                return outhost;
            }
            if (!puny)
                return outhost;
        }

        buf = puny;
        outhost = buf.Get();

        return outhost;
    }

    TStringBuf TUri::HostToAscii(const TStringBuf& host, TMallocPtr<char>& buf, bool allowIDN, ECharset enc) {
        // find what we have
        long haveFlags = 0;
        for (size_t i = 0; i != host.length(); ++i)
            haveFlags |= TEncoder::GetFlags(host[i]).FeatFlags;

        // interested in encoded characters or (if IDN is allowed) extended ascii
        TStringBuf outhost;
        const bool haveExtended = haveFlags & FeatureEncodeExtendedASCII;

        if (!haveExtended || allowIDN) {
            if (!haveExtended && 0 == (haveFlags & FeatureDecodeANY))
                outhost = host;
            else
                outhost = HostToAscii(host, buf, haveExtended, allowIDN, enc);
        }

        return outhost;
    }

    static inline bool AppendField(TMemoryWriteBuffer& out, TField::EField fld, const TStringBuf& val, long flags) {
        if (val.empty()) 
            return false;
        if (flags & TFeature::FeaturesAllEncoder)
            TUri::ReEncodeField(out, val, fld, flags);
        else
            out << val;
        return true;
    }

    TState::EParsed TUri::AssignImpl(const TParser& parser, TScheme::EKind defscheme) {
        Clear();

        TState::EParsed ret = parser.State;
        if (ParsedBadFormat <= ret)
            return ret;

        const TSection& scheme = parser.Get(FieldScheme);
        const TSchemeInfo& schemeInfo = SetSchemeImpl(parser.Scheme);

        // set the scheme always if available
        if (schemeInfo.Str.empty() && scheme.IsSet()) 
            FldSet(FieldScheme, scheme.Get());

        if (ParsedOK != ret)
            return ret;

        size_t buflen = 0;

        // special processing for fields

        const bool convertIDN = parser.Flags & FeatureConvertHostIDN;
        long flags = parser.Flags.Allow;
        if (convertIDN)
            flags |= FeatureAllowHostIDN | FeatureCheckHost;

        // process non-ASCII host for punycode

        TMallocPtr<char> hostptr;
        TStringBuf hostascii;       // empty: use host field; non-empty: ascii
        bool hostConverted = false; // hostascii is empty or the original
        const TSection& host = parser.Get(FieldHost);
        if (host.IsSet() && !FldIsSet(FieldHost)) {
            const bool allowIDN = (flags & FeatureAllowHostIDN);
            const TStringBuf hostbuf = host.Get();

            // if we know we have and allow extended-ASCII chars, no need to check further
            if (allowIDN && (host.GetFlagsAllPlaintext() & FeatureEncodeExtendedASCII))
                hostascii = HostToAscii(hostbuf, hostptr, true, true, parser.Enc);
            else
                hostascii = HostToAscii(hostbuf, hostptr, allowIDN, parser.Enc);

            if (hostascii.empty()) 
                ret = ParsedBadHost; // exists but cannot be converted
            else if (hostbuf.data() != hostascii.data()) {
                hostConverted = true;
                buflen += 1 + hostascii.length();
                if (convertIDN)
                    FldMarkSet(FieldHost); // so that we don't process host below
            }
        }

        // add unprocessed fields

        for (int idx = 0; idx < FieldUrlMAX; ++idx) {
            const EField fld = EField(idx);
            const TSection& section = parser.Get(fld);
            if (section.IsSet() && !FldIsSet(fld))
                buflen += 1 + section.EncodedLen(); // includes null
        }
        if (0 == buflen) // no more sections set?
            return ret;

        // process #! fragments
        // https://developers.google.com/webmasters/ajax-crawling/docs/specification

        static const TStringBuf escFragPrefix(TStringBuf("_escaped_fragment_="));

        bool encHashBangFrag = false;
        TStringBuf qryBeforeEscapedFragment;
        TStringBuf qryEscapedFragment;
        do {
            if (FldIsSet(FieldFrag) || FldIsSet(FieldQuery))
                break;

            const TSection& frag = parser.Get(FieldFrag);
            if (frag.IsSet()) {
                if (0 == (parser.Flags & FeatureHashBangToEscapedFragment))
                    break;
                const TStringBuf fragbuf = frag.Get();
                if (fragbuf.empty() || '!' != fragbuf[0]) 
                    break;
                encHashBangFrag = true;
                // '!' will make space for '&' or '\0' if needed
                buflen += escFragPrefix.length();
                buflen += 2 * fragbuf.length(); // we don't know how many will be encoded
            } else {
                const TSection& qry = parser.Get(FieldQuery);
                if (!qry.IsSet())
                    break;
                // FeatureHashBangToEscapedFragment has preference
                if (FeatureEscapedToHashBangFragment != (parser.Flags & FeaturesEscapedFragment))
                    break;
                qry.Get().RSplit('&', qryBeforeEscapedFragment, qryEscapedFragment);
                if (!qryEscapedFragment.StartsWith(escFragPrefix)) {
                    qryEscapedFragment.Clear();
                    break;
                }
                qryEscapedFragment.Skip(escFragPrefix.length());
                buflen += 2; // for '!' and '\0' in fragment
                buflen -= escFragPrefix.length();
            }
        } while (false);

        // now set all fields prior to validating

        Alloc(buflen);

        TMemoryWriteBuffer out(Buffer.data(), Buffer.size());
        for (int idx = 0; idx < FieldUrlMAX; ++idx) {
            const EField fld = EField(idx);

            const TSection& section = parser.Get(fld);
            if (!section.IsSet() || FldIsSet(fld))
                continue;

            if (FieldQuery == fld && encHashBangFrag)
                continue;

            if (FieldFrag == fld && qryEscapedFragment.IsInited())
                continue;

            char* beg = out.Buf();
            TStringBuf val = section.Get();
            long careFlags = section.GetFlagsEncode();

            switch (fld) {
                default:
                    break;

                case FieldQuery:
                    if (qryEscapedFragment.IsInited()) {
                        const EField dstfld = FieldFrag; // that's where we will store
                        out << '!';
                        if (!qryEscapedFragment.empty()) 
                            ReEncodeToField(out, qryEscapedFragment, fld, FeatureDecodeANY | careFlags, dstfld, FeatureDecodeANY | parser.GetFieldFlags(dstfld));
                        FldSetNoDirty(dstfld, TStringBuf(beg, out.Buf()));
                        if (qryBeforeEscapedFragment.empty()) 
                            continue;
                        out << '\0';
                        beg = out.Buf();
                        val = qryBeforeEscapedFragment;
                    }
                    break;

                case FieldFrag:
                    if (encHashBangFrag) {
                        const EField dstfld = FieldQuery; // that's where we will store
                        const TSection& qry = parser.Get(dstfld);
                        if (qry.IsSet())
                            if (AppendField(out, dstfld, qry.Get(), qry.GetFlagsEncode()))
                                out << '&';
                        out << escFragPrefix;
                        val.Skip(1); // skip '!'
                        ReEncodeToField(out, val, fld, careFlags, dstfld, parser.GetFieldFlags(dstfld));
                        FldSetNoDirty(dstfld, TStringBuf(beg, out.Buf()));
                        continue;
                    }
                    break;
            }

            AppendField(out, fld, val, careFlags);
            char* end = out.Buf();

            if (careFlags & FeaturePathOperation) {
                if (!PathOperation(beg, end, PathOperationFlag(parser.Flags)))
                    return ParsedBadPath;

                Y_ASSERT(beg >= out.Beg());
                out.SetPos(end);
            }

            FldSetNoDirty(fld, TStringBuf(beg, end));

            // special character case
            const long checkChars = section.GetFlagsAllPlaintext() & FeaturesCheckSpecialChar;
            if (0 != checkChars) { // has unencoded special chars: check permission
                const long allowChars = parser.GetFieldFlags(fld) & checkChars;
                if (checkChars != allowChars)
                    ret = ParsedBadFormat;
            }

            out << '\0';
        }

        if (hostConverted) {
            char* beg = out.Buf();
            out << hostascii;
            char* end = out.Buf();
            const EField fld = convertIDN ? FieldHost : FieldHostAscii;
            FldSetNoDirty(fld, TStringBuf(beg, end));
            out << '\0';
        }

        Buffer.Resize(out.Len());

        if (GetScheme() == SchemeEmpty && SchemeEmpty != defscheme) {
            if (SchemeUnknown == defscheme)
                ret = ParsedBadScheme;
            else
                SetSchemeImpl(defscheme);
        }

        if (0 == (parser.Flags & FeatureAllowEmptyPath))
            CheckMissingFields();

        const TStringBuf& port = GetField(FieldPort);
        if (!port.empty()) { 
            if (!TryFromString<ui16>(port, Port))
                ret = ParsedBadPort;
        }

        if (ParsedOK != ret)
            return ret;

        // run validity checks now that all fields are set

        // check the host for DNS compliance
        do {
            if (0 == (flags & FeatureCheckHost))
                break;
            if (hostascii.empty()) 
                hostascii = GetField(FieldHost);
            if (hostascii.empty()) 
                break;
            // IP literal
            if ('[' == hostascii[0] && ']' == hostascii.back())
                break;
            ret = CheckHost(hostascii);
            if (ParsedOK != ret)
                return ret;
        } while (false);

        return ret;
    }

    TState::EParsed TUri::ParseImpl(const TStringBuf& url, const TParseFlags& flags, ui32 maxlen, TScheme::EKind defscheme, ECharset enc) {
        Clear();

        if (url.empty()) 
            return ParsedEmpty;

        if (maxlen > 0 && url.length() > maxlen)
            return ParsedTooLong;

        const TParser parser(flags, url, enc);

        return AssignImpl(parser, defscheme);
    }

    TState::EParsed TUri::Parse(const TStringBuf& url, const TParseFlags& flags, const TStringBuf& url_base, ui32 maxlen, ECharset enc) {
        const TParseFlags flags1 = flags.Exclude(FeatureNoRelPath);
        TState::EParsed ret = ParseImpl(url, url_base.empty() ? flags : flags1, maxlen, SchemeEmpty, enc); 
        if (ParsedOK != ret)
            return ret;

        if (!url_base.empty() && !IsValidAbs()) { 
            TUri base;
            ret = base.ParseImpl(url_base, flags, maxlen, SchemeEmpty, enc);
            if (ParsedOK != ret)
                return ret;
            Merge(base, PathOperationFlag(flags));
        }

        Rewrite();
        return ret;
    }

    TState::EParsed TUri::Parse(const TStringBuf& url, const TUri& base, const TParseFlags& flags, ui32 maxlen, ECharset enc) {
        const TState::EParsed ret = ParseImpl(url, flags, maxlen, SchemeEmpty, enc);
        if (ParsedOK != ret)
            return ret;

        if (!IsValidAbs())
            Merge(base, PathOperationFlag(flags));

        Rewrite();
        return ret;
    }

    TState::EParsed TUri::ParseAbsUri(const TStringBuf& url, const TParseFlags& flags, ui32 maxlen, TScheme::EKind defscheme, ECharset enc) {
        const TState::EParsed ret = ParseImpl(
            url, flags | FeatureNoRelPath, maxlen, defscheme, enc);
        if (ParsedOK != ret)
            return ret;

        if (IsNull(FlagHost))
            return ParsedBadHost;

        Rewrite();
        return ParsedOK;
    }

}
