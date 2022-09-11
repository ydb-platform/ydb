#include "encode.h"

#include <util/generic/singleton.h>

namespace NUri {
    namespace NEncode {
// http://tools.ietf.org/html/rfc3986#section-2.2
#define GENDELIMS0 ":/?#[]@"
#define SUBDELIMS0 "!$&'()*+,;="
// http://tools.ietf.org/html/rfc3986#section-2.3
#define UNRESERVED "-._~"

// now find subsets which can sometimes be decoded

// remove '#' which can't ever be decoded
// don't mark anything allowed for pass (pass is completely encoded)
// safe in path, qry, frag, hashbang
#define GENDELIMS1 ":@"
// allowed in qry, frag, hashbang
#define GENDELIMS2 "/?"

// qry-unsafe chars
#define SUBDELIMS1 "&+=;"
// rest allowed in qry, frag, hashbang
#define SUBDELIMS2 "!$'()*,"

        const TEncoder::TGrammar& TEncoder::Grammar() {
            return *Singleton<TEncoder::TGrammar>();
        }

        // initialize the grammar map
        TEncoder::TGrammar::TGrammar() {
            // first set up unreserved characters safe in any field
            const ui64 featUnres = TFeature::FeatureDecodeUnreserved;
            AddRng('0', '9', ECFDigit, featUnres);
            AddRng('A', 'Z', ECFUpper, featUnres | TFeature::FeatureToLower);
            AddRng('a', 'z', ECFLower, featUnres);
            Add(UNRESERVED, ECFUnres, featUnres);

            // XXX: standard "safe" set used previously "-_.!~*();/:@$,", with comment:
            //  alnum + reserved + mark + ( '[', ']') - ('=' '+' '&' '\'' '"' '\\' '?')
            Add("!*();/:@$,", ECFStdrd, TFeature::FeatureDecodeStandardExtra);

            // now field-specific subsets of reserved characters (gen-delims + sub-delims)
            const ui64 featSafe = TFeature::FeatureDecodeFieldAllowed;

            Add(GENDELIMS1, 0, featSafe, TField::FlagPath | TField::FlagQuery | TField::FlagFrag | TField::FlagHashBang);
            Add(GENDELIMS2, 0, featSafe, TField::FlagQuery | TField::FlagFrag | TField::FlagHashBang);

            Add(SUBDELIMS1, 0, featSafe, TField::FlagUser);
            Add(SUBDELIMS2, 0, featSafe, TField::FlagUser | TField::FlagQuery | TField::FlagFrag | TField::FlagHashBang);

            // control chars
            AddRng(0x00, 0x20, TFeature::FeatureEncodeCntrl);
            Add(0x7f, TFeature::FeatureEncodeCntrl);

            // '%' starts a percent-encoded sequence
            Add('%', TFeature::FeatureDecodeANY | TFeature::FeatureEncodePercent);

            // extended ASCII
            AddRng(128, 255, TFeature::FeatureEncodeExtendedASCII | TFeature::FeatureDecodeExtendedASCII);

            // extended delims
            Add("\"<>[\\]^`{|}", TFeature::FeatureEncodeExtendedDelim | TFeature::FeatureDecodeExtendedDelim);

            // add characters with other features
            Add(' ', TFeature::FeatureEncodeSpace | TFeature::FeatureEncodeSpaceAsPlus);
            Add("'\"\\", TFeature::FeatureEncodeForSQL);

            GetMutable(':').EncodeFld |= TField::FlagUser | TField::FlagHashBang;
            GetMutable('?').EncodeFld |= TField::FlagPath | TField::FlagHashBang;
            GetMutable('#').EncodeFld |= TField::FlagPath | TField::FlagQuery | TField::FlagHashBang;
            GetMutable('&').EncodeFld |= TField::FlagQuery | TField::FlagHashBang;
            GetMutable('+').EncodeFld |= TField::FlagQuery | TField::FlagHashBang;
        }

        // should we decode an encoded character
        bool TCharFlags::IsDecode(ui32 fldmask, ui64 flags) const {
            const ui64 myflags = flags & FeatFlags;
            if (myflags & TFeature::FeaturesEncode)
                return false;
            if (myflags & TFeature::FeaturesDecode)
                return true;
            return (fldmask & DecodeFld) && (flags & TFeature::FeatureDecodeFieldAllowed);
        }

        const int dD = 'a' - 'A';

        int TEncodeMapper::EncodeSym(unsigned char& ch) const {
            const TCharFlags& chflags = TEncoder::GetFlags(ch);
            const ui64 flags = Flags & chflags.FeatFlags;

            if (flags & TFeature::FeatureToLower)
                ch += dD;

            if (Q_DecodeAny)
                return -1;

            if (flags & TFeature::FeaturesEncode)
                return 1;

            if (' ' == ch) {
                if (Q_EncodeSpcAsPlus)
                    ch = '+';
                return 0;
            }

            return 0;
        }

        int TEncodeMapper::EncodeHex(unsigned char& ch) const {
            const TCharFlags& chflags = TEncoder::GetFlags(ch);
            const ui64 flags = Flags & chflags.FeatFlags;

            if (flags & TFeature::FeatureToLower)
                ch += dD;

            if (Q_DecodeAny)
                return -1;

            if (chflags.IsDecode(FldMask, Flags))
                return 0;

            if (' ' == ch) {
                if (!Q_EncodeSpcAsPlus)
                    return 1;
                ch = '+';
                return 0;
            }

            return 1;
        }

        bool TEncodeToMapper::Encode(unsigned char ch) const {
            if (Q_DecodeAny)
                return false;

            const TCharFlags& chflags = TEncoder::GetFlags(ch);
            if (FldMask & chflags.EncodeFld)
                return true;

            const ui64 flags = Flags & chflags.FeatFlags;
            return (flags & TFeature::FeaturesEncode);
        }

        TEncoder::TEncoder(IOutputStream& out, const TEncodeMapper& fldsrc, const TEncodeToMapper& flddst)
            : Out(out)
            , FldSrc(fldsrc)
            , FldDst(flddst)
            , OutFlags(0)
            , HexValue(0)
        {
        }

        IOutputStream& TEncoder::Hex(IOutputStream& out, unsigned char val) {
            static const char sHexCodes[] = "0123456789ABCDEF";
            return out << sHexCodes[(val >> 4) & 0xF] << sHexCodes[val & 0xF];
        }

        IOutputStream& TEncoder::EncodeAll(IOutputStream& out, const TStringBuf& val) {
            for (size_t i = 0; i != val.length(); ++i)
                Encode(out, val[i]);
            return out;
        }

        IOutputStream& TEncoder::EncodeNotAlnum(IOutputStream& out, const TStringBuf& val) {
            for (size_t i = 0; i != val.length(); ++i) {
                const char c = val[i];
                if (IsAlnum(c))
                    out << c;
                else
                    Encode(out, c);
            }
            return out;
        }

        IOutputStream& TEncoder::EncodeField(
            IOutputStream& out, const TStringBuf& val, TField::EField fld) {
            const ui32 fldmask = ui32(1) << fld;
            for (size_t i = 0; i != val.length(); ++i) {
                const char ch = val[i];
                if (GetFlags(ch).IsAllowed(fldmask))
                    out << ch;
                else
                    Encode(out, ch);
            }
            return out;
        }

        IOutputStream& TEncoder::EncodeField(
            IOutputStream& out, const TStringBuf& val, TField::EField fld, ui64 flags) {
            const ui32 fldmask = ui32(1) << fld;
            for (size_t i = 0; i != val.length(); ++i) {
                const char ch = val[i];
                if (GetFlags(ch).IsDecode(fldmask, flags))
                    out << ch;
                else
                    Encode(out, ch);
            }
            return out;
        }

        void TEncoder::Do(unsigned char ch, int res) {
            OutFlags |= GetFlags(ch).FeatFlags;

            bool escapepct = false;
            if (0 < res) // definitely encode
                escapepct = FldDst.Enabled() && !FldDst.Is(TField::FieldHashBang);
            else if (0 != res || !FldDst.Enabled() || !FldDst.Encode(ch)) {
                Out << ch;
                return;
            }

            Out << '%';
            if (escapepct) {
                Out.Write("25", 2); // '%'
            }
            Hex(Out, ch);
        }
    }
}
