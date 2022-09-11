#pragma once

#include "common.h"

#include <util/stream/output.h>

namespace NUri {
    namespace NEncode {
#define CHAR_TYPE_NAME(f) _ECT##f
#define CHAR_TYPE_FLAG(f) ECF##f = 1u << CHAR_TYPE_NAME(f)

        enum ECharType {
            CHAR_TYPE_NAME(Digit),
            CHAR_TYPE_NAME(Lower),
            CHAR_TYPE_NAME(Upper),
            CHAR_TYPE_NAME(Unres),
            CHAR_TYPE_NAME(Stdrd),
        };

        enum ECharFlag {
            CHAR_TYPE_FLAG(Digit),
            CHAR_TYPE_FLAG(Lower),
            CHAR_TYPE_FLAG(Upper),
            CHAR_TYPE_FLAG(Unres),
            CHAR_TYPE_FLAG(Stdrd),
            // compound group flags
            ECGAlpha = ECFUpper | ECFLower,
            ECGAlnum = ECGAlpha | ECFDigit,
            ECGUnres = ECGAlnum | ECFUnres,
            ECGStdrd = ECGUnres | ECFStdrd,
        };

#undef CHAR_TYPE_NAME
#undef CHAR_TYPE_FLAG

        struct TCharFlags {
            ui32 TypeFlags;
            ui64 FeatFlags;
            ui32 DecodeFld; // decode if FeatureDecodeFieldAllowed
            ui32 EncodeFld; // encode if shouldn't be treated as delimiter
            TCharFlags(ui64 feat = 0)
                : TypeFlags(0)
                , FeatFlags(feat)
                , DecodeFld(0)
                , EncodeFld(0)
            {
            }
            TCharFlags(ui32 type, ui64 feat, ui32 decmask = 0, ui32 encmask = 0)
                : TypeFlags(type)
                , FeatFlags(feat)
                , DecodeFld(decmask)
                , EncodeFld(encmask)
            {
            }
            TCharFlags& Add(const TCharFlags& val) {
                TypeFlags |= val.TypeFlags;
                FeatFlags |= val.FeatFlags;
                DecodeFld |= val.DecodeFld;
                EncodeFld |= val.EncodeFld;
                return *this;
            }
            bool IsAllowed(ui32 fldmask) const {
                return (TypeFlags & ECGUnres) || (DecodeFld & ~EncodeFld & fldmask);
            }
            // should we decode an encoded character
            bool IsDecode(ui32 fldmask, ui64 flags) const;
        };

        class TEncodeMapperBase {
        protected:
            TEncodeMapperBase()
                : Flags(0)
                , FldMask(0)
                , Q_DecodeAny(false)
            {
            }
            TEncodeMapperBase(ui64 flags, TField::EField fld)
                : Flags(flags)
                , FldMask(1u << fld)
                , Q_DecodeAny(flags & TFeature::FeatureDecodeANY)
            {
            }

        public:
            bool Is(TField::EField fld) const {
                return FldMask & (1u << fld);
            }

        protected:
            const ui64 Flags;
            const ui32 FldMask;
            const bool Q_DecodeAny; // this is a special option for username/password
        };

        // maps a sym or hex character and indicates whether it has to be encoded
        class TEncodeMapper
           : public TEncodeMapperBase {
        public:
            TEncodeMapper(ui64 flags, TField::EField fld = TField::FieldAllMAX)
                : TEncodeMapperBase(flags, fld)
                , Q_EncodeSpcAsPlus(flags & TFeature::FeatureEncodeSpaceAsPlus)
            {
            }
            // negative=sym, positive=hex, zero=maybesym
            int EncodeSym(unsigned char&) const;
            int EncodeHex(unsigned char&) const;

        protected:
            const bool Q_EncodeSpcAsPlus;
        };

        // indicates whether a character has to be encoded when copying to a field
        class TEncodeToMapper
           : public TEncodeMapperBase {
        public:
            TEncodeToMapper()
                : TEncodeMapperBase()
            {
            }
            TEncodeToMapper(ui64 flags, TField::EField fld = TField::FieldAllMAX)
                : TEncodeMapperBase(flags, fld)
            {
            }
            bool Enabled() const {
                return 0 != FldMask;
            }
            bool Encode(unsigned char) const;
        };

        class TEncoder {
        public:
            TEncoder(IOutputStream& out, const TEncodeMapper& fldsrc, const TEncodeToMapper& flddst = TEncodeToMapper());

            ui64 ReEncode(const TStringBuf& url);
            ui64 ReEncode(const char* str, size_t len) {
                return ReEncode(TStringBuf(str, len));
            }

        protected:
            static bool IsType(unsigned char c, ui64 flags) {
                return GetFlags(c).TypeFlags & flags;
            }

        public:
            static bool IsDigit(unsigned char c) {
                return IsType(c, ECFDigit);
            }
            static bool IsUpper(unsigned char c) {
                return IsType(c, ECFUpper);
            }
            static bool IsLower(unsigned char c) {
                return IsType(c, ECFLower);
            }
            static bool IsAlpha(unsigned char c) {
                return IsType(c, ECGAlpha);
            }
            static bool IsAlnum(unsigned char c) {
                return IsType(c, ECGAlnum);
            }
            static bool IsUnres(unsigned char c) {
                return IsType(c, ECGUnres);
            }
            static const TCharFlags& GetFlags(unsigned char c) {
                return Grammar().Get(c);
            }

        public:
            // process an encoded string, decoding safe chars and encoding unsafe
            static IOutputStream& ReEncode(IOutputStream& out, const TStringBuf& val, const TEncodeMapper& srcfld) {
                TEncoder(out, srcfld).ReEncode(val);
                return out;
            }
            static IOutputStream& ReEncodeTo(IOutputStream& out, const TStringBuf& val, const TEncodeMapper& srcfld, const TEncodeToMapper& dstfld) {
                TEncoder(out, srcfld, dstfld).ReEncode(val);
                return out;
            }

            // see also UrlUnescape() from string/quote.h
            static IOutputStream& Decode(
                IOutputStream& out, const TStringBuf& val, ui64 flags) {
                return ReEncode(out, val, flags | TFeature::FeatureDecodeANY);
            }

        public:
            // process a raw string or char, encode as needed
            static IOutputStream& Hex(IOutputStream& out, unsigned char val);
            static IOutputStream& Encode(IOutputStream& out, unsigned char val) {
                out << '%';
                return Hex(out, val);
            }
            static IOutputStream& EncodeAll(IOutputStream& out, const TStringBuf& val);
            static IOutputStream& EncodeNotAlnum(IOutputStream& out, const TStringBuf& val);

            static IOutputStream& EncodeField(IOutputStream& out, const TStringBuf& val, TField::EField fld);
            static IOutputStream& EncodeField(IOutputStream& out, const TStringBuf& val, TField::EField fld, ui64 flags);

            static IOutputStream& Encode(IOutputStream& out, const TStringBuf& val) {
                return EncodeField(out, val, TField::FieldAllMAX);
            }

            static IOutputStream& Encode(IOutputStream& out, const TStringBuf& val, ui64 flags) {
                return EncodeField(out, val, TField::FieldAllMAX, flags);
            }

        public:
            class TGrammar {
                TCharFlags Map_[256];

            public:
                TGrammar();
                const TCharFlags& Get(unsigned char ch) const {
                    return Map_[ch];
                }

                TCharFlags& GetMutable(unsigned char ch) {
                    return Map_[ch];
                }
                TCharFlags& Add(unsigned char ch, const TCharFlags& val) {
                    return GetMutable(ch).Add(val);
                }

                void AddRng(unsigned char lo, unsigned char hi, const TCharFlags& val) {
                    for (unsigned i = lo; i <= hi; ++i)
                        Add(i, val);
                }
                void AddRng(unsigned char lo, unsigned char hi, ui32 type, ui64 feat, ui32 decmask = 0, ui32 encmask = 0) {
                    AddRng(lo, hi, TCharFlags(type, feat, decmask, encmask));
                }

                void Add(const TStringBuf& set, const TCharFlags& val) {
                    for (size_t i = 0; i != set.length(); ++i)
                        Add(set[i], val);
                }
                void Add(const TStringBuf& set, ui32 type, ui64 feat, ui32 decmask = 0, ui32 encmask = 0) {
                    Add(set, TCharFlags(type, feat, decmask, encmask));
                }
            };

            static const TGrammar& Grammar();

        protected:
            IOutputStream& Out;
            const TEncodeMapper FldSrc;
            const TEncodeToMapper FldDst;
            ui64 OutFlags;
            int HexValue;

        protected:
            void HexReset() {
                HexValue = 0;
            }

            void HexDigit(char c) {
                HexAdd(c - '0');
            }
            void HexUpper(char c) {
                HexAdd(c - 'A' + 10);
            }
            void HexLower(char c) {
                HexAdd(c - 'a' + 10);
            }

            void HexAdd(int val) {
                HexValue <<= 4;
                HexValue += val;
            }

        protected:
            void DoSym(unsigned char ch) {
                const int res = FldSrc.EncodeSym(ch);
                Do(ch, res);
            }
            void DoHex(unsigned char ch) {
                const int res = FldSrc.EncodeHex(ch);
                Do(ch, res);
            }
            void DoHex() {
                DoHex(HexValue);
                HexValue = 0;
            }
            void Do(unsigned char, int);
        };
    }

    using TEncoder = NEncode::TEncoder;

}
