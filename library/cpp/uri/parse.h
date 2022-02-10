#pragma once

// #define DO_PRN

#include <cstddef>

#include "common.h"

#include <library/cpp/charset/doccodes.h>
#include <util/generic/strbuf.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

namespace NUri {
    class TParser;

    namespace NParse {
        class TRange {
        public:
            const char* Beg;
            ui64 FlagsEncodeMasked;
            ui64 FlagsAllPlaintext;
            ui32 Encode;
            ui32 Decode;

        public:
            TRange(const char* beg = nullptr)
                : Beg(beg)
                , FlagsEncodeMasked(0)
                , FlagsAllPlaintext(0)
                , Encode(0)
                , Decode(0)
            {
            }

            void Reset(const char* beg = nullptr) {
                *this = TRange(beg);
            }

            void AddRange(const TRange& range, ui64 mask);

            void AddFlag(const char* ptr, ui64 mask, ui64 flag) {
                if (0 != flag)
                    AddFlagImpl(ptr, mask, flag, flag);
            }

            void AddFlagExcept(const char* ptr, ui64 mask, ui64 flag, ui64 exclflag) {
                if (0 != flag)
                    AddFlagImpl(ptr, mask, flag & ~exclflag, flag);
            }

            void AddFlagUnless(const char* ptr, ui64 mask, ui64 flag, ui64 exclmask) {
                if (0 != flag)
                    AddFlagImpl(ptr, mask, flag, flag, exclmask);
            }

            void AddFlag(const char* ptr, ui64 mask, ui64 flag, ui64 exclflag, ui64 exclmask) {
                if (0 != flag)
                    AddFlagImpl(ptr, mask, flag & ~exclflag, flag, exclmask);
            }

        private:
            void AddFlagImpl(const char* ptr, ui64 mask, ui64 plainflag, ui64 encflag) {
                AddFlagAllPlaintextImpl(ptr, plainflag);
                AddFlagEncodeMaskedImpl(encflag & mask);
            }

            void AddFlagImpl(const char* ptr, ui64 mask, ui64 plainflag, ui64 encflag, ui64 exclmask) {
                AddFlagAllPlaintextImpl(ptr, plainflag);
                if (0 == (mask & exclmask))
                    AddFlagEncodeMaskedImpl(encflag & mask);
            }

            void AddFlagAllPlaintextImpl(const char* ptr, ui64 flag) {
                if (nullptr == Beg)
                    Beg = ptr;
                FlagsAllPlaintext |= flag;
            }

            void AddFlagEncodeMaskedImpl(ui64 flag) {
                if (0 == flag)
                    return;
                FlagsEncodeMasked |= flag;
                if (flag & TFeature::FeaturesMaybeEncode)
                    ++Encode;
                else if (flag & TFeature::FeaturesDecode)
                    ++Decode;
            }
        };

    }

    class TSection
       : protected  NParse::TRange {
    private:
        friend class TParser;

    private:
        const char* End;

        TSection(const char* beg = nullptr)
            : NParse::TRange(beg)
            , End(nullptr)
        {
        }

        void Reset() {
            Enter(nullptr);
        }

        void Reset(const char* pc) {
            Y_ASSERT(!Beg || !pc || Beg < pc);
            Reset();
        }

        void Enter(const char* pc) {
            *this = TSection(pc);
        }

        bool Leave(const char* pc) {
            Y_ASSERT(Beg);
            End = pc;
            return true;
        }

        void Set(const TStringBuf& buf) {
            Enter(buf.data());
            Leave(buf.data() + buf.length());
        }

    public:
        bool IsSet() const {
            return End;
        }

        TStringBuf Get() const {
            return TStringBuf(Beg, End);
        }

        size_t Len() const {
            return End - Beg;
        }

        size_t DecodedLen() const {
            return Len() - 2 * Decode;
        }

        size_t EncodedLen() const {
            return 2 * Encode + DecodedLen();
        }

        ui32 GetEncode() const {
            return Encode;
        }

        ui32 GetDecode() const {
            return Decode;
        }

        ui64 GetFlagsEncode() const {
            return FlagsEncodeMasked;
        }

        ui64 GetFlagsAllPlaintext() const {
            return FlagsAllPlaintext;
        }
    };

    class TParser {
    public:
        TSection Sections[TField::FieldUrlMAX];
        TScheme::EKind Scheme;
        const TParseFlags Flags;
        const TStringBuf UriStr;
        TState::EParsed State;
        ECharset Enc;

    public:
        TParser(const TParseFlags& flags, const TStringBuf& uri, ECharset enc = CODES_UTF8)
            : Scheme(TScheme::SchemeEmpty)
            , Flags(flags | TFeature::FeatureDecodeANY)
            , UriStr(uri)
            , State(TState::ParsedEmpty)
            , Enc(enc)
            , HexValue(0)
            , PctBegin(nullptr)
        {
            Y_ASSERT(0 == (Flags & TFeature::FeaturePathOperation)
                     // can't define all of them
                     || TFeature::FeaturesPath != (Flags & TFeature::FeaturesPath));
            State = ParseImpl();
        }

    public:
        const TSection& Get(TField::EField fld) const {
            return Sections[fld];
        }
        TSection& GetMutable(TField::EField fld) {
            return Sections[fld];
        }
        bool Has(TField::EField fld) const {
            return Get(fld).IsSet();
        }
        bool IsNetPath() const {
            return Has(TField::FieldHost) && 2 < UriStr.length() && '/' == UriStr[0] && '/' == UriStr[1];
        }
        bool IsRootless() const {
            return Has(TField::FieldScheme) && !Has(TField::FieldHost) && (!Has(TField::FieldPath) || '/' != Get(TField::FieldPath).Get()[0]);
        }
        // for RFC 2396 compatibility
        bool IsOpaque() const {
            return IsRootless();
        }
        static ui64 GetFieldFlags(TField::EField fld, const TParseFlags& flags) {
            return FieldFlags[fld] & flags;
        }
        ui64 GetFieldFlags(TField::EField fld) const {
            return GetFieldFlags(fld, Flags);
        }

    protected:
        static const TParseFlags FieldFlags[TField::FieldUrlMAX];
        TSection::TRange CurRange;
        unsigned HexValue;
        const char* PctBegin;

#ifdef DO_PRN
        IOutputStream& PrintAddr(const char* ptr) const {
            return Cdbg << "[" << IntToString<16>(ui64(ptr)) << "] ";
        }

        IOutputStream& PrintHead(const char* ptr, const char* func) const {
            return PrintAddr(ptr) << func << " ";
        }

        IOutputStream& PrintHead(const char* ptr, const char* func, const TField::EField& fld) const {
            return PrintHead(ptr, func) << fld;
        }

        IOutputStream& PrintTail(const TStringBuf& val) const {
            return Cdbg << " [" << val << "]" << Endl;
        }
        IOutputStream& PrintTail(const char* beg, const char* end) const {
            return PrintTail(TStringBuf(beg, end));
        }
#endif

        void ResetSection(TField::EField fld, const char* pc = nullptr) {
#ifdef DO_PRN
            PrintHead(pc, __FUNCTION__, fld);
            PrintTail(pc);
#endif
            Sections[fld].Reset(pc);
        }

        void storeSection(const TStringBuf& val, TField::EField fld) {
#ifdef DO_PRN
            PrintHead(val.data(), __FUNCTION__, fld);
            PrintTail(val);
#endif
            Sections[fld].Set(val);
        }

        void startSection(const char* pc, TField::EField fld) {
#ifdef DO_PRN
            PrintHead(pc, __FUNCTION__, fld);
            PrintTail(pc);
#endif
            copyRequirements(pc);
            Sections[fld].Enter(pc);
        }

        void finishSection(const char* pc, TField::EField fld) {
#ifdef DO_PRN
            PrintHead(pc, __FUNCTION__, fld);
            PrintTail(pc);
#endif
            if (Sections[fld].Leave(pc))
                copyRequirements(pc);
        }

        void setRequirement(const char* ptr, ui64 flags) {
#ifdef DO_PRN
            PrintHead(ptr, __FUNCTION__) << IntToString<16>(flags)
                                         << " & mask=" << IntToString<16>(Flags.Allow | Flags.Extra);
            PrintTail(ptr);
#endif
            CurRange.AddFlag(ptr, Flags.Allow | Flags.Extra, flags);
        }

        void setRequirementExcept(const char* ptr, ui64 flags, ui64 exclflag) {
#ifdef DO_PRN
            PrintHead(ptr, __FUNCTION__) << IntToString<16>(flags)
                                         << " & exclflag=" << IntToString<16>(exclflag)
                                         << " & mask=" << IntToString<16>(Flags.Allow | Flags.Extra);
            PrintTail(ptr);
#endif
            CurRange.AddFlagExcept(ptr, Flags.Allow | Flags.Extra, flags, exclflag);
        }

        void setRequirementUnless(const char* ptr, ui64 flags, ui64 exclmask) {
#ifdef DO_PRN
            PrintHead(ptr, __FUNCTION__) << IntToString<16>(flags)
                                         << " & exclmask=" << IntToString<16>(exclmask)
                                         << " & mask=" << IntToString<16>(Flags.Allow | Flags.Extra);
            PrintTail(ptr);
#endif
            CurRange.AddFlagUnless(ptr, Flags.Allow | Flags.Extra, flags, exclmask);
        }

        void copyRequirementsImpl(const char* ptr);
        void copyRequirements(const char* ptr) {
            PctEnd(ptr);
            if (nullptr != CurRange.Beg && CurRange.Beg != ptr)
                copyRequirementsImpl(ptr);
        }

        void HexDigit(const char* ptr, char c) {
            Y_UNUSED(ptr);
            HexAdd(c - '0');
        }
        void HexUpper(const char* ptr, char c) {
            setRequirementUnless(ptr, TFeature::FeatureToLower, TFeature::FeatureUpperEncoded);
            HexAdd(c - 'A' + 10);
        }
        void HexLower(const char* ptr, char c) {
            setRequirement(ptr, TFeature::FeatureUpperEncoded);
            HexAdd(c - 'a' + 10);
        }
        void HexAdd(unsigned val) {
            HexValue <<= 4;
            HexValue += val;
        }
        void HexReset() {
            HexValue = 0;
        }
        void HexSet(const char* ptr);

        void PctEndImpl(const char* ptr);
        void PctEnd(const char* ptr) {
            if (nullptr != PctBegin && ptr != PctBegin)
                PctEndImpl(ptr);
        }
        void PctBeg(const char* ptr) {
            PctEnd(ptr);
            HexReset();
            PctBegin = ptr;
        }

        void checkSectionCollision(TField::EField fld1, TField::EField fld2) {
            if (Sections[fld1].IsSet() && Sections[fld2].IsSet() && Sections[fld1].Beg == Sections[fld2].Beg) {
                Sections[fld1].Reset();
            }
        }

        bool doParse(const char* str_beg, size_t length);
        TState::EParsed ParseImpl();
    };

}
