#pragma once

#include <library/cpp/unicode/normalization/normalization.h>
#include <library/cpp/langs/langs.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>

#include <bitset>

namespace NUF {
    using TLanguages = std::bitset<LANG_MAX>;
    using TScripts = std::bitset<SCRIPT_MAX>;

    /* language-sensitive
 * insignificant diacritics are removed
 * significant diacritics are either left in place or turned into diftongs (i.e. umlauts in german)
 * ligatures and special symbols are decomposed
 * all control and space characters are made spaces and duplicates are collapsed
 * all dash characters are made dashes
 * all invisible characters (shy, zwspaces) are removed
 * all other characters are left intact
 * designed to be more robust and aggressive than lemmer normalization
 * MAY CONTAIN INCORRECT DATA OR DISCONTAIN SOME IMPORTANT DATA!
 *
 * TODO: make a tool to generate rules automatically on ICU and lemmer data
 *
 * @maintainer: velavokr
 */

    using TOffsets = TVector<size_t>;
    class TNormalizer {
        TLanguages Languages;
        TScripts Scripts;

        TVector<wchar16> CDBuf;
        TVector<wchar16> OutBuf;
        TVector<wchar16> TmpBuf;
        TOffsets CDOffsets;

        NUnicode::TNormalizer<NUnicode::NFD> Decomposer;
        NUnicode::TNormalizer<NUnicode::NFC> Recomposer;

        const wchar16* p;
        const wchar16* p0;
        const wchar16* pe;
        const wchar16* eof;
        const wchar16* ts;
        const wchar16* te;
        const wchar16* ret;
        int cs;
        int act;

        bool DoRenyxa;
        bool DoLowerCase;
        bool DoSimpleCyr;
        bool FillOffsets;

    public:
        TNormalizer(ELanguage lmain = LANG_UNK, ELanguage laux = LANG_UNK);
        TNormalizer(const TLanguages& langs);

        void SetDoRenyxa(bool);
        void SetDoLowerCase(bool);
        void SetDoSimpleCyr(bool);
        void SetFillOffsets(bool);
        void SetLanguages(ELanguage lmain, ELanguage laux = LANG_UNK);
        void SetLanguages(const TLanguages& langs);

        void Reset();
        void SetInput(TWtringBuf b);

        TWtringBuf GetOutput() const {
            return TWtringBuf(OutBuf.data(), OutBuf.size());
        }

        TWtringBuf GetCanonDenormalizedInput() const {
            return TWtringBuf(CDBuf.data(), CDBuf.size());
        }

        const TOffsets& GetOffsetsInCanonDenormalizedInput() const {
            return CDOffsets;
        }

        void DoNormalize();

    protected:
        static const ui64 ZERO_WIDTH =
            (ULL(1) << (Cf_FORMAT)) | (ULL(1) << (Cf_JOIN)) | (ULL(1) << (Cf_BIDI)) | (ULL(1) << (Cf_ZWNBSP)) | (ULL(1) << (Zs_ZWSPACE)) | (ULL(1) << (Mc_SPACING)) | (ULL(1) << (Mn_NONSPACING)) | (ULL(1) << (Me_ENCLOSING));

        static const ui64 SPACE =
            (ULL(1) << (Cc_SPACE)) | (ULL(1) << (Zs_SPACE)) | (ULL(1) << (Zl_LINE)) | (ULL(1) << (Zp_PARAGRAPH)) | (ULL(1) << (Cc_ASCII)) | (ULL(1) << (Cc_SEPARATOR)) | (ULL(1) << (Cn_UNASSIGNED)) | (ULL(1) << (Co_PRIVATE));

        bool Is(ELanguage lang) const {
            return Languages.test(lang);
        }
        bool Is(EScript scr) const {
            return Scripts.test(scr);
        }

        bool IsSpace() const {
            return NUnicode::CharHasType(*p, SPACE);
        }
        bool IsNothing() const {
            return NUnicode::CharHasType(*p, ZERO_WIDTH) || wchar16(0xAD) /*shy*/ == *p;
        }
        bool IsDash() const {
            return ::IsDash(*p);
        }

        void Emit(wchar16 c, size_t off = 0) {
            OutBuf.push_back(c);
            if (FillOffsets)
                CDOffsets.push_back(ts - p0 + off);
        }

        void EmitUpper(wchar16 c, size_t off = 0) {
            if (DoLowerCase)
                Emit(ToLower(c), off);
            else
                Emit(c, off);
        }

        void EmitRenyxa(wchar16 c, size_t off = 0) {
            if (DoRenyxa)
                EmitUpper(c, off);
            else
                EmitUpper(*ts, off);
        }

        void EmitSimpleCyr(wchar16 c, size_t off = 0) {
            if (DoSimpleCyr)
                EmitUpper(c, off);
            else
                EmitUpper(*ts, off);
        }

        wchar16 Last() const {
            return OutBuf.empty() ? 0 : OutBuf.back();
        }
    };
}
