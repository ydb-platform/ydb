#include "parse.h"
#include "common.h"
#include "encode.h"

namespace NUri {
    const TParseFlags TParser::FieldFlags[] =
        {
            TParseFlags(0 // FieldScheme
                            | TFeature::FeatureToLower,
                        0)

                ,
            TParseFlags(0 // FieldUsername
                            | TFeature::FeatureDecodeANY | TFeature::FeaturesDecode | TFeature::FeatureEncodePercent,
                        0 | TFeature::FeatureToLower)

                ,
            TParseFlags(0 // FieldPassword
                            | TFeature::FeatureDecodeANY | TFeature::FeaturesDecode | TFeature::FeatureEncodePercent,
                        0 | TFeature::FeatureToLower)

                ,
            TParseFlags(0 // FieldHost
                            | TFeature::FeatureToLower | TFeature::FeatureUpperEncoded | (TFeature::FeaturesMaybeEncode & ~TFeature::FeatureEncodeExtendedDelim),
                        0 | TFeature::FeaturesMaybeDecode)

                ,
            TParseFlags(0 // FieldPort
                        ,
                        0)

                ,
            TParseFlags(0 // FieldPath
                            | TFeature::FeaturesEncodePChar | TFeature::FeaturePathOperation,
                        0 | TFeature::FeatureToLower | TFeature::FeatureEncodeSpaceAsPlus)

                ,
            TParseFlags(0 // FieldQuery
                            | TFeature::FeaturesEncodePChar | TFeature::FeatureEncodeSpaceAsPlus,
                        0 | TFeature::FeatureToLower)

                ,
            TParseFlags(0 // FieldFragment
                            | TFeature::FeaturesEncodePChar,
                        0 | TFeature::FeatureToLower | TFeature::FeatureEncodeSpaceAsPlus)

                ,
            TParseFlags(0 // FieldHashBang
                            | TFeature::FeaturesEncodePChar,
                        0 | TFeature::FeatureToLower | TFeature::FeatureEncodeSpaceAsPlus)};

    namespace NParse {
        void TRange::AddRange(const TRange& range, ui64 mask) {
            FlagsAllPlaintext |= range.FlagsAllPlaintext;
            // update only if flags apply here
            mask &= range.FlagsEncodeMasked;
            if (0 == mask)
                return;
            FlagsEncodeMasked |= mask;
            if (mask & TFeature::FeaturesMaybeEncode)
                Encode += range.Encode;
            if (mask & TFeature::FeaturesDecode)
                Decode += range.Decode;
        }

    }

    void TParser::copyRequirementsImpl(const char* ptr) {
        Y_ASSERT(0 != CurRange.FlagsAllPlaintext);
        Y_UNUSED(ptr);
#ifdef DO_PRN
        PrintHead(ptr, __FUNCTION__)
            << " all=[" << IntToString<16>(CurRange.FlagsAllPlaintext)
            << "] enc=[" << IntToString<16>(CurRange.FlagsEncodeMasked)
            << " & " << IntToString<16>(Flags.Allow | Flags.Extra) << "]";
        PrintTail(CurRange.Beg, ptr);
#endif
        for (int i = 0; i < TField::FieldUrlMAX; ++i) {
            const TField::EField fld = TField::EField(i);
            TSection& section = Sections[fld];
            // update only sections in progress
            if (nullptr == section.Beg)
                continue;
            // and overlapping with the range
            if (nullptr != section.End && section.End < CurRange.Beg)
                continue;
#ifdef DO_PRN
            PrintHead(ptr, __FUNCTION__, fld)
                << " all=[" << IntToString<16>(CurRange.FlagsAllPlaintext)
                << "] enc=[" << IntToString<16>(CurRange.FlagsEncodeMasked)
                << " & " << IntToString<16>(GetFieldFlags(fld)) << "]";
            PrintTail(section.Beg, ptr);
#endif
            section.AddRange(CurRange, GetFieldFlags(fld));
        }
        CurRange.Reset();
    }

    void TParser::PctEndImpl(const char* ptr) {
#ifdef DO_PRN
        PrintHead(PctBegin, __FUNCTION__);
        PrintTail(PctBegin, ptr);
#else
        Y_UNUSED(ptr);
#endif
        setRequirement(PctBegin, TEncoder::GetFlags('%').FeatFlags);
        PctBegin = nullptr;
    }

    void TParser::HexSet(const char* ptr) {
        Y_ASSERT(nullptr != PctBegin);
#ifdef DO_PRN
        PrintHead(ptr, __FUNCTION__);
        PrintTail(PctBegin, ptr + 1);
#endif
        PctBegin = nullptr;
        const unsigned char ch = HexValue;
        ui64 flags = TEncoder::GetFlags('%').FeatFlags | TEncoder::GetFlags(ch).FeatFlags;

        setRequirementExcept(ptr, flags, TFeature::FeaturesMaybeEncode);
    }

    TState::EParsed TParser::ParseImpl() {
#ifdef DO_PRN
        PrintHead(UriStr.data(), "[Parsing]") << "URL";
        PrintTail(UriStr);
#endif

        const bool ok = doParse(UriStr.data(), UriStr.length());

#ifdef DO_PRN
        Cdbg << (ok ? "[Parsed]" : "[Failed]");
        for (int idx = 0; idx < TField::FieldUrlMAX; ++idx) {
            const TSection& section = Sections[idx];
            if (section.IsSet())
                Cdbg << ' ' << TField::EField(idx) << "=[" << section.Get() << ']';
        }
        Cdbg << Endl;
#endif

        if (!ok) {
            if (!(Flags & TFeature::FeatureTryToFix) || !Sections[TField::FieldFrag].Beg)
                return TState::ParsedBadFormat;
            //Here: error was in fragment, just ignore it
            ResetSection(TField::FieldFrag);
        }

        if ((Flags & TFeature::FeatureDenyNetworkPath) && IsNetPath())
            return TState::ParsedBadFormat;

        const TSection& scheme = Sections[TField::FieldScheme];
        Scheme = scheme.IsSet() ? TSchemeInfo::GetKind(scheme.Get()) : TScheme::SchemeEmpty;
        const TSchemeInfo& schemeInfo = TSchemeInfo::Get(Scheme);

        if (IsRootless()) {
            // opaque case happens
            if (schemeInfo.FldReq & TField::FlagHost)
                return TState::ParsedBadFormat;

            if (TScheme::SchemeEmpty == Scheme)
                return TState::ParsedBadScheme;

            if (Flags & TFeature::FeatureAllowRootless)
                return TState::ParsedOK;

            if (!(Flags & TFeature::FeatureSchemeFlexible))
                return TState::ParsedBadScheme;

            return TState::ParsedRootless;
        }

        checkSectionCollision(TField::FieldUser, TField::FieldHost);
        checkSectionCollision(TField::FieldPass, TField::FieldPort);

        if (0 == (Flags & TFeature::FeatureAuthSupported))
            if (Sections[TField::FieldUser].IsSet() || Sections[TField::FieldPass].IsSet())
                return TState::ParsedBadAuth;

        TSection& host = Sections[TField::FieldHost];
        if (host.IsSet())
            for (; host.End != host.Beg && '.' == host.End[-1];)
                --host.End;

        if (scheme.IsSet()) {
            ui64 wantCareFlags = 0;
            switch (Scheme) {
                case TScheme::SchemeHTTP:
                    break;
                case TScheme::SchemeEmpty:
                    Scheme = TScheme::SchemeUnknown;
                    [[fallthrough]];
                case TScheme::SchemeUnknown:
                    wantCareFlags =
                        TFeature::FeatureSchemeFlexible | TFeature::FeatureNoRelPath;
                    break;
                default:
                    wantCareFlags =
                        TFeature::FeatureSchemeFlexible | TFeature::FeatureSchemeKnown;
                    break;
            }

            if (0 != wantCareFlags && 0 == (Flags & wantCareFlags))
                return TState::ParsedBadScheme;
            if ((schemeInfo.FldReq & TField::FlagHost) || (Flags & TFeature::FeatureRemoteOnly))
                if (!host.IsSet() || 0 == host.Len())
                    return TState::ParsedBadFormat;
        }

        return TState::ParsedOK;
    }

}
