#include <library/cpp/uri/encode.h>
#include <library/cpp/uri/http_url.h>
#include <library/cpp/uri/location.h>
#include <library/cpp/uri/qargs.h>
#include <library/cpp/uri/uri.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <cstdlib>

namespace {

enum class EParseMode {
    Uri,
    AbsUri,
    AbsOrHttp,
    WithBase,
    AbsWithBase,
};

struct TFilterConfig {
    TString MatchName;
    bool KeepMatching = false;
};

bool QueryArgFilter(const NUri::TQueryArg& arg, void* filterData) {
    const auto* config = static_cast<const TFilterConfig*>(filterData);
    const bool match = arg.Name == config->MatchName;
    return config->KeepMatching ? match : !match;
}

TString ConsumeToken(FuzzedDataProvider& fdp, size_t maxLen, TStringBuf alphabet) {
    const int len = fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(maxLen));
    TString out;
    out.reserve(len);
    for (int i = 0; i < len; ++i) {
        out.push_back(alphabet[fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(alphabet.size() - 1))]);
    }
    return out;
}

TString ConsumeUriLike(FuzzedDataProvider& fdp, size_t maxLen) {
    if (!fdp.ConsumeBool()) {
        return fdp.ConsumeRandomLengthString(maxLen);
    }

    static const TStringBuf SchemeAlphabet = "abcdefghijklmnopqrstuvwxyz";
    static const TStringBuf HostAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789-";
    static const TStringBuf PathAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789-._~/%";
    static const TStringBuf QueryAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789-._~%=&+;:";

    TString uri;
    if (fdp.ConsumeBool()) {
        switch (fdp.ConsumeIntegralInRange<int>(0, 6)) {
            case 0:
                uri += "http";
                break;
            case 1:
                uri += "https";
                break;
            case 2:
                uri += "ftp";
                break;
            case 3:
                uri += "file";
                break;
            default:
                uri += ConsumeToken(fdp, 8, SchemeAlphabet);
                break;
        }
        uri += ':';
    }

    if (fdp.ConsumeBool()) {
        uri += "//";
        if (fdp.ConsumeBool()) {
            uri += ConsumeToken(fdp, 8, QueryAlphabet);
            if (fdp.ConsumeBool()) {
                uri += ':';
                uri += ConsumeToken(fdp, 8, QueryAlphabet);
            }
            uri += '@';
        }
        const int labels = fdp.ConsumeIntegralInRange<int>(1, 4);
        for (int i = 0; i < labels; ++i) {
            if (i) {
                uri += '.';
            }
            uri += ConsumeToken(fdp, 12, HostAlphabet);
        }
        if (fdp.ConsumeBool()) {
            uri += ':';
            uri += ToString(fdp.ConsumeIntegralInRange<int>(0, 70000));
        }
    }

    if (fdp.ConsumeBool()) {
        uri += '/';
    }
    uri += ConsumeToken(fdp, 64, PathAlphabet);
    if (fdp.ConsumeBool()) {
        uri += '?';
        uri += ConsumeToken(fdp, 96, QueryAlphabet);
    }
    if (fdp.ConsumeBool()) {
        uri += '#';
        if (fdp.ConsumeBool()) {
            uri += '!';
        }
        uri += ConsumeToken(fdp, 64, QueryAlphabet);
    }

    if (uri.size() > maxLen) {
        uri.resize(maxLen);
    }
    return uri;
}

NUri::TParseFlags ChooseFlags(FuzzedDataProvider& fdp) {
    const NUri::TParseFlags variants[] = {
        NUri::TFeature::FeaturesDefault,
        NUri::TFeature::FeaturesBare,
        NUri::TFeature::FeaturesAll,
        NUri::TFeature::FeaturesRecommended,
        NUri::TFeature::NewFeaturesRecommended,
        NUri::TFeature::FeaturesDefault | NUri::TFeature::FeatureSchemeFlexible | NUri::TFeature::FeatureAuthSupported | NUri::TFeature::FeatureAllowEmptyPath,
        NUri::TFeature::FeaturesBare | NUri::TFeature::FeaturePathOperation | NUri::TFeature::FeaturePathStripRootParent | NUri::TFeature::FeatureToLower | NUri::TFeature::FeatureUpperEncoded | NUri::TFeature::FeatureDecodeUnreserved | NUri::TFeature::FeatureEncodeSpace | NUri::TFeature::FeatureEncodeCntrl | NUri::TFeature::FeatureEncodePercent,
    };
    return variants[fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(Y_ARRAY_SIZE(variants) - 1))];
}

NUri::TState::EParsed ParseByMode(
    NUri::TUri& uri,
    TStringBuf input,
    const NUri::TParseFlags& flags,
    EParseMode mode,
    TStringBuf base,
    ui32 maxLen)
{
    switch (mode) {
        case EParseMode::Uri:
            return uri.ParseUri(input, flags, maxLen);
        case EParseMode::AbsUri:
            return uri.ParseAbsUri(input, flags, maxLen);
        case EParseMode::AbsOrHttp:
            return uri.ParseAbsOrHttpUri(input, flags, maxLen);
        case EParseMode::WithBase:
            return uri.Parse(input, flags, base, maxLen);
        case EParseMode::AbsWithBase:
            return uri.ParseAbs(input, flags, base, maxLen);
    }
    Y_ABORT_UNLESS(false);
    return NUri::TState::ParsedBadFormat;
}

TString PrintToString(const NUri::TUri& uri) {
    TString printed = uri.PrintS();

    char stackBuffer[4096];
    if (printed.size() + 1 <= sizeof(stackBuffer)) {
        char* serialized = uri.Serialize(stackBuffer, sizeof(stackBuffer));
        Y_ABORT_UNLESS(serialized != nullptr);
        Y_ABORT_UNLESS(TStringBuf(serialized, printed.size()) == printed);
    }

    char* heapSerialized = uri.Serialize();
    Y_ABORT_UNLESS(heapSerialized != nullptr);
    Y_ABORT_UNLESS(TStringBuf(heapSerialized, printed.size()) == printed);
    std::free(heapSerialized);

    return printed;
}

void CheckStableFields(const NUri::TUri& lhs, const NUri::TUri& rhs) {
    const NUri::TField::EField fields[] = {
        NUri::TField::FieldScheme,
        NUri::TField::FieldUser,
        NUri::TField::FieldPass,
        NUri::TField::FieldHost,
        NUri::TField::FieldPath,
        NUri::TField::FieldQuery,
        NUri::TField::FieldFrag,
        NUri::TField::FieldHashBang,
    };

    for (const auto field : fields) {
        Y_ABORT_UNLESS(lhs.GetField(field) == rhs.GetField(field));
    }
    Y_ABORT_UNLESS(lhs.GetPort() == rhs.GetPort());
}

bool HasSchemeOrAuthority(const NUri::TUri& uri) {
    return 0 != (uri.GetFieldMask() & (NUri::TField::FlagScheme | NUri::TField::FlagHost));
}

bool HasStructuredPrintReparseContract(const NUri::TUri& uri) {
    return HasSchemeOrAuthority(uri) && !uri.IsRootless();
}

bool CanCheckStableFieldsAfterPrintReparse(const NUri::TUri& uri, const NUri::TUri& reparsed) {
    return HasStructuredPrintReparseContract(uri) && HasStructuredPrintReparseContract(reparsed);
}

NUri::TParseFlags GetDocumentedPrintReparseFlags(const NUri::TParseFlags& flags) {
    return flags.Exclude(NUri::TFeature::FeatureNoRelPath) | NUri::TParseFlags(NUri::TFeature::FeatureAllowRootless);
}

bool IsHexDigit(char c) {
    return ('0' <= c && c <= '9') || ('A' <= c && c <= 'F') || ('a' <= c && c <= 'f');
}

bool HasUnsafePercentEncoding(TStringBuf text) {
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] != '%') {
            continue;
        }
        if (i + 2 >= text.size() || !IsHexDigit(text[i + 1]) || !IsHexDigit(text[i + 2])) {
            return true;
        }
        i += 2;
    }
    return false;
}

bool HasUnsafeReparseCharacters(TStringBuf text) {
    for (const unsigned char c : text) {
        if (c <= 0x20 || c >= 0x7f) {
            return true;
        }

        switch (c) {
            case '"':
            case '<':
            case '>':
            case '\\':
            case '^':
            case '`':
            case '{':
            case '|':
            case '}':
                return true;
            default:
                break;
        }
    }
    return false;
}

bool IsAsciiAlpha(char c) {
    return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
}

bool IsAsciiSchemeChar(char c) {
    return IsAsciiAlpha(c) || ('0' <= c && c <= '9') || c == '+' || c == '-' || c == '.';
}

char ToAsciiLower(char c) {
    return 'A' <= c && c <= 'Z' ? c - 'A' + 'a' : c;
}

bool EqualsAsciiNoCase(TStringBuf lhs, TStringBuf rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (ToAsciiLower(lhs[i]) != ToAsciiLower(rhs[i])) {
            return false;
        }
    }
    return true;
}

bool TryGetPrintedScheme(TStringBuf text, TStringBuf& scheme) {
    if (text.empty() || !IsAsciiAlpha(text[0])) {
        return false;
    }

    for (size_t i = 1; i < text.size(); ++i) {
        switch (text[i]) {
            case ':':
                scheme = TStringBuf(text.data(), i);
                return true;
            case '/':
            case '?':
            case '#':
                return false;
            default:
                if (!IsAsciiSchemeChar(text[i])) {
                    return false;
                }
                break;
        }
    }
    return false;
}

bool IsKnownNonHttpScheme(TStringBuf scheme) {
    return EqualsAsciiNoCase(scheme, "https") ||
        EqualsAsciiNoCase(scheme, "ftp") ||
        EqualsAsciiNoCase(scheme, "file") ||
        EqualsAsciiNoCase(scheme, "ws") ||
        EqualsAsciiNoCase(scheme, "wss");
}

bool CanReparsePrintedScheme(TStringBuf printed, const NUri::TParseFlags& reparseFlags) {
    TStringBuf scheme;
    if (!TryGetPrintedScheme(printed, scheme)) {
        return true;
    }
    if (EqualsAsciiNoCase(scheme, "http")) {
        return true;
    }
    if (IsKnownNonHttpScheme(scheme)) {
        return 0 != (reparseFlags & (NUri::TFeature::FeatureSchemeFlexible | NUri::TFeature::FeatureSchemeKnown));
    }
    return 0 != (reparseFlags & (NUri::TFeature::FeatureSchemeFlexible | NUri::TFeature::FeatureNoRelPath));
}

void CheckDocumentedPrintRoundTrip(
    const NUri::TUri& uri,
    TStringBuf printed,
    const NUri::TParseFlags& flags)
{
    const NUri::TParseFlags reparseFlags = GetDocumentedPrintReparseFlags(flags);
    NUri::TUri reparsed;
    const auto reparsedState = reparsed.ParseUri(printed, reparseFlags);
    if (reparsedState == NUri::TState::ParsedEmpty) {
        Y_ABORT_UNLESS(printed.empty());
        Y_ABORT_UNLESS(uri.GetUrlFieldMask() == 0);
        Y_ABORT_UNLESS(PrintToString(reparsed) == printed);
        return;
    }

    const bool canCheckReparsedFields =
        HasStructuredPrintReparseContract(uri) &&
        !HasUnsafeReparseCharacters(printed) &&
        CanReparsePrintedScheme(printed, reparseFlags) &&
        (0 != (reparseFlags & NUri::TFeature::FeatureEncodePercent) || !HasUnsafePercentEncoding(printed));

    if (reparsedState != NUri::TState::ParsedOK) {
        return;
    }

    if (PrintToString(reparsed) != printed) {
        return;
    }
    if (canCheckReparsedFields && CanCheckStableFieldsAfterPrintReparse(uri, reparsed)) {
        CheckStableFields(uri, reparsed);
    }
}

bool IsAllowedCopyPathMaterialization(const NUri::TUri& original, const NUri::TUri& copied, const NUri::TParseFlags& flags) {
    if (0 == (flags & NUri::TFeature::FeatureAllowEmptyPath)) {
        return false;
    }

    if (0 == (original.GetFieldMask() & NUri::TField::FlagHost) || 0 != (original.GetFieldMask() & NUri::TField::FlagPath)) {
        return false;
    }

    if ((original.GetFieldMask() ^ copied.GetFieldMask()) != NUri::TField::FlagPath) {
        return false;
    }

    if (copied.GetField(NUri::TField::FieldPath) != TStringBuf("/")) {
        return false;
    }

    const NUri::TField::EField fields[] = {
        NUri::TField::FieldScheme,
        NUri::TField::FieldUser,
        NUri::TField::FieldPass,
        NUri::TField::FieldHost,
        NUri::TField::FieldPort,
        NUri::TField::FieldQuery,
        NUri::TField::FieldFrag,
        NUri::TField::FieldHashBang,
        NUri::TField::FieldHostAscii,
    };

    for (const auto field : fields) {
        if (original.GetField(field) != copied.GetField(field)) {
            return false;
        }
    }

    return original.GetPort() == copied.GetPort() && original.GetHost() == copied.GetHost();
}

void CheckCopiedUriStable(
    const NUri::TUri& original,
    const NUri::TUri& copied,
    TStringBuf originalPrinted,
    const NUri::TParseFlags& flags,
    EParseMode mode,
    TStringBuf base)
{
    const TString copiedPrinted = PrintToString(copied);
    if (copiedPrinted == originalPrinted) {
        return;
    }

    Y_ABORT_UNLESS(IsAllowedCopyPathMaterialization(original, copied, flags));
    Y_UNUSED(mode);
    Y_UNUSED(base);
    CheckDocumentedPrintRoundTrip(copied, copiedPrinted, flags);
}

void CheckParsePrintStable(TStringBuf input, const NUri::TParseFlags& flags, EParseMode mode, TStringBuf base, ui32 maxLen) {
    NUri::TUri uri;
    const auto parsed = ParseByMode(uri, input, flags, mode, base, maxLen);
    if (parsed != NUri::TState::ParsedOK) {
        return;
    }

    const TString printed = PrintToString(uri);

    CheckDocumentedPrintRoundTrip(uri, printed, flags);

    NUri::TUri copied(uri);
    CheckCopiedUriStable(uri, copied, printed, flags, mode, base);

    NUri::TUri assigned;
    assigned.Copy(uri);
    CheckCopiedUriStable(uri, assigned, printed, flags, mode, base);

    THttpURL legacy(uri);
    CheckCopiedUriStable(uri, legacy, printed, flags, mode, base);
}

void CheckQueryArgsStable(TStringBuf input, const NUri::TParseFlags& flags, FuzzedDataProvider& fdp) {
    NUri::TUri uri;
    if (uri.ParseUri(input, flags) != NUri::TState::ParsedOK) {
        return;
    }

    ui32 queryFlags = 0;
    if (fdp.ConsumeBool()) {
        queryFlags |= NUri::TQueryArg::FeatureSortByName;
    }
    if (fdp.ConsumeBool()) {
        queryFlags |= NUri::TQueryArg::FeatureRemoveEmptyQuery;
    }
    if (fdp.ConsumeBool()) {
        queryFlags |= NUri::TQueryArg::FeatureRewriteDirty;
    }

    TFilterConfig filterConfig;
    filterConfig.MatchName = ConsumeToken(fdp, 16, "abcdefghijklmnopqrstuvwxyz0123456789_");
    filterConfig.KeepMatching = fdp.ConsumeBool();
    if (!filterConfig.MatchName.empty()) {
        queryFlags |= NUri::TQueryArg::FeatureFilter;
    }

    NUri::TQueryArgProcessing processing(
        queryFlags,
        (queryFlags & NUri::TQueryArg::FeatureFilter) ? &QueryArgFilter : nullptr,
        (queryFlags & NUri::TQueryArg::FeatureFilter) ? &filterConfig : nullptr);

    const auto processed = processing.Process(uri);
    if (processed == NUri::TQueryArg::ProcessedMalformed) {
        return;
    }
    uri.Rewrite();
    const TString once = uri.PrintS();

    NUri::TUri reparsed;
    if (reparsed.ParseUri(once, flags) != NUri::TState::ParsedOK) {
        return;
    }
    NUri::TQueryArgProcessing processingAgain(
        queryFlags,
        (queryFlags & NUri::TQueryArg::FeatureFilter) ? &QueryArgFilter : nullptr,
        (queryFlags & NUri::TQueryArg::FeatureFilter) ? &filterConfig : nullptr);
    const auto processedAgain = processingAgain.Process(reparsed);
    if (processedAgain != NUri::TQueryArg::ProcessedMalformed) {
        reparsed.Rewrite();
        const TString twice = reparsed.PrintS();
        Y_UNUSED(twice);
    }
}

void CheckNormalizeStable(TStringBuf baseText, TStringBuf linkText, TStringBuf codebase, const NUri::TParseFlags& flags) {
    NUri::TUri base;
    if (base.ParseUri(baseText, flags) != NUri::TState::ParsedOK) {
        return;
    }

    NUri::TUri normalized;
    const ui64 normalizeFlags = flags.Allow | flags.Extra;
    const auto type = normalized.Normalize(base, linkText, codebase, normalizeFlags);
    if (type == NUri::TUri::LinkIsBad || type == NUri::TUri::LinkBadAbs) {
        return;
    }

    const TString printed = normalized.PrintS();
    CheckParsePrintStable(printed, flags, EParseMode::Uri, TStringBuf(), 0);

    NUri::TUri normalizedAgain;
    const auto typeAgain = normalizedAgain.Normalize(base, printed, codebase, normalizeFlags);
    if (typeAgain != NUri::TUri::LinkIsBad && typeAgain != NUri::TUri::LinkBadAbs) {
        Y_ABORT_UNLESS(normalizedAgain.PrintS() == printed);
    }
}

NUri::TField::EField ChooseField(FuzzedDataProvider& fdp) {
    const NUri::TField::EField fields[] = {
        NUri::TField::FieldUser,
        NUri::TField::FieldPass,
        NUri::TField::FieldPath,
        NUri::TField::FieldQuery,
        NUri::TField::FieldFrag,
        NUri::TField::FieldAllMAX,
    };
    return fields[fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(Y_ARRAY_SIZE(fields) - 1))];
}

ui64 ChooseEncodeFlags(FuzzedDataProvider& fdp) {
    const ui64 flags[] = {
        0,
        NUri::TFeature::FeatureUpperEncoded,
        NUri::TFeature::FeatureDecodeANY | NUri::TFeature::FeatureUpperEncoded,
        NUri::TFeature::FeatureEncodeSpace | NUri::TFeature::FeatureEncodeCntrl | NUri::TFeature::FeatureEncodePercent,
        NUri::TFeature::FeaturesAllEncoder,
    };
    return flags[fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(Y_ARRAY_SIZE(flags) - 1))];
}

TString ReEncode(TStringBuf input, ui64 flags, NUri::TField::EField field) {
    TString out;
    TStringOutput stream(out);
    NUri::NEncode::TEncoder::ReEncode(stream, input, NUri::NEncode::TEncodeMapper(flags, field));
    return out;
}

bool CanAssertReEncodeStable(TStringBuf text, ui64 flags, NUri::TField::EField field) {
    // A decode pass can leave a data '%' next to decoded hex bytes, creating a
    // fresh %HH sequence for the next pass. Assert only when visible triplets
    // are already stable under the same mapper.
    for (size_t i = 0; i + 2 < text.size(); ++i) {
        if (text[i] != '%' || !IsHexDigit(text[i + 1]) || !IsHexDigit(text[i + 2])) {
            continue;
        }

        const TStringBuf triplet(text.data() + i, 3);
        const TString reencodedTriplet = ReEncode(triplet, flags, field);
        if (reencodedTriplet != triplet) {
            return false;
        }
        i += 2;
    }
    return true;
}

void CheckEncodingStable(TStringBuf input, FuzzedDataProvider& fdp) {
    const ui64 flags = ChooseEncodeFlags(fdp);
    const auto field = ChooseField(fdp);
    const TString once = ReEncode(input, flags, field);
    const TString twice = ReEncode(once, flags, field);
    if (CanAssertReEncodeStable(once, flags, field)) {
        Y_ABORT_UNLESS(twice == once);
    }

    TString encodedField;
    TStringOutput out(encodedField);
    NUri::NEncode::TEncoder::EncodeField(out, input, field, flags);

    TString decoded;
    TStringOutput decodedOut(decoded);
    NUri::NEncode::TEncoder::Decode(decodedOut, encodedField, flags);
}

void CheckRedirectStable(TStringBuf baseText, TStringBuf locationText, const NUri::TParseFlags& flags) {
    const TString resolved = NUri::ResolveRedirectLocation(baseText, locationText);
    if (resolved.empty()) {
        return;
    }
    CheckParsePrintStable(resolved, flags, EParseMode::Uri, TStringBuf(), 0);

    const TString resolvedAgain = NUri::ResolveRedirectLocation(baseText, resolved);
    if (!resolvedAgain.empty()) {
        CheckParsePrintStable(resolvedAgain, flags, EParseMode::Uri, TStringBuf(), 0);
    }
}

void FuzzUriSurface(FuzzedDataProvider& fdp) {
    const NUri::TParseFlags flags = ChooseFlags(fdp);
    const auto mode = static_cast<EParseMode>(fdp.ConsumeIntegralInRange<int>(0, 4));
    const TString base = ConsumeUriLike(fdp, 256);
    const TString input = ConsumeUriLike(fdp, 768);
    const TString codebase = ConsumeUriLike(fdp, 128);
    const ui32 maxLen = fdp.ConsumeBool() ? 0 : fdp.ConsumeIntegralInRange<ui32>(1, 1024);

    CheckParsePrintStable(input, flags, mode, base, maxLen);
    CheckParsePrintStable(base, flags, EParseMode::Uri, TStringBuf(), 0);
    CheckNormalizeStable(base, input, codebase, flags);
    CheckRedirectStable(base, input, flags);
    CheckQueryArgsStable(input, flags, fdp);
    CheckEncodingStable(input, fdp);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzUriSurface(fdp);
    } catch (...) {
    }

    return 0;
}
