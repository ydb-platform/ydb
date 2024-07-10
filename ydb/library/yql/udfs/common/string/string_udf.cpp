#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <library/cpp/charset/codepage.h>
#include <library/cpp/deprecated/split/split_iterator.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/string_utils/base32/base32.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

#include <util/charset/wide.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>
#include <util/string/ascii.h>
#include <util/string/escape.h>
#include <util/string/hex.h>
#include <util/string/join.h>
#include <util/string/reverse.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/string/util.h>
#include <util/string/vector.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

#define STRING_UDF(udfName, function)                                                    \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##udfName, char*(TAutoMap<char*>)) {                 \
        const TString input(args[0].AsStringRef());                                     \
        const auto& result = function(input);                                           \
        return valueBuilder->NewString(result);                                         \
    }                                                                                   \
                                                                                        \
    struct T##udfName##KernelExec                                                       \
        : public TUnaryKernelExec<T##udfName##KernelExec>                               \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            const TString input(arg1.AsStringRef());                                    \
            const auto& result = function(input);                                       \
            sink(TBlockItem(result));                                                   \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do) \


// 'unsafe' udf is actually strict - it returns null on any exception
#define STRING_UNSAFE_UDF(udfName, function)                                             \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##udfName, TOptional<char*>(TOptional<char*>)) {     \
        EMPTY_RESULT_ON_EMPTY_ARG(0);                                                   \
        const TString input(args[0].AsStringRef());                                     \
        try {                                                                           \
            const auto& result = function(input);                                       \
            return valueBuilder->NewString(result);                                     \
        } catch (yexception&) {                                                         \
            return TUnboxedValue();                                                     \
        }                                                                               \
    }                                                                                   \
                                                                                        \
    struct T##udfName##KernelExec                                                       \
        : public TUnaryKernelExec<T##udfName##KernelExec>                               \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            if (!arg1) {                                                                \
                return sink(TBlockItem());                                              \
            }                                                                           \
                                                                                        \
            const TString input(arg1.AsStringRef());                                    \
            try {                                                                       \
                const auto& result = function(input);                                   \
                sink(TBlockItem(result));                                               \
            } catch (yexception&) {                                                     \
                return sink(TBlockItem());                                              \
            }                                                                           \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)

#define STROKA_UDF(udfName, function)                                   \
    SIMPLE_STRICT_UDF(T##udfName, TOptional<char*>(TOptional<char*>)) { \
        EMPTY_RESULT_ON_EMPTY_ARG(0)                                    \
        const TString input(args[0].AsStringRef());                     \
        try {                                                           \
            TUtf16String wide = UTF8ToWide(input);                      \
            function(wide);                                             \
            return valueBuilder->NewString(WideToUTF8(wide));           \
        } catch (yexception&) {                                         \
            return TUnboxedValue();                                     \
        }                                                               \
    }

#define STROKA_CASE_UDF(udfName, function)                              \
    SIMPLE_STRICT_UDF(T##udfName, TOptional<char*>(TOptional<char*>)) { \
        EMPTY_RESULT_ON_EMPTY_ARG(0)                                    \
        const TString input(args[0].AsStringRef());                     \
        try {                                                           \
            TUtf16String wide = UTF8ToWide(input);                      \
            function(wide.begin(), wide.size());                        \
            return valueBuilder->NewString(WideToUTF8(wide));           \
        } catch (yexception&) {                                         \
            return TUnboxedValue();                                     \
        }                                                               \
    }

#define STROKA_ASCII_CASE_UDF(udfName, function)                                         \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##udfName, char*(TAutoMap<char*>)) {                 \
        TString input(args[0].AsStringRef());                                           \
        if (input.function()) {                                                         \
            return valueBuilder->NewString(input);                                      \
        } else {                                                                        \
            return args[0];                                                             \
        }                                                                               \
    }                                                                                   \
                                                                                        \
    struct T##udfName##KernelExec                                                       \
        : public TUnaryKernelExec<T##udfName##KernelExec>                               \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            TString input(arg1.AsStringRef());                                          \
            if (input.function()) {                                                     \
                sink(TBlockItem(input));                                                \
            } else {                                                                    \
                sink(arg1);                                                             \
            }                                                                           \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)


#define STROKA_FIND_UDF(udfName, function)                             \
    SIMPLE_STRICT_UDF(T##udfName, bool(TOptional<char*>, char*)) {     \
        Y_UNUSED(valueBuilder);                                        \
        if (args[0]) {                                                 \
            const TString haystack(args[0].AsStringRef());             \
            const TString needle(args[1].AsStringRef());               \
            return TUnboxedValuePod(haystack.function(needle));        \
        } else {                                                       \
            return TUnboxedValuePod(false);                            \
        }                                                              \
    }

#define STRING_TWO_ARGS_UDF(udfName, function)                          \
    SIMPLE_STRICT_UDF(T##udfName, bool(TOptional<char*>, char*)) {      \
        Y_UNUSED(valueBuilder);                                         \
        if (args[0]) {                                                  \
            const TString haystack(args[0].AsStringRef());              \
            const TString needle(args[1].AsStringRef());                \
            return TUnboxedValuePod(function(haystack, needle));        \
        } else {                                                        \
            return TUnboxedValuePod(false);                             \
        }                                                               \
    }

#define IS_ASCII_UDF(function)                                                           \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##function, bool(TOptional<char*>)) {                \
        Y_UNUSED(valueBuilder);                                                         \
        if (args[0]) {                                                                  \
            const TStringBuf input(args[0].AsStringRef());                              \
            bool result = true;                                                         \
            for (auto c : input) {                                                      \
                if (!function(c)) {                                                     \
                    result = false;                                                     \
                    break;                                                              \
                }                                                                       \
            }                                                                           \
            return TUnboxedValuePod(result);                                            \
        } else {                                                                        \
            return TUnboxedValuePod(false);                                             \
        }                                                                               \
    }                                                                                   \
                                                                                        \
    struct T##function##KernelExec                                                      \
        : public TUnaryKernelExec<T##function##KernelExec>                              \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            if (arg1) {                                                                 \
                const TStringBuf input(arg1.AsStringRef());                             \
                bool result = true;                                                     \
                for (auto c : input) {                                                  \
                    if (!function(c)) {                                                 \
                        result = false;                                                 \
                        break;                                                          \
                    }                                                                   \
                }                                                                       \
                sink(TBlockItem(result));                                               \
            } else {                                                                    \
                sink(TBlockItem(false));                                                \
            }                                                                           \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##function, T##function##KernelExec::Do)



#define STRING_STREAM_PAD_FORMATTER_UDF(function)                                                    \
    BEGIN_SIMPLE_ARROW_UDF_WITH_OPTIONAL_ARGS(T##function,                                           \
                                              char*(TAutoMap<char*>, ui64, TOptional<char*>), 1)     \
    {                                                                                                \
        TStringStream result;                                                                        \
        const TStringBuf input(args[0].AsStringRef());                                               \
        char paddingSymbol = ' ';                                                                    \
        if (args[2]) {                                                                               \
            if (args[2].AsStringRef().Size() != 1) {                                                 \
                ythrow yexception() << "Not 1 symbol in paddingSymbol";                              \
            }                                                                                        \
            paddingSymbol = TString(args[2].AsStringRef())[0];                                       \
        }                                                                                            \
        const ui64 padLen = args[1].Get<ui64>();                                                     \
        if (padLen > padLim) {                                                                       \
             ythrow yexception() << "Padding length (" << padLen << ") exceeds maximum: " << padLim; \
        }                                                                                            \
        result << function(input, padLen, paddingSymbol);                                            \
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));                    \
    }                                                                                                \
                                                                                                     \
    struct T##function##KernelExec                                                                   \
        : public TGenericKernelExec<T##function##KernelExec, 3>                                      \
    {                                                                                                \
        template <typename TSink>                                                                    \
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {              \
            TStringStream result;                                                                    \
            const TStringBuf input(args.GetElement(0).AsStringRef());                                \
            char paddingSymbol = ' ';                                                                \
            if (args.GetElement(2)) {                                                                \
                if (args.GetElement(2).AsStringRef().Size() != 1) {                                  \
                    ythrow yexception() << "Not 1 symbol in paddingSymbol";                          \
                }                                                                                    \
                paddingSymbol = TString(args.GetElement(2).AsStringRef())[0];                        \
            }                                                                                        \
            const ui64 padLen = args.GetElement(1).Get<ui64>();                                      \
            if (padLen > padLim) {                                                                   \
                ythrow yexception() << "Padding length (" << padLen                                  \
                                    << ") exceeds maximum: " << padLim;                              \
            }                                                                                        \
            result << function(input, padLen, paddingSymbol);                                        \
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));                              \
        }                                                                                            \
    };                                                                                               \
                                                                                                     \
    END_SIMPLE_ARROW_UDF(T##function, T##function##KernelExec::Do)

#define STRING_STREAM_NUM_FORMATTER_UDF(function, argType)                               \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##function, char*(TAutoMap<argType>)) {              \
        TStringStream result;                                                           \
        result << function(args[0].Get<argType>());                                     \
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));       \
    }                                                                                   \
                                                                                        \
    struct T##function##KernelExec                                                      \
        : public TUnaryKernelExec<T##function##KernelExec>                              \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            TStringStream result;                                                       \
            result << function(arg1.Get<argType>());                                    \
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));                 \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##function, T##function##KernelExec::Do)

#define STRING_STREAM_TEXT_FORMATTER_UDF(function)                                       \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##function, char*(TAutoMap<char*>)) {                \
        TStringStream result;                                                           \
        const TStringBuf input(args[0].AsStringRef());                                  \
        result << function(input);                                                      \
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));       \
    }                                                                                   \
                                                                                        \
    struct T##function##KernelExec                                                      \
        : public TUnaryKernelExec<T##function##KernelExec>                              \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            TStringStream result;                                                       \
            const TStringBuf input(arg1.AsStringRef());                                 \
            result << function(input);                                                  \
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));                 \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##function, T##function##KernelExec::Do)


#define STRING_STREAM_HRSZ_FORMATTER_UDF(udfName, hrSize)                                \
    BEGIN_SIMPLE_STRICT_ARROW_UDF(T##udfName, char*(TAutoMap<ui64>)) {                  \
        TStringStream result;                                                           \
        result << HumanReadableSize(args[0].Get<ui64>(), hrSize);                       \
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));       \
    }                                                                                   \
                                                                                        \
    struct T##udfName##KernelExec                                                       \
        : public TUnaryKernelExec<T##udfName##KernelExec>                               \
    {                                                                                   \
        template <typename TSink>                                                       \
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) { \
            TStringStream result;                                                       \
            result << HumanReadableSize(arg1.Get<ui64>(), hrSize);                      \
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));                 \
        }                                                                               \
    };                                                                                  \
                                                                                        \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)

#define STRING_UDF_MAP(XX)           \
    XX(Base32Encode, Base32Encode)   \
    XX(Base64Encode, Base64Encode)   \
    XX(Base64EncodeUrl, Base64EncodeUrl)   \
    XX(EscapeC, EscapeC)             \
    XX(UnescapeC, UnescapeC)         \
    XX(HexEncode, HexEncode)         \
    XX(EncodeHtml, EncodeHtmlPcdata) \
    XX(DecodeHtml, DecodeHtmlPcdata) \
    XX(CgiEscape, CGIEscapeRet)      \
    XX(CgiUnescape, CGIUnescapeRet)  \
    XX(Strip, Strip)                 \
    XX(Collapse, Collapse)

#define STRING_UNSAFE_UDF_MAP(XX)  \
    XX(Base32Decode, Base32Decode)         \
    XX(Base32StrictDecode, Base32StrictDecode)         \
    XX(Base64Decode, Base64Decode) \
    XX(Base64StrictDecode, Base64StrictDecode)         \
    XX(HexDecode, HexDecode)

// NOTE: The functions below are marked as deprecated, so block implementation
// is not required for them. Hence, STROKA_CASE_UDF provides only the scalar
// one at the moment.
#define STROKA_CASE_UDF_MAP(XX) \
    XX(ToLower, ToLower)        \
    XX(ToUpper, ToUpper)        \
    XX(ToTitle, ToTitle)

#define STROKA_ASCII_CASE_UDF_MAP(XX) \
    XX(AsciiToLower, to_lower)        \
    XX(AsciiToUpper, to_upper)        \
    XX(AsciiToTitle, to_title)

// NOTE: The functions below are marked as deprecated, so block implementation
// is not required for them. Hence, STROKA_FIND_UDF provides only the scalar
// one at the moment.
#define STROKA_FIND_UDF_MAP(XX) \
    XX(StartsWith, StartsWith)  \
    XX(EndsWith, EndsWith)      \
    XX(HasPrefix, StartsWith)   \
    XX(HasSuffix, EndsWith)

// NOTE: The functions below are marked as deprecated, so block implementation
// is not required for them. Hence, STRING_TWO_ARGS_UDF provides only the
// scalar one at the moment.
#define STRING_TWO_ARGS_UDF_MAP(XX)                    \
    XX(StartsWithIgnoreCase, AsciiHasPrefixIgnoreCase) \
    XX(EndsWithIgnoreCase, AsciiHasSuffixIgnoreCase)   \
    XX(HasPrefixIgnoreCase, AsciiHasPrefixIgnoreCase)  \
    XX(HasSuffixIgnoreCase, AsciiHasSuffixIgnoreCase)

// NOTE: The functions below are marked as deprecated, so block implementation
// is not required for them. Hence, STROKA_UDF provides only the scalar one at
// the moment.
#define STROKA_UDF_MAP(XX) \
    XX(Reverse, ReverseInPlace)

#define IS_ASCII_UDF_MAP(XX) \
    XX(IsAscii)              \
    XX(IsAsciiSpace)         \
    XX(IsAsciiUpper)         \
    XX(IsAsciiLower)         \
    XX(IsAsciiDigit)         \
    XX(IsAsciiAlpha)         \
    XX(IsAsciiAlnum)         \
    XX(IsAsciiHex)

#define STRING_STREAM_PAD_FORMATTER_UDF_MAP(XX) \
    XX(LeftPad)                                 \
    XX(RightPad)

#define STRING_STREAM_NUM_FORMATTER_UDF_MAP(XX) \
    XX(Hex, ui64)                               \
    XX(SHex, i64)                               \
    XX(Bin, ui64)                               \
    XX(SBin, i64)

#define STRING_STREAM_TEXT_FORMATTER_UDF_MAP(XX) \
    XX(HexText)                                  \
    XX(BinText)

#define STRING_STREAM_HRSZ_FORMATTER_UDF_MAP(XX) \
    XX(HumanReadableQuantity, SF_QUANTITY)       \
    XX(HumanReadableBytes, SF_BYTES)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TCollapseText, char*(TAutoMap<char*>, ui64)) {
        TString input(args[0].AsStringRef());
        ui64 maxLength = args[1].Get<ui64>();
        CollapseText(input, maxLength);
        return valueBuilder->NewString(input);
    }

    struct TCollapseTextKernelExec
        : public TBinaryKernelExec<TCollapseTextKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            TString input(arg1.AsStringRef());
            ui64 maxLength = arg2.Get<ui64>();
            CollapseText(input, maxLength);
            return sink(TBlockItem(input));
        }
    };

    END_SIMPLE_ARROW_UDF(TCollapseText, TCollapseTextKernelExec::Do);


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TContains, bool(TOptional<char*>, char*)) {
        Y_UNUSED(valueBuilder);
        if (!args[0])
            return TUnboxedValuePod(false);

        const TString haystack(args[0].AsStringRef());
        const TString needle(args[1].AsStringRef());
        return TUnboxedValuePod(haystack.Contains(needle));
    }

    struct TContainsKernelExec : public TBinaryKernelExec<TContainsKernelExec> {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            if (!arg1)
                return sink(TBlockItem(false));

            const TString haystack(arg1.AsStringRef());
            const TString needle(arg2.AsStringRef());
            sink(TBlockItem(haystack.Contains(needle)));
        }
    };

    END_SIMPLE_ARROW_UDF(TContains, TContainsKernelExec::Do);


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TReplaceAll, char*(TAutoMap<char*>, char*, char*)) {
        if (TString result(args[0].AsStringRef()); SubstGlobal(result, args[1].AsStringRef(), args[2].AsStringRef()))
            return valueBuilder->NewString(result);
        else
            return args[0];
    }

    struct TReplaceAllKernelExec
        : public TGenericKernelExec<TReplaceAllKernelExec, 3>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            TString result(args.GetElement(0).AsStringRef());
            const TStringBuf what(args.GetElement(1).AsStringRef());
            const TStringBuf with(args.GetElement(2).AsStringRef());
            if (SubstGlobal(result, what, with)) {
                return sink(TBlockItem(result));
            } else {
                return sink(args.GetElement(0));
            }
        }
    };

    END_SIMPLE_ARROW_UDF(TReplaceAll, TReplaceAllKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TReplaceFirst, char*(TAutoMap<char*>, char*, char*)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.find(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    struct TReplaceFirstKernelExec
        : public TGenericKernelExec<TReplaceFirstKernelExec, 3>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            std::string result(args.GetElement(0).AsStringRef());
            const std::string_view what(args.GetElement(1).AsStringRef());
            const std::string_view with(args.GetElement(2).AsStringRef());
            if (const auto index = result.find(what); index != std::string::npos) {
                result.replace(index, what.size(), with);
                return sink(TBlockItem(result));
            }
            return sink(args.GetElement(0));
        }
    };

    END_SIMPLE_ARROW_UDF(TReplaceFirst, TReplaceFirstKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TReplaceLast, char*(TAutoMap<char*>, char*, char*)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.rfind(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    struct TReplaceLastKernelExec
        : public TGenericKernelExec<TReplaceLastKernelExec, 3>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem args, const TSink& sink) {
            std::string result(args.GetElement(0).AsStringRef());
            const std::string_view what(args.GetElement(1).AsStringRef());
            const std::string_view with(args.GetElement(2).AsStringRef());
            if (const auto index = result.rfind(what); index != std::string::npos) {
                result.replace(index, what.size(), with);
                return sink(TBlockItem(result));
            }
            return sink(args.GetElement(0));
        }
    };

    END_SIMPLE_ARROW_UDF(TReplaceLast, TReplaceLastKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TRemoveAll, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        std::array<bool, 256> chars{};
        for (const ui8 c : remove) {
            chars[c] = true;
        }
        size_t tpos = 0;
        for (const ui8 c : input) {
            if (!chars[c]) {
                input[tpos++] = c;
            }
        }
        if (tpos != input.size()) {
            input.resize(tpos);
            return valueBuilder->NewString(input);
        }
        return args[0];
    }

    struct TRemoveAllKernelExec
        : public TBinaryKernelExec<TRemoveAllKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            std::string input(arg1.AsStringRef());
            const std::string_view remove(arg2.AsStringRef());
            std::array<bool, 256> chars{};
            for (const ui8 c : remove) {
                chars[c] = true;
            }
            size_t tpos = 0;
            for (const ui8 c : input) {
                if (!chars[c]) {
                    input[tpos++] = c;
                }
            }
            if (tpos != input.size()) {
                input.resize(tpos);
                return sink(TBlockItem(input));
            }
            sink(arg1);
        }
    };

    END_SIMPLE_ARROW_UDF(TRemoveAll, TRemoveAllKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TRemoveFirst, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        std::array<bool, 256> chars{};
        for (const ui8 c : remove) {
            chars[c] = true;
        }
        for (auto it = input.cbegin(); it != input.cend(); ++it) {
            if (chars[static_cast<ui8>(*it)]) {
                input.erase(it);
                return valueBuilder->NewString(input);
            }
        }
        return args[0];
    }

    struct TRemoveFirstKernelExec
        : public TBinaryKernelExec<TRemoveFirstKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            std::string input(arg1.AsStringRef());
            const std::string_view remove(arg2.AsStringRef());
            std::array<bool, 256> chars{};
            for (const ui8 c : remove) {
                chars[c] = true;
            }
            for (auto it = input.cbegin(); it != input.cend(); ++it) {
                if (chars[static_cast<ui8>(*it)]) {
                    input.erase(it);
                    return sink(TBlockItem(input));
                }
            }
            sink(arg1);
        }
    };

    END_SIMPLE_ARROW_UDF(TRemoveFirst, TRemoveFirstKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TRemoveLast, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        std::array<bool, 256> chars{};
        for (const ui8 c : remove) {
            chars[c] = true;
        }
        for (auto it = input.crbegin(); it != input.crend(); ++it) {
            if (chars[static_cast<ui8>(*it)]) {
                input.erase(input.crend() - it - 1, 1);
                return valueBuilder->NewString(input);
            }
        }
        return args[0];
    }

    struct TRemoveLastKernelExec
        : public TBinaryKernelExec<TRemoveLastKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            std::string input(arg1.AsStringRef());
            const std::string_view remove(arg2.AsStringRef());
            std::array<bool, 256> chars{};
            for (const ui8 c : remove) {
                chars[c] = true;
            }
            for (auto it = input.crbegin(); it != input.crend(); ++it) {
                if (chars[static_cast<ui8>(*it)]) {
                    input.erase(input.crend() - it - 1, 1);
                    return sink(TBlockItem(input));
                }
            }
            sink(arg1);
        }
    };

    END_SIMPLE_ARROW_UDF(TRemoveLast, TRemoveLastKernelExec::Do)


    // NOTE: String::Find is marked as deprecated, so block implementation is
    // not required for them. Hence, only the scalar one is provided.
    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TFind, i64(TAutoMap<char*>, char*, TOptional<ui64>), 1) {
        Y_UNUSED(valueBuilder);
        const TString haystack(args[0].AsStringRef());
        const TString needle(args[1].AsStringRef());
        const ui64 pos = args[2].GetOrDefault<ui64>(0);
        return TUnboxedValuePod(haystack.find(needle, pos));
    }

    // NOTE: String::ReverseFind is marked as deprecated, so block
    // implementation is not required for them. Hence, only the scalar one is
    // provided.
    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TReverseFind, i64(TAutoMap<char*>, char*, TOptional<ui64>), 1) {
        Y_UNUSED(valueBuilder);
        const TString haystack(args[0].AsStringRef());
        const TString needle(args[1].AsStringRef());
        const ui64 pos = args[2].GetOrDefault<ui64>(TString::npos);
        return TUnboxedValuePod(haystack.rfind(needle, pos));
    }

    // NOTE: String::Substring is marked as deprecated, so block implementation
    // is not required for them. Hence, only the scalar one is provided.
    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TSubstring, char*(TAutoMap<char*>, TOptional<ui64>, TOptional<ui64>), 1) {
        const TString input(args[0].AsStringRef());
        const ui64 from = args[1].GetOrDefault<ui64>(0);
        const ui64 count = args[2].GetOrDefault<ui64>(TString::npos);
        return valueBuilder->NewString(input.substr(from, count));
    }

    using TTmpVector = TSmallVec<TUnboxedValue, TUnboxedValue::TAllocator>;

    template <typename TIt>
    static void SplitToListImpl(
            const IValueBuilder* valueBuilder,
            const TUnboxedValue& input,
            const std::string_view::const_iterator from,
            const TIt& it,
            TTmpVector& result) {
        for (const auto& elem : it) {
            result.emplace_back(valueBuilder->SubString(input, std::distance(from, elem.TokenStart()), std::distance(elem.TokenStart(), elem.TokenDelim())));
        }
    }
    template <typename TIt>
    static void SplitToListImpl(
            const IValueBuilder* valueBuilder,
            const TUnboxedValue& input,
            const std::string_view::const_iterator from,
            TIt& it,
            bool skipEmpty,
            TTmpVector& result) {
        if (skipEmpty) {
            SplitToListImpl(valueBuilder, input, from, it.SkipEmpty(), result);
        } else {
            SplitToListImpl(valueBuilder, input, from, it, result);
        }
    }

    constexpr char delimeterStringName[] = "DelimeterString";
    constexpr char skipEmptyName[] = "SkipEmpty";
    constexpr char limitName[] = "Limit";
    using TDelimeterStringArg = TNamedArg<bool, delimeterStringName>;
    using TSkipEmptyArg = TNamedArg<bool, skipEmptyName>;
    using TLimitArg = TNamedArg<ui64, limitName>;


    SIMPLE_STRICT_UDF_WITH_OPTIONAL_ARGS(TSplitToList, TListType<char*>(
                            TOptional<char*>,
                            char*,
                            TDelimeterStringArg,
                            TSkipEmptyArg,
                            TLimitArg
                       ),
                       3) {
        TTmpVector result;
        if (args[0]) {
            const std::string_view input(args[0].AsStringRef());
            const std::string_view delimeter(args[1].AsStringRef());
            const bool delimiterString = args[2].GetOrDefault<bool>(true);
            const bool skipEmpty = args[3].GetOrDefault<bool>(false);
            const auto limit = args[4].GetOrDefault<ui64>(0);
            if (delimiterString) {
                if (limit) {
                    auto it = StringSplitter(input).SplitByString(delimeter).Limit(limit + 1);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                } else {
                    auto it = StringSplitter(input).SplitByString(delimeter);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                }
            } else {
                if (limit) {
                    auto it = StringSplitter(input).SplitBySet(TString(delimeter).c_str()).Limit(limit + 1);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                } else {
                    auto it = StringSplitter(input).SplitBySet(TString(delimeter).c_str());
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                }
            }
        }
        return valueBuilder->NewList(result.data(), result.size());
    }

    SIMPLE_STRICT_UDF(TJoinFromList, char*(TAutoMap<TListType<TOptional<char*>>>, char*)) {
        auto input = args[0].GetListIterator();
        const TString delimeter(args[1].AsStringRef());
        TVector<TString> items;

        for (TUnboxedValue current; input.Next(current);) {
            if (current) {
                TString item(current.AsStringRef());
                items.push_back(std::move(item));
            }
        }

        return valueBuilder->NewString(JoinSeq(delimeter, items));
    }

    BEGIN_SIMPLE_STRICT_ARROW_UDF(TLevensteinDistance, ui64(TAutoMap<char*>, TAutoMap<char*>)) {
        Y_UNUSED(valueBuilder);
        const TStringBuf left(args[0].AsStringRef());
        const TStringBuf right(args[1].AsStringRef());
        const ui64 result = NLevenshtein::Distance(left, right);
        return TUnboxedValuePod(result);
    }

    struct TLevensteinDistanceKernelExec : public TBinaryKernelExec<TLevensteinDistanceKernelExec> {
    template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            const std::string_view left(arg1.AsStringRef());
            const std::string_view right(arg2.AsStringRef());
            const ui64 result = NLevenshtein::Distance(left, right);
            sink(TBlockItem(result));
        }
    };

    END_SIMPLE_ARROW_UDF(TLevensteinDistance, TLevensteinDistanceKernelExec::Do);



    BEGIN_SIMPLE_STRICT_ARROW_UDF(THumanReadableDuration, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << HumanReadable(TDuration::MicroSeconds(args[0].Get<ui64>()));
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    struct THumanReadableDurationKernelExec
        : public TUnaryKernelExec<THumanReadableDurationKernelExec>
    {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, const TSink& sink) {
            TStringStream result;
            result << HumanReadable(TDuration::MicroSeconds(arg1.Get<ui64>()));
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));
        }
    };

    END_SIMPLE_ARROW_UDF(THumanReadableDuration, THumanReadableDurationKernelExec::Do)


    BEGIN_SIMPLE_STRICT_ARROW_UDF(TPrec, char*(TAutoMap<double>, ui64)) {
        TStringStream result;
        result << Prec(args[0].Get<double>(), args[1].Get<ui64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    struct TPrecKernelExec : public TBinaryKernelExec<TPrecKernelExec> {
        template <typename TSink>
        static void Process(const IValueBuilder*, TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            TStringStream result;
            result << Prec(arg1.Get<double>(), arg2.Get<ui64>());
            sink(TBlockItem(TStringRef(result.Data(), result.Size())));
        }
    };

    END_SIMPLE_ARROW_UDF(TPrec, TPrecKernelExec::Do)


    SIMPLE_STRICT_UDF(TToByteList, TListType<ui8>(char*)) {
        const TStringBuf input(args[0].AsStringRef());
        TUnboxedValue* items = nullptr;
        TUnboxedValue result = valueBuilder->NewArray(input.size(), items);
        for (const unsigned char c : input) {
            *items++ = TUnboxedValuePod(c);
        }
        return result;
    }

    SIMPLE_STRICT_UDF(TFromByteList, char*(TListType<ui8>)) {
        auto input = args[0];

        if (auto elems = input.GetElements()) {
            const auto elemCount = input.GetListLength();
            TUnboxedValue result = valueBuilder->NewStringNotFilled(input.GetListLength());
            auto bufferPtr = result.AsStringRef().Data();
            for (ui64 i = 0; i != elemCount; ++i) {
                *(bufferPtr++) = elems[i].Get<ui8>();
            }
            return result;
        }

        std::vector<char, NKikimr::NUdf::TStdAllocatorForUdf<char>> buffer;
        buffer.reserve(TUnboxedValuePod::InternalBufferSize);

        const auto& iter = input.GetListIterator();
        for (NUdf::TUnboxedValue item; iter.Next(item); ) {
            buffer.push_back(item.Get<ui8>());
        }

        return valueBuilder->NewString(TStringRef(buffer.data(), buffer.size()));
    }

#define STRING_REGISTER_UDF(udfName, ...) T##udfName,

    STRING_UDF_MAP(STRING_UDF)
    STRING_UNSAFE_UDF_MAP(STRING_UNSAFE_UDF)
    STROKA_UDF_MAP(STROKA_UDF)
    STROKA_CASE_UDF_MAP(STROKA_CASE_UDF)
    STROKA_ASCII_CASE_UDF_MAP(STROKA_ASCII_CASE_UDF)
    STROKA_FIND_UDF_MAP(STROKA_FIND_UDF)
    STRING_TWO_ARGS_UDF_MAP(STRING_TWO_ARGS_UDF)
    IS_ASCII_UDF_MAP(IS_ASCII_UDF)

    static constexpr ui64 padLim = 1000000;
    STRING_STREAM_PAD_FORMATTER_UDF_MAP(STRING_STREAM_PAD_FORMATTER_UDF)
    STRING_STREAM_NUM_FORMATTER_UDF_MAP(STRING_STREAM_NUM_FORMATTER_UDF)
    STRING_STREAM_TEXT_FORMATTER_UDF_MAP(STRING_STREAM_TEXT_FORMATTER_UDF)
    STRING_STREAM_HRSZ_FORMATTER_UDF_MAP(STRING_STREAM_HRSZ_FORMATTER_UDF)

    SIMPLE_MODULE(TStringModule,
        STRING_UDF_MAP(STRING_REGISTER_UDF)
        STRING_UNSAFE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_CASE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_ASCII_CASE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_FIND_UDF_MAP(STRING_REGISTER_UDF)
        STRING_TWO_ARGS_UDF_MAP(STRING_REGISTER_UDF)
        IS_ASCII_UDF_MAP(STRING_REGISTER_UDF)
        STRING_STREAM_PAD_FORMATTER_UDF_MAP(STRING_REGISTER_UDF)
        STRING_STREAM_NUM_FORMATTER_UDF_MAP(STRING_REGISTER_UDF)
        STRING_STREAM_TEXT_FORMATTER_UDF_MAP(STRING_REGISTER_UDF)
        STRING_STREAM_HRSZ_FORMATTER_UDF_MAP(STRING_REGISTER_UDF)
        TCollapseText,
        TReplaceAll,
        TReplaceFirst,
        TReplaceLast,
        TRemoveAll,
        TRemoveFirst,
        TRemoveLast,
        TContains,
        TFind,
        TReverseFind,
        TSubstring,
        TSplitToList,
        TJoinFromList,
        TLevensteinDistance,
        THumanReadableDuration,
        TPrec,
        TToByteList,
        TFromByteList)
}

REGISTER_MODULES(TStringModule)
