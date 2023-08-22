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

#define STRING_UDF(udfName, function)                       \
    SIMPLE_STRICT_UDF(T##udfName, char*(TAutoMap<char*>)) { \
        const TString input(args[0].AsStringRef());         \
        const auto& result = function(input);               \
        return valueBuilder->NewString(result);             \
    }

// 'unsafe' udf is actually strict - it returns null on any exception
#define STRING_UNSAFE_UDF(udfName, function)                           \
    SIMPLE_STRICT_UDF(T##udfName, TOptional<char*>(TOptional<char*>)) {\
        EMPTY_RESULT_ON_EMPTY_ARG(0);                                  \
        const TString input(args[0].AsStringRef());                    \
        try {                                                          \
            const auto& result = function(input);                      \
            return valueBuilder->NewString(result);                    \
        } catch (yexception&) {                                        \
            return TUnboxedValue();                                    \
        }                                                              \
    }

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

#define STROKA_ASCII_CASE_UDF(udfName, function)                 \
    SIMPLE_STRICT_UDF(T##udfName, char*(TAutoMap<char*>)) {      \
        TString input(args[0].AsStringRef());                    \
        if (input.function()) {                                  \
            return valueBuilder->NewString(input);               \
        } else {                                                 \
            return args[0];                                      \
        }                                                        \
    }

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

#define IS_ASCII_UDF(function)                                    \
    SIMPLE_STRICT_UDF(T##function, bool(TOptional<char*>)) {      \
        Y_UNUSED(valueBuilder);                                   \
        if (args[0]) {                                            \
            const TStringBuf input(args[0].AsStringRef());        \
            bool result = true;                                   \
            for (auto c : input) {                                \
                if (!function(c)) {                               \
                    result = false;                               \
                    break;                                        \
                }                                                 \
            }                                                     \
            return TUnboxedValuePod(result);                      \
        } else {                                                  \
            return TUnboxedValuePod(false);                       \
        }                                                         \
    }

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

#define STROKA_CASE_UDF_MAP(XX) \
    XX(ToLower, ToLower)        \
    XX(ToUpper, ToUpper)        \
    XX(ToTitle, ToTitle)

#define STROKA_ASCII_CASE_UDF_MAP(XX) \
    XX(AsciiToLower, to_lower)        \
    XX(AsciiToUpper, to_upper)        \
    XX(AsciiToTitle, to_title)

#define STROKA_FIND_UDF_MAP(XX) \
    XX(Contains, Contains)      \
    XX(StartsWith, StartsWith)  \
    XX(EndsWith, EndsWith)      \
    XX(HasPrefix, StartsWith)   \
    XX(HasSuffix, EndsWith)

#define STRING_TWO_ARGS_UDF_MAP(XX)                    \
    XX(StartsWithIgnoreCase, AsciiHasPrefixIgnoreCase) \
    XX(EndsWithIgnoreCase, AsciiHasSuffixIgnoreCase)   \
    XX(HasPrefixIgnoreCase, AsciiHasPrefixIgnoreCase)  \
    XX(HasSuffixIgnoreCase, AsciiHasSuffixIgnoreCase)

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

    SIMPLE_STRICT_UDF(TCollapseText, char*(TAutoMap<char*>, ui64)) {
        TString input(args[0].AsStringRef());
        ui64 maxLength = args[1].Get<ui64>();
        CollapseText(input, maxLength);
        return valueBuilder->NewString(input);
    }

    SIMPLE_STRICT_UDF(TReplaceAll, char*(TAutoMap<char*>, char*, char*)) {
        if (TString result(args[0].AsStringRef()); SubstGlobal(result, args[1].AsStringRef(), args[2].AsStringRef()))
            return valueBuilder->NewString(result);
        else
            return args[0];
    }

    SIMPLE_STRICT_UDF(TReplaceFirst, char*(TAutoMap<char*>, char*, char*)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.find(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    SIMPLE_STRICT_UDF(TReplaceLast, char*(TAutoMap<char*>, char*, char*)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.rfind(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    SIMPLE_STRICT_UDF(TRemoveAll, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        const std::unordered_set<char> chars(remove.cbegin(), remove.cend());
        size_t tpos = 0;
        for (const char c : input) {
            if (!chars.contains(c)) {
                input[tpos++] = c;
            }
        }
        if (tpos != input.size()) {
            input.resize(tpos);
            return valueBuilder->NewString(input);
        }
        return args[0];
    }

    SIMPLE_STRICT_UDF(TRemoveFirst, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        std::unordered_set<char> chars(remove.cbegin(), remove.cend());
        for (auto it = input.cbegin(); it != input.cend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(it);
                return valueBuilder->NewString(input);
            }
        }
        return args[0];
    }

    SIMPLE_STRICT_UDF(TRemoveLast, char*(TAutoMap<char*>, char*)) {
        std::string input(args[0].AsStringRef());
        const std::string_view remove(args[1].AsStringRef());
        std::unordered_set<char> chars(remove.cbegin(), remove.cend());
        for (auto it = input.crbegin(); it != input.crend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(input.crend() - it - 1, 1);
                return valueBuilder->NewString(input);
            }
        }
        return args[0];
    }

    SIMPLE_STRICT_UDF_OPTIONS(TFind, i64(TAutoMap<char*>, char*, TOptional<ui64>),
                       builder.OptionalArgs(1)) {
        Y_UNUSED(valueBuilder);
        const TString haystack(args[0].AsStringRef());
        const TString needle(args[1].AsStringRef());
        const ui64 pos = args[2].GetOrDefault<ui64>(0);
        return TUnboxedValuePod(haystack.find(needle, pos));
    }

    SIMPLE_STRICT_UDF_OPTIONS(TReverseFind, i64(TAutoMap<char*>, char*, TOptional<ui64>),
                              builder.OptionalArgs(1)) {
        Y_UNUSED(valueBuilder);
        const TString haystack(args[0].AsStringRef());
        const TString needle(args[1].AsStringRef());
        const ui64 pos = args[2].GetOrDefault<ui64>(TString::npos);
        return TUnboxedValuePod(haystack.rfind(needle, pos));
    }

    SIMPLE_STRICT_UDF_OPTIONS(TSubstring, char*(TAutoMap<char*>, TOptional<ui64>, TOptional<ui64>),
                              builder.OptionalArgs(1)) {
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


    SIMPLE_STRICT_UDF_OPTIONS(TSplitToList, TListType<char*>(
                            TOptional<char*>,
                            char*,
                            TDelimeterStringArg,
                            TSkipEmptyArg,
                            TLimitArg
                       ),
                       builder.OptionalArgs(3)) {
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
        static void Process(TBlockItem arg1, TBlockItem arg2, const TSink& sink) {
            const std::string_view left(arg1.AsStringRef());
            const std::string_view right(arg2.AsStringRef());
            const ui64 result = NLevenshtein::Distance(left, right);
            sink(TBlockItem(result));
        }
    };

    END_SIMPLE_ARROW_UDF(TLevensteinDistance, TLevensteinDistanceKernelExec::Do);

    static constexpr ui64 padLim = 1000000;

    SIMPLE_UDF_OPTIONS(TRightPad, char*(TAutoMap<char*>, ui64, TOptional<char*>), builder.OptionalArgs(1)) {
        TStringStream result;
        const TStringBuf input(args[0].AsStringRef());
        char paddingSymbol = ' ';
        if (args[2]) {
            if (args[2].AsStringRef().Size() != 1) {
                ythrow yexception() << "Not 1 symbol in paddingSymbol";
            }
            paddingSymbol = TString(args[2].AsStringRef())[0];
        }
        const ui64 padLen = args[1].Get<ui64>();
        if (padLen > padLim) {
             ythrow yexception() << "Padding length (" << padLen << ") exceeds maximum: " << padLim;
        }
        result << RightPad(input, padLen, paddingSymbol);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_UDF_OPTIONS(TLeftPad, char*(TAutoMap<char*>, ui64, TOptional<char*>), builder.OptionalArgs(1)) {
        TStringStream result;
        const TStringBuf input(args[0].AsStringRef());
        char paddingSymbol = ' ';
        if (args[2]) {
            if (args[2].AsStringRef().Size() != 1) {
                ythrow yexception() << "Not 1 symbol in paddingSymbol";
            }
            paddingSymbol = TString(args[2].AsStringRef())[0];
        }
        const ui64 padLen = args[1].Get<ui64>();
        if (padLen > padLim) {
             ythrow yexception() << "Padding length (" << padLen << ") exceeds maximum: " << padLim;
        }
        result << LeftPad(input, padLen, paddingSymbol);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(THex, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << Hex(args[0].Get<ui64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(TSHex, char*(TAutoMap<i64>)) {
        TStringStream result;
        result << SHex(args[0].Get<i64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(TBin, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << Bin(args[0].Get<ui64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(TSBin, char*(TAutoMap<i64>)) {
        TStringStream result;
        result << SBin(args[0].Get<i64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(THexText, char*(TAutoMap<char*>)) {
        TStringStream result;
        const TStringBuf input(args[0].AsStringRef());
        result << HexText(input);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(TBinText, char*(TAutoMap<char*>)) {
        TStringStream result;
        const TStringBuf input(args[0].AsStringRef());
        result << BinText(input);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(THumanReadableDuration, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << HumanReadable(TDuration::MicroSeconds(args[0].Get<ui64>()));
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(THumanReadableQuantity, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << HumanReadableSize(args[0].Get<ui64>(), SF_QUANTITY);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(THumanReadableBytes, char*(TAutoMap<ui64>)) {
        TStringStream result;
        result << HumanReadableSize(args[0].Get<ui64>(), SF_BYTES);
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

    SIMPLE_STRICT_UDF(TPrec, char*(TAutoMap<double>, ui64)) {
        TStringStream result;
        result << Prec(args[0].Get<double>(), args[1].Get<ui64>());
        return valueBuilder->NewString(TStringRef(result.Data(), result.Size()));
    }

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

    SIMPLE_MODULE(TStringModule,
        STRING_UDF_MAP(STRING_REGISTER_UDF)
        STRING_UNSAFE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_CASE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_ASCII_CASE_UDF_MAP(STRING_REGISTER_UDF)
        STROKA_FIND_UDF_MAP(STRING_REGISTER_UDF)
        STRING_TWO_ARGS_UDF_MAP(STRING_REGISTER_UDF)
        IS_ASCII_UDF_MAP(STRING_REGISTER_UDF)
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
        TRightPad,
        TLeftPad,
        THex,
        TSHex,
        TBin,
        TSBin,
        THexText,
        TBinText,
        THumanReadableDuration,
        THumanReadableQuantity,
        THumanReadableBytes,
        TPrec,
        TToByteList,
        TFromByteList)
}

REGISTER_MODULES(TStringModule)
