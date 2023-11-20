#pragma once

#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/utils/utf8.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>
#include <library/cpp/unicode/normalization/normalization.h>
#include <library/cpp/unicode/set/unicode_set.h>

#include <library/cpp/deprecated/split/split_iterator.h>
#include <util/string/join.h>
#include <util/string/reverse.h>
#include <util/string/split.h>
#include <util/string/subst.h>
#include <util/charset/wide.h>
#include <util/charset/utf8.h>
#include <util/string/strip.h>
#include <util/string/ascii.h>
#include <util/charset/unidata.h>

using namespace NYql;
using namespace NUdf;
using namespace NUnicode;

namespace {

    template <class It>
    struct TIsUnicodeSpaceAdapter {
        bool operator()(const It& it) const noexcept {
            return IsSpace(*it);
        }
    };

    template <class It>
    TIsUnicodeSpaceAdapter<It> IsUnicodeSpaceAdapter(It) {
        return {};
    }

#define NORMALIZE_UDF_MAP(XX) \
    XX(Normalize, NFC)        \
    XX(NormalizeNFD, NFD)     \
    XX(NormalizeNFC, NFC)     \
    XX(NormalizeNFKD, NFKD)   \
    XX(NormalizeNFKC, NFKC)

#define IS_CATEGORY_UDF_MAP(XX) \
    XX(IsAscii, IsAscii)   \
    XX(IsSpace, IsSpace)        \
    XX(IsUpper, IsUpper)        \
    XX(IsLower, IsLower)        \
    XX(IsDigit, IsDigit)        \
    XX(IsAlpha, IsAlpha)        \
    XX(IsAlnum, IsAlnum)        \
    XX(IsHex, IsHexdigit)

#define NORMALIZE_UDF(name, mode)                                                 \
    SIMPLE_UDF(T##name, TUtf8(TAutoMap<TUtf8>)) {                                 \
        const auto& inputRef = args[0].AsStringRef();                             \
        const TUtf16String& input = UTF8ToWide(inputRef.Data(), inputRef.Size()); \
        const TString& output = WideToUTF8(Normalize<mode>(input));               \
        return valueBuilder->NewString(output);                                   \
    }

#define IS_CATEGORY_UDF(udfName, function)                                                \
    SIMPLE_UDF(T##udfName, bool(TAutoMap<TUtf8>)) {                                       \
        Y_UNUSED(valueBuilder);                                                           \
        const TStringBuf input(args[0].AsStringRef());                                    \
        bool result = true;                                                               \
        wchar32 rune;                                                                     \
        const unsigned char* cur = reinterpret_cast<const unsigned char*>(input.begin()); \
        const unsigned char* last = reinterpret_cast<const unsigned char*>(input.end());  \
        while (cur != last) {                                                             \
            ReadUTF8CharAndAdvance(rune, cur, last);                                      \
            if (!function(rune)) {                                                        \
                result = false;                                                           \
                break;                                                                    \
            }                                                                             \
        }                                                                                 \
        return TUnboxedValuePod(result);                                                  \
    }

    NORMALIZE_UDF_MAP(NORMALIZE_UDF)
    IS_CATEGORY_UDF_MAP(IS_CATEGORY_UDF)

    SIMPLE_UDF(TIsUtf, bool(TOptional<char*>)) {
        Y_UNUSED(valueBuilder);
        if (args[0]) {
            return TUnboxedValuePod(IsUtf8(args[0].AsStringRef()));
        } else {
            return TUnboxedValuePod(false);
        }
    }

    SIMPLE_UDF(TGetLength, ui64(TAutoMap<TUtf8>)) {
        Y_UNUSED(valueBuilder);
        const auto& inputRef = args[0].AsStringRef();
        size_t result;
        GetNumberOfUTF8Chars(inputRef.Data(), inputRef.Size(), result);
        return TUnboxedValuePod(static_cast<ui64>(result));
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TToUint64, ui64(TAutoMap<TUtf8>, TOptional<ui16>), 1) {
        Y_UNUSED(valueBuilder);
        const TString inputStr(args[0].AsStringRef());
        const char* input = inputStr.Data();
        const int base = static_cast<int>(args[1].GetOrDefault<ui16>(0));
        char* pos = nullptr;
        unsigned long long res = std::strtoull(input, &pos, base);
        ui64 ret = static_cast<ui64>(res);
        if (!res && pos == input) {
            UdfTerminate("Input string is not a number");
        } else if ((res == ULLONG_MAX && errno == ERANGE) || ret != res) {
            UdfTerminate("Converted value falls out of Uint64 range");
        } else if (*pos) {
            UdfTerminate("Input string contains junk after the number");
        }
        return TUnboxedValuePod(ret);
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TTryToUint64, TOptional<ui64>(TAutoMap<TUtf8>, TOptional<ui16>), 1) {
        Y_UNUSED(valueBuilder);
        const TString inputStr(args[0].AsStringRef());
        const char* input = inputStr.Data();
        const int base = static_cast<int>(args[1].GetOrDefault<ui16>(0));
        char* pos = nullptr;
        unsigned long long res = std::strtoull(input, &pos, base);
        ui64 ret = static_cast<ui64>(res);
        if (!res && pos == input) {
            return TUnboxedValuePod();
        }
        if ((res == ULLONG_MAX && errno == ERANGE) || ret != res) {
            return TUnboxedValuePod();
        } 
        if (*pos) {
            return TUnboxedValuePod();
        }
        return TUnboxedValuePod(ret);
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TSubstring, TUtf8(TAutoMap<TUtf8>, TOptional<ui64>, TOptional<ui64>), 1) {
        const TStringBuf input(args[0].AsStringRef());
        size_t from = args[1].GetOrDefault<ui64>(0);
        size_t len = !args[2] ? TStringBuf::npos : size_t(args[2].Get<ui64>());
        return valueBuilder->NewString(SubstrUTF8(input, from, len));
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TFind, TOptional<ui64>(TAutoMap<TUtf8>, TUtf8, TOptional<ui64>), 1) {
        Y_UNUSED(valueBuilder);
        const std::string_view string(args[0].AsStringRef());
        const std::string_view needle(args[1].AsStringRef());
        std::string_view::size_type pos = 0U;

        if (auto p = args[2].GetOrDefault<ui64>(0ULL)) {
            for (auto ptr = string.data(); p && pos < string.size(); --p) {
                const auto width = WideCharSize(*ptr);
                pos += width;
                ptr += width;
            }
        }

        if (const auto find = string.find(needle, pos); std::string_view::npos != find) {
            size_t result;
            GetNumberOfUTF8Chars(string.data(), find, result);
            return TUnboxedValuePod(static_cast<ui64>(result));
        }
        return TUnboxedValuePod();
    }

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TRFind, TOptional<ui64>(TAutoMap<TUtf8>, TUtf8, TOptional<ui64>), 1) {
        Y_UNUSED(valueBuilder);
        const std::string_view string(args[0].AsStringRef());
        const std::string_view needle(args[1].AsStringRef());
        std::string_view::size_type pos = std::string_view::npos;

        if (auto p = args[2].GetOrDefault<ui64>(std::string_view::npos); std::string_view::npos != p) {
            pos = 0ULL;
            for (auto ptr = string.data(); p && pos < string.size(); --p) {
                const auto width = WideCharSize(*ptr);
                pos += width;
                ptr += width;
            }
        }

        if (const auto find = string.rfind(needle, pos); std::string_view::npos != find) {
            size_t result;
            GetNumberOfUTF8Chars(string.data(), find, result);
            return TUnboxedValuePod(static_cast<ui64>(result));
        }
        return TUnboxedValuePod();
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
            const TUtf16String::const_iterator start,
            const TIt& it,
            TTmpVector& result) {
        const std::string_view& original = input.AsStringRef();
        size_t charPos = 0U, bytePos = 0U;
        for (const auto& elem : it) {
            for (const size_t next = std::distance(start, elem.TokenStart()); charPos < next; ++charPos)
                bytePos += WideCharSize(original[bytePos]);
            const auto from = bytePos;

            for (const size_t next = charPos + std::distance(elem.TokenStart(), elem.TokenDelim()); charPos < next; ++charPos)
                bytePos += WideCharSize(original[bytePos]);
            const auto size = bytePos - from;
            result.emplace_back(valueBuilder->SubString(input, from, size));
        }
    }

    template <typename TIt, typename TStrIt>
    static void SplitToListImpl(
            const IValueBuilder* valueBuilder,
            const TUnboxedValue& input,
            const TStrIt from,
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

    SIMPLE_UDF_WITH_OPTIONAL_ARGS(TSplitToList, TListType<TUtf8>(
                            TOptional<TUtf8>,
                            TUtf8,
                            TDelimeterStringArg,
                            TSkipEmptyArg,
                            TLimitArg
                       ),
                       3) {
        TTmpVector result;
        if (args[0]) {
            const bool delimiterString = args[2].GetOrDefault<bool>(true);
            const bool skipEmpty = args[3].GetOrDefault<bool>(false);
            const auto limit = args[4].GetOrDefault<ui64>(0);
            if (delimiterString) {
                const std::string_view input(args[0].AsStringRef());
                const std::string_view delimeter(args[1].AsStringRef());
                if (limit) {
                    auto it = StringSplitter(input).SplitByString(delimeter).Limit(limit + 1);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                } else {
                    auto it = StringSplitter(input).SplitByString(delimeter);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                }
            } else {
                const auto& input = UTF8ToWide(args[0].AsStringRef());
                const auto& delimeter = UTF8ToWide(args[1].AsStringRef());
                if (limit) {
                    auto it = StringSplitter(input).SplitBySet(delimeter.c_str()).Limit(limit + 1);
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                } else {
                    auto it = StringSplitter(input).SplitBySet(delimeter.c_str());
                    SplitToListImpl(valueBuilder, args[0], input.cbegin(), it, skipEmpty, result);
                }
            }
        }
        return valueBuilder->NewList(result.data(), result.size());
    }

    SIMPLE_UDF(TJoinFromList, TUtf8(TAutoMap<TListType<TOptional<TUtf8>>>, TUtf8)) {
        const auto input = args[0].GetListIterator();
        const std::string_view delimeter(args[1].AsStringRef());
        std::vector<TString> items;

        for (TUnboxedValue current; input.Next(current);) {
            if (current) {
                items.emplace_back(current.AsStringRef());
            }
        }

        return valueBuilder->NewString(JoinSeq(delimeter, items));
    }

    SIMPLE_UDF(TLevensteinDistance, ui64(TAutoMap<TUtf8>, TAutoMap<TUtf8>)) {
        Y_UNUSED(valueBuilder);
        const TStringBuf left(args[0].AsStringRef());
        const TStringBuf right(args[1].AsStringRef());
        const TUtf16String& leftWide = UTF8ToWide(left);
        const TUtf16String& rightWide = UTF8ToWide(right);
        const ui64 result = NLevenshtein::Distance(leftWide, rightWide);
        return TUnboxedValuePod(result);
    }

    SIMPLE_UDF(TReplaceAll, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8)) {
        if (TString result(args[0].AsStringRef()); SubstGlobal(result, args[1].AsStringRef(), args[2].AsStringRef()))
            return valueBuilder->NewString(result);
        else
            return args[0];
    }

    SIMPLE_UDF(TReplaceFirst, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.find(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    SIMPLE_UDF(TReplaceLast, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8)) {
        std::string result(args[0].AsStringRef());
        const std::string_view what(args[1].AsStringRef());
        if (const auto index = result.rfind(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(args[2].AsStringRef()));
            return valueBuilder->NewString(result);
        }
        return args[0];
    }

    SIMPLE_UDF(TRemoveAll, TUtf8(TAutoMap<TUtf8>, TUtf8)) {
        TUtf32String input = UTF8ToUTF32<true>(args[0].AsStringRef());
        const TUtf32String remove = UTF8ToUTF32<true>(args[1].AsStringRef());
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        size_t tpos = 0;
        for (const wchar32 c : input) {
            if (!chars.contains(c)) {
                input[tpos++] = c;
            }
        }
        if (tpos != input.size()) {
            input.resize(tpos);
            return valueBuilder->NewString(WideToUTF8(input));
        }
        return args[0];
    }

    SIMPLE_UDF(TRemoveFirst, TUtf8(TAutoMap<TUtf8>, TUtf8)) {
        TUtf32String input = UTF8ToUTF32<true>(args[0].AsStringRef());
        const TUtf32String remove =  UTF8ToUTF32<true>(args[1].AsStringRef());
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        for (auto it = input.cbegin(); it != input.cend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(it);
                return valueBuilder->NewString(WideToUTF8(input));
            }
        }
        return args[0];
    }

    SIMPLE_UDF(TRemoveLast, TUtf8(TAutoMap<TUtf8>, TUtf8)) {
        TUtf32String input = UTF8ToUTF32<true>(args[0].AsStringRef());
        const TUtf32String remove = UTF8ToUTF32<true>(args[1].AsStringRef());
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        for (auto it = input.crbegin(); it != input.crend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(input.crend() - it - 1, 1);
                return valueBuilder->NewString(WideToUTF8(input));
            }
        }
        return args[0];
    }

    SIMPLE_UDF(TToCodePointList, TListType<ui32>(TAutoMap<TUtf8>)) {
        size_t codePointCount = 0;
        const auto& inputRef = args[0].AsStringRef();
        if (!GetNumberOfUTF8Chars(inputRef.Data(), inputRef.Size(), codePointCount)) {
            // should not happen but still we have to check return code
            ythrow yexception() << "Unable to count code points";
        }

        TUnboxedValue* itemsPtr = nullptr;
        auto result = valueBuilder->NewArray(codePointCount, itemsPtr);
        const unsigned char* current = reinterpret_cast<const unsigned char*>(inputRef.Data());
        const unsigned char* end = current + inputRef.Size();
        wchar32 rune = BROKEN_RUNE;
        ui32 codePointIndex = 0;
        RECODE_RESULT retcode = RECODE_OK;
        while (current < end && RECODE_OK == (retcode = ReadUTF8CharAndAdvance(rune, current, end))) {
            if (codePointIndex >= codePointCount) {
                // sanity check
                ythrow yexception() << "Too big code point index " << codePointIndex << ", expecting only " << codePointCount << " code points";
            }
            itemsPtr[codePointIndex++] = TUnboxedValuePod(static_cast<ui32>(rune));
        }

        if (retcode != RECODE_OK) {
            ythrow yexception() << "Malformed UTF-8 string";
        }

        return result;
    }

    SIMPLE_UDF(TFromCodePointList, TUtf8(TAutoMap<TListType<ui32>>)) {
        auto input = args[0];
        if (auto elems = input.GetElements()) {
            const auto elemCount = input.GetListLength();
            auto bufferSize = WideToUTF8BufferSize(elemCount);
            TTempBuf buffer(bufferSize);
            auto bufferPtr = buffer.Data();
            auto bufferEnd = buffer.Data() + bufferSize;
            for (ui64 i = 0; i != elemCount; ++i) {
                const auto& item = elems[i];
                const wchar32 rune = item.Get<ui32>();
                size_t written = 0;
                WideToUTF8(&rune, 1, bufferPtr, written);
                Y_ENSURE(written <= 4);
                bufferPtr += written;
                Y_ENSURE(bufferPtr <= bufferEnd);
            }
            return valueBuilder->NewString(TStringRef(buffer.Data(), bufferPtr - buffer.Data()));
        }

        std::vector<char, NUdf::TStdAllocatorForUdf<char>> buffer;
        buffer.reserve(TUnboxedValuePod::InternalBufferSize);

        const auto& iter = input.GetListIterator();
        char runeBuffer[4] = {};
        for (NUdf::TUnboxedValue item; iter.Next(item); ) {
            const wchar32 rune = item.Get<ui32>();
            size_t written = 0;
            WideToUTF8(&rune, 1, runeBuffer, written);
            Y_ENSURE(written <= 4);
            buffer.insert(buffer.end(), runeBuffer, runeBuffer + written);
        }

        return valueBuilder->NewString(TStringRef(buffer.data(), buffer.size()));
    }

    SIMPLE_UDF(TReverse, TUtf8(TAutoMap<TUtf8>)) {
        auto wide = UTF8ToWide(args[0].AsStringRef());
        ReverseInPlace(wide);
        return valueBuilder->NewString(WideToUTF8(wide));
    }

    SIMPLE_UDF(TToLower, TUtf8(TAutoMap<TUtf8>)) {
        if (auto wide = UTF8ToWide(args->AsStringRef()); ToLower(wide))
            return valueBuilder->NewString(WideToUTF8(wide));
        else
            return *args;
    }

    SIMPLE_UDF(TToUpper, TUtf8(TAutoMap<TUtf8>)) {
        if (auto wide = UTF8ToWide(args->AsStringRef()); ToUpper(wide))
            return valueBuilder->NewString(WideToUTF8(wide));
        else
            return *args;
    }

    SIMPLE_UDF(TToTitle, TUtf8(TAutoMap<TUtf8>)) {
        if (auto wide = UTF8ToWide(args->AsStringRef()); ToTitle(wide))
            return valueBuilder->NewString(WideToUTF8(wide));
        else
            return *args;
    }

    SIMPLE_UDF(TStrip, TUtf8(TAutoMap<TUtf8>)) {
        const TUtf32String input = UTF8ToUTF32<true>(args[0].AsStringRef());
        const auto& result = StripString(input, IsUnicodeSpaceAdapter(input.begin()));
        return valueBuilder->NewString(WideToUTF8(result));
    }

    SIMPLE_UDF(TIsUnicodeSet, bool(TAutoMap<TUtf8>, TUtf8)) {
        Y_UNUSED(valueBuilder);
        const TStringBuf input(args[0].AsStringRef());
        const TUtf16String& customCategory = UTF8ToWide(args[1].AsStringRef());
        TUnicodeSet unicodeSet;
        try {
            unicodeSet.Parse(customCategory);
        } catch (...) {
            UdfTerminate((TStringBuilder() << "Failed to parse unicode set: " << CurrentExceptionMessage()).c_str());
        }
        bool result = true;
        wchar32 rune;
        const unsigned char* cur = reinterpret_cast<const unsigned char*>(input.begin());
        const unsigned char* last = reinterpret_cast<const unsigned char*>(input.end());
        while (cur != last) {
            ReadUTF8CharAndAdvance(rune, cur, last);
            if (!unicodeSet.Has(rune)) {
                result = false;
                break;
            }
        }
        return TUnboxedValuePod(result);
    }

#define REGISTER_NORMALIZE_UDF(name, mode) T##name,
#define REGISTER_IS_CATEGORY_UDF(name, function) T##name,
#define EXPORTED_UNICODE_BASE_UDF \
    NORMALIZE_UDF_MAP(REGISTER_NORMALIZE_UDF) \
    IS_CATEGORY_UDF_MAP(REGISTER_IS_CATEGORY_UDF) \
    TIsUtf, \
    TGetLength, \
    TSubstring, \
    TFind, \
    TRFind, \
    TSplitToList, \
    TJoinFromList, \
    TLevensteinDistance, \
    TReplaceAll, \
    TReplaceFirst, \
    TReplaceLast, \
    TRemoveAll, \
    TRemoveFirst, \
    TRemoveLast, \
    TToCodePointList, \
    TFromCodePointList, \
    TReverse, \
    TToLower, \
    TToUpper, \
    TToTitle, \
    TToUint64, \
    TTryToUint64, \
    TStrip, \
    TIsUnicodeSet
}
