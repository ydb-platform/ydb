#pragma once

#include <yql/essentials/public/udf/udf_allocator.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/utils/utf8.h>
#include <yql/essentials/public/udf/arrow/udf_arrow_helpers.h>

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
#include <util/generic/scope.h>
#include <util/string/strip.h>
#include <util/string/ascii.h>
#include <util/charset/unidata.h>

using namespace NYql;
using namespace NUdf;
using namespace NUnicode;

namespace {
#define DISABLE_IMPICT_ARGUMENT_CAST \
    template <typename... Args>      \
    static auto Execute(Args&&... args) = delete;

inline constexpr bool IsAscii(wchar32 c) noexcept {
    return ::IsAscii(c);
}

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

struct TNoChangesTag {};

template <typename TDerived>
struct TScalarOperationMixin {
    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef()); }
    {
        Y_DEBUG_ABORT_UNLESS(IsUtf8(args[0].AsStringRef()));
        auto executeResult = TDerived::Execute(args[0].AsStringRef());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TMaybe<TStringRef>(TStringRef())); }
    {
        auto executeResult = TDerived::Execute(args[0] ? TMaybe<TStringRef>(args[0].AsStringRef()) : Nothing());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef(), TStringRef()); }
    {
        auto executeResult = TDerived::Execute(args[0].AsStringRef(), args[1].AsStringRef());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef(), TMaybe<ui16>()); }
    {
        auto executeResult = TDerived::Execute(args[0].AsStringRef(), args[1] ? TMaybe<ui16>(args[1].Get<ui16>()) : Nothing());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef(), TStringRef(), TStringRef()); }
    {
        auto executeResult = TDerived::Execute(args[0].AsStringRef(), args[1].AsStringRef(), args[2].AsStringRef());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef(), TStringRef(), TMaybe<ui64>()); }
    {
        auto executeResult = TDerived::Execute(args[0].AsStringRef(), args[1].AsStringRef(), args[2] ? TMaybe<ui64>(args[2].Get<ui64>()) : Nothing());
        return ProcessResult(builder, std::move(executeResult), args);
    }

    static TUnboxedValue DoExecute(const IValueBuilder* builder, const TUnboxedValuePod* args)
        requires requires { TDerived::Execute(TStringRef(), TMaybe<ui64>(), TMaybe<ui64>()); }
    {
        auto executeResult = TDerived::Execute(args[0].AsStringRef(),
                                               args[1] ? TMaybe<ui64>(args[1].Get<ui64>()) : Nothing(),
                                               args[2] ? TMaybe<ui64>(args[2].Get<ui64>()) : Nothing());
        return ProcessResult(builder, std::move(executeResult), args);
    }

private:
    static TUnboxedValue ProcessResult(const IValueBuilder* builder, const TString& newString, const TUnboxedValuePod*) {
        return builder->NewString(newString);
    }

    static TUnboxedValue ProcessResult(const IValueBuilder* builder, const TStringBuf newString, const TUnboxedValuePod*) {
        return builder->NewString(newString);
    }

    template <typename T>
    static TUnboxedValue ProcessResult(const IValueBuilder* builder, const std::variant<TNoChangesTag, T>& newValue, const TUnboxedValuePod* initialArg) {
        if (std::holds_alternative<T>(newValue)) {
            return ProcessResult(builder, std::move(std::get<T>(newValue)), initialArg);
        } else {
            return initialArg[0];
        }
    }

    template <typename T>
    static TUnboxedValue ProcessResult(const IValueBuilder* builder, const TMaybe<T>& newValue, const TUnboxedValuePod* initialArg) {
        if (newValue.Defined()) {
            return ProcessResult(builder, *newValue, initialArg);
        } else {
            return TUnboxedValuePod();
        }
    }

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    static TUnboxedValue ProcessResult(const IValueBuilder* builder, T result, const TUnboxedValuePod*) {
        Y_UNUSED(builder);
        return TUnboxedValuePod(result);
    }
};

template <typename TDerived>
struct TBlockOperationMixin {
    template <typename TSink>
    static void BlockDoExecute(const TBlockItem arg, const TSink& sink)
        requires requires { TDerived::Execute(TStringRef()); }
    {
        Y_DEBUG_ABORT_UNLESS(IsUtf8(arg.AsStringRef()));
        auto executeResult = TDerived::Execute(arg.AsStringRef());
        TBlockItem boxedValue = ProcessResult(executeResult, arg);
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem arg, const TSink& sink)
        requires requires { TDerived::Execute(TMaybe<TStringRef>(TStringRef())); }
    {
        auto executeResult = TDerived::Execute(arg ? TMaybe<TStringRef>(arg.AsStringRef()) : Nothing());
        TBlockItem boxedValue = ProcessResult(executeResult, arg);
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem arg1, const TBlockItem arg2, const TSink& sink)
        requires requires { TDerived::Execute(TStringRef(), TStringRef()); }
    {
        auto executeResult = TDerived::Execute(arg1.AsStringRef(),
                                               arg2.AsStringRef());
        TBlockItem boxedValue = ProcessResult(executeResult, arg1);
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem arg1, const TBlockItem arg2, const TSink& sink)
        requires requires { TDerived::Execute(TStringRef(), TMaybe<ui16>()); }
    {
        auto executeResult = TDerived::Execute(arg1.AsStringRef(), arg2 ? TMaybe<ui16>(arg2.Get<ui16>()) : Nothing());
        TBlockItem boxedValue = ProcessResult(executeResult, arg1);
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem args, const TSink& sink)
        requires(requires { TDerived::Execute(TStringRef(), TStringRef(), TStringRef()); })
    {
        auto executeResult = TDerived::Execute(args.GetElement(0).AsStringRef(),
                                               args.GetElement(1).AsStringRef(),
                                               args.GetElement(2).AsStringRef());
        TBlockItem boxedValue = ProcessResult(executeResult, args.GetElement(0));
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem args, const TSink& sink)
        requires(requires { TDerived::Execute(TStringRef(), TStringRef(), TMaybe<ui64>(0ULL)); })
    {
        auto executeResult = TDerived::Execute(args.GetElement(0).AsStringRef(),
                                               args.GetElement(1).AsStringRef(),
                                               (args.GetElement(2) ? TMaybe<ui64>(args.GetElement(2).Get<ui64>()) : Nothing()));
        TBlockItem boxedValue = ProcessResult(executeResult, args.GetElement(0));
        sink(boxedValue);
    }

    template <typename TSink>
    static void BlockDoExecute(const TBlockItem args, const TSink& sink)
        requires(requires { TDerived::Execute(TStringRef(), TMaybe<ui64>(0ULL), TMaybe<ui64>(0ULL)); })
    {
        auto executeResult = TDerived::Execute(args.GetElement(0).AsStringRef(),
                                               (args.GetElement(1) ? TMaybe<ui64>(args.GetElement(1).Get<ui64>()) : Nothing()),
                                               (args.GetElement(2) ? TMaybe<ui64>(args.GetElement(2).Get<ui64>()) : Nothing()));
        TBlockItem boxedValue = ProcessResult(executeResult, args.GetElement(0));
        sink(boxedValue);
    }

private:
    static TBlockItem ProcessResult(const TString& newString, const TBlockItem arg) {
        Y_UNUSED(arg);
        return TBlockItem(newString);
    }

    static TBlockItem ProcessResult(const TStringBuf newString, const TBlockItem arg) {
        Y_UNUSED(arg);
        return TBlockItem(newString);
    }

    template <typename T>
    static TBlockItem ProcessResult(const TMaybe<T>& newValue, const TBlockItem arg) {
        if (newValue.Defined()) {
            return ProcessResult(*newValue, arg);
        } else {
            return TBlockItem();
        }
    }

    template <typename T>
    static TBlockItem ProcessResult(const std::variant<TNoChangesTag, T>& newValue, const TBlockItem arg) {
        if (std::holds_alternative<T>(newValue)) {
            return ProcessResult(std::get<T>(newValue), arg);
        } else {
            return arg;
        }
    }

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    static TBlockItem ProcessResult(T result, const TBlockItem arg) {
        Y_UNUSED(arg);
        return TBlockItem(result);
    }
};

template <typename TDerived>
struct TOperationMixin: public TBlockOperationMixin<TDerived>, public TScalarOperationMixin<TDerived> {};

template <auto mode>
struct TNormalizeUTF8: public TOperationMixin<TNormalizeUTF8<mode>> {
    static TString Execute(TStringRef arg) {
        const TUtf16String& input = UTF8ToWide(arg.Data(), arg.Size());
        return WideToUTF8(Normalize<mode>(input));
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

template <bool (*Function)(wchar32)>
struct TCheckAllChars: public TOperationMixin<TCheckAllChars<Function>> {
    static bool Execute(TStringRef arg) {
        const TStringBuf input(arg);
        wchar32 rune;
        const unsigned char* cur = reinterpret_cast<const unsigned char*>(input.begin());
        const unsigned char* last = reinterpret_cast<const unsigned char*>(input.end());
        while (cur != last) {
            ReadUTF8CharAndAdvance(rune, cur, last);
            if (!static_cast<bool (*)(wchar32)>(Function)(rune)) {
                return false;
            }
        }
        return true;
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

template <bool (*Function)(TUtf16String&, size_t pos, size_t count)>
struct TStringToStringMapper: public TOperationMixin<TStringToStringMapper<Function>> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef arg) {
        if (auto wide = UTF8ToWide(arg);
            static_cast<bool (*)(TUtf16String&, size_t pos, size_t count)>(Function)(wide, 0, TUtf16String::npos)) {
            return WideToUTF8(std::move(wide));
        } else {
            return TNoChangesTag{};
        }
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TLengthGetter: public TOperationMixin<TLengthGetter> {
    static ui64 Execute(TStringRef inputRef) {
        size_t result;
        GetNumberOfUTF8Chars(inputRef.Data(), inputRef.Size(), result);
        return static_cast<ui64>(result);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TReverser: public TOperationMixin<TReverser> {
    static TString Execute(TStringRef inputRef) {
        auto wide = UTF8ToWide(inputRef);
        ReverseInPlace(wide);
        return WideToUTF8(wide);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TStripper: public TOperationMixin<TStripper> {
    static TString Execute(TStringRef inputRef) {
        const TUtf32String input = UTF8ToUTF32<true>(inputRef);
        const auto& result = StripString(input, IsUnicodeSpaceAdapter(input.begin()));
        return WideToUTF8(result);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TAllRemover: public TOperationMixin<TAllRemover> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef removeRef) {
        TUtf32String input = UTF8ToUTF32<true>(inputRef);
        const TUtf32String remove = UTF8ToUTF32<true>(removeRef);
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        size_t tpos = 0;
        for (const wchar32 c : input) {
            if (!chars.contains(c)) {
                input[tpos++] = c;
            }
        }
        if (tpos != input.size()) {
            input.resize(tpos);
            return WideToUTF8(input);
        }
        return TNoChangesTag{};
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TFirstRemover: public TOperationMixin<TFirstRemover> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef removeRef) {
        TUtf32String input = UTF8ToUTF32<true>(inputRef);
        const auto remove = UTF8ToUTF32<true>(removeRef);
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        for (auto it = input.cbegin(); it != input.cend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(it);
                return WideToUTF8(input);
            }
        }
        return TNoChangesTag{};
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TUnicodeSetMatcher: public TOperationMixin<TUnicodeSetMatcher> {
    static bool Execute(TStringRef inputRef, TStringRef customCategoryRef) {
        const TStringBuf input(inputRef);
        const TUtf16String& customCategory = UTF8ToWide(customCategoryRef);
        TUnicodeSet unicodeSet;
        try {
            unicodeSet.Parse(customCategory);
        } catch (...) {
            throw yexception() << "Failed to parse unicode set: " << CurrentExceptionMessage();
        }
        wchar32 rune;
        const unsigned char* cur = reinterpret_cast<const unsigned char*>(input.begin());
        const unsigned char* last = reinterpret_cast<const unsigned char*>(input.end());
        while (cur != last) {
            ReadUTF8CharAndAdvance(rune, cur, last);
            if (!unicodeSet.Has(rune)) {
                return false;
            }
        }
        return true;
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TLevensteinDistanceFinder: public TOperationMixin<TLevensteinDistanceFinder> {
    static ui64 Execute(TStringRef leftRef, TStringRef rightRef) {
        const TStringBuf left(leftRef);
        const TStringBuf right(rightRef);
        const auto& leftUtf32 = UTF8ToUTF32<true>(left);
        const auto& rightUtf32 = UTF8ToUTF32<true>(right);
        return NLevenshtein::Distance(leftUtf32, rightUtf32);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TLastRemoval: public TOperationMixin<TLastRemoval> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef removeRef) {
        TUtf32String input = UTF8ToUTF32<true>(inputRef);
        const TUtf32String remove = UTF8ToUTF32<true>(removeRef);
        const std::unordered_set<wchar32> chars(remove.cbegin(), remove.cend());
        for (auto it = input.crbegin(); it != input.crend(); ++it) {
            if (chars.contains(*it)) {
                input.erase(input.crend() - it - 1, 1);
                return WideToUTF8(input);
            }
        }
        return TNoChangesTag{};
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TAllReplacer: public TOperationMixin<TAllReplacer> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef whatReplace, TStringRef toReplace) {
        if (TString result(inputRef); SubstGlobal(result, whatReplace, toReplace)) {
            return result;
        } else {
            return TNoChangesTag{};
        }
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TFirstReplacer: public TOperationMixin<TFirstReplacer> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef whatReplace, TStringRef toReplace) {
        std::string result(inputRef);
        const std::string_view what(whatReplace);
        if (const auto index = result.find(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(toReplace));
            return result;
        }
        return TNoChangesTag{};
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TLastReplacer: public TOperationMixin<TLastReplacer> {
    static std::variant<TNoChangesTag, TString> Execute(TStringRef inputRef, TStringRef whatReplace, TStringRef toReplace) {
        std::string result(inputRef);
        const std::string_view what(whatReplace);
        if (const auto index = result.rfind(what); index != std::string::npos) {
            result.replace(index, what.size(), std::string_view(toReplace));
            return result;
        }
        return TNoChangesTag{};
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TFinder: public TOperationMixin<TFinder> {
    static TMaybe<ui64> Execute(TStringRef inputRef, TStringRef whatFind, TMaybe<ui64> whereFind) {
        const std::string_view string(inputRef);
        const std::string_view needle(whatFind);
        std::string_view::size_type pos = 0U;

        if (auto p = whereFind.GetOrElse(0ULL)) {
            for (auto ptr = string.data(); p && pos < string.size(); --p) {
                const auto width = WideCharSize(*ptr);
                pos += width;
                ptr += width;
            }
        }

        if (const auto find = string.find(needle, pos); std::string_view::npos != find) {
            size_t result;
            GetNumberOfUTF8Chars(string.data(), find, result);
            return static_cast<ui64>(result);
        }
        return Nothing();
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TRFinder: public TOperationMixin<TRFinder> {
    static TMaybe<ui64> Execute(TStringRef inputRef, TStringRef whatFind, TMaybe<ui64> whereFind) {
        const std::string_view string(inputRef);
        const std::string_view needle(whatFind);
        std::string_view::size_type pos = std::string_view::npos;

        if (auto p = whereFind.GetOrElse(std::string_view::npos); std::string_view::npos != p) {
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
            return static_cast<ui64>(result);
        }
        return Nothing();
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

template <bool strict>
struct TToUint64Converter: public TOperationMixin<TToUint64Converter<strict>> {
    static TNothing Terminate(const char* message) {
        if constexpr (strict) {
            return Nothing();
        } else {
            throw yexception() << message;
        }
    };

    static TMaybe<ui64> Execute(TStringRef inputRef, TMaybe<ui16> inputBase) {
        const TString inputStr(inputRef);
        const char* input = inputStr.data();
        const int base = inputBase.GetOrElse(0);
        char* pos = nullptr;
        auto prevErrno = errno;
        errno = 0;
        Y_DEFER {
            errno = prevErrno;
        };
        unsigned long long res = std::strtoull(input, &pos, base);
        if (!res && errno == EINVAL) {
            return Terminate("Incorrect base");
        }

        ui64 ret = static_cast<ui64>(res);
        if (!res && pos == input) {
            return Terminate("Input string is not a number");
        } else if ((res == ULLONG_MAX && errno == ERANGE) || ret != res) {
            return Terminate("Converted value falls out of Uint64 range");
        } else if (*pos) {
            return Terminate("Input string contains junk after the number");
        }
        return ret;
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TUtf8Checker: public TOperationMixin<TUtf8Checker> {
    static bool Execute(TMaybe<TStringRef> inputRef) {
        if (!inputRef.Defined()) {
            return false;
        }
        return IsUtf8(*inputRef);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

struct TSubstringGetter: public TOperationMixin<TSubstringGetter> {
    static TStringBuf Execute(TStringRef inputRef Y_LIFETIME_BOUND, TMaybe<ui64> inputFrom, TMaybe<ui64> inputLen) {
        const TStringBuf input(inputRef);
        size_t from = inputFrom.GetOrElse(0);
        size_t len = inputLen.GetOrElse(TStringBuf::npos);
        return SubstrUTF8(input, from, len);
    }
    DISABLE_IMPICT_ARGUMENT_CAST;
};

#define DEFINE_UTF8_OPERATION_STRICT(udfName, Executor, signature, optionalArgs)                     \
    BEGIN_SIMPLE_STRICT_ARROW_UDF_WITH_OPTIONAL_ARGS(T##udfName, signature, optionalArgs) {          \
        return Executor::DoExecute(valueBuilder, args);                                              \
    }                                                                                                \
                                                                                                     \
    struct T##udfName##KernelExec                                                                    \
        : public TUnaryKernelExec<T##udfName##KernelExec> {                                          \
        template <typename TSink>                                                                    \
        static void Process(const IValueBuilder* valueBuilder, TBlockItem arg1, const TSink& sink) { \
            Y_UNUSED(valueBuilder);                                                                  \
            Executor::BlockDoExecute(arg1, sink);                                                    \
        }                                                                                            \
    };                                                                                               \
                                                                                                     \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)

#define DEFINE_UTF8_OPERATION_BIN_BASE(macro, udfName, Executor, signature, optionalArgs)                             \
    macro(T##udfName, signature, optionalArgs) {                                                                      \
        return Executor::DoExecute(valueBuilder, args);                                                               \
    }                                                                                                                 \
                                                                                                                      \
    struct T##udfName##KernelExec                                                                                     \
        : public TBinaryKernelExec<T##udfName##KernelExec> {                                                          \
        template <typename TSink>                                                                                     \
        static void Process(const IValueBuilder* valueBuilder, TBlockItem arg1, TBlockItem arg2, const TSink& sink) { \
            Y_UNUSED(valueBuilder);                                                                                   \
            Executor::BlockDoExecute(arg1, arg2, sink);                                                               \
        }                                                                                                             \
    };                                                                                                                \
                                                                                                                      \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)

#define DEFINE_UTF8_OPERATION_BIN_STRICT(udfName, Executor, signature, optionalArgs) \
    DEFINE_UTF8_OPERATION_BIN_BASE(BEGIN_SIMPLE_STRICT_ARROW_UDF_WITH_OPTIONAL_ARGS, udfName, Executor, signature, optionalArgs)

#define DEFINE_UTF8_OPERATION_BIN_NOT_STRICT(udfName, Executor, signature, optionalArgs) \
    DEFINE_UTF8_OPERATION_BIN_BASE(BEGIN_SIMPLE_ARROW_UDF_WITH_OPTIONAL_ARGS, udfName, Executor, signature, optionalArgs)

#define DEFINE_UTF8_OPERATION_MANY_STRICT(udfName, Executor, signature, argsCount, optionalArgsCount) \
    BEGIN_SIMPLE_STRICT_ARROW_UDF_WITH_OPTIONAL_ARGS(T##udfName, signature, optionalArgsCount) {      \
        return Executor::DoExecute(valueBuilder, args);                                               \
    }                                                                                                 \
                                                                                                      \
    struct T##udfName##KernelExec                                                                     \
        : public TGenericKernelExec<T##udfName##KernelExec, argsCount> {                              \
        template <typename TSink>                                                                     \
        static void Process(const IValueBuilder* valueBuilder, TBlockItem args, const TSink& sink) {  \
            Y_UNUSED(valueBuilder);                                                                   \
            Executor::BlockDoExecute(args, sink);                                                     \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    END_SIMPLE_ARROW_UDF(T##udfName, T##udfName##KernelExec::Do)

DEFINE_UTF8_OPERATION_STRICT(IsUtf, TUtf8Checker, bool(TOptional<char*>), /*optionalArgs=*/1);

DEFINE_UTF8_OPERATION_STRICT(Normalize, TNormalizeUTF8<NFC>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(NormalizeNFD, TNormalizeUTF8<NFD>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(NormalizeNFC, TNormalizeUTF8<NFC>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(NormalizeNFKD, TNormalizeUTF8<NFKD>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(NormalizeNFKC, TNormalizeUTF8<NFKC>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_STRICT(IsAscii, TCheckAllChars<IsAscii>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsSpace, TCheckAllChars<IsSpace>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsUpper, TCheckAllChars<IsUpper>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsLower, TCheckAllChars<IsLower>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsDigit, TCheckAllChars<IsDigit>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsAlpha, TCheckAllChars<IsAlpha>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsAlnum, TCheckAllChars<IsAlnum>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(IsHex, TCheckAllChars<IsHexdigit>, bool(TAutoMap<TUtf8>), /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_STRICT(ToTitle, TStringToStringMapper<ToTitle>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(ToUpper, TStringToStringMapper<ToUpper>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(ToLower, TStringToStringMapper<ToLower>, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_STRICT(GetLength, TLengthGetter, ui64(TAutoMap<TUtf8>), /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_STRICT(Reverse, TReverser, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_STRICT(Strip, TStripper, TUtf8(TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_MANY_STRICT(Substring, TSubstringGetter, TUtf8(TAutoMap<TUtf8>, TOptional<ui64>, TOptional<ui64>), /*argsCount=*/3, /*optionalArgs=*/1);

DEFINE_UTF8_OPERATION_BIN_STRICT(RemoveAll, TAllRemover, TUtf8(TAutoMap<TUtf8>, TUtf8), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_BIN_STRICT(RemoveFirst, TFirstRemover, TUtf8(TAutoMap<TUtf8>, TUtf8), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_BIN_NOT_STRICT(IsUnicodeSet, TUnicodeSetMatcher, bool(TAutoMap<TUtf8>, TUtf8), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_BIN_STRICT(LevensteinDistance, TLevensteinDistanceFinder, ui64(TAutoMap<TUtf8>, TAutoMap<TUtf8>), /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_BIN_STRICT(RemoveLast, TLastRemoval, TUtf8(TAutoMap<TUtf8>, TUtf8), /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_MANY_STRICT(ReplaceAll, TAllReplacer, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8), /*argsCount=*/3, /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_MANY_STRICT(ReplaceFirst, TFirstReplacer, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8), /*argsCount=*/3, /*optionalArgs=*/0);
DEFINE_UTF8_OPERATION_MANY_STRICT(ReplaceLast, TLastReplacer, TUtf8(TAutoMap<TUtf8>, TUtf8, TUtf8), /*argsCount=*/3, /*optionalArgs=*/0);

DEFINE_UTF8_OPERATION_MANY_STRICT(Find, TFinder, TOptional<ui64>(TAutoMap<TUtf8>, TUtf8, TOptional<ui64>), /*argsCount=*/3, /*optionalArgs=*/1);
DEFINE_UTF8_OPERATION_MANY_STRICT(RFind, TRFinder, TOptional<ui64>(TAutoMap<TUtf8>, TUtf8, TOptional<ui64>), /*argsCount=*/3, /*optionalArgs=*/1);

DEFINE_UTF8_OPERATION_BIN_NOT_STRICT(ToUint64, TToUint64Converter</*strict=*/false>, ui64(TAutoMap<TUtf8>, TOptional<ui16>), /*optionalArgs=*/1);
DEFINE_UTF8_OPERATION_BIN_STRICT(TryToUint64, TToUint64Converter</*strict=*/true>, TOptional<ui64>(TAutoMap<TUtf8>, TOptional<ui16>), /*optionalArgs=*/1);

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
    const TUtf32String::const_iterator start,
    const TIt& it,
    TTmpVector& result) {
    const std::string_view& original = input.AsStringRef();
    size_t charPos = 0U, bytePos = 0U;
    for (const auto& elem : it) {
        for (const size_t next = std::distance(start, elem.TokenStart()); charPos < next; ++charPos) {
            bytePos += WideCharSize(original[bytePos]);
        }
        const auto from = bytePos;

        for (const size_t next = charPos + std::distance(elem.TokenStart(), elem.TokenDelim()); charPos < next; ++charPos) {
            bytePos += WideCharSize(original[bytePos]);
        }
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

SIMPLE_UDF_WITH_OPTIONAL_ARGS(TSplitToList, TListType<TUtf8>(TOptional<TUtf8>,
                                                             TUtf8,
                                                             TDelimeterStringArg,
                                                             TSkipEmptyArg,
                                                             TLimitArg),
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
            const auto& input = UTF8ToUTF32<true>(args[0].AsStringRef());
            const auto& delimeter = UTF8ToUTF32<true>(args[1].AsStringRef());
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
    std::array<char, 4> runeBuffer = {};
    for (NUdf::TUnboxedValue item; iter.Next(item);) {
        const wchar32 rune = item.Get<ui32>();
        size_t written = 0;
        WideToUTF8(&rune, 1, runeBuffer.data(), written);
        Y_ENSURE(written <= 4);
        buffer.insert(buffer.end(), runeBuffer.data(), runeBuffer.data() + written);
    }

    return valueBuilder->NewString(TStringRef(buffer.data(), buffer.size()));
}

#define EXPORTED_UNICODE_BASE_UDF \
    TIsUtf,                       \
        TGetLength,               \
        TSubstring,               \
        TFind,                    \
        TRFind,                   \
        TSplitToList,             \
        TJoinFromList,            \
        TLevensteinDistance,      \
        TReplaceAll,              \
        TReplaceFirst,            \
        TReplaceLast,             \
        TRemoveAll,               \
        TRemoveFirst,             \
        TRemoveLast,              \
        TToCodePointList,         \
        TFromCodePointList,       \
        TReverse,                 \
        TToLower,                 \
        TToUpper,                 \
        TToTitle,                 \
        TToUint64,                \
        TTryToUint64,             \
        TStrip,                   \
        TIsUnicodeSet,            \
        TNormalize,               \
        TNormalizeNFD,            \
        TNormalizeNFC,            \
        TNormalizeNFKD,           \
        TNormalizeNFKC,           \
        TIsAscii,                 \
        TIsSpace,                 \
        TIsUpper,                 \
        TIsLower,                 \
        TIsDigit,                 \
        TIsAlpha,                 \
        TIsAlnum,                 \
        TIsHex
} // namespace
