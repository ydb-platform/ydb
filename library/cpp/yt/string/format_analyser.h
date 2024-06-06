#pragma once

#include <array>
#include <string_view>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TFormatAnalyser
{
public:
    template <class... TArgs>
    static consteval void ValidateFormat(std::string_view fmt);

private:
    // Non-constexpr function call will terminate compilation.
    // Purposefully undefined and non-constexpr/consteval
    static void CrashCompilerNotEnoughArguments(std::string_view msg);
    static void CrashCompilerTooManyArguments(std::string_view msg);
    static void CrashCompilerWrongTermination(std::string_view msg);
    static void CrashCompilerMissingTermination(std::string_view msg);
    static void CrashCompilerWrongFlagSpecifier(std::string_view msg);

    struct TSpecifiers
    {
        std::string_view Conversion;
        std::string_view Flags;
    };
    template <class TArg>
    static consteval auto GetSpecifiers();

    static constexpr char IntroductorySymbol = '%';
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// Base used for flag checks for each type independently.
// Use it for overrides.
struct TFormatArgBase
{
public:
    // TODO(arkady-e1ppa): Consider more strict formatting rules.
    static constexpr std::array ConversionSpecifiers = {
            'v', '1', 'c', 's', 'd', 'i', 'o',
            'x', 'X', 'u', 'f', 'F', 'e', 'E',
            'a', 'A', 'g', 'G', 'n', 'p'
        };

    static constexpr std::array FlagSpecifiers = {
        '-', '+', ' ', '#', '0',
        '1', '2', '3', '4', '5',
        '6', '7', '8', '9',
        '*', '.', 'h', 'l', 'j',
        'z', 't', 'L', 'q', 'Q'
    };

    template <class T>
    static constexpr bool IsSpecifierList = requires (T t) {
        [] <size_t N> (std::array<char, N>) { } (t);
    };

    // Hot = |true| adds specifiers to the beggining
    // of a new array.
    template <bool Hot, size_t N, std::array<char, N> List, class TFrom = TFormatArgBase>
    static consteval auto ExtendConversion();

    template <bool Hot, size_t N, std::array<char, N> List, class TFrom = TFormatArgBase>
    static consteval auto ExtendFlags();

private:
    template <bool Hot, size_t N, std::array<char, N> List, auto* From>
    static consteval auto AppendArrays();
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFormatArg
    : public TFormatArgBase
{ };

// Semantic requirement:
// Said field must be constexpr.
template <class T>
concept CFormattable = requires {
    TFormatArg<T>::ConversionSpecifiers;
    requires TFormatArgBase::IsSpecifierList<decltype(TFormatArg<T>::ConversionSpecifiers)>;

    TFormatArg<T>::FlagSpecifiers;
    requires TFormatArgBase::IsSpecifierList<decltype(TFormatArg<T>::FlagSpecifiers)>;
};

} // namespace NYT

#define FORMAT_ANALYSER_INL_H_
#include "format_analyser-inl.h"
#undef FORMAT_ANALYSER_INL_H_
