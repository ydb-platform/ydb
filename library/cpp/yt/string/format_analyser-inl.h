#ifndef FORMAT_ANALYSER_INL_H_
#error "Direct inclusion of this file is not allowed, include format_analyser.h"
// For the sake of sane code completion.
#include "format_analyser.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... TArgs>
consteval void TFormatAnalyser::ValidateFormat(std::string_view fmt)
{
    std::array<std::string_view, sizeof...(TArgs)> markers = {};
    std::array<TSpecifiers, sizeof...(TArgs)> specifiers{GetSpecifiers<TArgs>()...};

    int markerCount = 0;
    int currentMarkerStart = -1;

    for (int idx = 0; idx < std::ssize(fmt); ++idx) {
        auto symbol = fmt[idx];

        // Parse verbatim text.
        if (currentMarkerStart == -1) {
            if (symbol == IntroductorySymbol) {
                // Marker maybe begins.
                currentMarkerStart = idx;
            }
            continue;
        }

        // NB: We check for %% first since
        // in order to verify if symbol is a specifier
        // we need markerCount to be within range of our
        // specifier array.
        if (symbol == IntroductorySymbol) {
            if (currentMarkerStart + 1 != idx) {
                // '%a% detected'
                CrashCompilerWrongTermination("You may not terminate flag sequence other than %% with \'%\' symbol");
                return;
            }
            // '%%' detected --- skip
            currentMarkerStart = -1;
            continue;
        }

        // We are inside of marker.
        if (markerCount == std::ssize(markers)) {
            // To many markers
            CrashCompilerNotEnoughArguments("Number of arguments supplied to format is smaller than the number of flag sequences");
            return;
        }

        if (specifiers[markerCount].Conversion.contains(symbol)) {
            // Marker has finished.

            markers[markerCount]
                = std::string_view(fmt.begin() + currentMarkerStart, idx - currentMarkerStart + 1);
            currentMarkerStart = -1;
            ++markerCount;

            continue;
        }

        if (!specifiers[markerCount].Flags.contains(symbol)) {
            CrashCompilerWrongFlagSpecifier("Symbol is not a valid flag specifier; See FlagSpecifiers");
        }
    }

    if (currentMarkerStart != -1) {
        // Runaway marker.
        CrashCompilerMissingTermination("Unterminated flag sequence detected; Use \'%%\' to type plain %");
        return;
    }

    if (markerCount < std::ssize(markers)) {
        // Missing markers.
        CrashCompilerTooManyArguments("Number of arguments supplied to format is greater than the number of flag sequences");
        return;
    }

    // TODO(arkady-e1ppa): Consider per-type verification
    // of markers.
}

template <class TArg>
consteval auto TFormatAnalyser::GetSpecifiers()
{
    static_assert(CFormattable<TArg>, "Your specialization of TFormatArg is broken");

    return TSpecifiers{
        .Conversion = std::string_view{
            std::data(TFormatArg<TArg>::ConversionSpecifiers),
            std::size(TFormatArg<TArg>::ConversionSpecifiers)},
        .Flags = std::string_view{
            std::data(TFormatArg<TArg>::FlagSpecifiers),
            std::size(TFormatArg<TArg>::FlagSpecifiers)},
    };
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <bool Hot, size_t N, std::array<char, N> List, class TFrom>
consteval auto TFormatArgBase::ExtendConversion()
{
    static_assert(CFormattable<TFrom>);
    return AppendArrays<Hot, N, List, &TFrom::ConversionSpecifiers>();
}

template <bool Hot, size_t N, std::array<char, N> List, class TFrom>
consteval auto TFormatArgBase::ExtendFlags()
{
    static_assert(CFormattable<TFrom>);
    return AppendArrays<Hot, N, List, &TFrom::FlagSpecifiers>();
}

template <bool Hot, size_t N, std::array<char, N> List, auto* From>
consteval auto TFormatArgBase::AppendArrays()
{
    auto& from = *From;
    return [] <size_t... I, size_t... J> (
        std::index_sequence<I...>,
        std::index_sequence<J...>) {
            if constexpr (Hot) {
                return std::array{List[J]..., from[I]...};
            } else {
                return std::array{from[I]..., List[J]...};
            }
        } (
            std::make_index_sequence<std::size(from)>(),
            std::make_index_sequence<N>());
    }

} // namespace NYT::NDetail
