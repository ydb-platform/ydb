#pragma once

#include <library/cpp/random_provider/random_provider.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <array>
#include <limits>
#include <tuple>
#include <type_traits>
#include <variant>

namespace NKikimr::NMiniKQL {

namespace NPrivate {

template <typename T, typename = void>
struct TRandomDataGenerator {
    static_assert(sizeof(T) == 0,
                  "TRandomDataGenerator is not specialized for this type. "
                  "Add a specialization of TRandomDataGenerator<T> to support it.");
};

template <>
struct TRandomDataGenerator<bool> {
    struct TSettings {
        double TrueProbability = 0.5;
    };

    static bool Generate(IRandomProvider& provider, const TSettings& settings) {
        Y_ENSURE(settings.TrueProbability >= 0.0 && settings.TrueProbability <= 1.0);
        return provider.GenRandReal2() < settings.TrueProbability;
    }
};

template <typename T>
    requires std::is_integral_v<T> && (!std::is_same_v<T, bool>)
struct TRandomDataGenerator<T> {
    struct TSettings {
        T Min = std::numeric_limits<T>::min();
        T Max = std::numeric_limits<T>::max();
    };

    static T Generate(IRandomProvider& provider, const TSettings& settings) {
        Y_ENSURE(settings.Min < settings.Max);
        const auto range = static_cast<ui64>(settings.Max) - static_cast<ui64>(settings.Min);
        return static_cast<T>(static_cast<ui64>(settings.Min) + provider.GenRand64() % range);
    }
};

template <typename T>
    requires std::is_floating_point_v<T>
struct TRandomDataGenerator<T> {
    struct TSettings {
        T Min = T(0);
        T Max = T(1);
    };

    static T Generate(IRandomProvider& provider, const TSettings& settings) {
        Y_ENSURE(settings.Min < settings.Max);
        return static_cast<T>(
            settings.Min +
            provider.GenRandReal2() * static_cast<double>(settings.Max - settings.Min));
    }
};

template <>
struct TRandomDataGenerator<TString> {
    struct TSettings {
        size_t MinSize = 0;
        size_t MaxSize = 32;
        TStringBuf Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    };

    static TString Generate(IRandomProvider& provider, const TSettings& settings) {
        Y_ENSURE(settings.MinSize < settings.MaxSize);
        Y_ENSURE(!settings.Alphabet.empty());
        const size_t len = settings.MinSize + provider.GenRand64() % (settings.MaxSize - settings.MinSize);
        TString result(len, '\0');
        for (size_t i = 0; i < len; ++i) {
            result[i] = settings.Alphabet[provider.GenRand64() % settings.Alphabet.size()];
        }
        return result;
    }
};

template <typename T>
struct TRandomDataGenerator<TMaybe<T>> {
    struct TSettings {
        double NullProbability = 0.1;
        typename TRandomDataGenerator<T>::TSettings Inner{};
    };

    static TMaybe<T> Generate(IRandomProvider& provider, const TSettings& settings) {
        Y_ENSURE(settings.NullProbability >= 0.0 && settings.NullProbability <= 1.0);
        if (provider.GenRandReal2() < settings.NullProbability) {
            return Nothing();
        }
        return TRandomDataGenerator<T>::Generate(provider, settings.Inner);
    }
};

template <typename... Ts>
struct TRandomDataGenerator<std::tuple<Ts...>> {
    using TSettings = std::tuple<typename TRandomDataGenerator<Ts>::TSettings...>;

    static std::tuple<Ts...> Generate(IRandomProvider& provider, const TSettings& settings) {
        return [&]<size_t... Is>(std::index_sequence<Is...>) {
            return std::make_tuple(TRandomDataGenerator<Ts>::Generate(provider, std::get<Is>(settings))...);
        }(std::index_sequence_for<Ts...>{});
    }
};

template <typename... Ts>
struct TRandomDataGenerator<std::variant<Ts...>> {
    struct TSettings {
        std::array<double, sizeof...(Ts)> Weights = [] {
            std::array<double, sizeof...(Ts)> w{};
            w.fill(1.0);
            return w;
        }();
        std::tuple<typename TRandomDataGenerator<Ts>::TSettings...> InnerSettings{};
    };

    static std::variant<Ts...> Generate(IRandomProvider& provider, const TSettings& settings) {
        const size_t idx = SelectWeightedIndex(provider, settings.Weights);
        std::variant<Ts...> result;
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            Y_UNUSED(((Is == idx && (result = TRandomDataGenerator<Ts>::Generate(
                                         provider, std::get<Is>(settings.InnerSettings)), true)) ||
                      ...));
        }(std::index_sequence_for<Ts...>{});
        return result;
    }

private:
    static size_t SelectWeightedIndex(IRandomProvider& provider, const std::array<double, sizeof...(Ts)>& weights) {
        double total = 0.0;
        for (double w : weights) {
            Y_ENSURE(w >= 0.0);
            total += w;
        }
        Y_ENSURE(total > 0.0);
        const double roll = provider.GenRandReal2() * total;
        double cumulative = 0.0;
        for (size_t i = 0; i + 1 < sizeof...(Ts); ++i) {
            cumulative += weights[i];
            if (roll < cumulative) {
                return i;
            }
        }
        return sizeof...(Ts) - 1;
    }
};

} // namespace NPrivate

template <typename T>
using TGeneratorSettings = typename NPrivate::TRandomDataGenerator<T>::TSettings;

template <typename T>
TVector<T> GenerateRandomData(TIntrusivePtr<IRandomProvider> provider, TGeneratorSettings<T> settings, size_t count) {
    TVector<T> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        result.push_back(NPrivate::TRandomDataGenerator<T>::Generate(*provider, settings));
    }
    return result;
}

template <typename T>
TVector<T> GenerateRandomData(TIntrusivePtr<IRandomProvider> provider, size_t count) {
    return GenerateRandomData<T>(std::move(provider), TGeneratorSettings<T>{}, count);
}

} // namespace NKikimr::NMiniKQL
