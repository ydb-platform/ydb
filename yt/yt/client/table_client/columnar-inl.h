#ifndef COLUMNAR_INL_H_
#error "Direct inclusion of this file is not allowed, include columnar.h"
// For the sake of sane code completion.
#include "columnar.h"
#endif

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline i64 GetBitmapByteSize(i64 bitCount)
{
    return (bitCount + 7) / 8;
}

inline i64 DecodeStringOffset(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 index)
{
    YT_ASSERT(index >= 0 && index <= static_cast<i64>(offsets.Size()));
    return index == 0
        ? 0
        : static_cast<ui32>(avgLength * index + ZigZagDecode64(offsets[index - 1]));
}

inline std::pair<i64, i64> DecodeStringRange(
    TRange<ui32> offsets,
    ui32 avgLength,
    i64 index)
{
    if (Y_UNLIKELY(index == 0)) {
        return {
            0,
            static_cast<i64>(avgLength + ZigZagDecode64(offsets[0]))
        };
    } else {
        auto avgLengthTimesIndex = static_cast<ui32>(avgLength * index);
        return {
            static_cast<i64>(avgLengthTimesIndex + ZigZagDecode64(offsets[index - 1])),
            static_cast<i64>(avgLengthTimesIndex + avgLength + ZigZagDecode64(offsets[index]))
        };
    }
}

namespace {

template <
    bool WithDictionary,
    bool WithBaseValue,
    bool WithZigZag,
    class T,
    template <
        bool WithBaseValue_,
        bool WithZigZag_,
        class T_
    >
    class TValueDecoder,
    class TGetter,
    class TConsumer
>
void DecodeVectorRleImpl(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TGetter getter,
    TConsumer consumer)
{
    auto startRleIndex = TranslateRleStartIndex(rleIndexes, startIndex);
    auto currentIndex = startIndex;
    auto currentRleIndex = startRleIndex;
    T currentDecodedValue;
    i64 thresholdIndex = -1;
    while (true) {
        if (currentIndex >= thresholdIndex) {
            if (currentIndex >= endIndex) {
                break;
            }
            decltype(getter(0)) currentValue;
            if constexpr(WithDictionary) {
                auto dictionaryIndex = dictionaryIndexes[currentRleIndex];
                if (dictionaryIndex == 0) {
                    currentValue = {};
                } else {
                    currentValue = getter(dictionaryIndex - 1);
                }
            } else {
                currentValue = getter(currentRleIndex);
            }
            currentDecodedValue = TValueDecoder<WithBaseValue, WithZigZag, T>::Run(currentValue, baseValue);
            ++currentRleIndex;
            thresholdIndex = currentRleIndex < static_cast<i64>(rleIndexes.Size())
                ? std::min(static_cast<i64>(rleIndexes[currentRleIndex]), endIndex)
                : endIndex;
        }

        if (currentIndex + 4 <= thresholdIndex) {
            // Unrolled loop.
            consumer(currentDecodedValue);
            consumer(currentDecodedValue);
            consumer(currentDecodedValue);
            consumer(currentDecodedValue);
            currentIndex += 4;
        } else {
            consumer(currentDecodedValue);
            ++currentIndex;
        }
    }
}

template <
    bool WithDictionary,
    bool WithBaseValue,
    bool WithZigZag,
    class T,
    template <
        bool WithBaseValue_,
        bool WithZigZag_,
        class T_
    >
    class TValueDecoder,
    class TGetter,
    class TConsumer
>
void DecodeVectorDirectImpl(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    TRange<ui32> dictionaryIndexes,
    TGetter getter,
    TConsumer consumer)
{
    for (i64 index = startIndex; index < endIndex; ++index) {
        decltype(getter(0)) value;
        if constexpr(WithDictionary) {
            auto dictionaryIndex = dictionaryIndexes[index];
            if (dictionaryIndex == 0) {
                value = {};
            } else {
                value = getter(dictionaryIndex - 1);
            }
        } else {
            value = getter(index);
        }
        auto decodedValue = TValueDecoder<WithBaseValue, WithZigZag, T>::Run(value, baseValue);
        consumer(decodedValue);
    }
}

template <
    bool WithRle,
    bool WithDictionary,
    bool WithBaseValue,
    bool WithZigZag,
    class T,
    template <
        bool WithBaseValue_,
        bool WithZigZag_,
        class T_
    >
    class TValueDecoder,
    class TGetter,
    class TConsumer
>
void DecodeVectorImpl(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TGetter getter,
    TConsumer consumer)
{
    if constexpr(WithRle) {
        DecodeVectorRleImpl<WithDictionary, WithBaseValue, WithZigZag, T, TValueDecoder>(
            startIndex,
            endIndex,
            baseValue,
            dictionaryIndexes,
            rleIndexes,
            std::forward<TGetter>(getter),
            std::forward<TConsumer>(consumer));
    } else {
        DecodeVectorDirectImpl<WithDictionary, WithBaseValue, WithZigZag, T, TValueDecoder>(
            startIndex,
            endIndex,
            baseValue,
            dictionaryIndexes,
            std::forward<TGetter>(getter),
            std::forward<TConsumer>(consumer));
    }
}

template <
    bool WithBaseValue,
    bool WithZigZag,
    class T
>
inline T DecodeIntegerValueImpl(
    ui64 value,
    ui64 baseValue)
{
    if constexpr(WithBaseValue) {
        value += baseValue;
    }
    if constexpr(WithZigZag) {
        value = static_cast<ui64>(ZigZagDecode64(value));
    }
    return static_cast<T>(value);
}

template <
    bool WithBaseValue,
    bool WithZigZag,
    class T
>
struct TIntegerValueDecoder
{
    static T Run(ui64 value, ui64 baseValue)
    {
        return DecodeIntegerValueImpl<WithBaseValue, WithZigZag, T>(value, baseValue);
    }
};

template <
    bool WithBaseValue,
    bool WithZigZag,
    class T
>
struct TIdentityValueDecoder
{
    template <class V>
    static V Run(V value, ui64 /*baseValue*/)
    {
        return value;
    }
};

template <
    class T,
    template <
        bool WithBaseValue_,
        bool WithZigZag_,
        class T_
    >
    class TValueDecoder,
    class TGetter,
    class TConsumer
>
void DecodeVector(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    bool zigZagEncoded,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TGetter getter,
    TConsumer consumer)
{
    YT_VERIFY(startIndex >= 0 && startIndex <= endIndex);
    YT_VERIFY(!rleIndexes || rleIndexes[0] == 0);

    #define XX_0(...) \
        DecodeVectorImpl<__VA_ARGS__, T, TValueDecoder>( \
            startIndex, \
            endIndex, \
            baseValue, \
            dictionaryIndexes, \
            rleIndexes, \
            std::forward<TGetter>(getter), \
            std::forward<TConsumer>(consumer));

    #define XX_1(...) \
        if (zigZagEncoded) { \
            XX_0(__VA_ARGS__, true) \
        } else { \
            XX_0(__VA_ARGS__, false) \
        }

    #define XX_2(...) \
        if (baseValue != 0) { \
            XX_1(__VA_ARGS__, true) \
        } else { \
            XX_1(__VA_ARGS__, false) \
        }

    #define XX_3(...) \
        if (dictionaryIndexes) { \
            XX_2(__VA_ARGS__, true) \
        } else { \
            XX_2(__VA_ARGS__, false) \
        }

    #define XX_4() \
        if (rleIndexes) { \
            XX_3(true) \
        } else { \
            XX_3(false) \
        }

    XX_4()

    #undef XX_0
    #undef XX_1
    #undef XX_2
    #undef XX_3
    #undef XX_4
}

} // namespace

template <
    class TFetcher,
    class TConsumer
>
void DecodeIntegerVector(
    i64 startIndex,
    i64 endIndex,
    ui64 baseValue,
    bool zigZagEncoded,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TFetcher fetcher,
    TConsumer consumer)
{
    DecodeVector<ui64, TIntegerValueDecoder>(
        startIndex,
        endIndex,
        baseValue,
        zigZagEncoded,
        dictionaryIndexes,
        rleIndexes,
        std::forward<TFetcher>(fetcher),
        std::forward<TConsumer>(consumer));
}

template <
    class T,
    class TFetcher,
    class TConsumer
>
void DecodeRawVector(
    i64 startIndex,
    i64 endIndex,
    TRange<ui32> dictionaryIndexes,
    TRange<ui64> rleIndexes,
    TFetcher fetcher,
    TConsumer consumer)
{
    DecodeVector<T, TIdentityValueDecoder>(
        startIndex,
        endIndex,
        0,
        false,
        dictionaryIndexes,
        rleIndexes,
        std::forward<TFetcher>(fetcher),
        std::forward<TConsumer>(consumer));
}

template <class T>
T DecodeIntegerValue(
    ui64 value,
    ui64 baseValue,
    bool zigZagEncoded)
{
    return zigZagEncoded
        ? DecodeIntegerValueImpl<true, true, T>(value, baseValue)
        : DecodeIntegerValueImpl<true, false, T>(value, baseValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
