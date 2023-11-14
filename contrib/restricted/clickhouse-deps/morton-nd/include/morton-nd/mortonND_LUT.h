//
//  mortonND_encoder.h
//  morton-nd
//
//  Copyright (c) 2015 Kevin Hartman.
//

#ifndef mortonND_h
#define mortonND_h

#include <cmath>
#include <array>
#include <tuple>
#include <type_traits>
#include <limits>

namespace mortonnd {

/**
 * Resolves to the narrowest native unsigned integer type that is at
 * least 'Bits' wide.
 *
 * If no native type can handle the requested width, this becomes type
 * 'void()', which will fail compilation when substituted later.
 *
 * This is used as the LutValue type to ensure that the lookup table
 * is as small as possible (and to improve cache hits in subsequent
 * lookups).
 *
 * Note:
 * For current use cases (i.e. as LutValue), a value 'Bits' > 64
 * will not be requested (it's guarded against by a static assertion).
 */
template<size_t Bits>
using MinInt =
    typename std::conditional<(Bits <= 8), uint_least8_t,
        typename std::conditional<(Bits <= 16), uint_least16_t,
            typename std::conditional<(Bits <= 32), uint_least32_t,
                typename std::conditional<(Bits <= 64), uint_least64_t, void()>
                    ::type>::type>::type>::type;

/**
 * Resolves to the fastest *non-implicitly-promotable* native unsigned
 * integer type that is at least 'Bits' wide, or 'Default' if no such
 * type exists.
 *
 * This is used to select the interface type (T) used for encoding
 * inputs as well as the result.
 *
 * Note:
 * It's important that this type is not implicitly promotable to a
 * *signed* integer type, which could produce incorrect results.
 *
 * Signed types could be supported with input masking, but this would
 * add extra overhead.
 */
template<size_t Bits, typename Default = void()>
using FastInt =
    typename std::conditional<(Bits <= 32), uint_fast32_t,
        typename std::conditional<(Bits <= 64), uint_fast64_t, Default>
            ::type>::type;

/**
 * The mapping function used by lookup table generation.
 *
 * Maps unsigned integers to their "split" form.
 *
 * This is done by taking the binary representation of 'input' and
 * injecting 'fields' - 1 padding bits between each bit.
 *
 * Example:
 *   7 (dec) => 111 (bin) => 1001001 (bin) => 73 (dec)
 *
 * @tparam BitsRemaining the number of least-significant-bits left to process.
 * @param input the integer to distribute over the result.
 * @param fields the stride between value bits.
 * @return the lookup table value for 'input'
 */
template<std::size_t BitsRemaining>
constexpr auto SplitByN(std::size_t input, std::size_t fields) {
    return (SplitByN<BitsRemaining - 1>(input >> 1U, fields) << fields) | (input & 1U);
}

/**
 * Special case to avoid shifting >= width of 'input'.
 */
template<>
constexpr auto SplitByN<1>(std::size_t input, std::size_t) {
    return input & 1U;
}

/**
 * A fast portable N-dimensional LUT-based Morton encoder.
 *
 * This class literal (constexpr class) generates a suitable LUT (based on template parameters)
 * along with an efficient (i.e. no func calls, no branches) Morton encoding algorithm.
 *
 * This implementation supports up to 128-bit encodings using native integer types, but
 * can be also be used with user-provided encoding types (e.g. a BigInteger class) to support
 * encodings of any size. Note that user-provided encoding types must define the standard C++
 * integral operators, and additionally must be able to construct a std::size_t.
 *
 * For most use-cases (i.e. when the encoding result can fit within a 64-bit unsigned integer),
 * it should be sufficient to omit the encoding type altogether (a suitable native unsigned
 * integer type will be selected automatically).
 *
 * Configuration:
 *
 * Dimensions
 *   You must define the number of dimensions (number of inputs) with which this encoder instance
 *   will be used via template parameter. The resulting instance's 'Encode' function will require
 *   exactly this number of inputs to be provided.
 *
 * FieldBits
 *   The number of bits (least-significant) in each field must also be provided via template
 *   parameter. For example, if encoding N 10-bit fields, this would be 10. While other Morton
 *   encoding libraries do not require this information, Morton ND uses it to perform
 *   compile-time optimizations, such as early termination.
 *
 *   WARNING: results will be incorrect if encoder input values exceed this width.
 *
 * LutBits
 *   The LUT lookup width (in bits) must be provided as a template parameter. This is perhaps
 *   the most interesting parameter, since it allows the generated LUT and algorithm to be tuned
 *   for a particular CPU target and application.
 *
 *   This value dictates the size of the generated lookup table, as well as the number of lookups
 *   performed by the generated 'Encode' function.
 *
 *   LUT size in memory will be:    2^^LutBits * sizeof(LutValue)
 *   Look-ups per Encode call:      Dimensions * ChunkCount
 *
 *   Note: ChunkCount and LutValue are available as static members for debugging.
 *
 *   To properly tune this value, consider the following:
 *
 *   - A larger value will result in an exponentially larger LUT and exponentially higher
 *     compilation times. For most use cases, 'LutBits' should not exceed 16.
 *   - A larger value will result in an 'Encode' function with fewer operations ("faster"),
 *     iff it reduces ChunkCount (inspect this with the ChunkCount static member).
 *   - An 'Encode' function with minimal operations (see above) will not necessarily out-perform
 *     one that does more (smaller LUT) due to CPU caching. Smaller LUT configurations tend to do
 *     better for applications which call 'Encode' with random inputs. Larger configurations do
 *     better when encoder inputs are close in value, or when consecutive calls to 'Encode'
 *     include close values.
 *
 *   For performance critical applications, run benchmarks.
 *
 * T
 *   The type of the components to encode, as well as the result. This is optional (the fastest type
 *   which can fit the encoding result will be selected automatically), unless the requested config
 *   would result in an encoding too big to fit in a 64-bit unsigned integer.
 *
 *   If you need support for a result width > 64 but <= 128, you may be able to provide '__uint128_t'
 *   if your compiler supports it.
 *
 *   For > 128-bit results, a "BitInteger"-like class should work, but must support standard C++ integral
 *   operators. A std::size_t must also be constructible from this type (e.g. T provides an explicit
 *   conversion operator for std::size_t).
 *
 * @param Dimensions the number of fields (components) to encode.
 * @param FieldBits the number of bits in each input field, starting with the LSb. Higher bits are not cleared.
 * @param LutBits the number of bits for the LUT. Each field will be looked-up 'LutBits' bits at a time.
 * @param T the type of the components to encode, as well as the type of the result.
 */
template<std::size_t Dimensions, std::size_t FieldBits, std::size_t LutBits, typename T = FastInt<Dimensions * FieldBits>>
class MortonNDLutEncoder
{
    static constexpr auto LutValueWidth = LutBits * Dimensions;

    static_assert(Dimensions > 0, "'Dimensions' must be > 0.");
    static_assert(FieldBits > 0, "'FieldBits' must be > 0. ");
    static_assert(LutBits > 0, "'LutBits' must be > 0.");
    static_assert(LutBits <= FieldBits, "'LutBits' must be <= 'FieldBits'.");

    // Note: there's no technical reason for '32', but a larger value would be unreasonable.
    static_assert(LutBits <= 32, "'LutBits' must be <= 32.");

    static_assert(LutValueWidth <= 64, "'LutBits' * 'Dimensions' must be <= 64.");
    static_assert(LutValueWidth <= std::numeric_limits<std::size_t>::digits,
        "'LutBits' * 'Dimensions' must be <= width of std::size_t.");

    static_assert(!std::is_integral<T>::value || (std::numeric_limits<T>::digits >= (Dimensions * FieldBits)),
        "'T' must be able to hold 'Dimensions' * 'FieldBits' bits (the result size).");

    static_assert(!std::is_integral<T>::value || std::is_unsigned<T>::value,
        "'T' must be unsigned.");

    // LUT lookups require conversion from T to std::size_t.
    // Note: this does not imply that T must fit within std::size_t.
    static_assert(std::is_constructible<std::size_t, T>::value, "std::size_t must be constructible from 'T'");

public:
    /**
     * The type used for encoding inputs as well as the result.
     */
    typedef T type;

    /**
     * The number of chunks into which each input field is partitioned. This is also
     * the number of LUT lookups performed for each field.
     *
     * For debugging / perf tuning.
     */
    static constexpr std::size_t ChunkCount = 1 + ((FieldBits - 1) / LutBits);

    /**
     * Equivalent to class template parameter 'LutBits'.
     */
    static constexpr std::size_t LutBitsCount = LutBits;

    /**
     * A mask which can be used to clear the upper bits of encoder inputs prior to
     * a call to 'Encode', if they're expected to be dirty.
     */
    constexpr T InputMask() const {
        static_assert(std::is_integral<T>::value, "Input masks are only provided for integral types.");
        return ~T(0) >> (std::numeric_limits<T>::digits - FieldBits);
    }

    /**
     * The type selected internally for the LUT's value.
     * Will always accommodate Dimensions * LutBits bits.
     *
     * For debugging / perf tuning.
     */
    using LutValue = MinInt<LutValueWidth>;

    /**
     * Constexpr constructor.
     *
     * The resulting class literal instance holds the generated LUT (configurable via class
     * template parameters) and provides an 'Encode' function which is optimized to perform
     * encodings using that LUT.
     *
     * Note that the 'Encode' function is also constexpr, and can be used to produce encodings
     * at compile-time.
     */
    constexpr MortonNDLutEncoder() = default;

    /**
     * Calculates the Morton encoding of the specified input fields by interleaving the bits
     * of each. The first bit (LSb) of each field in the interleaved result starts at its offset in
     * the parameter list.
     *
     * Can be used in constant expressions.
     *
     * WARNING: Inputs must NOT use more than 'FieldBits' least-significant bits.
     *          Use static member 'InputMask' to clear upper bits if necessary.
     *
     * Example:
     * Encode(xxxxxxxx, yyyyyyyy, zzzzzzzz) => zyxzyxzyxzyxzyxzyxzyxzyx
     *
     * Field X starts at offset 0 (the LSb of the result)
     * Field Y starts at offset 1
     * Field Z starts at offset 2
     *
     * @param field0 the first field (will start at offset 0 in the result)
     * @param fields the rest. Must be convertible to 'T' without precision loss for a correct result.
     * @return the calculated Morton code.
     */
    template<typename...Args>
    constexpr T Encode(T field0, Args... fields) const
    {
        static_assert(sizeof...(Args) == Dimensions - 1, "'Encode' must be called with exactly 'Dimensions' arguments.");
        return EncodeInternal(field0, fields...);
    }

private:
    template<typename...Args>
    constexpr T EncodeInternal(T field1, Args... fields) const
    {
        return (EncodeInternal(fields...) << 1U) | LookupField(field1, std::make_index_sequence<ChunkCount>{});
    }

    constexpr T EncodeInternal(T field) const
    {
        return LookupField(field, std::make_index_sequence<ChunkCount>{});
    }

    template <size_t ...I>
    constexpr T LookupField(T field, std::index_sequence<I...>) const {
        return LookupField(field, I...);
    }

    template <typename ...Args>
    constexpr T LookupField(T field, std::size_t, Args... args) const
    {
        // Note: an explicit conversion from T to std::size_t will occur during
        // table lookups, though precision loss will not occur (the value will
        // not exceed that of ChunkMask (which has type std::size_t)).
        return (LookupField(field >> LutBits, args...) << (Dimensions * LutBits))
            | T(LookupTable[std::size_t(field & ChunkMask)]);
    }

    constexpr T LookupField(T field, std::size_t) const
    {
        return LookupTable[field & ChunkMask];
    }

    // NOTE: this is implemented at namespace level due to CWG727.
    // http://www.open-std.org/jtc1/sc22/wg21/docs/cwg_defects.html#727
    static constexpr LutValue SplitByN(std::size_t input) {
        return mortonnd::SplitByN<LutBits>(input, Dimensions);
    }

    template<size_t... i>
    static constexpr auto BuildLut(std::index_sequence<i...>) noexcept {
        return std::array<LutValue, sizeof...(i)>{{SplitByN(i)...}};
    }

    static constexpr std::size_t pow(std::size_t base, std::size_t exp) {
        return exp == 0 ? 1 : base * pow(base, exp - 1);
    }

    static constexpr std::size_t ComputeLutSize() {
        return pow(2, LutBits);
    }

    static constexpr std::size_t LutSize = ComputeLutSize();
    static constexpr std::size_t ChunkMask = ~std::size_t(0) >> (std::numeric_limits<std::size_t>::digits - LutBits);
    const std::array<LutValue, LutSize> LookupTable = BuildLut(std::make_index_sequence<LutSize>{});
};

/**
 * The mapping function used by lookup table entry generation.
 *
 * Extract every 'fields' bits from 'input', depositing each to the
 * result, filling from LSb to MSb without gaps.
 *
 * Example ('fields' == 3):
 *   73 (dec) => 1001001 (bin) => 111 (bin) => 7 (dec)
 *
 * @tparam BitsRemaining the number of least-significant-bits left to process.
 * @param input the source integer.
 * @param fields the stride between bits to extract from the source.
 * @return the extracted component of 'input'.
 */
template<std::size_t BitsRemaining>
constexpr auto JoinByN(std::size_t input, std::size_t fields) {
    return (JoinByN<BitsRemaining - 1>(input >> fields, fields) << 1U) | (input & 1U);
}

/**
 * Special case to avoid shifting >= width of 'input'.
 */
template<>
constexpr auto JoinByN<1>(std::size_t input, std::size_t) {
    return input & 1U;
}

/**
 * A fast portable N-dimensional LUT-based Morton decoder.
 *
 * This class literal (constexpr class) generates a suitable LUT (based on template parameters)
 * along with an efficient (i.e. no func calls, no branches) Morton decoding algorithm.
 *
 * This implementation supports up to 128-bit Morton codes using native integer types, but
 * can be also be used with user-provided encoding types (e.g. a BigInteger class) to support
 * encodings of any size. Note that user-provided encoding types must define the standard C++
 * integral operators, and additionally must be able to construct a std::size_t.
 *
 * Configuration:
 *
 * Dimensions
 *   You must define the number of dimensions with which this decoder instance
 *   will be used via template parameter. The resulting instance's 'Decode' function will
 *   return an std::tuple of this cardinality.
 *
 * FieldBits
 *   The number of bits (least-significant) in each decoded field.
 *   For example, if decoding a 3D Morton code of width 30, this would be 10. While other Morton
 *   decoding libraries do not require this information, Morton ND uses it to perform
 *   compile-time optimizations, such as early termination.
 *
 *   WARNING: results will be incorrect if the decoder input width exceeds FieldBits * Dimensions.
 *
 * LutBits
 *   The LUT lookup width (in bits) must be provided as a template parameter. This is perhaps
 *   the most interesting parameter, since it allows the generated LUT and algorithm to be tuned
 *   for a particular CPU target and application.
 *
 *   This value dictates the size of the generated lookup table, as well as the number of lookups
 *   performed by the generated 'Decode' function.
 *
 *   LUT size in memory will be (appx.):    2^^LutBits * sizeof(std::array<LutValue, Dimensions>)
 *   Look-ups per Decode call:              ChunkCount
 *
 *   Note: ChunkCount and LutValue are available as static members for debugging.
 *
 *   To properly tune this value, consider the following:
 *
 *   - A larger value will result in an exponentially larger LUT and exponentially higher
 *     compilation times. For most use cases, 'LutBits' should not exceed 16.
 *   - A larger value will result in an 'Decode' function with fewer operations,
 *     iff it reduces ChunkCount (inspect this with the ChunkCount static member).
 *   - A 'Decode' function with minimal operations (see above) will not necessarily out-perform
 *     one that does more (smaller LUT) due to CPU caching. Smaller LUT configurations tend to do
 *     better for applications which call 'Decode' with random inputs.
 *
 *   For performance critical applications, run benchmarks.
 *
 * T
 *   The type of the Morton code to decode, as well as the element type of decoded fields in the
 *   returned std::tuple. This is optional (the fastest type which can fit the Morton code will be
 *   selected automatically), unless it would be too big to fit in a 64-bit unsigned integer.
 *
 *   If you need support for a result width > 64 but <= 128, you may be able to provide '__uint128_t'
 *   if your compiler supports it.
 *
 *   For > 128-bit results, a "BitInteger"-like class should work, but must support standard C++ integral
 *   operators. A std::size_t must also be constructible from this type (e.g. T provides an explicit
 *   conversion operator for std::size_t).
 *
 * @param Dimensions the number of fields (components) to decode.
 * @param FieldBits the number of bits in each input field, starting with the LSb.
 * @param LutBits the number of bits for the LUT.
 * @param T the type of the Morton code to decode, as well as the tuple types of the result.
 */
template<std::size_t Dimensions, std::size_t FieldBits, std::size_t LutBits, typename T = FastInt<Dimensions * FieldBits>>
class MortonNDLutDecoder
{
    static constexpr auto MortonCodeWidth = FieldBits * Dimensions;
    static constexpr auto LutValueWidth = LutBits / Dimensions + (LutBits % Dimensions != 0);

    static_assert(Dimensions > 0, "'Dimensions' must be > 0.");
    static_assert(FieldBits > 0, "'FieldBits' must be > 0. ");
    static_assert(LutBits > 0, "'LutBits' must be > 0.");
    static_assert(LutBits <= MortonCodeWidth, "'LutBits' must be <= computed 'MortonCodeWidth'.");

    // Note: there's no technical reason for '32', but a larger value would be unreasonable.
    static_assert(LutBits <= 32, "'LutBits' must be <= 32.");

    static_assert(LutValueWidth <= std::numeric_limits<std::size_t>::digits,
        "Computed 'LutValueWidth' must be <= width of std::size_t.");

    static_assert(!std::is_integral<T>::value || (std::numeric_limits<T>::digits >= (Dimensions * FieldBits)),
        "'T' must be able to hold 'Dimensions' * 'FieldBits' bits (the Morton code size).");

    static_assert(!std::is_integral<T>::value || std::is_unsigned<T>::value,
        "'T' must be unsigned.");

    // LUT lookups require conversion from T to std::size_t.
    // Note: this does not imply that T must fit within std::size_t.
    static_assert(std::is_constructible<std::size_t, T>::value, "std::size_t must be constructible from 'T'");

public:
    /**
     * The type used for encoding inputs as well as the result.
     */
    typedef T type;

    /**
     * The number of chunks into which the input Morton code is partitioned,
     * i.e. the number of LUT lookups performed when decoding.
     *
     * For debugging / perf tuning.
     */
    static constexpr std::size_t ChunkCount = MortonCodeWidth / LutBits + (MortonCodeWidth % LutBits != 0);

    /**
     * Equivalent to class template parameter 'LutBits'.
     */
    static constexpr std::size_t LutBitsCount = LutBits;

    /**
     * A mask which can be used to clear the upper bits of the input Morton code prior to
     * a call to 'Decode', if they're expected to be dirty.
     */
    constexpr T InputMask() const {
        static_assert(std::is_integral<T>::value, "Input masks are only provided for integral types.");
        return ~T(0) >> (std::numeric_limits<T>::digits - MortonCodeWidth);
    }

    /**
     * The type selected internally for LUT entry array values.
     * I.e. Each LUT entry is of type std::array<LutValue, Dimensions>.
     *
     * For debugging / perf tuning.
     */
    using LutValue = MinInt<LutValueWidth>;

    /**
     * Constexpr constructor.
     *
     * The resulting class literal instance holds the generated LUT (configurable via class
     * template parameters) and provides a 'Decode' function which is optimized to perform
     * decodings using that LUT.
     *
     * Note that the 'Decode' function is also constexpr, and can be used to decode
     * at compile-time.
     */
    constexpr MortonNDLutDecoder() = default;

    /**
     * Decode a Morton code.
     * @param input The Morton code.
     * @return The decoded components of 'input' as an std::tuple.
     */
    constexpr auto Decode(T input) const
    {
        return DecodeInternal(input, std::make_index_sequence<ChunkCount>{});
    }

private:
    template<std::size_t ChunkStartBit, typename ResultTuple, std::size_t ...I>
    constexpr auto MapComponents(ResultTuple& result, const std::array<LutValue, Dimensions>& chunkLookupResult, std::index_sequence<I...>) const {
        MapComponents<ChunkStartBit>(result, chunkLookupResult, I...);
    }

    template<std::size_t ChunkStartBit, typename ResultTuple, typename ...Args>
    constexpr void MapComponents(ResultTuple& result, const std::array<LutValue, Dimensions>& chunkLookupResult, std::size_t, Args... args) const {
        InjectBits<ChunkStartBit, sizeof...(Args)>(result, chunkLookupResult);
        MapComponents<ChunkStartBit>(result, chunkLookupResult, args...);
    }

    template<std::size_t ChunkStartBit, typename ResultTuple, typename ...Args>
    constexpr void MapComponents(ResultTuple& result, const std::array<LutValue, Dimensions>& chunkLookupResult, std::size_t) const {
        InjectBits<ChunkStartBit, 0>(result, chunkLookupResult);
    }

    template<std::size_t ChunkStartBit, std::size_t SourceIndex, typename ResultTuple>
    constexpr void InjectBits(ResultTuple& result, const std::array<LutValue, Dimensions>& chunkLookupResult) const {
        constexpr auto FieldStartIndex = ChunkStartBit + SourceIndex;
        constexpr auto DestIndex = FieldStartIndex % Dimensions;
        constexpr auto InsertOffset = FieldStartIndex / Dimensions;
        std::get<DestIndex>(result) = (T(chunkLookupResult[SourceIndex]) << InsertOffset) | std::get<DestIndex>(result);
    }

    /**
     * Algorithm steps:
     *   - Split 'field' into chunks of width 'LutBits'.
     *   - For each chunk, look up the chunk in 'LookupTable', which returns an array of
     *     its de-interleaved components. E.g.: LookupTable[yxzyxzyx] => [xxx, yyy, zz]
     *     - For each component,
     *       - Determine the destination field to which the component belongs:
     *         (ChunkStartBit + ComponentIndex) % Dimensions.
     *       - Inject the component's bits at the current write offset for the destination field:
     *         (ChunkStartBit + ComponentIndex) / Dimensions.
     *
     * Example:
     *   Dimensions: 3
     *   Input:      zyxzyxzyxzyx
     *   LutBits:    5
     *
     *   Chunks: [ yxzyx, xzyxz, zy ]
     *
     *   - Chunks[0]:
     *     LookupResult = Lookup[yxzyx] => [xx, yy, z]:
     *     - LookupResult[0] => xx
     *       DestIndex =   (0 + 0) % 3 => 0
     *       InsertIndex = (0 + 0) / 3 => 0
     *       In English: Write xx at bit 0 of field 0.
     *
     *     - LookupResult[1] => yy
     *       DestIndex =   (0 + 1) % 3 => 1
     *       InsertIndex = (0 + 1) / 3 => 0
     *       In English: Write yy at bit 0 of field 1.
     *
     *     - LookupResult[2] => z
     *       DestIndex =   (0 + 2) % 3 => 2
     *       InsertIndex = (0 + 2) / 3 => 0
     *       In English: Write z at bit 0 of field 2.
     *
     *   Result so far: (xx, yy, z)
     *                   ^^  ^^  ^
     *   - Chunks[1]:
     *     LookupResult = Lookup[xzyxz] => [zz, xx, y]:
     *     - LookupResult[0] => zz
     *       DestIndex =   (5 + 0) % 3 => 2
     *       InsertIndex = (5 + 0) / 3 => 1
     *       In English: Write zz at bit 1 of field 2.
     *
     *     - LookupResult[1] => xx
     *       DestIndex =   (5 + 1) % 3 => 0
     *       InsertIndex = (5 + 1) / 3 => 2
     *       In English: Write xx at bit 2 of field 0.
     *
     *     - LookupResult[2] => y
     *       DestIndex =   (5 + 2) % 3 => 1
     *       InsertIndex = (5 + 2) / 3 => 2
     *       In English: Write y at bit 2 of field 1.
     *
     *   Result so far: (xxxx, yyy, zzz)
     *                   ^^    ^    ^^
     *   - Chunks[2]:
     *     LookupResult = Lookup[000zy] => [y, z, 0]:
     *     - LookupResult[0] => y
     *       DestIndex =   (10 + 0) % 3 => 1
     *       InsertIndex = (10 + 0) / 3 => 3
     *       In English: Write y at bit 3 of field 1.
     *
     *     - LookupResult[1] => z
     *       DestIndex =   (10 + 1) % 3 => 2
     *       InsertIndex = (10 + 1) / 3 => 3
     *       In English: Write z at bit 3 of field 2.
     *
     *     - LookupResult[2] => 0
     *       DestIndex =   (10 + 2) % 3 => 0
     *       InsertIndex = (10 + 2) / 3 => 4
     *       In English: Write 0 at bit 4 of field 0.
     *
     *   Final result: (0xxxx, yyyy, zzzz)
     *                  ^      ^     ^
     * @tparam Args The type sequence corresponding to 'args'.
     * @param field The Morton code to decode.
     * @param args The tail of the chunk index sequence, used to pump the template.
     * @return The decoded components, as an 'std::tuple'.
     */
    template <typename ...Args>
    constexpr auto DecodeInternal(T field, std::size_t, Args... args) const
    {
        auto constexpr ChunkIndex = sizeof...(args);
        auto constexpr ChunkStartBit = ChunkIndex * LutBits;

        auto result = DecodeInternal(field, args...);
        auto chunkLookupResult = LookupTable[std::size_t((field >> ChunkStartBit) & ChunkMask)];

        // 'MapComponents' equivalent logic:
        //
        // for (std::size_t i = 0; i < Dimensions; i++) {
        //     auto fieldStartIdx = ChunkStartBit + i;
        //     auto destIdx = fieldStartIdx % Dimensions;
        //     auto insertOffset = fieldStartIdx / Dimensions;
        //     result[destIdx] = (chunkLookupResult[i] << insertOffset) | result[destIdx];
        // }
        MapComponents<ChunkStartBit>(result, chunkLookupResult, std::make_index_sequence<Dimensions>{});

        return result;
    }

    template <typename Array, std::size_t ...I>
    constexpr auto CreateTuple(Array arr, std::index_sequence<I...>) const {
        return std::make_tuple(T(arr[I])...);
    }

    constexpr auto DecodeInternal(T field, std::size_t) const
    {
        // This is the 0th chunk, so it lines up with the decode result array.
        return CreateTuple(LookupTable[field & ChunkMask], std::make_index_sequence<Dimensions>{});
    }

    template <size_t ...I>
    constexpr auto DecodeInternal(T field, std::index_sequence<I...>) const {
        return DecodeInternal(field, I ...);
    }

    // NOTE: this is implemented at namespace level due to CWG727.
    // http://www.open-std.org/jtc1/sc22/wg21/docs/cwg_defects.html#727
    static constexpr LutValue JoinByN(std::size_t input) {
        return mortonnd::JoinByN<FieldBits>(input, Dimensions);
    }

    template<size_t... i>
    static constexpr auto BuildLutEntry(std::size_t input, std::index_sequence<i...>) {
        return std::array<LutValue, Dimensions>{{JoinByN(input >> i) ...}};
    }

    template<size_t... i>
    static constexpr auto BuildLut(std::index_sequence<i...>) noexcept {
        return std::array<std::array<LutValue, Dimensions>, sizeof...(i)>{{BuildLutEntry(i, std::make_index_sequence<Dimensions>{})...}};
    }

    static constexpr std::size_t pow(std::size_t base, std::size_t exp) {
        return exp == 0 ? 1 : base * pow(base, exp - 1);
    }

    static constexpr std::size_t ComputeLutSize() {
        return pow(2, LutBits);
    }
public:
    static constexpr std::size_t LutSize = ComputeLutSize();
    static constexpr std::size_t ChunkMask = ~std::size_t(0) >> (std::numeric_limits<std::size_t>::digits - LutBits);
    const std::array<std::array<LutValue, Dimensions>, LutSize> LookupTable = BuildLut(std::make_index_sequence<LutSize>{});
};

// Note: The below type aliases define default configurations for common use-cases,
//       including 2D and 3D encodings that fit in 32 and 64 bit results.
//
//       The defaulted 'LUTBits' values attempt to reduce LUT look-up operations
//       while keeping LUT table sizes relatively small to: take advantage of caching,
//       keep programs small, and minimize build times.

/**
 * Type alias for a 2D encoder with 16-bit inputs, 32-bit result.
 *
 * Inputs must NOT use more than 16 least-significant bits.
 */
using MortonNDLutEncoder_2D_32 = MortonNDLutEncoder<2, 16, 8>;

/**
 * Type alias for a 2D decoder for 32-bit input, 16-bit results.
 */
using MortonNDLutDecoder_2D_32 = MortonNDLutDecoder<2, 16, 8>;

/**
 * Type alias for a 2D encoder with 32-bit inputs, 64-bit result.
 *
 * Inputs must NOT use more than 32 least-significant bits.
 */
using MortonNDLutEncoder_2D_64 = MortonNDLutEncoder<2, 32, 11>;

/**
 * Type alias for a 2D decoder for 64-bit input, 32-bit results.
 */
using MortonNDLutDecoder_2D_64 = MortonNDLutDecoder<2, 32, 11>;

/**
 * Type alias for a 3D encoder with 10-bit inputs, 30-bit result.
 *
 * Inputs must NOT use more than 10 least-significant bits.
 */
using MortonNDLutEncoder_3D_32 = MortonNDLutEncoder<3, 10, 10>;

/**
 * Type alias for a 3D decoder for 30-bit input, 10-bit results.
 *
 * Input must NOT use more than 30 least-significant bits.
 */
using MortonNDLutDecoder_3D_32 = MortonNDLutDecoder<3, 10, 10>;

/**
 * Type alias for a 3D encoder with 21-bit inputs, 63-bit result.
 *
 * Inputs must NOT use more than 21 least-significant bits.
 */
using MortonNDLutEncoder_3D_64 = MortonNDLutEncoder<3, 21, 11>;

/**
 * Type alias for a 3D decoder for 63-bit input, 21-bit results.
 *
 * Inputs must NOT use more than 63 least-significant bits.
 */
using MortonNDLutDecoder_3D_64 = MortonNDLutDecoder<3, 21, 11>;

}

#endif