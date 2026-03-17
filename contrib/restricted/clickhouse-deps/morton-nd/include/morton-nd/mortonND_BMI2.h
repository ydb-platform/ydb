//
//  mortonND_BMI2.h
//  morton-nd
//
//  Copyright (c) 2015 Kevin Hartman.
//

#ifndef MORTON_ND_MORTONND_BMI2_H
#define MORTON_ND_MORTONND_BMI2_H

#if defined(__BMI2__) || __AVX2__
#define MORTON_ND_BMI2_ENABLED 1

#include <array>
#include <cmath>
#include <limits>
#include <tuple>
#include <type_traits>
#include <immintrin.h>

namespace mortonnd {

/**
 * Returns a selector mask to be used by 'pdep' and 'pext' intrinsic operations.
 *
 * This value is a repeating string of 'BitsRemaining' 1s separated by 'fields' - 1
 * 0s, starting with the least-significant-bit set.
 *
 * Example:
 * BuildSelector<6>(3) => 1001001001001001
 *
 * @tparam BitsRemaining the number of 1-bits to include in the mask.
 * @param fields the stride of the 1 bits in the mask.
 * @return the selector mask suitable for use by 'pdep' and 'pext'
 */
template<std::size_t BitsRemaining>
constexpr uint64_t BuildSelector(std::size_t fields) {
    return (BuildSelector<(BitsRemaining - 1)>(fields) << fields) | 1UL;
}

/**
 * Special case to avoid shifting >= width of uint64_t.
 */
template<>
constexpr uint64_t BuildSelector<1>(std::size_t) {
    return 1UL;
}

/**
 * A fast N-dimensional Morton encoder/decoder for targets supporting BMI2/AVX2 instruction
 * set extensions.
 *
 * This implementation supports up to 64-bit encodings. If you need support for larger results,
 * consider using 'MortonNDLutEncoder' which can support 128-bit results natively, or even
 * larger results using a custom BigInteger-like class.
 *
 * Configuration:
 *
 * Dimensions
 *   You must define the number of dimensions (number of inputs) that encoder/decoder
 *   invocations will require via template parameter. The static 'Encode' function will require
 *   exactly this number of inputs to be provided.
 *
 * T
 *   The type of the components to encode/decode, as well as the result. This must be either 'uint32_t'
 *   or 'uint64_t', since the underlying BMI instructions only operate on 32 and 64 bit fields.
 *
 * @tparam Dimensions the number of fields (components) to encode.
 * @tparam T the type of the components to encode/decode, as well as the type of the result.
 *         Must be either uint32_t or uint64_t.
 */
template<std::size_t Dimensions, typename T>
class MortonNDBmi
{
public:
    static constexpr auto FieldBits = std::size_t(std::numeric_limits<T>::digits) / Dimensions;

    static_assert(Dimensions > 0, "'Dimensions' must be > 0.");
    static_assert(std::is_same<T, uint32_t>::value | std::is_same<T, uint64_t>::value,
        "'T' must be either uint32_t or uint64_t.");

    /**
     * Calculates the Morton encoding of the specified input fields by interleaving the bits
     * of each. The first bit (LSb) of each field in the interleaved result starts at its offset in
     * the parameter list.
     *
     * WARNING: Inputs must NOT use more than 'FieldBits' least-significant bits.
     *
     * Example:
     *   Encode(xxxxxxxx, yyyyyyyy, zzzzzzzz) => zyxzyxzyxzyxzyxzyxzyxzyx
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
    static inline T Encode(T field1, Args... fields)
    {
        static_assert(sizeof...(Args) == Dimensions - 1, "'Encode' must be called with exactly 'Dimensions' arguments.");
        return EncodeInternal(field1, fields...);
    }

    /**
     * Decodes a Morton code by de-interleaving it into its components.
     *
     * Example:
     *   Decode(zyxzyxzyxzyxzyxzyxzyxzyx) => std::tuple { xxxxxxxx, yyyyyyyy, zzzzzzzz }
     *
     * @param encoding the Morton code to decode.
     * @return a tuple containing the code's individual components.
     */
    static inline auto Decode(T encoding)
    {
        return DecodeInternal(encoding, std::make_index_sequence<Dimensions>{});
    }

private:
    MortonNDBmi() = default;

    static const T Selector = BuildSelector<FieldBits>(Dimensions);

    template<typename...Args>
    static inline T EncodeInternal(T field1, Args... fields)
    {
        return EncodeInternal(fields...) | Deposit<Dimensions - sizeof...(fields) - 1>(field1);
    }

    static inline T EncodeInternal(T field)
    {
        return Deposit<Dimensions - 1>(field);
    }

    template<size_t... i>
    static inline auto DecodeInternal(T encoding, std::index_sequence<i...>)
    {
        return std::make_tuple(Extract<i>(encoding)...);
    }

    template<size_t FieldIndex>
    static inline uint32_t Deposit(uint32_t field) {
        return _pdep_u32(field, Selector << FieldIndex);
    }

    template<size_t FieldIndex>
    static inline uint64_t Deposit(uint64_t field) {
        return _pdep_u64(field, Selector << FieldIndex);
    }

    template<size_t FieldIndex>
    static inline uint32_t Extract(uint32_t encoding) {
        return _pext_u32(encoding, Selector << FieldIndex);
    }

    template<size_t FieldIndex>
    static inline uint64_t Extract(uint64_t encoding) {
        return _pext_u64(encoding, Selector << FieldIndex);
    }
};

/**
 * Type alias for 2D encodings that fit in a 32-bit result.
 *
 * Inputs must NOT use more than 16 least-significant bits.
 */
using MortonNDBmi_2D_32 = MortonNDBmi<2, uint32_t>;

/**
 * Type alias for 2D encodings that fit in a 64-bit result.
 *
 * Inputs must NOT use more than 32 least-significant bits.
 */
using MortonNDBmi_2D_64 = MortonNDBmi<2, uint64_t>;

/**
 * Type alias for 3D encodings that fit in a 32-bit result.
 *
 * Inputs must NOT use more than 10 least-significant bits.
 */
using MortonNDBmi_3D_32 = MortonNDBmi<3, uint32_t>;

/**
 * Type alias for 3D encodings that fit in a 64-bit result.
 *
 * Inputs must NOT use more than 21 least-significant bits.
 */
using MortonNDBmi_3D_64 = MortonNDBmi<3, uint64_t>;

}

#endif
#endif
