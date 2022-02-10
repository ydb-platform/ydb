#pragma once

#include "byte_reader.h"
#include "byte_writer.h"
#include "traits.h"
#include "zigzag.h"

#include <util/system/types.h>

#include <cstddef>
#include <type_traits>

namespace NYsonPull {
    namespace NDetail {
        namespace NVarInt {
            namespace NImpl {
                template <typename T>
                constexpr inline size_t max_size() {
                    return (8 * sizeof(T) - 1) / 7 + 1;
                }

                template <typename T>
                inline size_t write(ui64 value, T&& consume) {
                    auto stop = false;
                    auto nwritten = size_t{0};
                    while (!stop) {
                        ++nwritten;
                        auto byte = static_cast<ui8>(value | 0x80);
                        value >>= 7;
                        if (value == 0) {
                            stop = true;
                            byte &= 0x7F;
                        }
                        consume(byte);
                    }
                    return nwritten;
                }

                template <typename U>
                inline bool read_fast(byte_reader<U>& reader, ui64* value) {
                    auto& buf = reader.stream().buffer();
                    auto* ptr = buf.pos();
                    ui32 b;

                    // Splitting into 32-bit pieces gives better performance on 32-bit
                    // processors.
                    ui32 part0 = 0, part1 = 0, part2 = 0;

                    b = *(ptr++);
                    part0 = (b & 0x7F);
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part0 |= (b & 0x7F) << 7;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part0 |= (b & 0x7F) << 14;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part0 |= (b & 0x7F) << 21;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part1 = (b & 0x7F);
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part1 |= (b & 0x7F) << 7;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part1 |= (b & 0x7F) << 14;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part1 |= (b & 0x7F) << 21;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part2 = (b & 0x7F);
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    part2 |= (b & 0x7F) << 7;
                    if (!(b & 0x80))
                        goto done;

                    // We have overrun the maximum size of a Varint (10 bytes).  The data
                    // must be corrupt.
                    return false;

                done:
                    reader.advance(ptr - buf.pos());
                    *value = (static_cast<ui64>(part0)) | (static_cast<ui64>(part1) << 28) | (static_cast<ui64>(part2) << 56);
                    return true;
                }

                template <typename U>
                inline bool read_fast(byte_reader<U>& reader, ui32* value) {
                    // Fast path:  We have enough bytes left in the buffer to guarantee that
                    // this read won't cross the end, so we can skip the checks.
                    auto& buf = reader.stream().buffer();
                    auto* ptr = buf.pos();
                    ui32 b;
                    ui32 result;

                    b = *(ptr++);
                    result = (b & 0x7F);
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    result |= (b & 0x7F) << 7;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    result |= (b & 0x7F) << 14;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    result |= (b & 0x7F) << 21;
                    if (!(b & 0x80))
                        goto done;
                    b = *(ptr++);
                    result |= b << 28;
                    if (!(b & 0x80))
                        goto done;

                    // FIXME
                    // If the input is larger than 32 bits, we still need to read it all
                    // and discard the high-order bits.

                    for (size_t i = 0; i < max_size<ui64>() - max_size<ui32>(); i++) {
                        b = *(ptr++);
                        if (!(b & 0x80))
                            goto done;
                    }

                    // We have overrun the maximum size of a Varint (10 bytes).  Assume
                    // the data is corrupt.
                    return false;

                done:
                    reader.advance(ptr - buf.pos());
                    *value = result;
                    return true;
                }

                template <typename U>
                inline bool read_slow(byte_reader<U>& reader, ui64* value) {
                    // Slow path:  This read might cross the end of the buffer, so we
                    // need to check and refresh the buffer if and when it does.

                    auto& buf = reader.stream().buffer();
                    ui64 result = 0;
                    int count = 0;
                    ui32 b;

                    do {
                        if (count == max_size<ui64>()) {
                            return false;
                        }
                        reader.fill_buffer();
                        if (reader.stream().at_end()) {
                            return false;
                        }
                        b = *buf.pos();
                        result |= static_cast<ui64>(b & 0x7F) << (7 * count);
                        reader.advance(1);
                        ++count;
                    } while (b & 0x80);

                    *value = result;
                    return true;
                }

                template <typename U>
                inline bool read_slow(byte_reader<U>& reader, ui32* value) {
                    ui64 result;
                    // fallback to 64-bit reading
                    if (read_slow(reader, &result) && result <= std::numeric_limits<ui32>::max()) {
                        *value = static_cast<ui32>(result);
                        return true;
                    }

                    return false;
                }

                // Following functions is an adaptation
                // of Protobuf code from coded_stream.cc
                template <typename T, typename U>
                inline bool read_dispatch(byte_reader<U>& reader, T* value) {
                    auto& buf = reader.stream().buffer();
                    // NOTE: checking for 64-bit max_size(), since 32-bit
                    // read_fast() might fallback to 64-bit reading
                    if (buf.available() >= max_size<ui64>() ||
                        // Optimization:  If the Varint ends at exactly the end of the buffer,
                        // we can detect that and still use the fast path.
                        (!buf.is_empty() && !(buf.end()[-1] & 0x80)))
                    {
                        return read_fast(reader, value);
                    } else {
                        // Really slow case: we will incur the cost of an extra function call here,
                        // but moving this out of line reduces the size of this function, which
                        // improves the common case. In micro benchmarks, this is worth about 10-15%
                        return read_slow(reader, value);
                    }
                }

            }

            // Various functions to read/write varints.

            // Returns the number of bytes written.
            template <typename T>
            inline NTraits::if_unsigned<T, size_t> write(ui8* data, T value) {
                return NImpl::write(
                    static_cast<ui64>(value),
                    [&](ui8 byte) { *data++ = byte; });
            }

            template <typename T>
            inline NTraits::if_signed<T, size_t> write(ui8* data, T value) {
                return NImpl::write(
                    static_cast<ui64>(NZigZag::encode(value)),
                    [&](ui8 byte) { *data++ = byte; });
            }

            template <typename T, typename U>
            inline void write(byte_writer<U>& stream, T value) {
                ui8 data[NImpl::max_size<T>()];
                auto size = write(data, value);
                stream.write(data, size);
            }

            template <typename T, typename U>
            inline NTraits::if_unsigned<T, T> read(byte_reader<U>& reader) {
                auto value = T{};
                auto& buf = reader.stream().buffer();
                if (!buf.is_empty() && *buf.pos() < 0x80) {
                    value = *buf.pos();
                    reader.advance(1);
                    return value;
                }

                if (Y_UNLIKELY(!NImpl::read_dispatch(reader, &value))) {
                    reader.fail("Error parsing varint value");
                }
                return value;
            }

            template <typename T, typename U>
            inline NTraits::if_signed<T, T> read(byte_reader<U>& reader) {
                return NZigZag::decode(
                    read<NTraits::to_unsigned<T>>(reader));
            }
        }
    }     // namespace NDetail
}
