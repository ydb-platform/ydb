#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <bit>

#include <ydb/library/conclusion/result.h>
#include "bit_writer.h"

namespace NKikimr::NArrow::NSerialization::NGorilla {
    constexpr int32_t TIMESTAMP_FIRST_DELTA_BITS = 14;
    constexpr uint64_t TIMESTAMP_FINISH_FIRST_UNCOMPRESSED_DELTA = (1 << TIMESTAMP_FIRST_DELTA_BITS) - 1;
    constexpr uint64_t TIMESTAMP_FINISH_DOD = 0xFFFFFFFFFFFFFFFF;

    constexpr uint64_t VALUE_FINISH_FIRST_UNCOMPRESSED_FLAG = 0xFFFFFFFFFFFFFFFF;
    constexpr uint64_t VALUE_FINISH_LEADING_ZEROS = 0x3F;
    constexpr uint64_t VALUE_FINISH_SIGNIFICANT_BITS = 0x3F;

    uint8_t leadingZeros(uint64_t v) {
        uint64_t mask = 0x8000000000000000;
        uint8_t ret = 0;
        while (ret < 64 && (v & mask) == 0) {
            mask >>= 1;
            ret++;
        }
        return ret;
    }

    uint8_t trailingZeros(uint64_t v) {
        uint64_t mask = 0x0000000000000001;
        uint8_t ret = 0;
        while (ret < 64 && (v & mask) == 0) {
            mask <<= 1;
            ret++;
        }
        return ret;
    }

    // Header is a first time aligned to 2 hours window.
    //
    // We need header, because it helps us to deal with a case when `finish`
    // was called without `compress`
    uint64_t getHeaderFromTimestamp(uint64_t first_time) {
        auto seconds_after_2_hour_window = first_time % (60 * 60 * 2);
        return first_time - seconds_after_2_hour_window;
    }

    template<typename T>
    class CompressorBase {
    public:
        explicit CompressorBase(std::shared_ptr<BitWriter> bw) : bw_(std::move(bw)), first_compressed_(false) {}

        virtual ~CompressorBase() = default;

        void compressFirst(T entity) {
            compressFirstInner(entity);
            first_compressed_ = true;
        }

        virtual void compressFirstInner(T) = 0;

        virtual void compressNonFirst(T) = 0;

        void compress(T entity) {
            if (first_compressed_) {
                compressNonFirst(entity);
            } else {
                compressFirst(entity);
            }
        }

        virtual void finish() = 0;

    protected:
        std::shared_ptr<BitWriter> bw_;
        bool first_compressed_;
    };

    class TimestampsCompressor : public CompressorBase<uint64_t> {
    public:
        explicit TimestampsCompressor(std::shared_ptr<BitWriter> bw) : CompressorBase(std::move(bw)), header_(0) {}

        void compressFirstInner(uint64_t t) override {
            header_ = getHeaderFromTimestamp(t);
            bw_->writeBits(header_, 64);
            if (t - header_ < 0) {
                Y_ABORT("First time passed for compression is less than header.");
            }
            int64_t delta = static_cast<int64_t>(t) - static_cast<int64_t>(header_);
            t_ = t;
            t_delta_ = delta;
            bw_->writeBits(delta, TIMESTAMP_FIRST_DELTA_BITS);
        }

        void compressNonFirst(uint64_t t) override {
            auto delta = static_cast<int64_t>(t) - static_cast<int64_t>(t_);
            int64_t dod = delta - t_delta_;

            t_ = t;
            t_delta_ = delta;

            if (dod == 0) {
                bw_->writeBit(false);
            } else if (-63 <= dod && dod <= 64) {
                bw_->writeBits(0x02, 2);
                writeInt64Bits(dod, 7);
            } else if (-255 <= dod && dod <= 256) {
                bw_->writeBits(0x06, 3);
                writeInt64Bits(dod, 9);
            } else if (-2047 <= dod && dod <= 2048) {
                bw_->writeBits(0x0E, 4);
                writeInt64Bits(dod, 12);
            } else {
                bw_->writeBits(0x0F, 4);
                writeInt64Bits(dod, 64);
            }
        }

        void finish() override {
            if (!first_compressed_) {
                bw_->writeBits(TIMESTAMP_FINISH_FIRST_UNCOMPRESSED_DELTA, TIMESTAMP_FIRST_DELTA_BITS);
                bw_->writeBits(0, 64);
                bw_->flush(false);
                return;
            }

            // 0x0F           = 00001111 -> 1111 (cutted).
            bw_->writeBits(0x0F, 4);
            // 0xFFFFFFFF     = 11111111 11111111 11111111 11111111
            bw_->writeBits(TIMESTAMP_FINISH_DOD, 64);
            bw_->writeBit(false);
            bw_->flush(false);
        }

    private:
        void writeInt64Bits(int64_t i, int nbits) {
            uint64_t u = 0;
            if (i >= 0 || nbits >= 64) {
                u = static_cast<uint64_t>(i);
            } else {
                u = static_cast<uint64_t>((1 << nbits) + i);
            }
            bw_->writeBits(u, int(nbits));
        }

        // Header bits.
        uint64_t header_;
        // Last time passed for compression.
        uint64_t t_ = 0;
        // 1.) In case first (time, value) pair passed after header, find delta with header time.
        // 2.) Otherwise, last time delta with new passed time and `t_`.
        int64_t t_delta_ = 0;
    };

    class ValuesCompressor : public CompressorBase<uint64_t> {
    public:
        explicit ValuesCompressor(std::shared_ptr<BitWriter> bw) : CompressorBase(std::move(bw)),
                                                                   leading_zeros_(INT8_MAX) {}

        void compressFirstInner(uint64_t v) override {
            value_ = v;
            bw_->writeBits(value_, 64);
            // Write additional `true` bit indicating that written value
            // is a value itself and not a `VALUE_FINISH_FIRST_UNCOMPRESSED_FLAG`.
            bw_->writeBit(true);
        }

        void compressNonFirst(uint64_t v) override {
            uint64_t xor_val = value_ ^ v;
            value_ = v;

            if (xor_val == 0) {
                bw_->writeBit(false);
                return;
            }

            uint8_t leading_zeros_val = leadingZeros(xor_val);
            uint8_t trailing_zeros_val = trailingZeros(xor_val);

            bw_->writeBit(true);

            if (leading_zeros_ <= leading_zeros_val && trailing_zeros_ <= trailing_zeros_val) {
                bw_->writeBit(false);
                int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
                bw_->writeBits(xor_val >> trailing_zeros_, significant_bits);
                return;
            }

            leading_zeros_ = leading_zeros_val;
            trailing_zeros_ = trailing_zeros_val;

            bw_->writeBit(true);
            bw_->writeBits(leading_zeros_, 6);
            int significant_bits = 64 - leading_zeros_ - trailing_zeros_;
            bw_->writeBits(static_cast<uint64_t>(significant_bits), 6);
            bw_->writeBits(xor_val >> trailing_zeros_val, significant_bits);
        }

        void finish() override {
            if (!first_compressed_) {
                bw_->writeBits(VALUE_FINISH_FIRST_UNCOMPRESSED_FLAG, 64);
                // Write additional `false` bit indicating that written value is
                // a `VALUE_FINISH_FIRST_UNCOMPRESSED_FLAG` ant not a value itself.
                bw_->writeBit(false);
                bw_->flush(false);
                return;
            }

            bw_->writeBit(true);
            bw_->writeBit(true);

            // 0x3F = 00111111 -> 111111 (cutted).
            bw_->writeBits(VALUE_FINISH_LEADING_ZEROS, 6);
            bw_->writeBits(VALUE_FINISH_SIGNIFICANT_BITS, 6);
            bw_->flush(false);
        }

    private:
        uint8_t leading_zeros_ = 0;
        uint8_t trailing_zeros_ = 0;
        // Last value passed for compression.
        uint64_t value_ = 0;
    };

    // Diff from initial article implementation:
    // 1.) Leading zeroes are encoded and decoded as 6 bits and not as 5 (as it's done in the article).
    // 2.) Max DOD encoded as 64 bits and not as 32.
    class PairsCompressor : public CompressorBase<std::pair<uint64_t, uint64_t>> {
    public:
        explicit PairsCompressor(const std::shared_ptr<BitWriter> &bw) : CompressorBase(bw), compressor_ts_(bw),
                                                                         compressor_value_(bw) {}

        void compressFirstInner(std::pair<uint64_t, uint64_t> entity) override {
            auto [t, v] = entity;
            compressor_ts_.compressFirst(t);
            compressor_value_.compressFirst(v);
        }

        void compressNonFirst(std::pair<uint64_t, uint64_t> entity) override {
            auto [t, v] = entity;
            compressor_ts_.compressNonFirst(t);
            compressor_value_.compressNonFirst(v);
        }

        void finish() override {
            compressor_ts_.finish();
        }

    private:
        TimestampsCompressor compressor_ts_;
        ValuesCompressor compressor_value_;
    };
}