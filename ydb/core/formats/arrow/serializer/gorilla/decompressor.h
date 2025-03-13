#pragma once

#include <iostream>
#include <fstream>
#include <optional>
#include <cstdint>
#include <stdexcept>
#include <cmath>

#include <ydb/library/conclusion/result.h>
#include "bit_reader.h"

namespace NKikimr::NArrow::NSerialization::NGorilla {
    template<typename T>
    class DecompressorBase {
    public:
        explicit DecompressorBase(std::shared_ptr<BitReader> bw) : br_(std::move(bw)), first_decompressed_(false) {}

        virtual ~DecompressorBase() = default;

        std::optional<T> next() {
            if (first_decompressed_) {
                return decompressNonFirst();
            } else {
                return {decompressFirst() };
            }
        }

        std::optional<T> decompressFirst() {
            auto res = decompressFirstInner();
            if (res) {
                first_decompressed_ = true;
            }
            return res;
        }

    private:
        virtual std::optional<T> decompressFirstInner() = 0;

        virtual std::optional<T> decompressNonFirst() = 0;

    protected:
        std::shared_ptr<BitReader> br_;
        bool first_decompressed_ = true;
    };

    class TimestampsDecompressor : public DecompressorBase<uint64_t> {
    public:
        explicit TimestampsDecompressor(std::shared_ptr<BitReader> br) : DecompressorBase(std::move(br)) {}

        [[nodiscard]] uint64_t getHeader() const {
            return header_;
        }

        std::optional<uint64_t> decompressFirstInner() override {
            header_ = br_->readBits(64);

            // In case there were no data to compress, header will be missing.
            // Instead of it would be a finish flag.
            uint64_t first_bits_flag = header_ >> (64 - TIMESTAMP_FIRST_DELTA_BITS);
            if (first_bits_flag == TIMESTAMP_FINISH_FIRST_UNCOMPRESSED_DELTA) {
                return std::nullopt;
            }

            uint64_t delta_u64 = br_->readBits(TIMESTAMP_FIRST_DELTA_BITS);
            int64_t delta = *reinterpret_cast<int64_t *>(&delta_u64);

            t_delta_ = delta;
            t_ = header_ + t_delta_;
            return {t_};
        }

        std::optional<uint64_t> decompressNonFirst() override {
            uint8_t n = dodTimestampBits();

            if (n == 0) {
                t_ += t_delta_;
                return t_;
            }

            uint64_t bits = br_->readBits(n);

            if (n == 64 && bits == TIMESTAMP_FINISH_DOD) {
                return std::nullopt;
            }

            int64_t bits_int64 = *reinterpret_cast<int64_t *>(&bits);
            int64_t dod = bits_int64;
            if (n != 64 && (1 << (n - 1)) < bits_int64) {
                dod = bits_int64 - (1 << n);
            }

            t_delta_ += dod;
            t_ += t_delta_;
            return t_;
        }

    private:
        uint8_t dodTimestampBits() {
            uint8_t dod = 0;
            for (int i = 0; i < 4; i++) {
                dod <<= 1;
                bool bit = br_->readBit();
                if (bit) {
                    dod |= 1;
                } else {
                    break;
                }
            }

            if (dod == 0x00) {
                // Case of dod == 0.
                return 0;
            } else if (dod == 0x02) {
                // Case of dod == 10.
                return 7;
            } else if (dod == 0x06) {
                return 9;
            } else if (dod == 0x0E) {
                return 12;
            } else if (dod == 0x0F) {
                return 64;
            } else {
                Y_ABORT("Invalid bit header for bit length to read.");
            }
        }

        uint64_t header_ = 0;
        uint64_t t_ = 0;
        int64_t t_delta_ = 0;
    };

    class ValuesDecompressor : public DecompressorBase<uint64_t> {
    public:
        explicit ValuesDecompressor(std::shared_ptr<BitReader> br) : DecompressorBase(std::move(br)) {}

        std::optional<uint64_t> decompressFirstInner() override {
            uint64_t value = br_->readBits(64);
            bool finish_flag_bit = br_->readBit();

            if (value == VALUE_FINISH_FIRST_UNCOMPRESSED_FLAG && !finish_flag_bit) {
                return std::nullopt;
            }

            return {value};
        }

        std::optional<uint64_t> decompressNonFirst() override {
            uint8_t read = 0;
            for (int i = 0; i < 2; i++) {
                bool bit = br_->readBit();
                if (bit) {
                    read <<= 1;
                    read++;
                } else {
                    break;
                }
            }

            if (read == 0x1 || read == 0x3) {
                if (read == 0x3) {
                    uint8_t leading_zeroes = br_->readBits(6);
                    uint8_t significant_bits = br_->readBits(6);

                    if (leading_zeroes == VALUE_FINISH_LEADING_ZEROS && significant_bits == VALUE_FINISH_SIGNIFICANT_BITS) {
                        return std::nullopt;
                    }

                    if (significant_bits == 0) {
                        significant_bits = 64;
                    }
                    leading_zeros_ = leading_zeroes;
                    trailing_zeros_ = 64 - significant_bits - leading_zeros_;
                }

                uint64_t value_bits = br_->readBits(64 - leading_zeros_ - trailing_zeros_);
                value_bits <<= trailing_zeros_;
                value_ ^= value_bits;
            }

            return value_;
        }

    private:
        uint8_t leading_zeros_ = 0;
        uint8_t trailing_zeros_ = 0;
        uint64_t value_ = 0;
    };

    class PairsDecompressor : public DecompressorBase<std::pair<uint64_t, uint64_t>> {
    public:
    explicit PairsDecompressor(const std::shared_ptr<BitReader> &br) : DecompressorBase(br), decompressor_ts_(br),
                                                                       decompressor_value_(br) {}

    [[nodiscard]] uint64_t getHeader() const {
        return decompressor_ts_.getHeader();
    }

    private:
        [[nodiscard]] std::optional<std::pair<uint64_t, uint64_t>> decompressFirstInner() override {
            auto t = decompressor_ts_.decompressFirst();
            if (!t) {
                return std::nullopt;
            }
            auto v = decompressor_value_.decompressFirst();
            if (!v) {
                return std::nullopt;
            }

            return {std::make_pair(*t, *v)};
        }

        std::optional<std::pair<uint64_t, uint64_t>> decompressNonFirst() override {
            std::optional<uint64_t> t = decompressor_ts_.decompressNonFirst();
            if (!t) {
                return std::nullopt;
            }
            std::optional<uint64_t> v = decompressor_value_.decompressNonFirst();
            if (!v) {
                return std::nullopt;
            }
            return {std::make_pair(*t, *v)};
        }

        TimestampsDecompressor decompressor_ts_;
        ValuesDecompressor decompressor_value_;
    };
}
