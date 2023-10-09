#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NKikimr {

    // Unique Lexicographically sortable Identifier
    // Based on https://github.com/oklog/ulid
    struct TULID {
        // 48 bit time (milliseconds)
        // 80 bit random value
        ui8 Data[16];

        // Note: default constructor leaves data uninitialized!
        TULID() = default;

        // Explicit constructor from binary data
        explicit TULID(TStringBuf buf) noexcept {
            SetBinary(buf);
        }

        bool operator<(const TULID& rhs) const { return ::memcmp(Data, rhs.Data, 16) < 0; }
        bool operator>(const TULID& rhs) const { return ::memcmp(Data, rhs.Data, 16) > 0; }
        bool operator==(const TULID& rhs) const { return ::memcmp(Data, rhs.Data, 16) == 0; }
        bool operator!=(const TULID& rhs) const { return ::memcmp(Data, rhs.Data, 16) != 0; }

        /**
         * Returns timestamp in milliseconds
         */
        ui64 GetTimeMs() const {
            return (
                (ui64(Data[0]) << 40) |
                (ui64(Data[1]) << 32) |
                (ui64(Data[2]) << 24) |
                (ui64(Data[3]) << 16) |
                (ui64(Data[4]) << 8) |
                (ui64(Data[5]))
            );
        }

        /**
         * Returns timestamp of ULID generation
         */
        TInstant GetTime() const {
            return TInstant::MilliSeconds(GetTimeMs());
        }

        ui16 GetRandomHi() const {
            return (ui16(Data[6]) << 8) | ui16(Data[7]);
        }

        ui64 GetRandomLo() const {
            return (
                (ui64(Data[8]) << 56) |
                (ui64(Data[9]) << 48) |
                (ui64(Data[10]) << 40) |
                (ui64(Data[11]) << 32) |
                (ui64(Data[12]) << 24) |
                (ui64(Data[13]) << 16) |
                (ui64(Data[14]) << 8) |
                (ui64(Data[15]))
            );
        }

        /**
         * Sets time in milliseconds
         */
        void SetTimeMs(ui64 ms) {
            Data[0] = ms >> 40;
            Data[1] = ms >> 32;
            Data[2] = ms >> 24;
            Data[3] = ms >> 16;
            Data[4] = ms >> 8;
            Data[5] = ms;
        }

        void SetRandomHi(ui16 r) {
            Data[6] = r >> 8;
            Data[7] = r;
        }

        void SetRandomLo(ui64 r) {
            Data[8] = r >> 56;
            Data[9] = r >> 48;
            Data[10] = r >> 40;
            Data[11] = r >> 32;
            Data[12] = r >> 24;
            Data[13] = r >> 16;
            Data[14] = r >> 8;
            Data[15] = r;
        }

        TStringBuf GetBuffer() const noexcept {
            const char* p = reinterpret_cast<const char*>(Data);
            return TStringBuf(p, p + 16);
        }

        static TULID Min() noexcept {
            TULID res;
            ::memset(res.Data, 0, 16);
            return res;
        }

        static TULID Max() noexcept {
            TULID res;
            ::memset(res.Data, 255, 16);
            return res;
        }

        /**
         * Constructs ULID from its binary representation
         */
        static TULID FromBinary(TStringBuf buf) noexcept {
            TULID res;
            res.SetBinary(buf);
            return res;
        }

        /**
         * Sets binary representation of ULID that must be exactly 16 bytes
         */
        void SetBinary(TStringBuf buf) noexcept {
            Y_ABORT_UNLESS(buf.size() == 16, "ULID binary representation must have exactly 16 bytes");
            ::memcpy(Data, buf.data(), 16);
        }

        /**
         * Returns a binary representation of ULID with exactly 16 bytes
         */
        TString ToBinary() const {
            return TString(GetBuffer());
        }

        /**
         * Returns a human-readable string representation of ULID
         */
        TString ToString() const;

        /**
         * Parses a human-readable string representation of ULID
         *
         * Returns true if the given buffer is a valid ULID string
         */
        bool ParseString(TStringBuf buf) noexcept;
    };

    enum class EULIDMode {
        Monotonic,
        Random,
    };

    /**
     * Generator for unique TULID identifiers
     */
    class TULIDGenerator {
    public:
        TULIDGenerator() = default;

        /**
         * Returns the last generated ULID
         */
        TULID Last() const;

        /**
         * Returns the next unique ULID in the given time instance
         */
        TULID Next(TInstant now, EULIDMode mode = EULIDMode::Monotonic);

        /**
         * Returns the next unique ULID
         */
        TULID Next(EULIDMode mode = EULIDMode::Monotonic);

    private:
        ui64 LastTimeMs = 0;
        ui64 LastRandomLo = 0;
        ui16 LastRandomHi = 0;
    };

} // namespace NKikimr

template<>
struct THash<NKikimr::TULID> {
    inline size_t operator()(const NKikimr::TULID& id) const noexcept {
        return THash<TStringBuf>()(id.GetBuffer());
    }
};

namespace std {
    template<>
    struct hash<::NKikimr::TULID> {
        inline size_t operator()(const ::NKikimr::TULID& id) const noexcept {
            return ::THash<::NKikimr::TULID>()(id);
        }
    };
}
