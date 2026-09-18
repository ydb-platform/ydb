#pragma once
#include "defs.h"

#include <google/protobuf/message.h>

#include <array>
#include <cstring>

namespace NKikimrProto {
    class TLogoBlobID;
}

namespace NKikimr {

    struct TLogoBlobID {
        static constexpr ui32 MaxChannel = 255ul;
        static constexpr ui32 MaxBlobSize = 67108863ul;
        static constexpr ui32 MaxCookie = 16777215ul;
        static constexpr ui32 MaxPartId = 15ul;
        static constexpr ui32 MaxCrcMode = 3ul;
        static constexpr size_t BinarySize = 3 * sizeof(ui64);

        constexpr TLogoBlobID() noexcept
            : Raw{}
        {}

        constexpr explicit TLogoBlobID(const TLogoBlobID &source, ui32 partId) noexcept
            : Raw{source.Raw[0], source.Raw[1], (source.Raw[2] & ~PartIdMask) | (partId & PartIdMask)}
        {
            Y_DEBUG_ABORT_UNLESS(partId <= MaxPartId);
        }

        constexpr explicit TLogoBlobID(
                ui64 tabletId,
                ui32 generation,
                ui32 step,
                ui32 channel,
                ui32 blobSize,
                ui32 cookie) noexcept
            : TLogoBlobID(tabletId, generation, step, channel, blobSize, cookie, 0, 0)
        {}

        constexpr explicit TLogoBlobID(
                ui64 tabletId,
                ui32 generation,
                ui32 step,
                ui32 channel,
                ui32 blobSize,
                ui32 cookie,
                ui32 partId) noexcept
            : TLogoBlobID(tabletId, generation, step, channel, blobSize, cookie, partId, 0)
        {
            Y_DEBUG_ABORT_UNLESS(partId != 0);
        }

        constexpr explicit TLogoBlobID(
                ui64 tabletId,
                ui32 generation,
                ui32 step,
                ui32 channel,
                ui32 blobSize,
                ui32 cookie,
                ui32 partId,
                ui32 crcMode) noexcept
            : Raw{
                tabletId,
                (static_cast<ui64>(channel & MaxChannel) << ChannelShift)
                    | (static_cast<ui64>(generation) << GenerationShift)
                    | (step >> 8),
                (static_cast<ui64>(step & 0xFF) << StepLowShift)
                    | (static_cast<ui64>(cookie & MaxCookie) << CookieShift)
                    | (static_cast<ui64>(crcMode) << CrcModeShift)
                    | (static_cast<ui64>(blobSize) << BlobSizeShift)
                    | (partId & MaxPartId)}
        {
            Y_DEBUG_ABORT_UNLESS(channel <= MaxChannel);
            Y_ABORT_UNLESS(blobSize <= MaxBlobSize);
            Y_DEBUG_ABORT_UNLESS(cookie <= MaxCookie);
            Y_DEBUG_ABORT_UNLESS(partId <= MaxPartId);
            Y_ABORT_UNLESS(crcMode <= MaxCrcMode);
        }

        constexpr explicit TLogoBlobID(ui64 raw1, ui64 raw2, ui64 raw3) noexcept
            : Raw{raw1, raw2, raw3}
        {}

        constexpr explicit TLogoBlobID(const ui64 raw[3]) noexcept
            : Raw{raw[0], raw[1], raw[2]}
        {}

        static constexpr TLogoBlobID PrevFull(const TLogoBlobID& id, ui32 size) noexcept {
            Y_ABORT_UNLESS(!id.PartId());
            ui64 tablet = id.TabletID();
            ui32 channel = id.Channel();
            ui32 generation = id.Generation();
            ui32 step = id.Step();
            ui32 cookie = id.Cookie();
            // decrement tuple and check for overflow condition
            const bool overflow = ((--cookie &= MaxCookie) == MaxCookie) && (--step == Max<ui32>()) &&
                (--generation == Max<ui32>()) && ((--channel &= MaxChannel) == MaxChannel) &&
                (--tablet == Max<ui64>());
            Y_ABORT_UNLESS(!overflow);
            return TLogoBlobID(tablet, generation, step, channel, size, cookie);
        }

        static constexpr TLogoBlobID Make(
                ui64 tabletId,
                ui32 generation,
                ui32 step,
                ui32 channel,
                ui32 blobSize,
                ui32 cookie,
                ui32 crcMode) noexcept
        {
            return TLogoBlobID(tabletId, generation, step, channel, blobSize, cookie, 0, crcMode);
        }

        constexpr ui32 Hash() const noexcept {
            const ui64 x1 = 0x001DFF3D8DC48F5Dull * (Raw[0] & 0xFFFFFFFFull);
            const ui64 x2 = 0x179CA10C9242235Dull * (Raw[0] >> 32);
            const ui64 x3 = 0x0F530CAD458B0FB1ull * (Raw[1] & 0xFFFFFFFFull);
            const ui64 x4 = 0xB5026F5AA96619E9ull * (Raw[1] >> 32);
            const ui64 x5 = 0x5851F42D4C957F2Dull * (Raw[2] >> 32);

            const ui64 sum = 0x06C9C021156EAA1Full + x1 + x2 + x3 + x4 + x5;

            return (sum >> 32);
        }

        constexpr ui64 TabletID() const noexcept { return Raw[0]; }
        constexpr ui32 Generation() const noexcept { return (Raw[1] >> GenerationShift) & GenerationMask; }
        constexpr ui32 Step() const noexcept {
            return ((Raw[1] & StepHighMask) << 8) | (Raw[2] >> StepLowShift);
        }
        constexpr ui32 Channel() const noexcept { return Raw[1] >> ChannelShift; }
        constexpr ui32 BlobSize() const noexcept { return (Raw[2] >> BlobSizeShift) & MaxBlobSize; }
        constexpr ui32 Cookie() const noexcept { return (Raw[2] >> CookieShift) & MaxCookie; }
        constexpr ui32 PartId() const noexcept { return Raw[2] & PartIdMask; }
        constexpr ui32 CrcMode() const noexcept { return (Raw[2] >> CrcModeShift) & MaxCrcMode; }

        constexpr const ui64* GetRaw() const noexcept { return Raw.data(); }

        void ToBinary(void *data) const {
            const std::array<ui64, 3> x = {
                HostToInet(Raw[0]),
                HostToInet(Raw[1]),
                HostToInet(Raw[2]),
            };
            memcpy(data, x.data(), sizeof(x));
        }

        TString AsBinaryString() const {
            std::array<char, BinarySize> data;
            ToBinary(data.data());
            return TString(data.data(), data.size());
        }

        static TLogoBlobID FromBinary(const void *data) {
            std::array<ui64, 3> x;
            memcpy(x.data(), data, sizeof(x));
            return TLogoBlobID(InetToHost(x[0]), InetToHost(x[1]), InetToHost(x[2]));
        }

        static TLogoBlobID FromBinary(TStringBuf data) {
            Y_ABORT_UNLESS(data.size() == BinarySize);
            return FromBinary(data.data());
        }

        TString ToString() const;
        void Out(IOutputStream &o) const;
        static bool Parse(TLogoBlobID &out, const TString &buf, TString &errorExplanation);
        static void Out(IOutputStream &o, const TVector<TLogoBlobID> &vec);

        void Save(IOutputStream *out) const {
            ::Save(out, Raw);
        }

        void Load(IInputStream *in) {
            ::Load(in, Raw);
        }

        // Returns -1 if *this < x, 0 if *this == x, 1 if *this > x
        constexpr int Compare(const TLogoBlobID &x) const noexcept {
            const auto result = *this <=> x;
            return result < 0 ? -1 : result > 0 ? 1 : 0;
        }

        constexpr auto operator<=>(const TLogoBlobID&) const noexcept = default;

        // The defaulted <=> costs an extra branch per comparison on an array
        // member; sorting is hot enough to spell out the short-circuiting form.
        constexpr bool operator<(const TLogoBlobID &x) const noexcept {
            return Raw[0] != x.Raw[0] ? Raw[0] < x.Raw[0]
                 : Raw[1] != x.Raw[1] ? Raw[1] < x.Raw[1]
                 : Raw[2] < x.Raw[2];
        }

        constexpr explicit operator bool() const noexcept {
            return (TabletID() != 0);
        }

        constexpr bool IsValid() const noexcept {
            return (TabletID() != 0);
        }

        // compares only main part (without part id)
        constexpr bool IsSameBlob(const TLogoBlobID &x) const noexcept {
            return Raw[0] == x.Raw[0]
                && Raw[1] == x.Raw[1]
                && (Raw[2] & ~PartIdMask) == (x.Raw[2] & ~PartIdMask);
        }

        constexpr TLogoBlobID FullID() const noexcept {
            return TLogoBlobID(*this, 0);
        }
    private:
        static constexpr ui64 PartIdMask = MaxPartId;
        static constexpr ui64 GenerationMask = 0xFFFFFFFFull;
        static constexpr ui64 StepHighMask = 0xFFFFFFull;
        static constexpr ui32 BlobSizeShift = 4;
        static constexpr ui32 GenerationShift = 24;
        static constexpr ui32 CrcModeShift = 30;
        static constexpr ui32 CookieShift = 32;
        static constexpr ui32 ChannelShift = 56;
        static constexpr ui32 StepLowShift = 56;

        std::array<ui64, 3> Raw;

    public:
        struct THash {
            constexpr ui32 operator()(const TLogoBlobID &id) const noexcept {
                return id.Hash();
            }
        };
    };

    static_assert(sizeof(TLogoBlobID) == 24, "expect sizeof(TLogoBlobID) == 24");

    struct TLogoBlob {
        TLogoBlobID Id;
        TString Buffer;

        TLogoBlob()
        {}

        TLogoBlob(const TLogoBlobID &id, const TString &buffer)
            : Id(id)
            , Buffer(buffer)
        {}
    };

    struct TLogoBlobRef {
        TLogoBlobID Id;
        ui32 Status;
        ui32 Shift;
        TString Buffer;

        explicit TLogoBlobRef(const TLogoBlobID &id, ui32 status, ui32 shift, const TString &buffer)
            : Id(id)
            , Status(status)
            , Shift(shift)
            , Buffer(buffer)
        {}
    };

    struct TLogoBlobRequest {
        TLogoBlobID Id;
        ui32 Shift;
        ui32 Size;

        TLogoBlobRequest(const TLogoBlobID &id, ui32 shift, ui32 sz)
            : Id(id)
            , Shift(shift)
            , Size(sz)
        {}
    };

    TLogoBlobID LogoBlobIDFromLogoBlobID(const NKikimrProto::TLogoBlobID &proto);
    void LogoBlobIDFromLogoBlobID(const TLogoBlobID &id, NKikimrProto::TLogoBlobID *proto);
    void LogoBlobIDVectorFromLogoBlobIDRepeated(
                TVector<TLogoBlobID> *to,
                const ::google::protobuf::RepeatedPtrField<NKikimrProto::TLogoBlobID> &proto);

    template<typename TIterator>
    void LogoBlobIDRepatedFromLogoBlobIDVector(
        ::google::protobuf::RepeatedPtrField<NKikimrProto::TLogoBlobID> *proto,
        TIterator begin, TIterator end)
    {
        proto->Reserve(end - begin);
        proto->Clear();
        while (begin != end) {
            LogoBlobIDFromLogoBlobID(*begin, proto->Add());
            ++begin;
        }
    }

    template<typename TContainer>
    void LogoBlobIDRepatedFromLogoBlobIDUniversal(
        ::google::protobuf::RepeatedPtrField<NKikimrProto::TLogoBlobID> *proto,
        TContainer& container)
    {
        auto begin = container.begin();
        auto end = container.end();
        proto->Reserve(container.size());
        proto->Clear();
        while (begin != end) {
            LogoBlobIDFromLogoBlobID(*begin, proto->Add());
            ++begin;
        }
    }
}

template<>
inline void Out<NKikimr::TLogoBlobID>(IOutputStream& o, const NKikimr::TLogoBlobID &x) {
    return x.Out(o);
}

template<>
inline void Out<TVector<NKikimr::TLogoBlobID>>(IOutputStream& out, const TVector<NKikimr::TLogoBlobID> &xvec) {
    return NKikimr::TLogoBlobID::Out(out, xvec);
}

template<>
struct THash<NKikimr::TLogoBlobID> {
    inline ui64 operator()(const NKikimr::TLogoBlobID& x) const noexcept {
        return x.Hash();
    }
};

template<> struct std::hash<NKikimr::TLogoBlobID> : THash<NKikimr::TLogoBlobID> {};

template<>
inline NKikimr::TLogoBlobID Min<NKikimr::TLogoBlobID>() noexcept {
    return {};
}

template<>
inline NKikimr::TLogoBlobID Max<NKikimr::TLogoBlobID>() noexcept {
    return NKikimr::TLogoBlobID(Max<ui64>(), Max<ui32>(), Max<ui32>(), NKikimr::TLogoBlobID::MaxChannel,
        NKikimr::TLogoBlobID::MaxBlobSize, NKikimr::TLogoBlobID::MaxCookie, NKikimr::TLogoBlobID::MaxPartId,
        NKikimr::TLogoBlobID::MaxCrcMode);
}
