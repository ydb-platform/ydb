#pragma once
#include "defs.h"

namespace NKikimrProto {
    class TLogoBlobID;
}

namespace NKikimr {

    struct TLogoBlobID {
        static const ui32 MaxChannel = 255ul;
        static const ui32 MaxBlobSize = 67108863ul;
        static const ui32 MaxCookie = 16777215ul;
        static const ui32 MaxPartId = 15ul;
        static const ui32 MaxCrcMode = 3ul;

        static constexpr size_t BinarySize = 3 * sizeof(ui64);

        TLogoBlobID()
        {
            Set(0, 0, 0, 0, 0, 0, 0, 0);
        }

        explicit TLogoBlobID(const TLogoBlobID &source, ui32 partId)
        {
            Y_DEBUG_ABORT_UNLESS(partId < 16);
            Raw.X[0] = source.Raw.X[0];
            Raw.X[1] = source.Raw.X[1];
            Raw.X[2] = (source.Raw.X[2] & 0xFFFFFFFFFFFFFFF0ull) | partId;
        }

        explicit TLogoBlobID(ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie)
        {
            Set(tabletId, generation, step, channel, blobSize, cookie, 0, 0);
        }

        explicit TLogoBlobID(ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie, ui32 partId)
        {
            Y_DEBUG_ABORT_UNLESS(partId != 0);
            Set(tabletId, generation, step, channel, blobSize, cookie, partId, 0);
        }

        explicit TLogoBlobID(ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie,
                ui32 partId, ui32 crcMode)
        {
            Set(tabletId, generation, step, channel, blobSize, cookie, partId, crcMode);
        }

        explicit TLogoBlobID(ui64 raw1, ui64 raw2, ui64 raw3)
        {
            Raw.X[0] = raw1;
            Raw.X[1] = raw2;
            Raw.X[2] = raw3;
        }

        explicit TLogoBlobID(const ui64 raw[3]) {
            memcpy(Raw.X, raw, 3 * sizeof(ui64));
        }

        static TLogoBlobID PrevFull(const TLogoBlobID& id, ui32 size) {
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

        static TLogoBlobID Make(ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie,
                ui32 crcMode) {
            TLogoBlobID id;
            id.Set(tabletId, generation, step, channel, blobSize, cookie, 0, crcMode);
            return id;
        }

        ui32 Hash() const {
            const ui64 x1 = 0x001DFF3D8DC48F5Dull * (Raw.X[0] & 0xFFFFFFFFull);
            const ui64 x2 = 0x179CA10C9242235Dull * (Raw.X[0] >> 32);
            const ui64 x3 = 0x0F530CAD458B0FB1ull * (Raw.X[1] & 0xFFFFFFFFull);
            const ui64 x4 = 0xB5026F5AA96619E9ull * (Raw.X[1] >> 32);
            const ui64 x5 = 0x5851F42D4C957F2Dull * (Raw.X[2] >> 32);

            const ui64 sum = 0x06C9C021156EAA1Full + x1 + x2 + x3 + x4 + x5;

            return (sum >> 32);
        }

        ui64 TabletID() const { return Raw.N.TabletID; }
        ui32 Generation() const { return Raw.N.Generation; }
        ui32 Step() const { return (Raw.N.StepR1 << 8) | Raw.N.StepR2; }
        ui32 Channel() const { return Raw.N.Channel; }
        ui32 BlobSize() const { return Raw.N.BlobSize; }
        ui32 Cookie() const { return Raw.N.Cookie; }
        ui32 PartId() const { return Raw.N.PartId; }
        ui32 CrcMode() const { return Raw.N.CrcMode; }

        const ui64* GetRaw() const { return Raw.X; }

        void ToBinary(void *data) const {
            ui64 *x = static_cast<ui64*>(data);
            x[0] = HostToInet(Raw.X[0]);
            x[1] = HostToInet(Raw.X[1]);
            x[2] = HostToInet(Raw.X[2]);
        }

        TString AsBinaryString() const {
            std::array<char, BinarySize> data;
            ToBinary(data.data());
            return TString(data.data(), data.size());
        }

        static TLogoBlobID FromBinary(const void *data) {
            const ui64 *x = static_cast<const ui64*>(data);
            ui64 arr[3] = {InetToHost(x[0]), InetToHost(x[1]), InetToHost(x[2])};
            return TLogoBlobID(arr);
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
            ::Save(out, Raw.X);
        }

        void Load(IInputStream *in) {
            ::Load(in, Raw.X);
        }

        // Returns -1 if *this < x, 0 if *this == x, 1 if *this > x
        int Compare(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return
                 r1[0] != r2[0] ? (r1[0] < r2[0] ? -1 : 1) :
                 r1[1] != r2[1] ? (r1[1] < r2[1] ? -1 : 1) :
                 r1[2] != r2[2] ? (r1[2] < r2[2] ? -1 : 1) : 0;
        }

        bool operator<(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return
                r1[0] != r2[0] ? r1[0] < r2[0] :
                r1[1] != r2[1] ? r1[1] < r2[1] :
                r1[2] < r2[2];
        }

        bool operator>(const TLogoBlobID &x) const {
            return (x < *this);
        }

        bool operator<=(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return
                r1[0] != r2[0] ? r1[0] < r2[0] :
                r1[1] != r2[1] ? r1[1] < r2[1] :
                r1[2] <= r2[2];
        }

        bool operator>=(const TLogoBlobID &x) const {
            return (x <= *this);
        }

        bool operator==(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return
                r1[2] == r2[2] && r1[1] == r2[1] && r1[0] == r2[0];
        }

        bool operator!=(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return
                r1[2] != r2[2] || r1[1] != r2[1] || r1[0] != r2[0];
        }

        explicit operator bool() const noexcept {
            return (Raw.N.TabletID != 0);
        }

        bool IsValid() const noexcept {
            return (Raw.N.TabletID != 0);
        }

        // compares only main part (without part id)
        bool IsSameBlob(const TLogoBlobID &x) const {
            const ui64 *r1 = GetRaw();
            const ui64 *r2 = x.GetRaw();

            return r1[0] == r2[0] && r1[1] == r2[1] && (r1[2] & 0xFFFFFFFFFFFFFFF0ull) == (r2[2] & 0xFFFFFFFFFFFFFFF0ull);
        }

        TLogoBlobID FullID() const {
            return TLogoBlobID(*this, 0);
        }
    private:
        union {
            struct {
                ui64 TabletID; // 8

                ui64 StepR1 : 24; // 8
                ui64 Generation : 32;
                ui64 Channel : 8;

                ui64 PartId : 4; // 8
                ui64 BlobSize : 26;
                ui64 CrcMode : 2;

                ui64 Cookie : 24;
                ui64 StepR2 : 8;
            } N;

            ui64 X[3];
        } Raw;

        void Set(ui64 tabletId, ui32 generation, ui32 step, ui32 channel, ui32 blobSize, ui32 cookie, ui32 partId,
                ui32 crcMode) {
            Y_DEBUG_ABORT_UNLESS(channel <= MaxChannel);
            Y_ABORT_UNLESS(blobSize <= MaxBlobSize);
            Y_DEBUG_ABORT_UNLESS(cookie <= MaxCookie);
            Y_DEBUG_ABORT_UNLESS(partId <= MaxPartId);
            Y_ABORT_UNLESS(crcMode <= MaxCrcMode);

            Raw.N.TabletID = tabletId;
            Raw.N.Generation = generation;

            Raw.N.StepR1 = (step & 0xFFFFFF00ull) >> 8;
            Raw.N.StepR2 = (step & 0x000000FFull);

            Raw.N.Channel = channel;
            Raw.N.Cookie = cookie;
            Raw.N.BlobSize = blobSize;
            Raw.N.CrcMode = crcMode;
            Raw.N.PartId = partId;
        }

    public:
        struct THash {
            ui32 operator()(const TLogoBlobID &id) const noexcept {
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
