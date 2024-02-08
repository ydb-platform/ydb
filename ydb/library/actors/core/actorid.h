#pragma once

#include "defs.h"
#include <util/stream/output.h> // for IOutputStream
#include <util/generic/hash.h>

namespace NActors {
    // used as global uniq address of actor
    // also could be used to transport service id (12 byte strings placed in hint-localid)
    // highest 1 bit of node - mark of service id
    // next 11 bits of node-id - pool id
    // next 20 bits - node id itself

    struct TActorId {
        static constexpr ui32 MaxServiceIDLength = 12;
        static constexpr ui32 MaxPoolID = 0x000007FF;
        static constexpr ui32 MaxNodeId = 0x000FFFFF;
        static constexpr ui32 PoolIndexShift = 20;
        static constexpr ui32 PoolIndexMask = MaxPoolID << PoolIndexShift;
        static constexpr ui32 ServiceMask = 0x80000000;
        static constexpr ui32 NodeIdMask = MaxNodeId;

    private:
        union {
            struct {
                ui64 LocalId;
                ui32 Hint;
                ui32 NodeId;
            } N;

            struct {
                ui64 X1;
                ui64 X2;
            } X;

            ui8 Buf[16];
        } Raw;

    public:
        TActorId() noexcept {
            Raw.X.X1 = 0;
            Raw.X.X2 = 0;
        }

        explicit TActorId(ui32 nodeId, ui32 poolId, ui64 localId, ui32 hint) noexcept {
            Y_DEBUG_ABORT_UNLESS(poolId <= MaxPoolID);
            Raw.N.LocalId = localId;
            Raw.N.Hint = hint;
            Raw.N.NodeId = nodeId | (poolId << PoolIndexShift);
        }

        explicit TActorId(ui32 nodeId, const TStringBuf& x) noexcept {
            Y_ABORT_UNLESS(x.size() <= MaxServiceIDLength, "service id is too long");
            Raw.N.LocalId = 0;
            Raw.N.Hint = 0;
            Raw.N.NodeId = nodeId | ServiceMask;
            memcpy(Raw.Buf, x.data(), x.size());
        }

        explicit TActorId(ui64 x1, ui64 x2) noexcept {
            Raw.X.X1 = x1;
            Raw.X.X2 = x2;
        }

        explicit operator bool() const noexcept {
            return Raw.X.X1 != 0 || Raw.X.X2 != 0;
        }

        ui64 LocalId() const noexcept {
            return Raw.N.LocalId;
        }

        ui32 Hint() const noexcept {
            return Raw.N.Hint;
        }

        ui32 NodeId() const noexcept {
            return Raw.N.NodeId & NodeIdMask;
        }

        bool IsService() const noexcept {
            return (Raw.N.NodeId & ServiceMask);
        }

        TStringBuf ServiceId() const noexcept {
            Y_DEBUG_ABORT_UNLESS(IsService());
            return TStringBuf((const char*)Raw.Buf, MaxServiceIDLength);
        }

        static ui32 PoolIndex(ui32 nodeid) noexcept {
            return ((nodeid & PoolIndexMask) >> PoolIndexShift);
        }

        ui32 PoolID() const noexcept {
            return PoolIndex(Raw.N.NodeId);
        }

        ui64 RawX1() const noexcept {
            return Raw.X.X1;
        }

        ui64 RawX2() const noexcept {
            return Raw.X.X2;
        }

        bool operator<(const TActorId& x) const noexcept {
            const ui64 s1 = Raw.X.X1;
            const ui64 s2 = Raw.X.X2;
            const ui64 x1 = x.Raw.X.X1;
            const ui64 x2 = x.Raw.X.X2;

            return (s1 != x1) ? (s1 < x1) : (s2 < x2);
        }

        bool operator!=(const TActorId& x) const noexcept {
            return Raw.X.X1 != x.Raw.X.X1 || Raw.X.X2 != x.Raw.X.X2;
        }

        bool operator==(const TActorId& x) const noexcept {
            return !(x != *this);
        }

        ui64 Hash() const noexcept {
            const ui32* x = (const ui32*)Raw.Buf;

            const ui64 x1 = x[0] * 0x001DFF3D8DC48F5Dull;
            const ui64 x2 = x[1] * 0x179CA10C9242235Dull;
            const ui64 x3 = x[2] * 0x0F530CAD458B0FB1ull;
            const ui64 x4 = x[3] * 0xB5026F5AA96619E9ull;

            const ui64 z1 = x1 + x2;
            const ui64 z2 = x3 + x4;

            const ui64 sum = 0x5851F42D4C957F2D + z1 + z2;

            return (sum >> 32) | (sum << 32);
        }

        ui32 Hash32() const noexcept {
            const ui32* x = (const ui32*)Raw.Buf;

            const ui64 x1 = x[0] * 0x001DFF3D8DC48F5Dull;
            const ui64 x2 = x[1] * 0x179CA10C9242235Dull;
            const ui64 x3 = x[2] * 0x0F530CAD458B0FB1ull;
            const ui64 x4 = x[3] * 0xB5026F5AA96619E9ull;

            const ui64 z1 = x1 + x2;
            const ui64 z2 = x3 + x4;

            const ui64 sum = 0x5851F42D4C957F2D + z1 + z2;

            return sum >> 32;
        }

        struct THash {
            ui64 operator()(const TActorId& actorId) const noexcept {
                return actorId.Hash();
            }
        };

        struct THash32 {
            ui64 operator()(const TActorId& actorId) const noexcept {
                return actorId.Hash();
            }
        };

        struct TOrderedCmp {
            bool operator()(const TActorId &left, const TActorId &right) const noexcept {
                Y_DEBUG_ABORT_UNLESS(!left.IsService() && !right.IsService(), "ordered compare works for plain actorids only");
                const ui32 n1 = left.NodeId();
                const ui32 n2 = right.NodeId();

                return (n1 != n2) ? (n1 < n2) : left.LocalId() < right.LocalId();
            }
        };

        TString ToString() const;
        void Out(IOutputStream& o) const;
        bool Parse(const char* buf, ui32 sz);
    };

    static_assert(sizeof(TActorId) == 16, "expect sizeof(TActorId) == 16");
    static_assert(MaxPools < TActorId::MaxPoolID); // current implementation of united pool has limit MaxPools on pool id
}

template <>
inline void Out<NActors::TActorId>(IOutputStream& o, const NActors::TActorId& x) {
    return x.Out(o);
}

template <>
struct THash<NActors::TActorId> {
    inline ui64 operator()(const NActors::TActorId& x) const {
        return x.Hash();
    }
};

template<> struct std::hash<NActors::TActorId> : THash<NActors::TActorId> {};
