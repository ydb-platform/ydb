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
        TActorId() noexcept;
        explicit TActorId(ui32 nodeId, ui32 poolId, ui64 localId, ui32 hint) noexcept;
        explicit TActorId(ui32 nodeId, const TStringBuf& x);
        explicit TActorId(ui64 x1, ui64 x2) noexcept;

        explicit operator bool() const noexcept;

        ui64 LocalId() const noexcept;
        ui32 Hint() const noexcept;
        ui32 NodeId() const noexcept;

        bool IsService() const noexcept;
        TStringBuf ServiceId() const noexcept;
        static ui32 PoolIndex(ui32 nodeid) noexcept;
        ui32 PoolID() const noexcept;

        ui64 RawX1() const noexcept;
        ui64 RawX2() const noexcept;

        bool operator<(const TActorId& x) const noexcept;
        bool operator!=(const TActorId& x) const noexcept;
        bool operator==(const TActorId& x) const noexcept;

        ui64 Hash() const noexcept;
        ui32 Hash32() const noexcept;

        struct THash {
            ui64 operator()(const TActorId& actorId) const noexcept;
        };
        struct THash32 {
            ui64 operator()(const TActorId& actorId) const noexcept;
        };

        struct TOrderedCmp {
            bool operator()(const TActorId &left, const TActorId &right) const noexcept;
        };

        // Attention! The string representation of the actor identifier does not
        // contain all the necessary information to restore it using the Parse()
        // method.
        // Use the ToString() and Out() methods only for debugging purposes.
        TString ToString() const;
        void Out(IOutputStream& o) const;
        // Attention! The string representation of the actor identifier does not
        // contain all the necessary information for restoring it using Parse().
        // Please do not use it.
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
