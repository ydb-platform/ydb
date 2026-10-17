#include "actorid.h"
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NActors {

    TActorId::TActorId() noexcept {
        Raw.X.X1 = 0;
        Raw.X.X2 = 0;
    }

    TActorId::TActorId(ui32 nodeId, ui32 poolId, ui64 localId, ui32 hint) noexcept {
        Y_DEBUG_ABORT_UNLESS(poolId <= MaxPoolID);
        Raw.N.LocalId = localId;
        Raw.N.Hint = hint;
        Raw.N.NodeId = nodeId | (poolId << PoolIndexShift);
    }

    TActorId::TActorId(ui32 nodeId, const TStringBuf& x) {
        Y_ENSURE(x.size() <= MaxServiceIDLength, "service id is too long");
        Raw.N.LocalId = 0;
        Raw.N.Hint = 0;
        Raw.N.NodeId = nodeId | ServiceMask;
        memcpy(Raw.Buf, x.data(), x.size());
    }

    TActorId::TActorId(ui64 x1, ui64 x2) noexcept {
        Raw.X.X1 = x1;
        Raw.X.X2 = x2;
    }

    TActorId::operator bool() const noexcept {
        return Raw.X.X1 != 0 || Raw.X.X2 != 0;
    }

    ui64 TActorId::LocalId() const noexcept {
        return Raw.N.LocalId;
    }

    ui32 TActorId::Hint() const noexcept {
        return Raw.N.Hint;
    }

    ui32 TActorId::NodeId() const noexcept {
        return Raw.N.NodeId & NodeIdMask;
    }

    bool TActorId::IsService() const noexcept {
        return (Raw.N.NodeId & ServiceMask);
    }

    TStringBuf TActorId::ServiceId() const noexcept {
        Y_DEBUG_ABORT_UNLESS(IsService());
        return TStringBuf((const char*)Raw.Buf, MaxServiceIDLength);
    }

    ui32 TActorId::PoolIndex(ui32 nodeid) noexcept {
        return ((nodeid & PoolIndexMask) >> PoolIndexShift);
    }

    ui32 TActorId::PoolID() const noexcept {
        return PoolIndex(Raw.N.NodeId);
    }

    ui64 TActorId::RawX1() const noexcept {
        return Raw.X.X1;
    }

    ui64 TActorId::RawX2() const noexcept {
        return Raw.X.X2;
    }

    bool TActorId::operator<(const TActorId& x) const noexcept {
        const ui64 s1 = Raw.X.X1;
        const ui64 s2 = Raw.X.X2;
        const ui64 x1 = x.Raw.X.X1;
        const ui64 x2 = x.Raw.X.X2;

        return (s1 != x1) ? (s1 < x1) : (s2 < x2);
    }

    bool TActorId::operator!=(const TActorId& x) const noexcept {
        return Raw.X.X1 != x.Raw.X.X1 || Raw.X.X2 != x.Raw.X.X2;
    }

    bool TActorId::operator==(const TActorId& x) const noexcept {
        return !(x != *this);
    }

    ui64 TActorId::Hash() const noexcept {
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

    ui32 TActorId::Hash32() const noexcept {
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

    ui64 TActorId::THash::operator()(const TActorId& actorId) const noexcept {
        return actorId.Hash();
    }

    ui64 TActorId::THash32::operator()(const TActorId& actorId) const noexcept {
        return actorId.Hash();
    }

    bool TActorId::TOrderedCmp::operator()(const TActorId &left, const TActorId &right) const noexcept {
        Y_DEBUG_ABORT_UNLESS(!left.IsService() && !right.IsService(), "ordered compare works for plain actorids only");
        const ui32 n1 = left.NodeId();
        const ui32 n2 = right.NodeId();

        return (n1 != n2) ? (n1 < n2) : left.LocalId() < right.LocalId();
    }

    void TActorId::Out(IOutputStream& o) const {
        o << "[" << NodeId() << ":" << LocalId() << ":" << Hint() << "]";
    }

    TString TActorId::ToString() const {
        TString x;
        TStringOutput o(x);
        Out(o);
        return x;
    }

    bool TActorId::Parse(const char* buf, ui32 sz) {
        if (sz < 4 || buf[0] != '[' || buf[sz - 1] != ']')
            return false;

        size_t semicolons[2];
        TStringBuf str(buf, sz);
        semicolons[0] = str.find(':', 1);
        if (semicolons[0] == TStringBuf::npos)
            return false;
        semicolons[1] = str.find(':', semicolons[0] + 1);
        if (semicolons[1] == TStringBuf::npos)
            return false;

        bool success = TryFromString(buf + 1, semicolons[0] - 1, Raw.N.NodeId) && TryFromString(buf + semicolons[0] + 1, semicolons[1] - semicolons[0] - 1, Raw.N.LocalId) && TryFromString(buf + semicolons[1] + 1, sz - semicolons[1] - 2, Raw.N.Hint);

        return success;
    }
}
