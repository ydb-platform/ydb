#include "scheme_pathid.h"

#include <util/stream/str.h>

namespace NKikimr {

namespace {
inline ui64 PathIdHash(ui64 a, ui64 b) noexcept {
    const ui64 x1 = 0x001DFF3D8DC48F5Dull * (a & 0xFFFFFFFFull);
    const ui64 x2 = 0x179CA10C9242235Dull * (a >> 32);
    const ui64 x3 = 0x0F530CAD458B0FB1ull * (b & 0xFFFFFFFFull);
    const ui64 x4 = 0xB5026F5AA96619E9ull * (b >> 32);

    const ui64 sum = 0x06C9C021156EAA1Full + x1 + x2 + x3 + x4;

    return (sum >> 32);
}

}

ui64 TPathId::Hash() const {
    return PathIdHash(OwnerId, LocalPathId);
}

TString TPathId::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TPathId::Out(IOutputStream& o) const {
    if (!*this) {
        o << "<Invalid>";
    } else {
        o << "[OwnerId: " << OwnerId << ", LocalPathId: " << LocalPathId << "]";
    }
}

bool TPathId::operator<(const TPathId& x) const {
    return OwnerId != x.OwnerId ? OwnerId < x.OwnerId : LocalPathId < x.LocalPathId;
}

bool TPathId::operator>(const TPathId& x) const {
    return x < *this;
}

bool TPathId::operator<=(const TPathId& x) const {
    return OwnerId != x.OwnerId ? OwnerId < x.OwnerId : LocalPathId <= x.LocalPathId;
}

bool TPathId::operator>=(const TPathId& x) const {
    return x <= *this;
}

bool TPathId::operator==(const TPathId& x) const {
    return OwnerId == x.OwnerId && LocalPathId == x.LocalPathId;
}

bool TPathId::operator!=(const TPathId& x) const {
    return OwnerId != x.OwnerId || LocalPathId != x.LocalPathId;
}

TPathId TPathId::NextId() const {
    if (!this->operator bool()) {
        return TPathId();
    }

    if (LocalPathId + 1 == InvalidLocalPathId) {
        if (OwnerId + 1 == InvalidOwnerId) {
            return TPathId();
        }

        return TPathId(OwnerId + 1, 0);
    }

    return TPathId(OwnerId, LocalPathId + 1);
}

TPathId TPathId::PrevId() const {
    if (!this->operator bool()) {
        return TPathId();
    }

    if (LocalPathId == 0) {
        if (OwnerId == 0) {
            return TPathId();
        }

        return TPathId(OwnerId - 1, Max<ui64>() - 1);
    }

    return TPathId(OwnerId, LocalPathId - 1);
}

TPathId::operator bool() const {
    return OwnerId != InvalidOwnerId && LocalPathId != InvalidLocalPathId;
}

TPathId PathIdFromPathId(const NKikimrProto::TPathID& proto) {
    return TPathId(proto.GetOwnerId(), proto.GetLocalId());
}

void PathIdFromPathId(const TPathId& pathId, NKikimrProto::TPathID* proto) {
    proto->SetOwnerId(pathId.OwnerId);
    proto->SetLocalId(pathId.LocalPathId);
}

} // NKikimr
