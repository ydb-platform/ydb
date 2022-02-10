#include "pathid.h"

#include <util/stream/str.h>

namespace NKikimr {

ui64 TPathId::Hash() const {
    return Hash128to32(OwnerId, LocalPathId);
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
