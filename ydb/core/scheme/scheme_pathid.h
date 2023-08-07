#pragma once

#include <ydb/core/scheme/protos/pathid.pb.h>

namespace NKikimr {

using TOwnerId = ui64;
constexpr TOwnerId InvalidOwnerId = Max<ui64>();

using TLocalPathId = ui64;
constexpr TLocalPathId InvalidLocalPathId = Max<ui64>();

struct TPathId {
    TOwnerId OwnerId;
    TLocalPathId LocalPathId;

    constexpr TPathId()
        : OwnerId(InvalidOwnerId)
        , LocalPathId(InvalidLocalPathId)
    {}
    constexpr TPathId(const TOwnerId ownerId, const TLocalPathId localPathId)
        : OwnerId(ownerId)
        , LocalPathId(localPathId)
    {
    }

    template<typename H>
    friend H AbslHashValue(H h, const TPathId& pathId) {
        return H::combine(std::move(h), pathId.OwnerId, pathId.LocalPathId);
    }

    ui64 Hash() const;
    TString ToString() const;
    void Out(IOutputStream& o) const;

    bool operator<(const TPathId& x) const;
    bool operator>(const TPathId& x) const;
    bool operator<=(const TPathId& x) const;
    bool operator>=(const TPathId& x) const;
    bool operator==(const TPathId& x) const;
    bool operator!=(const TPathId& x) const;
    explicit operator bool() const;

    TPathId NextId() const;
    TPathId PrevId() const;

}; // TPathId

TPathId PathIdFromPathId(const NKikimrProto::TPathID& proto);
void PathIdFromPathId(const TPathId& pathId, NKikimrProto::TPathID* proto);

} // NKikimr

template<>
struct THash<NKikimr::TPathId> {
    inline ui64 operator()(const NKikimr::TPathId& x) const noexcept {
        return x.Hash();
    }
};

namespace std {
template <>
struct hash<NKikimr::TPathId> {
    size_t operator()(const NKikimr::TPathId& x) const {
        return x.Hash();
    }
};
}

template<>
inline void Out<NKikimr::TPathId>(IOutputStream& o, const NKikimr::TPathId& x) {
    return x.Out(o);
}
