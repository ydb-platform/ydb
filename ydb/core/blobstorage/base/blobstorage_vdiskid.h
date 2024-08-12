#pragma once

#include "defs.h"
#include <util/str_stl.h>
#include <ydb/core/base/blobstorage_common.h>
#include <util/digest/numeric.h>

namespace NKikimrBlobStorage {
    class TVDiskID;
} // NKikimrBlobStorage

namespace NKikimr {

class TBlobStorageGroupInfo;
struct TVDiskIdShort;

////////////////////////////////////////////////////////////////////////////
// TVDiskID -- global vdisk identifier
////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
struct TVDiskID {
    TGroupId GroupID = TGroupId::Zero();
    ui32 GroupGeneration = 0;
    ui8 FailRealm = 0;
    ui8 FailDomain = 0;
    ui8 VDisk = 0;
    ui8 Padding = 0;

    TVDiskID() = default;
    TVDiskID(TGroupId groupId, ui32 groupGen, TVDiskIdShort vdiskIdShort);
    TVDiskID(ui32 groupId, ui32 groupGen, TVDiskIdShort vdiskIdShort);
    TVDiskID(IInputStream &str);

    TVDiskID(TGroupId groupId, ui32 groupGen, ui8 failRealm, ui8 failDomain, ui8 vdisk)
        : GroupID(groupId)
        , GroupGeneration(groupGen)
        , FailRealm(failRealm)
        , FailDomain(failDomain)
        , VDisk(vdisk)
    {}
    TVDiskID(ui32 groupId, ui32 groupGen, ui8 failRealm, ui8 failDomain, ui8 vdisk)
        : GroupID(TGroupId::FromValue(groupId))
        , GroupGeneration(groupGen)
        , FailRealm(failRealm)
        , FailDomain(failDomain)
        , VDisk(vdisk)
    {}

    bool SameGroupAndGeneration(const TVDiskID &x) const {
        return x.GroupID == GroupID && x.GroupGeneration == GroupGeneration;
    }

    bool SameGroupAndGeneration(const NKikimrBlobStorage::TVDiskID &x) const;

    bool SameExceptGeneration(const TVDiskID &x) const {
        return x.GroupID == GroupID && x.FailRealm == FailRealm && x.FailDomain == FailDomain && x.VDisk == VDisk;
    }

    bool SameDisk(const TVDiskID &x) const {
        return *this == x;
    }

    bool SameDisk(const NKikimrBlobStorage::TVDiskID &x) const;

    auto ConvertToTuple() const {
        return std::make_tuple(GroupID, GroupGeneration, FailRealm, FailDomain, VDisk);
    }

    friend bool operator ==(const TVDiskID& x, const TVDiskID& y) { return x.ConvertToTuple() == y.ConvertToTuple(); }
    friend bool operator !=(const TVDiskID& x, const TVDiskID& y) { return x.ConvertToTuple() != y.ConvertToTuple(); }
    friend bool operator < (const TVDiskID& x, const TVDiskID& y) { return x.ConvertToTuple() <  y.ConvertToTuple(); }

    TString ToString() const;
    TString ToStringWOGeneration() const;
    void Serialize(IOutputStream &s) const;
    bool Deserialize(IInputStream &s);

    ui64 Hash() const {
        ui32 x = (((ui32(FailRealm) << 8) | ui32(FailDomain)) << 8) | ui32(VDisk);
        ui64 y = GroupID.GetRawId();
        y = y << 32ull;
        y |= GroupGeneration;
        return CombineHashes(IntHash<ui64>(y), IntHash<ui64>(x));
    }

    static const TVDiskID InvalidId;
};
#pragma pack(pop)

TVDiskID VDiskIDFromVDiskID(const NKikimrBlobStorage::TVDiskID &x);
void VDiskIDFromVDiskID(const TVDiskID &id, NKikimrBlobStorage::TVDiskID *proto);
// Takes a string in the same format as ToString output, sets isGenerationSet if second number is not '_'
TVDiskID VDiskIDFromString(TString str, bool* isGenerationSet = nullptr);

////////////////////////////////////////////////////////////////////////////
// TVDiskIdShort -- topology info about VDisk, it avoids runtime info like
// group generation
////////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
struct TVDiskIdShort {
    ui8 FailRealm = 0;
    ui8 FailDomain = 0;
    ui8 VDisk = 0;
    ui8 Padding = 0;

    TVDiskIdShort() = default;

    explicit TVDiskIdShort(ui64 raw)
        : FailRealm(ui8((raw >> 16) & 0xff))
        , FailDomain(ui8((raw >> 8) & 0xff))
        , VDisk(ui8(raw & 0xff))
    {}

    TVDiskIdShort(ui8 realm, ui8 domain, ui8 vDisk)
        : FailRealm(realm)
        , FailDomain(domain)
        , VDisk(vDisk)
    {}

    TVDiskIdShort(const TVDiskID &id)
        : FailRealm(id.FailRealm)
        , FailDomain(id.FailDomain)
        , VDisk(id.VDisk)
    {}

    ui64 GetRaw() const {
        return (ui64(FailRealm) << 16) | (ui64(FailDomain) << 8) | ui64(VDisk);
    }

    bool operator<(const TVDiskIdShort &x) const {
        return FailRealm != x.FailRealm ? FailRealm < x.FailRealm :
                FailDomain != x.FailDomain ? FailDomain < x.FailDomain :
                VDisk != x.VDisk ? VDisk < x.VDisk : false;
    }

    bool operator==(const TVDiskIdShort &x) const {
        return FailRealm == x.FailRealm &&
                FailDomain == x.FailDomain &&
                VDisk == x.VDisk;
    }

    bool operator!=(const TVDiskIdShort &x) const {
        return !((*this)==x);
    }

    TString ToString() const {
        TStringStream str;
        str << "{FailRealm# " << (ui32)FailRealm;
        str << " FailDomain# " << (ui32)FailDomain;
        str << " VDisk# " << (ui32)VDisk;
        str << "}";
        return str.Str();
    }

    ui64 Hash() const {
        ui32 x = (((ui32(FailRealm) << 8) | ui32(FailDomain)) << 8) | ui32(VDisk);
        return IntHash<ui64>(x);
    }
};
#pragma pack(pop)

} // NKikimr


template <>
struct THash<NKikimr::TVDiskID> {
    inline size_t operator()(const NKikimr::TVDiskID& d) const {
        return d.Hash();
    }
};

template <>
struct THash<NKikimr::TVDiskIdShort> {
    inline size_t operator()(const NKikimr::TVDiskIdShort& d) const {
        return d.Hash();
    }
};

template<>
inline void Out<NKikimr::TVDiskID>(IOutputStream& os, const NKikimr::TVDiskID& vdiskId) {
    os << vdiskId.ToString();
}

template<>
inline void Out<NKikimr::TVDiskIdShort>(IOutputStream& os, const NKikimr::TVDiskIdShort& vdiskId) {
    os << vdiskId.ToString();
}
