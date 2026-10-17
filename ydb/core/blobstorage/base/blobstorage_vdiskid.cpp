#include "blobstorage_vdiskid.h"
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <util/string/cast.h>
#include <util/string/type.h>
#include <util/string/vector.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TVDiskID
    ////////////////////////////////////////////////////////////////////////////
    const TVDiskID TVDiskID::InvalidId = TVDiskID(TGroupId::FromValue(-1), (ui32)-1, (ui8)-1, (ui8)-1, (ui8)-1);

    TVDiskID::TVDiskID(TGroupId groupId, ui32 groupGen, TVDiskIdShort vdiskIdShort)
        : GroupID(groupId)
        , GroupGeneration(groupGen)
        , FailRealm(vdiskIdShort.FailRealm)
        , FailDomain(vdiskIdShort.FailDomain)
        , VDisk(vdiskIdShort.VDisk)
    {}

    TVDiskID::TVDiskID(ui32 groupId, ui32 groupGen, TVDiskIdShort vdiskIdShort)
        : GroupID(TGroupId::FromValue(groupId))
        , GroupGeneration(groupGen)
        , FailRealm(vdiskIdShort.FailRealm)
        , FailDomain(vdiskIdShort.FailDomain)
        , VDisk(vdiskIdShort.VDisk)
    {}

    TVDiskID::TVDiskID(IInputStream &str) {
        if (!Deserialize(str))
            ythrow yexception() << "incorrect format";
    }

    bool TVDiskID::SameGroupAndGeneration(const NKikimrBlobStorage::TVDiskID &x) const {
        return TGroupId::FromProto(&x, &NKikimrBlobStorage::TVDiskID::GetGroupID) == GroupID && x.GetGroupGeneration() == GroupGeneration;
    }

    bool TVDiskID::SameDisk(const NKikimrBlobStorage::TVDiskID &x) const {
        TVDiskID vdisk = VDiskIDFromVDiskID(x);
        return *this == vdisk;
    }

    TString TVDiskID::ToString() const {
        return Sprintf("[%" PRIx32 ":%" PRIu32 ":%" PRIu8 ":%" PRIu8 ":%" PRIu8 "]",
                        GroupID.GetRawId(), GroupGeneration, FailRealm, FailDomain, VDisk).data();
    }

    TString TVDiskID::ToStringWOGeneration() const {
        return Sprintf("[%" PRIx32 ":_:%" PRIu8 ":%" PRIu8 ":%" PRIu8 "]",
                        GroupID.GetRawId(), FailRealm, FailDomain, VDisk).data();
    }

    void TVDiskID::Serialize(IOutputStream &s) const {
        s.Write(&GroupID, sizeof(GroupID));
        s.Write(&GroupGeneration, sizeof(GroupGeneration));
        s.Write(&FailRealm, sizeof(FailRealm));
        s.Write(&FailDomain, sizeof(FailDomain));
        s.Write(&VDisk, sizeof(VDisk));
    }

    bool TVDiskID::Deserialize(IInputStream &s) {
        if (s.Load(&GroupID, sizeof(GroupID)) != sizeof(GroupID))
            return false;
        if (s.Load(&GroupGeneration, sizeof(GroupGeneration)) != sizeof(GroupGeneration))
            return false;
        if (s.Load(&FailRealm, sizeof(FailRealm)) != sizeof(FailRealm))
            return false;
        if (s.Load(&FailDomain, sizeof(FailDomain)) != sizeof(FailDomain))
            return false;
        if (s.Load(&VDisk, sizeof(VDisk)) != sizeof(VDisk))
            return false;
        return true;
    }

    ////////////////////////////////////////////////////////////////////////////
    // TVDiskID conversions
    ////////////////////////////////////////////////////////////////////////////
    TVDiskID VDiskIDFromVDiskID(const NKikimrBlobStorage::TVDiskID &x) {
        return TVDiskID(TGroupId::FromProto(&x, &NKikimrBlobStorage::TVDiskID::GetGroupID), x.GetGroupGeneration(), x.GetRing(), x.GetDomain(), x.GetVDisk());
    }

    void VDiskIDFromVDiskID(const TVDiskID &id, NKikimrBlobStorage::TVDiskID *proto) {
        proto->SetGroupID(id.GroupID.GetRawId());
        proto->SetGroupGeneration(id.GroupGeneration);
        proto->SetRing(id.FailRealm);
        proto->SetDomain(id.FailDomain);
        proto->SetVDisk(id.VDisk);
    }

    TVDiskID VDiskIDFromString(TString str, bool* isGenerationSet) {
        if (str[0] != '[' || str.back() != ']') {
            return TVDiskID::InvalidId;
        }
        str.pop_back();
        str.erase(str.begin());
        TVector<TString> parts = SplitString(str, ":");
        if (parts.size() != 5) {
            return TVDiskID::InvalidId;
        }

        ui32 groupGeneration = 0;

        if (!IsHexNumber(parts[0]) || !IsNumber(parts[2]) || !IsNumber(parts[3]) || !IsNumber(parts[4])
            || !(IsNumber(parts[1]) || parts[1] == "_")) {
            return TVDiskID::InvalidId;
        }

        if (parts[1] == "_") {
            if (isGenerationSet) {
                *isGenerationSet = false;
            }
        } else {
            if (isGenerationSet) {
                *isGenerationSet = true;
            }
            groupGeneration = IntFromString<ui32, 10>(parts[1]);
        }
        return TVDiskID(TGroupId::FromValue(IntFromString<ui32, 16>(parts[0])),
            groupGeneration,
            IntFromString<ui8, 10>(parts[2]),
            IntFromString<ui8, 10>(parts[3]),
            IntFromString<ui8, 10>(parts[4]));
    }
} // NKikimr
