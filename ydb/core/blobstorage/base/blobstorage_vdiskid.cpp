#include "blobstorage_vdiskid.h"
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

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
} // NKikimr
